package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.config.PaymentMetrics
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.min
import kotlin.random.Random

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val paymentMetrics: PaymentMetrics
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        private val mapper = ObjectMapper().registerKotlinModule()
        private val emptyBody = RequestBody.create(null, ByteArray(0))
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    private val httpExecutor = Executors.newFixedThreadPool( min(256, (parallelRequests.coerceAtMost(20000)).coerceAtLeast(8)) )

    private val client: OkHttpClient = OkHttpClient.Builder()
        .connectTimeout(Duration.ofMillis(requestAverageProcessingTime.toMillis() * 2))
        .writeTimeout(30, TimeUnit.SECONDS)
        .readTimeout(0, TimeUnit.MILLISECONDS)
        .callTimeout(0, TimeUnit.MILLISECONDS)
        .dispatcher(Dispatcher().apply {
            maxRequests = parallelRequests.coerceAtLeast(64)
            maxRequestsPerHost = parallelRequests.coerceAtLeast(64)
        })
        .protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1))
        .build()

    private val dbExecutor = Executors.newFixedThreadPool(64)
    private val maxRetries = 5
    private val baseBackoff = Duration.ofMillis(150)
    private val maxBackoff = Duration.ofSeconds(5)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        rateLimiter.tickBlocking()
        ongoingWindow.acquire()

        val currentTime = now()
        if (currentTime > deadline) {
            logger.error("[$accountName] Payment $paymentId deadline exceeded. Started: $paymentStartedAt, deadline: $deadline, now: $currentTime")
            paymentMetrics.failedIncomingRequests()
            dbExecutor.submit {
                paymentESService.update(paymentId) {
                    it.logSubmission(
                        success = false,
                        transactionId,
                        currentTime,
                        Duration.ofMillis(currentTime - paymentStartedAt),
                    )
                }
            }
            ongoingWindow.release()
            return
        }

        paymentMetrics.outgoingRequests()
        dbExecutor.submit {
            paymentESService.update(paymentId) {
                it.logSubmission(true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        var attempt = 0
        var success = false
        var lastBodyMessage: String? = null

        while (attempt == 0 || (attempt < maxRetries && !success && now() <= deadline)) {
            val start = System.nanoTime()
            try {
                val nowTime = now()
                if (nowTime > deadline) {
                    paymentMetrics.failedOutgoingRequests()
                    dbExecutor.submit {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded.")
                        }
                    }
                    break
                }

                val timeoutMillis = min((deadline - nowTime).coerceAtLeast(1), requestAverageProcessingTime.toMillis().coerceAtLeast(1000L) + 2000L).coerceAtLeast(1000L)

                val request = Request.Builder()
                    .url(
                        "http://$paymentProviderHostPort/external/process" +
                                "?serviceName=$serviceName&token=$token&accountName=$accountName" +
                                "&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
                    )
                    .post(emptyBody)
                    .header("deadline", deadline.toString())
                    .header("timeout", timeoutMillis.toString())
                    .build()

                client.newCall(request).execute().use { response ->
                    paymentMetrics.recordExternalStatus(response.code)

                    val bodyStr = try {
                        response.body?.string()
                    } catch (e: Exception) {
                        null
                    }

                    val bodyObj = try {
                        if (bodyStr != null) mapper.readValue(bodyStr, ExternalSysResponse::class.java)
                        else ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "empty body")
                    } catch (e: Exception) {
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    val latency = Duration.ofNanos(System.nanoTime() - start)
                    paymentMetrics.recordExternalLatency(latency)

                    if (!bodyObj.result) {
                        lastBodyMessage = bodyObj.message
                        paymentMetrics.failedOutgoingRequests()
                    }

                    dbExecutor.submit {
                        paymentESService.update(paymentId) {
                            it.logProcessing(bodyObj.result, now(), transactionId, reason = bodyObj.message)
                        }
                    }

                    if (bodyObj.result) {
                        success = true
                    } else if (shouldRetry(response.code) && attempt + 1 < maxRetries && now() <= deadline) {
                        attempt++
                        paymentMetrics.incrementExternalRetry()
                        Thread.sleep(calcBackoffMillis(attempt))
                        continue
                    }
                }
            } catch (e: Exception) {
                val latency = Duration.ofNanos(System.nanoTime() - start)
                paymentMetrics.recordExternalLatency(latency)

                val retriable = isRetriableException(e)
                if (retriable && attempt + 1 < maxRetries && now() <= deadline) {
                    attempt++
                    paymentMetrics.incrementExternalRetry()
                    Thread.sleep(calcBackoffMillis(attempt))
                    continue
                } else {
                    paymentMetrics.failedOutgoingRequests()
                    if (e is SocketTimeoutException) {
                        dbExecutor.submit {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }
                    } else {
                        dbExecutor.submit {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }
                    break
                }
            }

            if (!success && attempt + 1 < maxRetries && now() <= deadline) {
                attempt++
                paymentMetrics.incrementExternalRetry()
                Thread.sleep(calcBackoffMillis(attempt))
            } else {
                break
            }
        }

        ongoingWindow.release()
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName

    private fun shouldRetry(status: Int): Boolean {
        return status == 408 || status == 429 || status in 500..599
    }

    private fun isRetriableException(e: Exception): Boolean {
        return e is SocketTimeoutException
                || e is java.net.ConnectException
                || e is java.net.SocketException
                || e is java.io.InterruptedIOException
    }

    private fun calcBackoffMillis(attempt: Int): Long {
        val exp = baseBackoff.multipliedBy((1L shl (attempt - 1).coerceAtLeast(0)))
        val capped = if (exp > maxBackoff) maxBackoff else exp
        return Random.nextLong(0, capped.toMillis().coerceAtLeast(1))
    }

}