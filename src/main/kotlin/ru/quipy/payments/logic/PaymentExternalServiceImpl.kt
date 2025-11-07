package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
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
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    private val client = OkHttpClient.Builder()
        .readTimeout(1500, TimeUnit.MILLISECONDS)
        .build()

    private val maxRetries = 5
    private val baseBackoff = Duration.ofMillis(150)
    private val maxBackoff = Duration.ofSeconds(5)

    private var cbody: ExternalSysResponse? = null

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        ongoingWindow.acquire()
        rateLimiter.tickBlocking()

        val currentTime = now()
        if (currentTime > deadline) {
            logger.error("[$accountName] Payment $paymentId deadline exceeded. Started: $paymentStartedAt, deadline: $deadline, now: $currentTime")
            paymentMetrics.failedIncomingRequests()
            paymentESService.update(paymentId) {
                it.logSubmission(
                    success = false,
                    transactionId,
                    currentTime,
                    Duration.ofMillis(currentTime - paymentStartedAt),
                )
            }
            ongoingWindow.release()
            return
        }

        paymentMetrics.outgoingRequests()
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        var attempt = 0
        var success = false

        while (attempt == 0 || (attempt < maxRetries && !success && now() <= deadline)) {
            val start = System.nanoTime()
            try {
                val request = Request.Builder()
                    .url(
                        "http://$paymentProviderHostPort/external/process" +
                                "?serviceName=$serviceName&token=$token&accountName=$accountName" +
                                "&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
                    )
                    .post(emptyBody)
                    .build()

                client.newCall(request).execute().use { response ->
                    paymentMetrics.recordExternalStatus(response.code)

                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error(
                            "[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, " +
                                    "result code: ${response.code}, reason: ${response.body?.string()}"
                        )
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    paymentMetrics.recordExternalLatency(Duration.ofNanos(System.nanoTime() - start))

                    if (!body.result) {
                        cbody = body
                        paymentMetrics.failedOutgoingRequests()
                    }

                    logger.warn(
                        "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                                "succeeded: ${body.result}, message: ${body.message}"
                    )

                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }

                    if (body.result) {
                        success = true
                    } else if (shouldRetry(response.code)) {
                        if (attempt + 1 < maxRetries && now() <= deadline) {
                            attempt++
                            paymentMetrics.incrementExternalRetry()
                            sleepBackoff(attempt)
                            continue
                        }
                    }
                }
            } catch (e: Exception) {
                paymentMetrics.recordExternalLatency(Duration.ofNanos(System.nanoTime() - start))

                val retriable = isRetriableException(e)
                if (retriable && attempt + 1 < maxRetries && now() <= deadline) {
                    attempt++
                    paymentMetrics.incrementExternalRetry()
                    logger.warn("[$accountName] Retriable exception for txId: $transactionId, payment: $paymentId, attempt: $attempt", e)
                    sleepBackoff(attempt)
                    continue
                } else {
                    paymentMetrics.failedOutgoingRequests()
                    when (e) {
                        is SocketTimeoutException -> {
                            logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }
                        else -> {
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }
                }
            }

            if (!success && attempt + 1 < maxRetries && now() <= deadline) {
                attempt++
                paymentMetrics.incrementExternalRetry()
                logger.warn(
                    "[$accountName] Payment failed for txId: $transactionId, response body: ${cbody?.message}, retry attempt: $attempt"
                )
                sleepBackoff(attempt)
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

    private fun sleepBackoff(attempt: Int) {
        val exp = baseBackoff.multipliedBy(1L shl (attempt - 1).coerceAtLeast(0))
        val capped = if (exp > maxBackoff) maxBackoff else exp
        val jitterMillis = Random.nextLong(0, capped.toMillis().coerceAtLeast(1))
        Thread.sleep(jitterMillis)
    }
}

public fun now() = System.currentTimeMillis()
