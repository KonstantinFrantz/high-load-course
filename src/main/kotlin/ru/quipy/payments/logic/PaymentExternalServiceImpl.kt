package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.config.PaymentMetrics
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
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
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(3000, Duration.ofMillis(500L))
    private val ongoingWindow = OngoingWindow(2000)

    private val httpThreadPoolSize = maxOf(100, parallelRequests / 10)
    private val httpExecutor = ThreadPoolExecutor(
        64,
        64,
        60L,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(200_000),
        Executors.defaultThreadFactory(),
        CallerBlockingRejectedExecutionHandler(Duration.ofSeconds(5))
    )

    private val client: HttpClient = HttpClient.newBuilder()
        .executor(httpExecutor)
        .connectTimeout(Duration.ofMillis(1000L))
        .version(HttpClient.Version.HTTP_2)
        .build()

    // 80 потоков под DB — считается как: 4000 rps * 2 ops * 5ms латентность = 40,
    // берём x2 запас = 80. Очередь большая чтобы абсорбировать пики
    // и не доходить до CallerBlocking который заблокирует httpExecutor.
    private val dbExecutor = ThreadPoolExecutor(
        16,
        16,
        60L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(100_000), // очень большая очередь
        Executors.defaultThreadFactory(),
        ThreadPoolExecutor.CallerRunsPolicy() // крайний случай — выполнит в вызывающем потоке, но не заблокирует
    )

    private val maxRetries = 3
    private val baseBackoff = Duration.ofMillis(100)
    private val maxBackoff = Duration.ofSeconds(1)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

      //  rateLimiter.tickBlocking()
        ongoingWindow.acquire()
        //logger.warn("${dbExecutor.activeCount}, ${dbExecutor.activeCount}, ${dbExecutor.completedTaskCount}, ${dbExecutor.queue.size}")
        val currentTime = now()
        if (currentTime > deadline) {
            logger.error("[$accountName] Payment $paymentId deadline exceeded before submission. Started: $paymentStartedAt, deadline: $deadline, now: $currentTime")
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

        fun complete() {
            ongoingWindow.release()
        }

        fun buildRequest(): HttpRequest {
            val uri = URI.create(
                "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
            )

            val nowTime = now()
            val remainingMillis = (deadline - nowTime).coerceIn(200L, 30_000L)
            return HttpRequest.newBuilder()
                .uri(uri)
                .timeout(Duration.ofMillis(remainingMillis))
                .POST(HttpRequest.BodyPublishers.noBody())
                .header("deadline", deadline.toString())
                .header("timeout", remainingMillis.toString())
                .build()
        }

        fun scheduleRetry(nextAttempt: Int, action: () -> Unit) {
            val delayMillis = calcBackoffMillis(nextAttempt)
            CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS, httpExecutor).execute(action)
        }

        fun attemptRequest(attempt: Int) {
            val nowTime = now()
            if (nowTime > deadline) {
                logger.error("[$accountName] Deadline exceeded before attempt $attempt for txId: $transactionId, payment: $paymentId")
                paymentMetrics.failedOutgoingRequests()

                dbExecutor.submit {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded.")
                    }
                }

                complete()
                return
            }

            val request = buildRequest()
            val start = System.nanoTime()

            client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .whenComplete { response, throwable ->
                    val latency = Duration.ofNanos(System.nanoTime() - start)
                    paymentMetrics.recordExternalLatency(latency)

                    if (throwable != null) {
                        val e = unwrapException(throwable)
                        val retriable = isRetriableException(e)
                        if (retriable && attempt + 1 < maxRetries && now() <= deadline) {
                            paymentMetrics.incrementExternalRetry()
                            logger.warn(
                                "[$accountName] Retriable exception for txId: $transactionId, payment: $paymentId, attempt: ${attempt + 1}",
                                e
                            )
                            scheduleRetry(attempt + 1) { attemptRequest(attempt + 1) }
                        } else {
                            paymentMetrics.failedOutgoingRequests()
                            val reason = if (e is SocketTimeoutException) "Request timeout." else e.message
                            logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                            dbExecutor.submit {
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = reason)
                                }
                            }

                            complete()
                        }
                    } else {
                        val status = response.statusCode()
                        paymentMetrics.recordExternalStatus(status)

                        val bodyObj = try {
                            mapper.readValue(response.body(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error(
                                "[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, " +
                                        "result code: $status, reason: ${response.body()}",
                                e
                            )
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        if (!bodyObj.result) {
                            paymentMetrics.failedOutgoingRequests()
                        }

                        logger.warn(
                            "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                                    "succeeded: ${bodyObj.result}, message: ${bodyObj.message}"
                        )

                        dbExecutor.submit {
                            paymentESService.update(paymentId) {
                                it.logProcessing(bodyObj.result, now(), transactionId, reason = bodyObj.message)
                            }
                        }

                        if (bodyObj.result) {
                            complete()
                        } else if (shouldRetry(status) && attempt + 1 < maxRetries && now() <= deadline) {
                            paymentMetrics.incrementExternalRetry()
                            logger.warn(
                                "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId, " +
                                        "retry attempt: ${attempt + 1}"
                            )
                            scheduleRetry(attempt + 1) { attemptRequest(attempt + 1) }
                        } else {
                            complete()
                        }
                    }
                }
        }

        attemptRequest(0)
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

    private fun unwrapException(throwable: Throwable): Exception {
        val cause = if (throwable is java.util.concurrent.CompletionException && throwable.cause != null) {
            throwable.cause!!
        } else throwable

        return if (cause is Exception) cause else Exception(cause)
    }

    private fun calcBackoffMillis(attempt: Int): Long {
        val exp = baseBackoff.multipliedBy(1L shl (attempt - 1).coerceAtLeast(0))
        val capped = if (exp > maxBackoff) maxBackoff else exp
        return Random.nextLong(0, capped.toMillis().coerceAtLeast(1))
    }
}

public fun now() = System.currentTimeMillis()