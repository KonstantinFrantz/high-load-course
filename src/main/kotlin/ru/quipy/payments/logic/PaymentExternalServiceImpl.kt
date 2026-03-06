package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
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
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.random.Random

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val paymentMetrics: PaymentMetrics,
    private val dbScope: CoroutineScope
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

    private val rateLimiter = SlidingWindowRateLimiter(4000, Duration.ofMillis(1000L))
    private val ongoingWindow = OngoingWindow(2000)

    private val httpExecutor = ThreadPoolExecutor(
        40,
        40,
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

    private val maxRetries = 3
    private val baseBackoff = Duration.ofMillis(100)
    private val maxBackoff = Duration.ofSeconds(1)


    private val hedgeDelayMs: Long = requestAverageProcessingTime.toMillis()
        .coerceAtLeast(50L)
        .coerceAtMost(500L)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        rateLimiter.tickBlocking()
        ongoingWindow.acquire()
        val currentTime = now()

        if (currentTime > deadline) {
            logger.error(
                "[$accountName] Payment $paymentId deadline exceeded before submission. " +
                        "Started: $paymentStartedAt, deadline: $deadline, now: $currentTime"
            )
            dbScope.launch {
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

        paymentMetrics.failedIncomingRequests()
        paymentMetrics.outgoingRequests()

        dbScope.launch {
            paymentESService.update(paymentId) {
                it.logSubmission(true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }

        logger.info("[$accountName] Submit: $paymentId, txId: $transactionId, hedgeDelayMs: $hedgeDelayMs")

        val windowReleased = AtomicBoolean(false)

        val successLogged = AtomicBoolean(false)

        fun complete() {
            if (windowReleased.compareAndSet(false, true)) {
                ongoingWindow.release()
            }
        }

        fun isAlreadyCompleted() = windowReleased.get()

        fun buildRequest(): HttpRequest {
            val nowTime = now()
            val remainingMillis = (deadline - nowTime).coerceIn(200L, 30_000L)
            val uri = URI.create(
                "http://$paymentProviderHostPort/external/process" +
                        "?serviceName=$serviceName" +
                        "&token=$token" +
                        "&accountName=$accountName" +
                        "&transactionId=$transactionId" +
                        "&paymentId=$paymentId" +
                        "&amount=$amount"
            )
            return HttpRequest.newBuilder()
                .uri(uri)
                .timeout(Duration.ofMillis(remainingMillis))
                .POST(HttpRequest.BodyPublishers.noBody())
                .header("deadline", deadline.toString())
                .header("timeout", remainingMillis.toString())
                .header("x-idempotency-key", transactionId.toString())
                .build()
        }

        fun scheduleRetry(nextAttempt: Int, label: String, action: () -> Unit) {
            val delayMillis = calcBackoffMillis(nextAttempt)
            logger.warn("[$accountName] [$label] Scheduling retry attempt $nextAttempt in ${delayMillis}ms for txId: $transactionId")
            CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS, httpExecutor).execute(action)
        }

        fun attemptRequest(attempt: Int, label: String) {
            if (isAlreadyCompleted()) {
                logger.debug("[$accountName] [$label] Skipping attempt $attempt — already completed. txId: $transactionId")
                return
            }

            if (now() > deadline) {
                logger.error("[$accountName] [$label] Deadline exceeded before attempt $attempt for txId: $transactionId, payment: $paymentId")
                paymentMetrics.failedOutgoingRequests()
                if (!successLogged.get()) {
                    dbScope.launch {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded.")
                        }
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

                    if (isAlreadyCompleted() && throwable != null) {
                        logger.debug("[$accountName] [$label] Late error ignored (already completed). txId: $transactionId")
                        return@whenComplete
                    }

                    if (throwable != null) {
                        val e = unwrapException(throwable)
                        val retriable = isRetriableException(e)
                        if (retriable && attempt + 1 < maxRetries && now() <= deadline) {
                            paymentMetrics.incrementExternalRetry()
                            logger.warn(
                                "[$accountName] [$label] Retriable exception on attempt $attempt for txId: $transactionId, payment: $paymentId",
                                e
                            )
                            scheduleRetry(attempt + 1, label) { attemptRequest(attempt + 1, label) }
                        } else {
                            paymentMetrics.failedOutgoingRequests()
                            val reason = if (e is SocketTimeoutException) "Request timeout." else e.message
                            logger.error(
                                "[$accountName] [$label] Payment failed for txId: $transactionId, payment: $paymentId",
                                e
                            )
                            if (!successLogged.get()) {
                                dbScope.launch {
                                    paymentESService.update(paymentId) {
                                        it.logProcessing(false, now(), transactionId, reason = reason)
                                    }
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
                                "[$accountName] [$label] Failed to parse response for txId: $transactionId, payment: $paymentId, " +
                                        "status: $status, body: ${response.body()}",
                                e
                            )
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        logger.warn(
                            "[$accountName] [$label] Payment processed for txId: $transactionId, payment: $paymentId, " +
                                    "succeeded: ${bodyObj.result}, message: ${bodyObj.message}"
                        )

                        if (bodyObj.result) {
                            if (successLogged.compareAndSet(false, true)) {
                                dbScope.launch {
                                    paymentESService.update(paymentId) {
                                        it.logProcessing(true, now(), transactionId, reason = bodyObj.message)
                                    }
                                }
                            } else {
                                logger.debug(
                                    "[$accountName] [$label] Duplicate success response discarded " +
                                            "(idempotent). txId: $transactionId"
                                )
                            }
                            complete()
                        } else {
                            paymentMetrics.failedOutgoingRequests()
                            if (shouldRetry(status) && attempt + 1 < maxRetries && now() <= deadline) {
                                paymentMetrics.incrementExternalRetry()
                                logger.warn(
                                    "[$accountName] [$label] Payment failed, retry attempt ${attempt + 1} " +
                                            "for txId: $transactionId, payment: $paymentId"
                                )
                                scheduleRetry(attempt + 1, label) { attemptRequest(attempt + 1, label) }
                            } else {
                                if (!successLogged.get()) {
                                    dbScope.launch {
                                        paymentESService.update(paymentId) {
                                            it.logProcessing(false, now(), transactionId, reason = bodyObj.message)
                                        }
                                    }
                                }
                                complete()
                            }
                        }
                    }
                }
        }

        attemptRequest(0, "primary")

        CompletableFuture.delayedExecutor(hedgeDelayMs, TimeUnit.MILLISECONDS, httpExecutor).execute {
            if (!isAlreadyCompleted() && now() <= deadline) {
                logger.info(
                    "[$accountName] Primary request exceeded hedgeDelayMs (${hedgeDelayMs}ms), " +
                            "firing hedged request for payment $paymentId, txId: $transactionId"
                )
                paymentMetrics.outgoingRequests()
                attemptRequest(0, "hedge")
            }
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName

    private fun shouldRetry(status: Int): Boolean =
        status == 408 || status == 429 || status in 500..599

    private fun isRetriableException(e: Exception): Boolean =
        e is SocketTimeoutException
                || e is java.net.ConnectException
                || e is java.net.SocketException
                || e is java.io.InterruptedIOException

    private fun unwrapException(throwable: Throwable): Exception {
        val cause = if (throwable is java.util.concurrent.CompletionException && throwable.cause != null)
            throwable.cause!! else throwable
        return if (cause is Exception) cause else Exception(cause)
    }

    private fun calcBackoffMillis(attempt: Int): Long {
        val exp = baseBackoff.multipliedBy(1L shl (attempt - 1).coerceAtLeast(0))
        val capped = if (exp > maxBackoff) maxBackoff else exp
        return Random.nextLong(0, capped.toMillis().coerceAtLeast(1))
    }
}

public fun now() = System.currentTimeMillis()