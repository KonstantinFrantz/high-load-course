package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType
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

    // CB: если >50% реальных ошибок за 30с — закрываемся на 2с, потом пробуем 3 запроса
    private val circuitBreaker: CircuitBreaker = CircuitBreaker.of(
        "payment-$accountName",
        CircuitBreakerConfig.custom()
            .failureRateThreshold(50.0f)
            .slowCallRateThreshold(100.0f)           // slow calls не триггерят CB
            .slowCallDurationThreshold(Duration.ofSeconds(30))
            .waitDurationInOpenState(Duration.ofSeconds(2))
            .automaticTransitionFromOpenToHalfOpenEnabled(true)
            .permittedNumberOfCallsInHalfOpenState(3)
            .slidingWindowType(SlidingWindowType.TIME_BASED)
            .slidingWindowSize(30)
            .minimumNumberOfCalls(10)
            .build()
    )

    init {
        circuitBreaker.eventPublisher.onStateTransition { event ->
            logger.warn(
                "[$accountName] Circuit breaker: " +
                        "${event.stateTransition.fromState} → ${event.stateTransition.toState}"
            )
        }
    }

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

        // CB OPEN — fast-fail, не тратим время на rate limiter и ongoing window
        if (circuitBreaker.state == CircuitBreaker.State.OPEN) {
            logger.warn("[$accountName] CB OPEN — fast-failing $paymentId")
            paymentMetrics.failedOutgoingRequests()
            dbScope.launch {
                paymentESService.update(paymentId) {
                    it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }
            }
            return
        }

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

        // CB permission — после rate limiter, чтобы в HALF_OPEN не пустить больше чем разрешено
        if (!circuitBreaker.tryAcquirePermission()) {
            logger.warn("[$accountName] CB ${circuitBreaker.state} — no permission for $paymentId")
            paymentMetrics.failedOutgoingRequests()
            dbScope.launch {
                paymentESService.update(paymentId) {
                    it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
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
                    val latencyMs = Duration.ofNanos(System.nanoTime() - start).toMillis()
                    paymentMetrics.recordExternalLatency(Duration.ofMillis(latencyMs))

                    if (isAlreadyCompleted()) {
                        circuitBreaker.releasePermission()
                        logger.debug("[$accountName] [$label] Late result ignored (already completed). txId: $transactionId")
                        return@whenComplete
                    }

                    if (throwable != null) {
                        val e = unwrapException(throwable)

                        // CB: сообщаем об ошибке
                        circuitBreaker.onError(latencyMs, TimeUnit.MILLISECONDS, e)

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
                            // CB: успех
                            circuitBreaker.onSuccess(latencyMs, TimeUnit.MILLISECONDS)

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

                            // CB: 429 — нейтрален, это rate limiting, не сбой сервиса
                            // 5xx/408 — реальная ошибка
                            // остальное — сервис ответил, значит жив
                            when {
                                status == 429 -> circuitBreaker.releasePermission()
                                shouldRetry(status) -> circuitBreaker.onError(
                                    latencyMs, TimeUnit.MILLISECONDS,
                                    RuntimeException("HTTP $status: ${bodyObj.message}")
                                )
                                else -> circuitBreaker.onSuccess(latencyMs, TimeUnit.MILLISECONDS)
                            }

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