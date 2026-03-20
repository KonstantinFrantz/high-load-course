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
import java.net.http.HttpTimeoutException
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

    // FIX 1: Rate limiter — не превышаем серверный лимит.
    // Если серверный ratePerSecond=100, а у нас rateLimitPerSec=200,
    // лучше быть чуть ниже серверного лимита, чтобы не получать 429.
    // Но если мы не знаем серверный лимит, хотя бы используем свой.
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofMillis(1000L))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    // FIX 2: Circuit Breaker — более разумные пороги
    private val circuitBreaker: CircuitBreaker = CircuitBreaker.of(
        "payment-$accountName",
        CircuitBreakerConfig.custom()
            // Порог ошибок — 50% вместо 10%. Реальные сбои сервиса, а не rate limiting.
            .failureRateThreshold(50.0f)
            // Slow calls — 80% вместо 10%. Не триггерим CB из-за пары медленных запросов.
            .slowCallRateThreshold(80.0f)
            // Slow call порог — осмысленное значение относительно таймаута
            .slowCallDurationThreshold(Duration.ofSeconds(2))
            // Время в OPEN — 10 секунд, ок
            .waitDurationInOpenState(Duration.ofSeconds(10))
            // HALF_OPEN: пускаем мало запросов для проверки, а не 50
            .permittedNumberOfCallsInHalfOpenState(5)
            // TIME_BASED окно — смотрим на последние N секунд, а не N вызовов
            .slidingWindowType(SlidingWindowType.TIME_BASED)
            .slidingWindowSize(30) // 30 секунд
            // Минимум вызовов перед оценкой — не открываем CB на малой выборке
            .minimumNumberOfCalls(20)
            // FIX 3: 429 НЕ считаем failure для CB — это rate limiting, а не сбой сервиса
            .recordException { e -> !isRateLimitException(e) }
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
        120, 120,
        60L, TimeUnit.SECONDS,
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
    private val maxBackoff = Duration.ofSeconds(2)

    private val hedgeDelayMs: Long = requestAverageProcessingTime.toMillis()
        .times(2)
        .coerceAtLeast(200L)
        .coerceAtMost(1000L)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Step 1: CB OPEN — fast-fail
        if (circuitBreaker.state == CircuitBreaker.State.OPEN) {
            logger.warn("[$accountName] CB OPEN — fast-failing $paymentId before rate limiter")
            paymentMetrics.failedOutgoingRequests()
            dbScope.launch {
                paymentESService.update(paymentId) {
                    it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }
            }
            return
        }

        // Step 2: Deadline check
        val currentTime = now()
        if (currentTime > deadline) {
            logger.error("[$accountName] Payment $paymentId deadline exceeded before submission.")
            dbScope.launch {
                paymentESService.update(paymentId) {
                    it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }
            }
            return
        }

        // Step 3: Rate limiter
        rateLimiter.tickBlocking()

        // Step 4: CB permission
        if (!circuitBreaker.tryAcquirePermission()) {
            val cbState = circuitBreaker.state
            logger.warn("[$accountName] CB $cbState — fast-failing $paymentId after rate limiter")
            paymentMetrics.failedOutgoingRequests()
            dbScope.launch {
                paymentESService.update(paymentId) {
                    it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }
            }
            return
        }

        // Step 5: Deadline check after rate limiter wait
        val afterWait = now()
        if (afterWait > deadline) {
            circuitBreaker.releasePermission()
            logger.error("[$accountName] Payment $paymentId deadline exceeded after rate limiter wait.")
            dbScope.launch {
                paymentESService.update(paymentId) {
                    it.logSubmission(false, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }
            }
            return
        }

        // Step 6: Ongoing window
        ongoingWindow.acquire()

        paymentMetrics.outgoingRequests()

        val submissionJob = dbScope.launch {
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

        // FIX 4: Более разумный таймаут запроса
        fun buildRequest(): HttpRequest {
            val nowTime = now()
            val maxRequestTimeout = requestAverageProcessingTime.toMillis()
                .times(10)
                .coerceIn(1_000L, 5_000L)
            val remainingMillis = (deadline - nowTime).coerceIn(200L, maxRequestTimeout)
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
            logger.warn("[$accountName] [$label] Retry $nextAttempt in ${delayMillis}ms, txId: $transactionId")
            CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS, httpExecutor).execute(action)
        }

        fun attemptRequest(attempt: Int, label: String) {
            if (isAlreadyCompleted()) {
                logger.debug("[$accountName] [$label] Skipping attempt $attempt — already completed. txId: $transactionId")
                return
            }

            if (now() > deadline) {
                logger.error("[$accountName] [$label] Deadline exceeded before attempt $attempt. txId: $transactionId")
                paymentMetrics.failedOutgoingRequests()
                if (!successLogged.get()) {
                    dbScope.launch {
                        submissionJob.join()
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded.")
                        }
                    }
                }
                complete()
                return
            }

            if (label == "hedge" && !circuitBreaker.tryAcquirePermission()) {
                logger.debug("[$accountName] Hedge CB-blocked, skipping. txId: $transactionId")
                return
            }

            val request = buildRequest()
            val start = System.nanoTime()

            client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .whenComplete { response, throwable ->
                    val latencyNanos = System.nanoTime() - start
                    val latency = Duration.ofNanos(latencyNanos)
                    val latencyMs = latency.toMillis()
                    paymentMetrics.recordExternalLatency(latency)

                    if (isAlreadyCompleted()) {
                        if (throwable != null) {
                            if (successLogged.get()) circuitBreaker.releasePermission()
                            else circuitBreaker.onError(latencyMs, TimeUnit.MILLISECONDS, unwrapException(throwable))
                        } else {
                            if (successLogged.get()) circuitBreaker.releasePermission()
                            else circuitBreaker.onSuccess(latencyMs, TimeUnit.MILLISECONDS)
                        }
                        logger.debug("[$accountName] [$label] Late result handled. txId: $transactionId")
                        return@whenComplete
                    }

                    if (throwable != null) {
                        val e = unwrapException(throwable)
                        circuitBreaker.onError(latencyMs, TimeUnit.MILLISECONDS, e)

                        if (isRetriableException(e) && attempt + 1 < maxRetries && now() <= deadline) {
                            paymentMetrics.incrementExternalRetry()
                            logger.warn("[$accountName] [$label] Retriable error attempt $attempt. txId: $transactionId", e)
                            scheduleRetry(attempt + 1, label) { attemptRequest(attempt + 1, label) }
                        } else {
                            paymentMetrics.failedOutgoingRequests()
                            val reason = if (e is HttpTimeoutException || e is SocketTimeoutException)
                                "Request timeout." else e.message
                            logger.error("[$accountName] [$label] Payment failed. txId: $transactionId", e)
                            if (!successLogged.get()) {
                                dbScope.launch {
                                    submissionJob.join()
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
                                "[$accountName] [$label] Failed to parse response. txId: $transactionId, " +
                                        "status: $status, body: ${response.body()}", e
                            )
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        logger.warn(
                            "[$accountName] [$label] Processed. txId: $transactionId, " +
                                    "succeeded: ${bodyObj.result}, message: ${bodyObj.message}"
                        )

                        if (bodyObj.result) {
                            circuitBreaker.onSuccess(latencyMs, TimeUnit.MILLISECONDS)

                            if (successLogged.compareAndSet(false, true)) {
                                dbScope.launch {
                                    submissionJob.join()
                                    paymentESService.update(paymentId) {
                                        it.logProcessing(true, now(), transactionId, reason = bodyObj.message)
                                    }
                                }
                            } else {
                                logger.debug("[$accountName] [$label] Duplicate success discarded. txId: $transactionId")
                            }
                            complete()
                        } else {
                            paymentMetrics.failedOutgoingRequests()

                            // FIX 5: 429 — не ошибка сервиса, а rate limiting.
                            // Не кормим CB ошибками от 429, иначе CB открывается
                            // из-за того что мы сами шлём слишком быстро.
                            if (status == 429) {
                                // Rate limited — CB не трогаем, просто ретраим с backoff
                                circuitBreaker.onSuccess(latencyMs, TimeUnit.MILLISECONDS)
                            } else if (shouldRetry(status)) {
                                circuitBreaker.onError(
                                    latencyMs, TimeUnit.MILLISECONDS,
                                    RuntimeException("HTTP $status: ${bodyObj.message}")
                                )
                            } else {
                                circuitBreaker.onSuccess(latencyMs, TimeUnit.MILLISECONDS)
                            }

                            if (shouldRetry(status) && attempt + 1 < maxRetries && now() <= deadline) {
                                paymentMetrics.incrementExternalRetry()
                                // FIX 6: Для 429 используем больший backoff
                                val retryDelay = if (status == 429) {
                                    calcBackoffMillis429(attempt + 1)
                                } else {
                                    calcBackoffMillis(attempt + 1)
                                }
                                logger.warn("[$accountName] [$label] HTTP $status, retry ${attempt + 1} in ${retryDelay}ms. txId: $transactionId")
                                CompletableFuture.delayedExecutor(retryDelay, TimeUnit.MILLISECONDS, httpExecutor).execute {
                                    attemptRequest(attempt + 1, label)
                                }
                            } else {
                                if (!successLogged.get()) {
                                    dbScope.launch {
                                        submissionJob.join()
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
            val cbState = circuitBreaker.state
            if (!isAlreadyCompleted()
                && now() <= deadline
                && cbState != CircuitBreaker.State.HALF_OPEN
                && cbState != CircuitBreaker.State.OPEN
                && rateLimiter.tick()
            ) {
                logger.info(
                    "[$accountName] Primary exceeded hedgeDelayMs (${hedgeDelayMs}ms), " +
                            "firing hedge for $paymentId, txId: $transactionId"
                )
                paymentMetrics.outgoingRequests()
                attemptRequest(0, "hedge")
            } else {
                logger.debug(
                    "[$accountName] Hedge suppressed — cbState=$cbState, " +
                            "completed=${isAlreadyCompleted()}. txId: $transactionId"
                )
            }
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName

    private fun shouldRetry(status: Int): Boolean =
        status == 408 || status == 429 || status in 500..599

    private fun isRetriableException(e: Exception): Boolean =
        e is java.net.ConnectException
                || e is java.net.SocketException
                || e is java.io.InterruptedIOException

    // Маркер-исключение для 429, чтобы CB его игнорировал через recordException
    private fun isRateLimitException(e: Throwable): Boolean =
        e is RateLimitException

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

    // FIX 7: Отдельный backoff для 429 — ждём дольше, даём серверу продохнуть
    private fun calcBackoffMillis429(attempt: Int): Long {
        val base = Duration.ofMillis(500)
        val exp = base.multipliedBy(1L shl (attempt - 1).coerceAtLeast(0))
        val capped = if (exp > Duration.ofSeconds(3)) Duration.ofSeconds(3) else exp
        return Random.nextLong(capped.toMillis() / 2, capped.toMillis().coerceAtLeast(1))
    }

    class RateLimitException(message: String?) : RuntimeException(message)
}

fun now() = System.currentTimeMillis()