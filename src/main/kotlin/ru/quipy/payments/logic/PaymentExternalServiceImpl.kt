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

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofMillis(1000L))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    // ── Circuit Breaker ────────────────────────────────────────────────────────
    // TIME_BASED 10 s window → at 100 rps ≈ 1000 calls, fresh data every second.
    // Trip on ≥50% failures OR ≥50% slow calls (slow = 3× avg, min 300 ms).
    // waitDuration = 15 s: give the external service real time to recover.
    // HALF_OPEN: only 3 probes; if any fails → back to OPEN immediately.
    // ──────────────────────────────────────────────────────────────────────────
    private val slowCallThreshold: Duration = requestAverageProcessingTime
        .multipliedBy(1)
        .coerceAtLeast(Duration.ofMillis(300))

    private val circuitBreaker: CircuitBreaker = CircuitBreaker.of(
        "payment-$accountName",
        CircuitBreakerConfig.custom()
            .slidingWindowType(SlidingWindowType.TIME_BASED)
            .slidingWindowSize(5)            // 10 секунд, ~1000 вызовов при 100 rps
            .failureRateThreshold(50.0f)      // триггер по ошибкам
            .slowCallRateThreshold(50.0f)     // триггер по медленным
            .slowCallDurationThreshold(slowCallThreshold) // 3× avg = 300ms
            .minimumNumberOfCalls(1)         // минимум для решения
            .waitDurationInOpenState(Duration.ofSeconds(2)) // пауза для восстановления
            .permittedNumberOfCallsInHalfOpenState(3)
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

    // Threads = parallelRequests in flight = rps × maxTimeout.
    // 200 rps × 3 s cap = 600 max concurrent, +headroom = 120 threads.
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
    private val maxBackoff = Duration.ofSeconds(1)

    // Hedge fires only when primary takes > 2× average — roughly the P85-P90 mark.
    // Using 1× average (old value) hedged ~50% of all requests, doubling outgoing rps.
    // Using 2× average ensures we only hedge genuinely slow outliers.
    private val hedgeDelayMs: Long = requestAverageProcessingTime.toMillis()
        .times(2)
        .coerceAtLeast(200L)
        .coerceAtMost(1000L)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // ── Fast-fail order (each step is cheaper / non-blocking) ─────────────
        // 1. CB state — non-consuming read. Prevents threads queuing in
        //    tickBlocking() while CB is OPEN (root cause of thundering herd).
        // 2. Deadline pre-check.
        // 3. tickBlocking() — only for payments that will actually be sent.
        // 4. tryAcquirePermission() — CB slot; state may have changed while waiting.
        // 5. Deadline recheck — time passed during tickBlocking wait.
        // 6. ongoingWindow.acquire().
        // ─────────────────────────────────────────────────────────────────────

        // Step 1
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

        // Step 2
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

        // Step 3
        rateLimiter.tickBlocking()

        // Step 4
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

        // Step 5
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

        // Step 6
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

        fun buildRequest(): HttpRequest {
            val nowTime = now()
            // Cap timeout at 4× average (min 500 ms, max 3 s).
            // A 30-s timeout fills CB window with in-flight calls; CB never sees failures.
            val maxRequestTimeout = requestAverageProcessingTime.toMillis()
                .times(4)
                .coerceIn(500L, 3_000L)
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

            // Hedge needs its own CB permission
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
                        // Late result from the losing leg — release CB cleanly
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

                            if (shouldRetry(status)) {
                                circuitBreaker.onError(
                                    latencyMs, TimeUnit.MILLISECONDS,
                                    RuntimeException("HTTP $status: ${bodyObj.message}")
                                )
                            } else {
                                // Business rejection — not a circuit-breaker failure
                                circuitBreaker.onSuccess(latencyMs, TimeUnit.MILLISECONDS)
                            }

                            if (shouldRetry(status) && attempt + 1 < maxRetries && now() <= deadline) {
                                paymentMetrics.incrementExternalRetry()
                                logger.warn("[$accountName] [$label] HTTP $status, retry ${attempt + 1}. txId: $transactionId")
                                scheduleRetry(attempt + 1, label) { attemptRequest(attempt + 1, label) }
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

        // Hedge fires only after 2× average processing time — genuine outliers only.
        // Skip when CB is OPEN or HALF_OPEN to avoid wasting probe permits.
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

fun now() = System.currentTimeMillis()