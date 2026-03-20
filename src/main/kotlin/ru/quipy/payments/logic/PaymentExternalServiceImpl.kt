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
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofMillis(1000L))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    private val circuitBreaker: CircuitBreaker = CircuitBreaker.of(
        "payment-$accountName",
        CircuitBreakerConfig.custom()
            .failureRateThreshold(50.0f)
            .slowCallRateThreshold(100.0f)
            .slowCallDurationThreshold(Duration.ofSeconds(30))
            .waitDurationInOpenState(Duration.ofSeconds(10))
            .permittedNumberOfCallsInHalfOpenState(5)
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
        120, 120,
        60L, TimeUnit.SECONDS,
        LinkedBlockingQueue(200_000),
        Executors.defaultThreadFactory(),
        CallerBlockingRejectedExecutionHandler(Duration.ofSeconds(5))
    )

    private val client: HttpClient = HttpClient.newBuilder()
        .executor(httpExecutor)
        .connectTimeout(Duration.ofMillis(1500L))
        .version(HttpClient.Version.HTTP_2)
        .build()

    private val maxRetries = 3
    private val baseBackoff = Duration.ofMillis(200)
    private val maxBackoff = Duration.ofSeconds(2)

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
        if (now() > deadline) {
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
        if (now() > deadline) {
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

        logger.info("[$accountName] Submit: $paymentId, txId: $transactionId")

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
            val remainingMillis = (deadline - nowTime).coerceIn(200L, 10_000L)
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

        fun attemptRequest(attempt: Int) {
            if (isAlreadyCompleted()) return

            if (now() > deadline) {
                logger.error("[$accountName] Deadline exceeded before attempt $attempt. txId: $transactionId")
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

            val request = buildRequest()
            val start = System.nanoTime()

            client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .whenComplete { response, throwable ->
                    val latencyNanos = System.nanoTime() - start
                    val latency = Duration.ofNanos(latencyNanos)
                    val latencyMs = latency.toMillis()
                    paymentMetrics.recordExternalLatency(latency)

                    if (isAlreadyCompleted()) {
                        circuitBreaker.releasePermission()
                        return@whenComplete
                    }

                    if (throwable != null) {
                        val e = unwrapException(throwable)
                        circuitBreaker.onError(latencyMs, TimeUnit.MILLISECONDS, e)

                        if (isRetriableException(e) && attempt + 1 < maxRetries && now() <= deadline) {
                            paymentMetrics.incrementExternalRetry()
                            logger.warn("[$accountName] Retriable error attempt $attempt. txId: $transactionId", e)
                            val delayMs = calcBackoffMillis(attempt + 1)
                            CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS, httpExecutor).execute {
                                attemptRequest(attempt + 1)
                            }
                        } else {
                            paymentMetrics.failedOutgoingRequests()
                            val reason = if (e is HttpTimeoutException || e is SocketTimeoutException)
                                "Request timeout." else e.message
                            logger.error("[$accountName] Payment failed. txId: $transactionId", e)
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
                                "[$accountName] Failed to parse response. txId: $transactionId, " +
                                        "status: $status, body: ${response.body()}", e
                            )
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        logger.warn(
                            "[$accountName] Processed. txId: $transactionId, " +
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
                            }
                            complete()
                        } else {
                            paymentMetrics.failedOutgoingRequests()

                            // 429 нейтрален для CB
                            if (status == 429) {
                                circuitBreaker.releasePermission()
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
                                val delayMs = if (status == 429) {
                                    calcBackoff429(attempt + 1)
                                } else {
                                    calcBackoffMillis(attempt + 1)
                                }
                                logger.warn("[$accountName] HTTP $status, retry ${attempt + 1} in ${delayMs}ms. txId: $transactionId")
                                CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS, httpExecutor).execute {
                                    attemptRequest(attempt + 1)
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

        attemptRequest(0)
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

    private fun calcBackoff429(attempt: Int): Long {
        val base = Duration.ofMillis(500)
        val exp = base.multipliedBy(1L shl (attempt - 1).coerceAtLeast(0))
        val capped = if (exp > Duration.ofSeconds(3)) Duration.ofSeconds(3) else exp
        return Random.nextLong(capped.toMillis() / 2, capped.toMillis().coerceAtLeast(1))
    }
}

fun now() = System.currentTimeMillis()