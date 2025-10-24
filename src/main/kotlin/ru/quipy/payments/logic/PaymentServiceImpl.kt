package ru.quipy.payments.logic

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
        private const val BUFFER_CAPACITY = 5000
        private const val BATCH_SIZE = 11
        private const val FLUSH_INTERVAL_MS = 40000L
    }

    private val paymentChannel = Channel<PaymentRequest>(BUFFER_CAPACITY)
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    private val executor = Executors.newSingleThreadScheduledExecutor()

    private val lock = ReentrantLock()
    private var batch = ArrayList<PaymentRequest>(BATCH_SIZE)
    private val lastFlushNs = AtomicLong(System.nanoTime())

    @PostConstruct
    fun init() {
        scope.launch {
            for (req in paymentChannel) {
                var toProcess: List<PaymentRequest>? = null
                lock.withLock {
                    batch.add(req)
                    if (batch.size >= BATCH_SIZE) {
                        toProcess = swapAndGet()
                    }
                }
                if (toProcess != null) {
                    launch { processBatch(toProcess!!) }
                }
            }
        }

        executor.scheduleAtFixedRate({
            try {
                var toProcess: List<PaymentRequest>? = null
                val now = System.nanoTime()
                val elapsedMs = TimeUnit.NANOSECONDS.toMillis(now - lastFlushNs.get())

                lock.withLock {
                    if (batch.isNotEmpty() && elapsedMs >= FLUSH_INTERVAL_MS) {
                        toProcess = swapAndGet(now)
                    }
                }
                if (toProcess != null) {
                    // обрабатываем в корутине, чтобы не блокировать поток экзекутора
                    scope.launch { processBatch(toProcess!!) }
                }
            } catch (t: Throwable) {
                logger.error("Scheduled flush failed", t)
            }
        }, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS)
    }

    private fun swapAndGet(nowNs: Long = System.nanoTime()): List<PaymentRequest> {
        val snapshot = batch
        batch = ArrayList(BATCH_SIZE)
        lastFlushNs.set(nowNs)
        return snapshot
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val request = PaymentRequest(paymentId, amount, paymentStartedAt, deadline)
        if (!paymentChannel.trySend(request).isSuccess) {
            logger.warn("Payment channel is full, rejecting payment: $paymentId")
        }
    }

    private suspend fun processBatch(batch: List<PaymentRequest>) = coroutineScope {
        logger.info("Processing batch of ${batch.size} payments")
        paymentAccounts.map { account ->
            launch {
                for (req in batch) {
                    try {
                        account.performPaymentAsync(
                            req.paymentId,
                            req.amount,
                            req.paymentStartedAt,
                            req.deadline
                        )
                    } catch (e: Exception) {
                        logger.error("Failed to process payment ${req.paymentId}", e)
                    }
                }
            }
        }.joinAll()
    }

    @PreDestroy
    fun cleanup() {
        try {
            paymentChannel.close()
            scope.cancel()
        } finally {
            executor.shutdown()
            executor.awaitTermination(2, TimeUnit.SECONDS)
        }
    }

    data class PaymentRequest(
        val paymentId: UUID,
        val amount: Int,
        val paymentStartedAt: Long,
        val deadline: Long
    )
}
