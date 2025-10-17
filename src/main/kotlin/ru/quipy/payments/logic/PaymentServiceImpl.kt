package ru.quipy.payments.logic

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import okhttp3.internal.ignoreIoExceptions
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.task.TaskExecutor
import org.springframework.scheduling.annotation.Async
import org.springframework.scheduling.annotation.AsyncConfigurer
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
        private const val BUFFER_CAPACITY = 5000
        private const val BATCH_SIZE = 11
        private const val FLUSH_INTERVAL_MS = 100L
    }

    private val paymentChannel = Channel<PaymentRequest>(BUFFER_CAPACITY)
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    @PostConstruct
    fun init() {
        scope.launch {
            val batch = mutableListOf<PaymentRequest>()
            while (true) {
                batch.add(paymentChannel.receive())

                val timeout = System.currentTimeMillis() + FLUSH_INTERVAL_MS
                while (batch.size < BATCH_SIZE && System.currentTimeMillis() < timeout) {
                    paymentChannel.tryReceive().getOrNull()?.let { batch.add(it) } }

                if (batch.isNotEmpty()) {
                    processBatch(batch.toList())
                    batch.clear()
                }
            }
        }
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val request = PaymentRequest(paymentId, amount, paymentStartedAt, deadline)

        if (!paymentChannel.trySend(request).isSuccess) {
            logger.warn("Payment channel is full, rejecting payment: $paymentId")
        }
    }

    private suspend fun processBatch(batch: List<PaymentRequest>) {
        logger.info("Processing batch of ${batch.size} payments")

        paymentAccounts.forEach { account ->
            scope.launch {
                batch.forEach { request ->
                    try {
                        account.performPaymentAsync(
                            request.paymentId,
                            request.amount,
                            request.paymentStartedAt,
                            request.deadline
                        )
                    } catch (e: Exception) {
                        logger.error("Failed to process payment ${request.paymentId}", e)
                    }
                }
            }
        }
    }

    @PreDestroy
    fun cleanup() {
        scope.cancel()
    }

    data class PaymentRequest(
        val paymentId: UUID,
        val amount: Int,
        val paymentStartedAt: Long,
        val deadline: Long
    )
}