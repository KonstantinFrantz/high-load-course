package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(val dbScope: CoroutineScope) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    // Executor for coroutine dispatcher — coroutines suspend (not block) on DB join,
    // so threads are freed while waiting. Queue is intentionally small to apply
    // back-pressure instead of accumulating expired-deadline payments.
    private val paymentExecutor = ThreadPoolExecutor(
        30, 30,
        60L, TimeUnit.SECONDS,
        LinkedBlockingQueue<Runnable>(500),
        NamedThreadFactory("pse"),
        ThreadPoolExecutor.CallerRunsPolicy()
    )

    // Coroutine scope on top of paymentExecutor — suspend points (join) yield the
    // thread back to the pool instead of blocking it like runBlocking would.
    private val scope = CoroutineScope(SupervisorJob() + paymentExecutor.asCoroutineDispatcher())

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        if (createdAt > deadline) {
            logger.warn("[$orderId] Payment $paymentId already past deadline at creation, dropping")
            return createdAt
        }

        scope.launch {
            // Recheck deadline — coroutine may have waited in the executor queue
            val now = System.currentTimeMillis()
            if (now > deadline) {
                logger.warn("[$orderId] Payment $paymentId expired while waiting in executor queue, dropping")
                return@launch
            }

            // Suspend (not block) until aggregate is written — required before
            // submitPaymentRequest calls logSubmission (update), which needs the aggregate.
            dbScope.launch {
                paymentESService.create {
                    it.create(paymentId, orderId, amount)
                }
            }.join()  // suspend point: yields thread back to pool while waiting

            logger.trace("Payment $paymentId for order $orderId created.")
            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }

        return createdAt
    }
}