package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.config.PaymentMetrics
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val processPaymentExecutor = ThreadPoolExecutor(
        40,
        40,
        5L,
        TimeUnit.SECONDS,
        LinkedBlockingQueue<Runnable>(800_000),
        NamedThreadFactory("pse"),
        CallerBlockingRejectedExecutionHandler()
    )

    var rateLimiter = LeakingBucketRateLimiter(4500, Duration.ofMillis(1000), 100_000)

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        val createdAt = System.currentTimeMillis()
        //logger.warn("${processPaymentExecutor.completedTaskCount}, ${processPaymentExecutor.activeCount}, ${processPaymentExecutor.queue.size}")
        val accepted = rateLimiter.tick {
            processPaymentExecutor.submit {
                val createdEvent = paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
                logger.trace("Payment {} for order {} created.", createdEvent.paymentId, orderId)

                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            }
        }

        if (!accepted) {
            logger.warn("[$orderId] Payment $paymentId DROPPED by rate limiter at $createdAt")
        }

        return createdAt.takeIf { accepted }
    }
}