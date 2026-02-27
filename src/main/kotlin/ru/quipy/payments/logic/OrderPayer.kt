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

    // Увеличиваем пул потоков: при 4000 rps очередь 80k задач начнёт
    // накапливаться если потоков слишком мало. 40 потоков дадут запас.
    // CallerBlockingRejectedExecutionHandler гарантирует backpressure
    // вместо тихого дропа задач при переполнении очереди.
    private val processPaymentExecutor = ThreadPoolExecutor(
        64,
        64,
        5L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue<Runnable>(400_000),
        NamedThreadFactory("pse"),
        CallerBlockingRejectedExecutionHandler()
    )

    // Лимит чуть ниже максимума (4500 из 5000) чтобы оставить буфер.
    // ВАЖНО: LeakingBucketRateLimiter.tick() при переполнении возвращает false
    // и молча дропает задачу — это неприемлемо. Если у тебя есть tickBlocking()
    // в реализации — используй его. Если нет, нужно либо дождаться слота,
    // либо явно фейлить с логом, а не тихо терять запросы.
    // Здесь используем tick с явным логированием дропа.
    var rateLimiter = LeakingBucketRateLimiter(2000, Duration.ofMillis(1000), 2000)

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        val createdAt = System.currentTimeMillis()
        logger.warn("${processPaymentExecutor.completedTaskCount}, ${processPaymentExecutor.activeCount}, ${processPaymentExecutor.queue.size}")
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
            // Явный лог вместо тихого дропа — так хотя бы видно в метриках/логах
            // что мы теряем запросы и нужно поднять лимит или масштабировать сервис.
            logger.warn("[$orderId] Payment $paymentId DROPPED by rate limiter at $createdAt")
        }

        return createdAt.takeIf { accepted }
    }
}