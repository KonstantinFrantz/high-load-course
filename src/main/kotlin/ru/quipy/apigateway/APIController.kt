package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import ru.quipy.config.PaymentMetrics
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import java.util.*

@RestController
class APIController {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    @Autowired
    private lateinit var paymentMetrics: PaymentMetrics

    @PostMapping("/users")
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    data class CreateUserRequest(val name: String, val password: String)

    data class User(val id: UUID, val name: String)

    @PostMapping("/orders")
    fun createOrder(@RequestParam userId: UUID, @RequestParam price: Int): Order {
        val order = Order(
            UUID.randomUUID(),
            userId,
            System.currentTimeMillis(),
            OrderStatus.COLLECTING,
            price,
        )
        return orderRepository.save(order)
    }

    data class Order(
        val id: UUID,
        val userId: UUID,
        val timeCreated: Long,
        val status: OrderStatus,
        val price: Int,
    )

    enum class OrderStatus {
        COLLECTING,
        PAYMENT_IN_PROGRESS,
        PAID,
    }

    @PostMapping("/orders/{orderId}/payment")
    fun payOrder(
        @PathVariable orderId: UUID,
        @RequestParam deadline: Long
    ): ResponseEntity<PaymentSubmissionDto> {

        val paymentId = UUID.randomUUID()
        paymentMetrics.incomingRequests()

        val order = orderRepository.findById(orderId)?.let {
            // перевели заказ в состояние "оплата идёт"
            orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            it
        } ?: throw IllegalArgumentException("No such order $orderId")

        // processPayment уже сам:
        // - проверяет rateLimiter
        // - кладёт задачу в свой ThreadPoolExecutor
        // - возвращает createdAt или null, но НЕ ждёт внешку
        val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)
            ?: return ResponseEntity
                .status(HttpStatus.TOO_MANY_REQUESTS)
                .header("Retry-After", "1")
                .build()

        // здесь мы только говорим клиенту:
        // "платёж принят в обработку", а не "платёж прошёл"
        return ResponseEntity
            .status(HttpStatus.ACCEPTED)
            .body(PaymentSubmissionDto(createdAt, paymentId))
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}
