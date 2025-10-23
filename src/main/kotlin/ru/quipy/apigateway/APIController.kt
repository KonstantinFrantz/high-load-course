package ru.quipy.apigateway

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import org.springframework.web.client.HttpClientErrorException
import ru.quipy.common.utils.CompositeRateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.config.PaymentMetrics
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

@RestController
class APIController {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @Autowired
    private lateinit var orderRepository: OrderRepository

    @Autowired
    private lateinit var orderPayer: OrderPayer

    @Autowired
    private lateinit var paymentMetrics: PaymentMetrics

    private val  paymentRateLimiter = CompositeRateLimiter(
        rl1 = TokenBucketRateLimiter(
            rate = 15,
            bucketMaxCapacity = 30,
            window = 1,
            timeUnit = TimeUnit.SECONDS
        ),
        rl2 = SlidingWindowRateLimiter(
            rate = 15,
            window = Duration.ofSeconds(1)
        )
    )

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
    fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): ResponseEntity<Any> {
        if (!paymentRateLimiter.tick()) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(
                mapOf(
                    "error" to "Too many requests",
                    "message" to "Rate limit exceeded. Please try again later."
                )
            )
        }

        paymentMetrics.incomingRequests()

        try {
            val paymentId = UUID.randomUUID()
            val order = orderRepository.findById(orderId)?.let {
                orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
                it
            } ?: return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Order not found")

            val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)
            return ResponseEntity.ok(PaymentSubmissionDto(createdAt, paymentId))
        } catch (e: Exception) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Payment processing failed")
        }
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}