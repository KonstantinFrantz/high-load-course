package ru.quipy.config

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.Timer.Builder
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class PaymentMetrics(private val registry: MeterRegistry) {

    private val incomingRequests = registry.counter("incoming_requests")
    private val failedIncomingRequests = registry.counter("failed_incoming_requests")
    private val failedOutgoingRequests = registry.counter("failed_outgoing_requests")
    private val outgoingRequests = registry.counter("outgoing_requests")

    private val externalRetries = registry.counter("external_request_retries")

    private val externalLatencyTimer: Timer = Timer
        .builder("external_request_latency")
        .description("Latency of calls to external payment system")
        .publishPercentileHistogram()
        .minimumExpectedValue(Duration.ofMillis(1))
        .maximumExpectedValue(Duration.ofSeconds(60))
        .register(registry)

    fun incomingRequests() = incomingRequests.increment()
    fun failedIncomingRequests() = failedIncomingRequests.increment()
    fun outgoingRequests() = outgoingRequests.increment()
    fun failedOutgoingRequests() = failedOutgoingRequests.increment()

    fun incrementExternalRetry() = externalRetries.increment()

    fun recordExternalLatency(duration: Duration) = externalLatencyTimer.record(duration)

    fun recordExternalStatus(code: Int) {
        registry.counter("external_request_status", "code", code.toString()).increment()
    }
}
