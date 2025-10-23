package ru.quipy.common.utils

class CompositeRateLimiter(
    private val rl1: RateLimiter,
    private val rl2: RateLimiter,
    private val mode: CompositeMode = CompositeMode.AND
) : RateLimiter {

    enum class CompositeMode { AND, OR }

    override fun tick(): Boolean {
        return when (mode) {
            CompositeMode.AND -> rl1.tick() && rl2.tick()
            CompositeMode.OR -> rl1.tick() || rl2.tick()
        }
    }
}