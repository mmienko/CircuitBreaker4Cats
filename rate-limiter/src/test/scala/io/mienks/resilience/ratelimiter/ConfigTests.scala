package io.mienks.resilience.ratelimiter

import io.mienks.resilience.ratelimiter.RateLimiter.{Config, RefillRate}
import munit.FunSuite

import scala.concurrent.duration._

class ConfigTests extends FunSuite {

  private val validRate             = RefillRate(requests = 1, period = 1.second)
  private val emissionIntervalNanos = 1.second.toNanos

  test("validate accepts valid config") {
    assert(Config(capacity = 5, initialCapacity = 3, refillRate = validRate).validate == Right(emissionIntervalNanos))
  }

  test("validate accepts config with initialCapacity = 0") {
    assert(Config(capacity = 5, initialCapacity = 0, refillRate = validRate).validate == Right(emissionIntervalNanos))
  }

  test("validate accepts config with initialCapacity = capacity") {
    assert(Config(capacity = 5, initialCapacity = 5, refillRate = validRate).validate == Right(emissionIntervalNanos))
  }

  test("validate rejects zero capacity") {
    val result = Config(capacity = 0, initialCapacity = 0, refillRate = validRate).validate
    assert(result.isLeft)
    assert(result.left.exists(_.isInstanceOf[IllegalArgumentException]))
    assert(result.left.exists(_.getMessage.contains("capacity must be positive")))
  }

  test("validate rejects negative capacity") {
    val result = Config(capacity = -1, initialCapacity = 0, refillRate = validRate).validate
    assert(result.isLeft)
    assert(result.left.exists(_.getMessage.contains("capacity must be positive")))
  }

  test("validate rejects negative initialCapacity") {
    val result = Config(capacity = 5, initialCapacity = -1, refillRate = validRate).validate
    assert(result.isLeft)
    assert(result.left.exists(_.isInstanceOf[IllegalArgumentException]))
    assert(result.left.exists(_.getMessage.contains("initialCapacity must be non-negative")))
  }

  test("validate rejects config that would overflow emissionInterval * capacity") {
    val result = Config(
      capacity = Int.MaxValue,
      initialCapacity = 0,
      refillRate = RefillRate(requests = 1, period = 24.hours)
    ).validate
    assert(result.isLeft)
    assert(result.left.exists(_.isInstanceOf[ArithmeticException]))
    assert(result.left.exists(_.getMessage.contains("Long overflow")))
  }

  test("validate rejects config that would overflow emissionInterval * initialCapacity") {
    val result = Config(
      capacity = 1,
      initialCapacity = Int.MaxValue,
      refillRate = RefillRate(requests = 1, period = 24.hours)
    ).validate
    assert(result.isLeft)
    assert(result.left.exists(_.isInstanceOf[ArithmeticException]))
    assert(result.left.exists(_.getMessage.contains("Long overflow")))
  }

  test("Config.full sets initialCapacity to capacity") {
    val config = Config.full(capacity = 10, refillRate = validRate)
    assertEquals(config.capacity, 10)
    assertEquals(config.initialCapacity, 10)
  }

  test("Config.empty sets initialCapacity to 0") {
    val config = Config.empty(capacity = 10, refillRate = validRate)
    assertEquals(config.capacity, 10)
    assertEquals(config.initialCapacity, 0)
  }

  test("Config.apply defaults to full") {
    val config = Config(capacity = 10, refillRate = validRate)
    assertEquals(config.initialCapacity, 10)
  }

}
