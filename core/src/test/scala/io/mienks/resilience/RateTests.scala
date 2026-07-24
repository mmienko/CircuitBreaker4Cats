package io.mienks.resilience

import cats.kernel.{Order, Semigroup}
import cats.syntax.eq._
import cats.syntax.option._
import cats.syntax.semigroup._
import io.mienks.resilience.Rate.syntax._
import munit.FunSuite

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

final class RateTests extends FunSuite {

  test("construction canonicalizes equivalent rates to events per day") {
    assert(Rate(1, 1.second) === Rate(60, 1.minute))
    assert(Rate(1, 1.second) =!= Rate(2, 1.second))
    assert(Rate(requests = 1, period = 30.seconds) === Rate(requests = 2, period = 1.minute))
    assertEquals(Rate(requests = 1, period = 1.second).perDay, 86_400L)
  }

  test("construction floors fractional events per day and clamps positive rates to bounds") {
    assertEquals(Rate(requests = 1, period = 7.seconds).perDay, 12_342L)
    assertEquals(Rate(requests = 1, period = 2.days), Rate.Min)
    assertEquals(Rate(requests = 100, period = 1.millisecond), Rate.Max)
    assertEquals(Rate(requests = 101, period = 1.millisecond), Rate.Max)
    assertEquals(Rate(perDay = Long.MaxValue), Rate.Max)
  }

  test("construction rejects non-positive values") {
    intercept[IllegalArgumentException](Rate(perDay = 0L))
    intercept[IllegalArgumentException](Rate(perDay = -1L))
    intercept[IllegalArgumentException](Rate(requests = 0, period = 1.second))
    intercept[IllegalArgumentException](Rate(requests = -1, period = 1.second))
    intercept[IllegalArgumentException](Rate(requests = 1, period = Duration.Zero))
    intercept[IllegalArgumentException](Rate(requests = 1, period = (-1).second))
  }

  test("Semigroup combines rates and saturates at Max") {
    val onePerSecond = Rate(1, 1.second)

    assert((onePerSecond |+| onePerSecond) === Rate(2, 1.second))
    assert((onePerSecond |+| onePerSecond) === 2.per(1.second))
    assertEquals(60.per(1.minute) |+| onePerSecond, 120.per(1.minute))
    assertEquals(onePerSecond |+| 60.per(1.minute), 2.per(1.second))
    assertEquals(Semigroup[Rate].combine(Rate.Max, onePerSecond), Rate.Max)
    assertEquals(Rate(perDay = Rate.Max.perDay - 1L) + Rate.Min, Rate.Max)
  }

  test("saturating addition is associative") {
    val a = Rate(requests = 1, period = 1.second)
    val b = Rate(requests = 2, period = 1.second)
    val c = Rate(requests = 1, period = 2.seconds)

    assert(((a |+| b) |+| c) === (a |+| (b |+| c)))
    assert(((Rate.Max |+| b) |+| c) === (Rate.Max |+| (b |+| c)))
  }

  test("parse accepts valid rates and rejects invalid rates") {
    assertEquals(Rate.parse("8 requests / 2 minutes"), Rate(8, 2.minutes).some)
    assertEquals(Rate.parse("500 requests / 4 hours"), Rate(500, 4.hours).some)
    assertEquals(Rate.parse("0 requests / 4 hours"), none[Rate])
    assertEquals(Rate.parse("abc requests / 4 hours"), none[Rate])
    assertEquals(Rate.parse("500 requests / xyz hours"), none[Rate])
  }

  test("rate syntax creates rates and rejects invalid input") {
    assertEquals(1.per(1.second), Rate(1, 1.second))
    assertEquals(12.per(6.seconds), Rate(12, 6.seconds))

    assertEquals(rate"8 requests / 2 minutes", Rate(8, 2.minutes))
    assertEquals(rate"500 requests / 4 hours", Rate(500, 4.hours))
    intercept[IllegalArgumentException](rate"abc requests / 4 hours")
    intercept[NumberFormatException](rate"500 requests / xyz hours")
  }

  test("compare orders by effective throughput") {
    val onePerSecond   = Rate(1, 1.second)
    val sixtyPerMinute = Rate(60, 1.minute)
    assertEquals(onePerSecond.compare(sixtyPerMinute), 0)

    val slower = Rate(1, 2.seconds)
    val faster = Rate(1, 1.second)
    assert(slower < faster)
    assert(faster > slower)

    val twoPerSecond = 2.per(1.second)
    assert(onePerSecond < twoPerSecond)
    assert(twoPerSecond.compare(onePerSecond) > 0)
    assertEquals(Order[Rate].compare(x = onePerSecond, y = twoPerSecond), onePerSecond.compare(twoPerSecond))
    assertEquals(Ordering[Rate].compare(x = onePerSecond, y = twoPerSecond), onePerSecond.compare(twoPerSecond))
  }

  test("min and max compare by effective throughput") {
    val onePerSecond    = Rate(1, 1.second)
    val sixtyPerMinute  = Rate(60, 1.minute)
    val thirtyPerMinute = Rate(30, 1.minute)
    val twoPerSecond    = Rate(2, 1.second)

    assertEquals(onePerSecond.min(that = thirtyPerMinute), thirtyPerMinute)
    assertEquals(thirtyPerMinute.max(that = onePerSecond), onePerSecond)
    assertEquals(onePerSecond.max(that = twoPerSecond), twoPerSecond)
    assertEquals(twoPerSecond.min(that = onePerSecond), onePerSecond)
    assertEquals(onePerSecond.min(that = sixtyPerMinute), onePerSecond)
    assertEquals(sixtyPerMinute.max(that = onePerSecond), sixtyPerMinute)
  }

  test("eventsPer converts the canonical rate to a TimeUnit") {
    val rate = Rate(requests = 2, period = 1.second)

    assertEquals(rate.eventsPer(unit = TimeUnit.NANOSECONDS), 2.0 / 1.second.toNanos.toDouble)
    assertEquals(rate.eventsPer(unit = TimeUnit.MILLISECONDS), 0.002)
    assertEquals(rate.eventsPer(unit = TimeUnit.SECONDS), 2.0)
    assertEquals(rate.eventsPer(unit = TimeUnit.MINUTES), 120.0)
    assertEquals(rate.eventsPer(unit = TimeUnit.HOURS), 7200.0)
    assertEquals(rate.eventsPer(unit = TimeUnit.DAYS), 172_800.0)

    assertEquals(rate.perNanosecond, rate.eventsPer(unit = TimeUnit.NANOSECONDS))
    assertEquals(rate.perMicrosecond, rate.eventsPer(unit = TimeUnit.MICROSECONDS))
    assertEquals(rate.perMillisecond, rate.eventsPer(unit = TimeUnit.MILLISECONDS))
    assertEquals(rate.perSecond, rate.eventsPer(unit = TimeUnit.SECONDS))
    assertEquals(rate.perMinute, rate.eventsPer(unit = TimeUnit.MINUTES))
    assertEquals(rate.perHour, rate.eventsPer(unit = TimeUnit.HOURS))
  }

  test("emissionIntervalNanos uses ceiling division") {
    assertEquals(Rate(requests = 1, period = 1.second).emissionIntervalNanos, 1.second.toNanos)
    assertEquals(Rate(requests = 2, period = 1.second).emissionIntervalNanos, 500.millis.toNanos)
    assertEquals(Rate(perDay = 7L).emissionIntervalNanos, 12_342_857_142_858L)
    assertEquals(Rate.Max.emissionIntervalNanos, 10_000L)
  }

  test("reduceBy rounds reductions up and preserves bounds") {
    val fivePerSecond = 5.per(1.second) // 1 / 200 ms

    assertEquals(fivePerSecond.reduceBy(factor = RateReduction.One), fivePerSecond)
    assertEquals(fivePerSecond.reduceBy(factor = RateReduction(value = 0.5)), Rate(1, 400.millis))
    assertEquals(1.per(1.second).reduceBy(factor = RateReduction(value = 0.2)), Rate(1, 5.seconds))
    assertEquals(1.per(30.seconds).reduceBy(factor = RateReduction(value = 0.5)), 1.per(1.minute))
    assertEquals(Rate.Min.reduceBy(factor = RateReduction(value = 0.5)), Rate.Min)
    assertEquals(
      Rate.Max.reduceBy(factor = RateReduction(value = 0.5)),
      Rate(perDay = Rate.Max.perDay / 2L)
    )
  }

  test("RateReduction rejects invalid or unrepresentable values") {
    assert(RateReduction.from(value = 0.0).isLeft)
    assert(RateReduction.from(value = -0.1).isLeft)
    assert(RateReduction.from(value = 1.1).isLeft)
    assert(RateReduction.from(value = Double.NaN).isLeft)
    assert(RateReduction.from(value = Double.PositiveInfinity).isLeft)
    assert(RateReduction.from(value = Double.MinPositiveValue).isLeft)
  }

  test("RateReduction supports total ordering") {
    val half = RateReduction(value = 0.5)

    assert(half < RateReduction.One)
    assertEquals(Order[RateReduction].compare(x = half, y = RateReduction.One), half.compare(RateReduction.One))
    assertEquals(Ordering[RateReduction].compare(x = half, y = RateReduction.One), half.compare(RateReduction.One))
  }

  test("repeated multiplicative decreases and additive increases cannot overflow the representation") {
    val minRate  = Rate(requests = 1, period = 10_000.seconds)
    val maxRate  = Rate(requests = 10_000, period = 1.second)
    val increase = Rate(requests = 100, period = 1.second)
    val decrease = RateReduction(value = 0.3)

    val decreasedRates = List.fill(10)(()).scanLeft(maxRate) { case (rate, _) =>
      rate.reduceBy(factor = decrease).max(minRate)
    }
    val recoveredRates = List.fill(22)(()).scanLeft(decreasedRates.last) { case (rate, _) =>
      (rate + increase).min(maxRate)
    }
    val allRates = decreasedRates ++ recoveredRates

    assertEquals(decreasedRates.length, 11)
    assertEquals(recoveredRates.length, 23)
    assert(allRates.forall(rate => rate >= minRate && rate <= maxRate))
  }
}
