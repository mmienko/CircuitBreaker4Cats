package io.mienks.resilience

import cats.kernel.{Order, Semigroup}
import cats.syntax.all._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

/** Positive event rate, stored as a fixed-point number of events per day.
  *
  * Values are clamped to the inclusive range from one event per day to 100 events per millisecond.
  */
final class Rate private (val perDay: Long) extends AnyVal with Ordered[Rate] {
  def emissionIntervalNanos: Long =
    Rate.ceilDivide(numerator = Rate.NanosPerDay, denominator = perDay)

  def perNanosecond: Double =
    eventsPer(unit = TimeUnit.NANOSECONDS)

  def perMicrosecond: Double =
    eventsPer(unit = TimeUnit.MICROSECONDS)

  def perMillisecond: Double =
    eventsPer(unit = TimeUnit.MILLISECONDS)

  def perSecond: Double =
    eventsPer(unit = TimeUnit.SECONDS)

  def perMinute: Double =
    eventsPer(unit = TimeUnit.MINUTES)

  def perHour: Double =
    eventsPer(unit = TimeUnit.HOURS)

  def eventsPer(unit: TimeUnit): Double =
    perDay.toDouble * unit.toNanos(1L).toDouble / Rate.NanosPerDay.toDouble

  override def compare(that: Rate): Int =
    java.lang.Long.compare(perDay, that.perDay)

  def min(that: Rate): Rate =
    if (this <= that) this else that

  def max(that: Rate): Rate =
    if (this >= that) this else that

  def +(that: Rate): Rate = {
    val newRate = perDay + that.perDay
    if (newRate >= Rate.MaxPerDay) Rate.Max
    else new Rate(perDay = newRate)
  }

  /** Reduces this rate by a fixed-point factor, rounding up to the next event per day. */
  def reduceBy(factor: RateReduction): Rate =
    new Rate(
      perDay = Rate.ceilDivide(
        numerator = perDay * factor.numerator,
        denominator = RateReduction.Denominator
      )
    )

  /** Multiplies this rate by a fixed-point factor, rounding up to the next event per day and saturating at
    * [[Rate.Max]].
    */
  def multiplyBy(factor: RateMultiplier): Rate =
    if (perDay > RateMultiplier.Max.numerator / factor.numerator) Rate.Max
    else
      new Rate(
        perDay = Rate.ceilDivide(
          numerator = perDay * factor.numerator,
          denominator = RateMultiplier.Denominator
        )
      )

  override def toString: String = s"${perSecond.toString}/second"
}

object Rate {

  private val RatePattern = """(\d+)\s*(request|requests)\s*/\s*(.+)\s*""".r

  private[resilience] val NanosPerDay: Long = TimeUnit.DAYS.toNanos(1L)
  private[resilience] val MinPerDay: Long   = 1L
  private[resilience] val MaxPerDay: Long   = 8_640_000_000L

  val Min: Rate = new Rate(perDay = MinPerDay)
  val Max: Rate = new Rate(perDay = MaxPerDay)

  def apply(perDay: Long): Rate = {
    require(perDay > 0L, s"Rate.perDay must be positive, got: ${perDay.toString}")
    if (perDay >= MaxPerDay) Max else new Rate(perDay = perDay)
  }

  def apply(requests: Int, period: FiniteDuration): Rate = {
    require(requests > 0, s"Rate.requests must be positive, got: ${requests.toString}")
    require(period.length > 0L, s"Rate.period must be positive, got: $period")

    val periodNanos = BigInt(period.length) * BigInt(period.unit.toNanos(1L))
    val perDay      = BigInt(requests) * BigInt(NanosPerDay) / periodNanos

    if (perDay < MinPerDay) Min
    else if (perDay >= MaxPerDay) Max
    else new Rate(perDay = perDay.toLong)
  }

  /** Total ordering; [[cats.kernel.Eq]] is derived from [[Order]]. */
  implicit val catsKernelOrderForRate: Order[Rate] =
    Order.from[Rate]((x, y) => x.compare(y))

  implicit val scalaOrderingForRate: Ordering[Rate] =
    Ordering.by[Rate, Long](_.perDay)

  implicit val rateSemigroup: Semigroup[Rate] = new Semigroup[Rate] {
    def combine(x: Rate, y: Rate): Rate = x + y
  }

  def parse(rate: String): Option[Rate] =
    rate match {
      case RatePattern(reqStr, _, durStr) =>
        try {
          Duration(durStr) match {
            case _: Duration.Infinite   => None
            case period: FiniteDuration =>
              Rate(requests = reqStr.toInt, period = period).some
          }
        } catch {
          case _: IllegalArgumentException => None
        }
      case _ => None
    }

  private def ceilDivide(numerator: Long, denominator: Long): Long =
    (numerator - 1L) / denominator + 1L

  object syntax {
    implicit class RateOps(val requests: Int) extends AnyVal {
      def per(period: FiniteDuration): Rate = Rate(requests = requests, period = period)
    }

    implicit class RateInterpolator(val sc: StringContext) extends AnyVal {
      def rate(args: Any*): Rate = {
        val input = sc.s(args: _*)
        // Example input: "5 requests / 1 minute"
        input match {
          case RatePattern(reqStr, _, durStr) =>
            Duration(durStr) match {
              case _: Duration.Infinite =>
                throw new IllegalArgumentException(s"Invalid period rate syntax: $input")
              case period: FiniteDuration =>
                Rate(requests = reqStr.toInt, period = period)
            }
          case _ =>
            throw new IllegalArgumentException(s"Invalid rate syntax: $input")
        }
      }
    }
  }
}
