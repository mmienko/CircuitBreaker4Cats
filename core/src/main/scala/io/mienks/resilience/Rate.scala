package io.mienks.resilience

import cats.kernel.{Monoid, Order}
import cats.syntax.all._
import io.mienks.resilience.Rate.{Zero, inReducedForm}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

/** Rate of requests / period
  * @param requests
  *   number of requests (numerator)
  * @param period
  *   unit of time (denominator)
  */
final case class Rate(requests: Int, period: FiniteDuration) extends Ordered[Rate] {
  def emissionIntervalNanos: Long = period.toNanos / requests

  // TODO: move this to a more appropriate class
  def validate: Either[Throwable, Long] =
    for {
      _ <- Either.cond(
        requests > 0,
        (),
        new IllegalArgumentException(s"rate.requests must be positive, got: ${requests.toString}") with NoStackTrace
      )
      _ <- Either.cond(
        period.toNanos > 0,
        (),
        new IllegalArgumentException(s"rate.period must be positive, got: $period") with NoStackTrace
      )
      interval = period.toNanos / requests
      _ <- Either.cond(
        interval > 0,
        (),
        new IllegalArgumentException(
          s"rate emission interval must be positive (period.toNanos / requests); " +
            s"got ${interval}ns for $requests requests / $period"
        ) with NoStackTrace
      )
    } yield interval

  override def compare(that: Rate): Int = {
    val lhs = BigInt(this.requests) * BigInt(that.period.toNanos)
    val rhs = BigInt(that.requests) * BigInt(this.period.toNanos)
    lhs.compare(rhs)
  }

  def min(that: Rate): Rate =
    if (this <= that) this else that

  def max(that: Rate): Rate =
    if (this >= that) this else that

  def +(that: Rate): Rate = {
    val x   = this
    val y   = that
    val px  = BigInt(x.period.toNanos)
    val py  = BigInt(y.period.toNanos)
    val num = BigInt(x.requests) * py + BigInt(y.requests) * px
    val den = px * py
    if (num == 0) Zero
    else {
      val requestsForXPeriod = num / py
      if (num % py == 0 && requestsForXPeriod.isValidInt)
        Rate(requests = requestsForXPeriod.toInt, period = x.period)
      else inReducedForm(numerator = num, denominator = den, opName = "combine")
    }
  }

  def -(that: Rate): Rate = subtract(that)

  /** `max(0, this - that)` */
  def subtract(that: Rate): Rate = {
    val p1  = BigInt(this.period.toNanos)
    val p2  = BigInt(that.period.toNanos)
    val num = BigInt(this.requests) * p2 - BigInt(that.requests) * p1
    val den = p1 * p2
    if (num <= 0) Rate.Zero
    else Rate.inReducedForm(numerator = num, denominator = den, opName = "subtract")
  }

  /** Re-express this rate in the given target period, truncating any sub-request remainder. For example,
    * `Rate(59049, 1000000.seconds).normalizedTo(1.second)` yields `Rate(0, 1.second)` because the throughput is less
    * than 1 request per second. This prevents period accumulation from inflating the `requests` field past
    * `Int.MaxValue` during repeated additions.
    */
  def normalizedTo(targetPeriod: FiniteDuration): Rate = {
    if (this.period == targetPeriod) this
    else if (this.requests == 0) Rate(requests = 0, period = targetPeriod)
    else {
      val tp               = BigInt(targetPeriod.toNanos)
      val requestsInTarget = (BigInt(this.requests) * tp) / BigInt(this.period.toNanos)
      if (requestsInTarget.isValidInt) Rate(requests = requestsInTarget.toInt, period = targetPeriod)
      else Rate.inReducedForm(numerator = requestsInTarget, denominator = tp, opName = "normalizedTo")
    }
  }

  /** Scale effective throughput: `factor == 1` leaves this unchanged; `factor < 1` slows the rate; `factor > 1` speeds
    * it up. `0` yields [[Rate.Zero]]; negative, NaN, or infinite `factor` throws.
    */
  def scaleBy(factor: Double): Rate = {
    if (factor == 1.0) this
    else if (this.requests == 0) Rate.Zero
    else if (factor.isNaN || factor.isInfinite)
      throw new IllegalArgumentException(s"Rate.scaleBy: factor must be finite, got: $factor")
    else if (factor < 0)
      throw new IllegalArgumentException(s"Rate.scaleBy: factor must be non-negative, got: $factor")
    else if (factor == 0.0) Rate.Zero
    else {
      // BigDecimal stores value as mantissa * 10^(-scale).
      val normalized = BigDecimal.valueOf(factor).bigDecimal.stripTrailingZeros()
      val mantissa   = BigInt(normalized.unscaledValue())
      val scale      = normalized.scale()

      val (factorNum, factorDen) =
        if (scale >= 0) // common case
          (mantissa, BigInt(10).pow(scale))
        else
          (mantissa * BigInt(10).pow(-scale), BigInt(1))

      val numerator = BigInt(this.requests) * factorNum
      if (numerator % factorDen == 0) {
        val requests = numerator / factorDen
        if (requests.isValidInt)
          Rate(requests = requests.toInt, period = this.period)
        else
          Rate.inReducedForm(
            numerator = numerator,
            denominator = BigInt(this.period.toNanos) * factorDen,
            opName = "scaleBy"
          )
      } else
        Rate.inReducedForm(
          numerator = numerator,
          denominator = BigInt(this.period.toNanos) * factorDen,
          opName = "scaleBy"
        )
    }
  }
}

object Rate {

  private val RatePattern = """(\d+)\s*(request|requests)\s*/\s*(.+)\s*""".r

  /** Zero throughput; identity for [[rateMonoid]]. Does not satisfy [[Rate.validate]]. */
  val Zero: Rate =
    Rate(requests = 0, period = FiniteDuration(length = 1L, unit = TimeUnit.SECONDS))

  /** Total throughput order; [[cats.kernel.Eq]] comes from [[Order]] (throughput may differ from case-class `==`). */
  implicit val catsKernelOrderForRate: Order[Rate] =
    Order.from[Rate]((x, y) => x.compare(y))

  /** Sum of effective rates (requests/time), i.e. rational addition of `requests/period`. */
  implicit val rateMonoid: Monoid[Rate] = new Monoid[Rate] {
    def empty: Rate                     = Zero
    def combine(x: Rate, y: Rate): Rate = x + y
  }

  private[resilience] def inReducedForm(numerator: BigInt, denominator: BigInt, opName: String): Rate = {
    val g = numerator.gcd(denominator)
    val n = numerator / g
    val d = denominator / g
    if (!n.isValidInt || n < 0)
      throw new IllegalArgumentException(
        s"Rate.$opName: resulting requests do not fit in Int (after reduction: $n)"
      )
    if (!d.isValidLong || d <= 0)
      throw new IllegalArgumentException(
        s"Rate.$opName: resulting period does not fit in Long or is non-positive ($d)"
      )
    Rate(n.toInt, Duration.fromNanos(d.toLong))
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
          case _: NumberFormatException => None
        }
      case _ => None
    }

  object syntax {
    implicit class RateOps(val requests: Int) extends AnyVal {
      def per(period: FiniteDuration): Rate = Rate(requests, period)
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
