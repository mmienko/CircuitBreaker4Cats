package io.mienks.resilience

import cats.kernel.Order

/** Fixed-point reduction or identity factor for a [[Rate]].
  *
  * The represented value is `numerator / 2^20` and is in the range `(0, 1]`.
  */
final class RateReduction private (val numerator: Long) extends AnyVal with Ordered[RateReduction] {
  override def compare(that: RateReduction): Int =
    java.lang.Long.compare(numerator, that.numerator)
}

object RateReduction {
  val Denominator: Long = 1L << 20

  val One: RateReduction = new RateReduction(numerator = Denominator)

  /** Approximates a number in the range `(0, 1]` with 20 fractional bits, rounding to the nearest value. */
  def apply(value: Double): RateReduction =
    from(value).fold(errMsg => throw new IllegalArgumentException(errMsg), identity)

  /** Approximates a number in the range `(0, 1]` with 20 fractional bits, rounding to the nearest value. */
  def from(value: Double): Either[String, RateReduction] =
    for {
      _ <- Either.cond(
        value > 0.0 && value <= 1.0 && value.isFinite,
        (),
        s"RateReduction must be finite and within (0, 1], got: ${value.toString}"
      )
      numerator = math.round(value * Denominator.toDouble)
      _ <- Either.cond(
        numerator > 0L,
        (),
        s"RateReduction is too small to represent, got: ${value.toString}"
      )
    } yield new RateReduction(numerator = numerator)

  implicit val catsKernelOrderForRateReduction: Order[RateReduction] =
    Order.from[RateReduction]((x, y) => x.compare(y))

  implicit val scalaOrderingForRateReduction: Ordering[RateReduction] =
    Ordering.by[RateReduction, Long](_.numerator)
}
