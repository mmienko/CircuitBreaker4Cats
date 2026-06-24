package io.mienks.resilience

import cats.kernel.Order

/** Fixed-point identity or growth factor for a [[Rate]].
  *
  * The represented value is `numerator / 2^20` and is in the range `[1, Rate.Max / Rate.Min]`.
  */
final class RateMultiplier private (val numerator: Long) extends AnyVal with Ordered[RateMultiplier] {
  override def compare(that: RateMultiplier): Int =
    java.lang.Long.compare(numerator, that.numerator)
}

object RateMultiplier {
  val Denominator: Long = 1L << 20

  private val MaxValue: Double = Rate.MaxPerDay.toDouble / Rate.MinPerDay.toDouble

  val One: RateMultiplier = new RateMultiplier(numerator = Denominator)
  val Max: RateMultiplier = new RateMultiplier(numerator = Rate.MaxPerDay * Denominator)

  /** Approximates a number in the range `[1, Rate.Max / Rate.Min]` with 20 fractional bits, rounding to the nearest
    * value.
    */
  def apply(value: Double): RateMultiplier =
    from(value).fold(errMsg => throw new IllegalArgumentException(errMsg), identity)

  /** Approximates a number in the range `[1, Rate.Max / Rate.Min]` with 20 fractional bits, rounding to the nearest
    * value.
    */
  def from(value: Double): Either[String, RateMultiplier] =
    Either.cond(
      value >= 1.0 && value <= MaxValue && value.isFinite,
      new RateMultiplier(numerator = math.round(value * Denominator.toDouble)),
      s"RateMultiplier must be finite and within [1, ${MaxValue.toString}], got: ${value.toString}"
    )

  implicit val catsKernelOrderForRateMultiplier: Order[RateMultiplier] =
    Order.from[RateMultiplier]((x, y) => x.compare(y))

  implicit val scalaOrderingForRateMultiplier: Ordering[RateMultiplier] =
    Ordering.by[RateMultiplier, Long](_.numerator)
}
