package io.mienks.resilience.ratelimiter

import cats.effect.Sync
import cats.syntax.all._
import io.mienks.resilience.Rate

import scala.util.control.NoStackTrace

trait RateLimiter[F[_]] {

  /** @return
    *   Max number of requests that can be made in a single burst
    */
  def capacity: F[Int]

  /** @return
    *   Configured refill rate for the limiter
    */
  def refillRate: F[RateLimiter.RefillRate]

  /** @return
    *   Current number of requests available before being rate limited
    */
  def requests: F[Int]

  /** Try to atomically consume specified requests
    * @param requests
    *   default 1
    * @return
    *   if consumed or not
    */
  def consume(requests: Int = 1): F[Boolean]

  /** Consume all available requests at once
    * @return
    *   number of requests consumed
    */
  def consumeRemaining(): F[Int]
}

object RateLimiter {

  def apply[F[_]: Sync](
      capacity: Int,
      initialCapacity: Int,
      rate: RefillRate
  ): F[RateLimiter[F]] = GCRA(capacity, initialCapacity, rate).widen

  def apply[F[_]: Sync](config: Config): F[RateLimiter[F]] = GCRA(config).widen

  def empty[F[_]: Sync](capacity: Int, rate: RefillRate): F[RateLimiter[F]] =
    apply(capacity, initialCapacity = 0, rate)

  def full[F[_]: Sync](capacity: Int, rate: RefillRate): F[RateLimiter[F]] =
    apply(capacity, initialCapacity = capacity, rate)

  def gcra[F[_]: Sync](capacity: Int, initial: Int, refillRate: RefillRate): F[GCRA[F]] =
    GCRA(capacity, initial, refillRate)

  object Dynamic {

    def apply[F[_]: Sync](config: Config): F[DynamicRateLimiter[F]] = DynamicGCRA(config).widen

    def apply[F[_]: Sync](capacity: Int, initialCapacity: Int, refillRate: RefillRate): F[DynamicRateLimiter[F]] =
      DynamicGCRA(capacity, initialCapacity, refillRate).widen

    def empty[F[_]: Sync](capacity: Int, refillRate: RefillRate): F[DynamicRateLimiter[F]] =
      DynamicGCRA.empty(capacity, refillRate).widen

    def full[F[_]: Sync](capacity: Int, refillRate: RefillRate): F[DynamicRateLimiter[F]] =
      DynamicGCRA.full(capacity, refillRate).widen

    def gcra[F[_]: Sync](capacity: Int, initial: Int, refillRate: RefillRate): F[DynamicRateLimiter[F]] =
      DynamicGCRA(capacity, initial, refillRate).widen
  }

  type RefillRate = Rate
  val RefillRate: Rate.type    = Rate
  val syntax: Rate.syntax.type = Rate.syntax

  final case class Config(capacity: Int, initialCapacity: Int, refillRate: RefillRate) {
    def validate: Either[Throwable, Long] =
      for {
        _ <- Either.cond(
          capacity >= 1,
          (),
          new IllegalArgumentException(s"capacity must be positive, got: $capacity") with NoStackTrace
        )
        _ <- Either.cond(
          initialCapacity >= 0,
          (),
          new IllegalArgumentException(s"initialCapacity must be non-negative, got: $initialCapacity") with NoStackTrace
        )
        emissionIntervalNanos = refillRate.emissionIntervalNanos
        _ <- Config.requireNoOverflow(emissionIntervalNanos, capacity.toLong, "emissionInterval * capacity")
        _ <- Config.requireNoOverflow(
          emissionIntervalNanos,
          initialCapacity.toLong,
          "emissionInterval * initialCapacity"
        )
      } yield emissionIntervalNanos
  }

  object Config {
    def full(capacity: Int, refillRate: RefillRate): Config =
      new Config(capacity, initialCapacity = capacity, refillRate)

    def apply(capacity: Int, refillRate: RefillRate): Config =
      full(capacity, refillRate)

    def empty(capacity: Int, refillRate: RefillRate): Config =
      new Config(capacity, initialCapacity = 0, refillRate)

    private[ratelimiter] def requireNoOverflow(a: Long, b: Long, label: String): Either[Throwable, Unit] =
      Either.cond(
        Math.multiplyHigh(a, b) == 0,
        (),
        new ArithmeticException(s"Long overflow: $label ($a * $b)") with NoStackTrace
      )
  }
}
