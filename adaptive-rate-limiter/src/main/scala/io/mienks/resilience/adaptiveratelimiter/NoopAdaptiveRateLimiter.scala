package io.mienks.resilience.adaptiveratelimiter

import cats.Applicative
import cats.syntax.all._
import io.mienks.resilience.Rate

import scala.concurrent.duration._

/** Always-passing [[AdaptiveRateLimiter]] for testing or as a safe default when adaptive behavior is disabled. Reports
  * a fixed sustainable [[Rate]] (the caller's stated assumption about the protected sink), records nothing, and always
  * allows `consume`.
  */
class NoopAdaptiveRateLimiter[F[_]: Applicative](
    private val configuredRate: Rate = Rate(requests = 1, period = 1.second)
) extends AdaptiveRateLimiter[F] {

  override def consume: F[Boolean] = true.pure[F]

  override def recordSuccess: F[Unit] = Applicative[F].unit

  override def recordFailure: F[Unit] = Applicative[F].unit

  override def rate: F[Rate] = configuredRate.pure[F]

  override def failureRatio: F[Option[Double]] = none[Double].pure[F]
}
