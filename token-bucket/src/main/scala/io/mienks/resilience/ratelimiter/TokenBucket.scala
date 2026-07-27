package io.mienks.resilience.ratelimiter

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import cats.effect.std.AtomicCell
import cats.effect.{Async, Clock, Ref}
import cats.syntax.all._
import cats.{Applicative, Monad}
import io.mienks.resilience.ratelimiter.RateLimiter.RefillRate

/** Thread safe, constant memory, constant time, & linear-refill-rate implementation of TokenBucket. All public API's
  * use discrete values for tokens and lazily call `refill` function to update bucket state. Under the hood, the bucket
  * is modeled as a continuous flow tokens using Double's.
  * @param bucketCapacity
  *   max allowed tokens
  * @param tokensPerMillisecond
  *   refill rate
  * @param lastRefill
  *   timestamp of last refill
  * @param tokens
  *   current tokens in bucket
  */
class TokenBucket[F[_]: Monad: Clock] private (
    bucketCapacity: Double,
    configuredRefillRate: RefillRate,
    lastRefill: AtomicCell[F, FiniteDuration],
    tokens: Ref[F, Double]
) extends RateLimiter[F] {

  private val tokensPerNanosecond: Double = configuredRefillRate.eventsPer(unit = TimeUnit.NANOSECONDS)

  def capacity: F[Int] = Applicative[F].pure(bucketCapacity.toInt)

  override def refillRate: F[RefillRate] = configuredRefillRate.pure[F]

  /** Get the amount of current tokens
    * @return
    */
  def requests: F[Int] =
    refill() *> tokens.get.map(_.toInt)

  /** Try to consume specified tokens
    * @param tokens
    *   default 1
    * @return
    *   if consumed or not
    */
  def consume(tokens: Int = 1): F[Boolean] =
    refill() *> this.tokens.modify { availableTokens =>
      if (availableTokens >= tokens)
        (availableTokens - tokens, true)
      else
        (availableTokens, false)
    }

  /** Consume all available tokens at once
    * @return
    */
  def consumeRemaining(): F[Int] =
    refill() *>
      this.tokens
        .getAndUpdate(availableTokens => availableTokens - math.floor(availableTokens))
        .map(math.floor(_).toInt)

  /*
  DEV NOTE: Public API's which read or update bucket (tokens ref) must first call this method.
   */
  private def refill(): F[Unit] = lastRefill.evalUpdate { last =>
    for {
      now <- Clock[F].monotonic
      nanosecondsSinceLastRefill = (now - last).toNanos
      // unit math: tokens/ms * ms = tokens
      newTokens = tokensPerNanosecond * nanosecondsSinceLastRefill
      _ <- tokens.update(t => math.min(bucketCapacity, t + newTokens))
    } yield now
  }

}

object TokenBucket {

  def apply[F[_]: Async](
      capacity: Int,
      initial: Int,
      refillRate: RefillRate
  ): F[TokenBucket[F]] =
    for {
      lastRefill <- Clock[F].monotonic.flatMap(AtomicCell[F].of)

      tokens <- Ref[F].of(
        (
          if (initial > capacity)
            capacity
          else if (initial < 0)
            0
          else
            initial
        ).toDouble
      )
    } yield new TokenBucket[F](
      capacity.toDouble,
      configuredRefillRate = refillRate,
      lastRefill,
      tokens
    )

  def full[F[_]: Async](capacity: Int, refillRate: RefillRate): F[TokenBucket[F]] =
    apply(capacity, initial = capacity, refillRate)

  def empty[F[_]: Async](capacity: Int, refillRate: RefillRate): F[TokenBucket[F]] =
    apply(capacity, initial = 0, refillRate)

}
