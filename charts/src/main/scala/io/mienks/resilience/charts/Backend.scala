package io.mienks.resilience.charts

import cats.data.NonEmptyList
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._
import io.mienks.resilience.Rate
import io.mienks.resilience.ratelimiter.{DynamicRateLimiter, RateLimiter}

import scala.concurrent.duration._
import scala.util.Random

/** A simulated downstream resource whose health is a function of the offered load.
  *
  * A phase is a hard ceiling plus zero or more soft ceilings. The hard ceiling is the base sustainable rate: any call
  * admitted above it fails outright (100%). Each [[Backend.SoftCeiling]] is a tighter rate that fails only a fraction
  * (`failProbability`) of the calls that overflow it. Every ceiling is a token bucket ([[DynamicRateLimiter]]); a call
  * consumes one token from each bucket and fails with the most severe `failProbability` among the ceilings it
  * overflowed (the hard ceiling contributing 1.0). This makes the observed failure ratio an emergent property of the
  * rate the adaptive limiter admits, closing the control loop.
  */
trait Backend[F[_]] {

  /** Issue one call. `true` = success, `false` = overload failure. */
  def call: F[Boolean]

  /** The active phase's hard ceiling (the bottleneck the limiter is trying to discover). */
  def baseCapacity: F[Rate]

  /** The active phase's offered load: the aggregate rate the workload should drive against the limiter. */
  def offeredLoad: F[Rate]
}

object Backend {

  /** A ceiling below the hard ceiling that fails only `failProbability` of the calls that overflow it. */
  final case class SoftCeiling(capacity: Rate, failProbability: Double) {
    require(
      failProbability >= 0.0 && failProbability < 1.0,
      s"SoftCeiling.failProbability must be in [0.0, 1.0), got: ${failProbability.toString}"
    )
  }

  /** One leg of a backend schedule: hold the backend at `hardCeiling` (plus `softCeilings`) for `duration`, with the
    * workload offering `offeredLoad`. The default offered load comfortably exceeds any limiter `maxRate`, so the
    * admitted throughput tracks the limiter's refill rate (the historical harness behavior); lower it to starve the
    * measurement window and exercise the limiter's slow-start.
    */
  final case class Phase(
      hardCeiling: Rate,
      softCeilings: List[SoftCeiling],
      duration: FiniteDuration,
      offeredLoad: Rate = DefaultOfferedLoad
  )

  /** Offered load high enough to swamp any limiter `maxRate`, so admitted throughput tracks the refill rate. */
  val DefaultOfferedLoad: Rate = Rate(requests = 4000, period = 1.second)

  // Burst tokens per ceiling; kept small so each limiter behaves like a rate ceiling rather than a buffer.
  private val BurstCapacity: Int = 8

  /** Build a backend that walks `schedule` on an internal fiber, reconfiguring its ceilings as each phase begins. The
    * buckets are created once from the head phase, so all phases must share the same number of soft ceilings; each
    * phase then updates both the refill rates and the failure probabilities.
    */
  def start(schedule: NonEmptyList[Phase], seed: Long): Resource[IO, Backend[IO]] = {
    for {
      rng <- Resource.eval(IO(new Random(seed)))
      initialPhase = schedule.head
      buckets <- Resource.eval(
        ceilingRates(initialPhase).traverse { rate =>
          RateLimiter.Dynamic.full[IO](capacity = BurstCapacity, refillRate = rate)
        }
      )
      capacity             <- Resource.eval(Ref[IO].of(initialPhase.hardCeiling))
      offeredLoadRef       <- Resource.eval(Ref[IO].of(initialPhase.offeredLoad))
      failProbabilitiesRef <- Resource.eval(Ref[IO].of(failProbabilities(initialPhase)))

      _ <- schedule.traverse_ { phase =>
        val updateBuckets = buckets.toList.zip(ceilingRates(phase).toList).traverse_ { case (limiter, rate) =>
          limiter.setRefillRate(rate)
        }

        updateBuckets >>
          failProbabilitiesRef.set(failProbabilities(phase)) >>
          capacity.set(phase.hardCeiling) >>
          offeredLoadRef.set(phase.offeredLoad) >>
          IO.sleep(phase.duration)
      }.background
    } yield new TokenBucketBackend(
      buckets = buckets,
      failProbabilitiesRef = failProbabilitiesRef,
      capacity = capacity,
      offeredLoadRef = offeredLoadRef,
      rng = rng
    )
  }

  private def ceilingRates(phase: Phase): NonEmptyList[Rate] =
    NonEmptyList(phase.hardCeiling, phase.softCeilings.map(_.capacity))

  private def failProbabilities(phase: Phase): NonEmptyList[Double] =
    NonEmptyList(1.0, phase.softCeilings.map(_.failProbability))

  private final class TokenBucketBackend(
      buckets: NonEmptyList[DynamicRateLimiter[IO]],
      failProbabilitiesRef: Ref[IO, NonEmptyList[Double]],
      capacity: Ref[IO, Rate],
      offeredLoadRef: Ref[IO, Rate],
      rng: Random
  ) extends Backend[IO] {

    override def call: IO[Boolean] =
      for {
        failProbabilities     <- failProbabilitiesRef.get
        overflowProbabilities <- failProbabilities.zip(buckets).traverse { case (failProbability, limiter) =>
          limiter.consume().map(admitted => if (admitted) 0.0 else failProbability)
        }
        failProbability = overflowProbabilities.maximum
        admit <-
          if (failProbability <= 0.0) true.pure[IO]
          else IO(rng.nextDouble()).map(_ >= failProbability)
      } yield admit

    override def baseCapacity: IO[Rate] = capacity.get

    override def offeredLoad: IO[Rate] = offeredLoadRef.get
  }
}
