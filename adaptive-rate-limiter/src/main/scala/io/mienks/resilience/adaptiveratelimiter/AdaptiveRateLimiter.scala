package io.mienks.resilience.adaptiveratelimiter

import cats.data.NonEmptyList
import cats.effect.{Async, Ref, Resource, Spawn, Sync, Temporal}
import cats.syntax.all._
import cats.{Applicative, ApplicativeThrow, Monad}
import fs2.{Chunk, Pipe}
import io.mienks.resilience.ratelimiter.{DynamicRateLimiter, RateLimiter}
import io.mienks.resilience.{Measurements, Rate, SampledMeasurements}

import scala.concurrent.duration._

/** Estimates and self-tunes the estimated rate at which a downstream resource (the "protected sink") can be invoked.
  * Internally wraps a `DynamicRateLimiter` whose refill rate is driven by an AIMD (Additive Increase / Multiplicative
  * Decrease) control loop reacting to observed failure rates.
  *
  * Outcomes are recorded with [[recordSuccess]] / [[recordFailure]] on a hot path (cheap, lock-free), then sampled on a
  * background fiber. The categorizer applies hysteresis bands to avoid flapping and emits [[FailureGradient]] events.
  * The AIMD loop additively grows the estimated rate on a fixed time tick and multiplicatively shrinks it on
  * [[FailureGradient.Worsening]].
  */
trait AdaptiveRateLimiter[F[_]] {

  /** Try to consume one request from the underlying rate limiter. */
  def consume: F[Boolean]

  /** Record one successful outcome from the protected sink. Safe to call from many fibers concurrently. */
  def recordSuccess: F[Unit]

  /** Record one failed outcome from the protected sink. Safe to call from many fibers concurrently. */
  def recordFailure: F[Unit]

  /** The AIMD's current estimate of the rate the protected sink can sustain. The underlying [[DynamicRateLimiter]] is
    * always configured at this same rate.
    */
  def rate: F[Rate]

  /** Most recently sampled failure ratio in `[0, 1]`; `0.0` until the measurement window is initialized. */
  def failureRatio: F[Double]
}

object AdaptiveRateLimiter {

  object syntax {

    implicit final class AdaptiveRateLimiterOps[F[_]](private val self: AdaptiveRateLimiter[F]) extends AnyVal {

      /** Try to consume one request and, on success, run `fa` while recording whether the result is a failure. When the
        * limiter is exhausted, return `orElse` without recording an outcome.
        */
      def protect[A](fa: F[A], isError: A => Boolean, orElse: => A)(implicit F: Monad[F]): F[A] =
        self.consume.flatMap { canProceed =>
          if (canProceed) fa.flatTap(a => if (isError(a)) self.recordFailure else self.recordSuccess)
          else orElse.pure[F]
        }

      /** Effectful variant of [[protect]]: the failure classifier itself returns `F[Boolean]`. */
      def protectF[A](fa: F[A], isError: A => F[Boolean], orElse: => F[A])(implicit F: Monad[F]): F[A] =
        self.consume.flatMap { canProceed =>
          if (canProceed)
            fa.flatTap(a => isError(a).flatMap(if (_) self.recordFailure else self.recordSuccess))
          else orElse
        }
    }
  }

  /** Hysteresis band keyed on failure ratios. `start` is the failure ratio at which the band engages; `exit` is the
    * failure ratio at which the band releases. `start >= exit` keeps the band hysteretic and prevents flapping.
    */
  final case class HysteresisBand(exit: Double, start: Double) {
    def validate: Either[String, Unit] =
      for {
        _ <- Either.cond(exit >= 0.0 && exit <= 1.0, (), "0 <= exit <= 1")
        _ <- Either.cond(start >= 0.0 && start <= 1.0, (), "0 <= start <= 1")
        _ <- Either.cond(start >= exit, (), "start >= exit to contain hysteresis")
      } yield ()
  }

  /** Configuration for [[AdaptiveRateLimiter.start]].
    *
    * The rate-shaped fields ([[initialRate]], [[minRate]], [[maxRate]], [[rateIncreaseBy]]) all refer to the AIMD's
    * estimate of the protected sink's sustainable throughput, not to the bucket-refill mechanism (the underlying rate
    * limiter is configured at that estimate as a side effect).
    *
    * @param capacity
    *   maximum burst size of the underlying rate limiter
    * @param initialRate
    *   initial estimate of the sink's sustainable rate; AIMD starts here
    * @param minRate
    *   floor on the estimate; multiplicative decrease will not push below this
    * @param maxRate
    *   ceiling on the estimate; additive increase will not push above this
    * @param rateIncreaseBy
    *   additive AIMD increase rate
    * @param rateIncreasePeriod
    *   interval at which the additive increase is applied
    * @param rateDecreaseBy
    *   multiplicative AIMD step in `[0, 1]`: on each [[FailureGradient.Worsening]] band crossed, the estimate is shrunk
    *   to `(1 - rateDecreaseBy) * current`
    * @param numberOfSlotsForMeasurements
    *   number of time-bucket slots in the failure-rate sliding window
    * @param slotDuration
    *   duration of each measurement slot
    * @param measurementPeriod
    *   how often the background fiber samples the LongAdder counters
    * @param minNumberOfMeasurements
    *   the sliding window must hold at least this many samples before the failure ratio is reported
    * @param failureLevels
    *   ordered (least- to most-severe) hysteresis bands keyed on failure ratio
    * @param insufficientDataRateGrowthFactor
    *   rate multiplier (`> 1`) while the window doesn't have enough samples, i.e. samples are below
    *   `minNumberOfMeasurements` (window doesn't know the failure rate). Similar to TCP-style slow-start, the estimated
    *   rate is grown by this factor to quickly re-initialize measurements
    */
  final case class Config(
      capacity: Int,
      initialRate: Rate,
      minRate: Rate,
      maxRate: Rate,
      rateIncreaseBy: Rate,
      rateIncreasePeriod: FiniteDuration,
      rateDecreaseBy: Double,
      numberOfSlotsForMeasurements: Int,
      slotDuration: FiniteDuration,
      measurementPeriod: FiniteDuration,
      minNumberOfMeasurements: Int,
      failureLevels: NonEmptyList[HysteresisBand],
      insufficientDataRateGrowthFactor: Double = Config.DefaultSlowStartGrowthFactor
  ) {

    val rateLimiterConfig =
      RateLimiter.Config(capacity = capacity, refillRate = initialRate)

    val approximateFailureRatesConfig = ApproximateFailureRates.Config(
      numberOfSlotsForMeasurements = numberOfSlotsForMeasurements,
      slotDuration = slotDuration,
      measurementPeriod = measurementPeriod,
      minNumberOfMeasurements = minNumberOfMeasurements
    )

    val failureRateCategorizerConfig = FailureRateCategorizer.Config(
      failureLevels = failureLevels
    )

    val aimdRateControllerConfig = AimdRateController.Config(
      initialRate = initialRate,
      minRate = minRate,
      maxRate = maxRate,
      rateIncreaseBy = AimdRateController.AimdRateIncrease(
        rate = rateIncreaseBy,
        tickInterval = rateIncreasePeriod
      ),
      rateDecreaseBy = rateDecreaseBy,
      insufficientDataRateGrowthFactor = insufficientDataRateGrowthFactor
    )
  }

  object Config {

    val DefaultSlowStartGrowthFactor: Double = 2.0

    /** Convenience builder for RPS-shaped configurations. Capacity is set to `maxRps`, the bucket window covers
      * `timeRangeForMeasurementInSeconds` one-second slots, and `minNumberOfMeasurements` equals the slot count.
      *
      * @see
      *   [[AdaptiveRateLimiter.Config]] for parameter descriptions.
      */
    def fromRps(
        minRps: Int,
        maxRps: Int,
        rpsIncreaseRate: Rate,
        rpsDecrease: Double,
        timeRangeForMeasurementInSeconds: Int,
        failureLevels: NonEmptyList[HysteresisBand],
        slowStartGrowthFactor: Double = DefaultSlowStartGrowthFactor
    ): Config =
      Config(
        capacity = maxRps,
        initialRate = Rate(requests = maxRps, period = 1.second),
        minRate = Rate(requests = minRps, period = 1.second),
        maxRate = Rate(requests = maxRps, period = 1.second),
        rateIncreaseBy = rpsIncreaseRate,
        rateIncreasePeriod = rpsIncreaseRate.period,
        rateDecreaseBy = rpsDecrease,
        numberOfSlotsForMeasurements = timeRangeForMeasurementInSeconds,
        slotDuration = 1.second,
        measurementPeriod = 1.second,
        minNumberOfMeasurements = timeRangeForMeasurementInSeconds,
        failureLevels = failureLevels,
        insufficientDataRateGrowthFactor = slowStartGrowthFactor
      )
  }

  /** Control-loop events emitted by the failure-rate categorizer. */
  sealed trait FailureGradient

  object FailureGradient {

    final case class Worsening(toLevel: Int) extends FailureGradient

    final case class Recovering(fromLevel: Int) extends FailureGradient

    case object Recovered extends FailureGradient
  }

  /** A no-op limiter that always allows consumption, never records outcomes, and reports a fixed sustainable rate. */
  def noop[F[_]: Applicative](rate: Rate = Rate(requests = 1, period = 1.second)): AdaptiveRateLimiter[F] =
    new NoopAdaptiveRateLimiter[F](configuredRate = rate)

  /** A test double that counts `recordSuccess` and `recordFailure` invocations and delegates `consume` to the supplied
    * effect.
    */
  def recording[F[_]: Sync](
      canConsume: F[Boolean],
      rate: Rate = Rate(requests = 1, period = 1.second)
  ): F[RecordingAdaptiveRateLimiter[F]] =
    (Ref[F].of(0), Ref[F].of(0)).mapN { (successes, failures) =>
      new RecordingAdaptiveRateLimiter[F](
        rate = rate,
        canConsume = canConsume,
        successes = successes,
        failures = failures
      )
    }

  def start[F[_]: Async](config: Config): Resource[F, AdaptiveRateLimiter[F]] =
    start[F](
      config = config,
      onFailureCategoryChange = (_: FailureGradient) => Async[F].unit,
      onRateChange = (_: Rate) => Async[F].unit
    )

  /** @param onFailureCategoryChange
    *   callback fired on each [[FailureGradient]] event (per band crossed on worsening or recovery)
    * @param onRateChange
    *   callback fired whenever the AIMD updates its estimated rate for the protected sink
    */
  def start[F[_]: Async](
      config: Config,
      onFailureCategoryChange: FailureGradient => F[Unit],
      onRateChange: Rate => F[Unit]
  ): Resource[F, AdaptiveRateLimiter[F]] =
    for {
      rateLimiter                  <- Resource.eval { RateLimiter.Dynamic[F](config = config.rateLimiterConfig) }
      failureRatesProducerConsumer <- Resource.eval {
        ApproximateFailureRates.createProducerAndConsumer(config = config.approximateFailureRatesConfig)
      }
      (measurements, failureRates) = failureRatesProducerConsumer
      failureRateCategorizer <- Resource.eval {
        FailureRateCategorizer[F](config = config.failureRateCategorizerConfig)
      }
      aimdRateController <- Resource.eval { AimdRateController[F](config = config.aimdRateControllerConfig) }

      adaptiveRateLimiter = new DefaultAdaptiveRateLimiter[F](rateLimiter = rateLimiter, measurements = measurements)

      _ <- Spawn[F].background {
        failureRates
          .evalTap(_.traverse_(adaptiveRateLimiter.setFailureRatio))
          .through(failureRateCategorizer)
          .evalTap(_.traverse_(onFailureCategoryChange))
          .through(aimdRateController)
          .evalTap(onRateChange)
          .evalMap(rateLimiter.setRefillRate)
          .compile
          .drain
      }
    } yield adaptiveRateLimiter

  private[adaptiveratelimiter] object ApproximateFailureRates {
    final case class Config(
        numberOfSlotsForMeasurements: Int,
        slotDuration: FiniteDuration,
        measurementPeriod: FiniteDuration,
        minNumberOfMeasurements: Int
    ) {

      def measurementWindow: FiniteDuration = slotDuration * numberOfSlotsForMeasurements

      def validate: Either[String, Unit] =
        for {
          _ <- check(numberOfSlotsForMeasurements > 0, "slots > 0")
          _ <- check(slotDuration > Duration.Zero, "slot duration > 0ms")
          _ <- check(measurementPeriod > Duration.Zero, "measurements period > 0ms")
          _ <- check(minNumberOfMeasurements > 0, "min measurements > 0")
        } yield ()
    }

    /** @return
      *   failureRates; `None` means the window held too few samples
      */
    private[adaptiveratelimiter] def createProducerAndConsumer[F[_]: Async](
        config: Config
    ): F[(SampledMeasurements[F], fs2.Stream[F, Option[Double]])] =
      for {
        _            <- ApplicativeThrow[F].fromEither(config.validate.leftMap(new IllegalArgumentException(_)))
        measurements <- Measurements.sampledTimeBasedSlidingWindow[F](
          numberOfBuckets = config.numberOfSlotsForMeasurements,
          bucketSize = config.slotDuration,
          minNumberOfCalls = config.minNumberOfMeasurements
        )
      } yield (
        measurements,
        fs2.Stream
          .awakeEvery[F](period = config.measurementPeriod)
          .evalMap(_ => measurements.sample)
          .map(_.failureRate)
      )

  }

  private[adaptiveratelimiter] object FailureRateCategorizer {

    private val NoChange = Chunk.empty[Option[FailureGradient]]

    private sealed abstract class FailureState extends Product with Serializable {
      def level: Int
    }

    private object FailureState {

      def apply(level: Int): FailureState =
        if (level == Healthy.level) Healthy
        else Failing(level)

      case object Healthy extends FailureState {
        override val level: Int = -1 // aligns with not-found index from `array.lastIndexWhere`
      }

      final case class Failing(level: Int) extends FailureState
    }

    final case class Config(failureLevels: NonEmptyList[HysteresisBand]) {
      def validate: Either[String, Unit] =
        for {
          _ <- failureLevels.toList.zipWithIndex.traverse_ { case (band, idx) =>
            band.validate.leftMap(s => s"band[$idx]: $s")
          }
          _ <- failureLevels.toList
            .sliding(2)
            .collect { case Seq(a, b) => (a, b) }
            .toList
            .traverse_ { case (a, b) =>
              for {
                _ <- check(a.start < b.start, "bands must have monotonically increasing start")
                _ <- check(a.exit < b.exit, "bands must have monotonically increasing exit")
                _ <- check(a.start < b.exit, "bands must not overlap")
              } yield ()
            }
        } yield ()
    }

    def apply[F[_]: ApplicativeThrow](config: Config): F[Pipe[F, Option[Double], Option[FailureGradient]]] =
      for {
        _ <- ApplicativeThrow[F].fromEither(config.validate.leftMap(new IllegalArgumentException(_)))
      } yield failureRateCategorizer(bands = config.failureLevels)

    private def failureRateCategorizer[F[_]](
        bands: NonEmptyList[HysteresisBand]
    ): Pipe[F, Option[Double], Option[FailureGradient]] = {
      /*
      Bands are ordered least-severe first (band 0 is the least severe). Hysteresis prevents flapping: a band engages
      at `start` and releases at `exit`. Every state transition emits one `Some(FailureGradient)` per band crossed.
      A `None` input (too few samples to report a ratio) is passed through unchanged as `None` without advancing the
      failure state: we cannot categorize, so we hold the last known state.
       */
      val bandsArr = bands.toList.toArray // already sorted in validation above

      def worsening(currentLevel: Int, nextLevel: Int): Chunk[Option[FailureGradient]] =
        Chunk.from(((currentLevel + 1) to nextLevel).map(FailureGradient.Worsening(_).some))

      def recovering(currentLevel: Int, nextLevel: Int): Chunk[Option[FailureGradient]] =
        Chunk.from((currentLevel until nextLevel by -1).map {
          // Fully releasing band 0 means the backend is healthy again.
          case 0     => FailureGradient.Recovered.some
          case level => FailureGradient.Recovering(fromLevel = level).some
        })

      _.scan((FailureState.Healthy: FailureState, NoChange)) {
        case ((current, _), None) =>
          (current, Chunk.singleton(none[FailureGradient]))

        case ((current, _), Some(failureRate)) =>
          val currentLevel = current.level

          // Start thresholds engage bands; exit thresholds keep already-engaged bands retained during recovery.
          val worseningTo  = bandsArr.lastIndexWhere(band => band.start <= failureRate)
          val recoveringTo = bandsArr.lastIndexWhere(band => band.exit < failureRate)

          if (worseningTo > currentLevel)
            (
              FailureState(level = worseningTo),
              worsening(currentLevel = currentLevel, nextLevel = worseningTo)
            )
          // If the current level is no longer retained by its exit threshold, emit one recovery event per released band.
          else if (recoveringTo < currentLevel)
            (
              FailureState(level = recoveringTo),
              recovering(currentLevel = currentLevel, nextLevel = recoveringTo)
            )
          else
            (current, NoChange)
      }.collect { case (_, gradients) => gradients }.unchunks
    }

  }

  private[adaptiveratelimiter] object AimdRateController {

    final case class AimdRateIncrease(rate: Rate, tickInterval: FiniteDuration)

    final case class Config(
        initialRate: Rate,
        minRate: Rate,
        maxRate: Rate,
        rateIncreaseBy: AimdRateIncrease,
        rateDecreaseBy: Double,
        insufficientDataRateGrowthFactor: Double = 2.0
    )

    private case object Tick

    /** @param rate
      *   current estimated rate
      * @param insufficientDataThreshold
      *   the last pre-decrease rate above which slow-start stops growing exponentially (`ssthresh` in TCP), i.e. The
      *   last "good" rate where measurements produce enough samples.
      */
    private final case class State(rate: Rate, insufficientDataThreshold: Rate)

    private[adaptiveratelimiter] def apply[F[_]: Temporal](
        config: AimdRateController.Config
    ): F[Pipe[F, Option[FailureGradient], Rate]] =
      ApplicativeThrow[F]
        .fromEither {
          import config._
          (for {
            _ <- check(initialRate =!= Rate.Zero, "initialRate must be nonzero")
            _ <- check(minRate =!= Rate.Zero, "minRate must be nonzero")
            _ <- check(maxRate =!= Rate.Zero, "maxRate must be nonzero")
            _ <- check(rateIncreaseBy.rate =!= Rate.Zero, "rateIncreaseBy.rate must be nonzero")
            _ <- rateIncreaseBy.rate.validate.leftMap(_.getMessage)
            _ <- check(rateIncreaseBy.tickInterval > Duration.Zero, "rateIncreaseBy.tickInterval > 0ms")
            _ <- check(minRate <= initialRate && initialRate <= maxRate, "min rate <= initial rate <= max rate")
            _ <- check(rateDecreaseBy >= 0.0 && rateDecreaseBy <= 1.0, "0 <= rateDecreaseBy <= 1")
            _ <- check(insufficientDataRateGrowthFactor > 1.0, "insufficientDataRateGrowthFactor > 1")
          } yield ())
            .leftMap(new IllegalArgumentException(_))
        }
        .as {
          val multiplicativeDecrease =
            (BigDecimal.valueOf(1.0) - BigDecimal.valueOf(config.rateDecreaseBy)).toDouble

          aimdRateController[F](
            initialRate = config.initialRate,
            minRate = config.minRate,
            maxRate = config.maxRate,
            rateIncreaseBy = config.rateIncreaseBy,
            multiplicativeDecrease = multiplicativeDecrease,
            slowStartGrowthFactor = config.insufficientDataRateGrowthFactor
          )
        }

    private def aimdRateController[F[_]: Temporal](
        initialRate: Rate,
        minRate: Rate,
        maxRate: Rate,
        rateIncreaseBy: AimdRateIncrease,
        multiplicativeDecrease: Double,
        slowStartGrowthFactor: Double
    ): Pipe[F, Option[FailureGradient], Rate] = { failureSignals =>
      val ticks =
        fs2.Stream
          .awakeEvery[F](period = rateIncreaseBy.tickInterval)
          .as(Tick.asLeft[Option[FailureGradient]])

      failureSignals
        .map(_.asRight[Tick.type])
        .merge(ticks)
        // insufficientDataThreshold starts high (maxRate) so the initial slow start can climb the full range,
        // mirroring TCP's ssthresh settings.
        .scan(State(rate = initialRate, insufficientDataThreshold = maxRate)) {
          case (state, Right(Some(FailureGradient.Worsening(_)))) =>
            // Multiplicative Decrease: Drop the rate
            State(
              rate = state.rate.scaleBy(multiplicativeDecrease).max(minRate),
              insufficientDataThreshold = state.rate
            )
          case (state, Right(Some(_))) =>
            state
          case (state, Right(None)) =>
            // Slow Start: Discover the rate when not enough samples, until the last known rate which produced samples to avoid overshooting.
            state.copy(rate =
              state.rate.scaleBy(slowStartGrowthFactor).min(state.insufficientDataThreshold).min(maxRate)
            )
          case (state, Left(Tick)) =>
            // Additive Increase: Grow the rate
            state.copy(rate = (state.rate + rateIncreaseBy.rate).min(maxRate))
        }
        .map(_.rate)
        .changes
    }
  }

  private def check(cond: Boolean, msg: String): Either[String, Unit] =
    Either.cond(cond, (), msg)

  @SuppressWarnings(Array("org.wartremover.warts.Var", "DisableSyntax.var"))
  private final class DefaultAdaptiveRateLimiter[F[_]: Sync](
      rateLimiter: DynamicRateLimiter[F],
      measurements: SampledMeasurements[F]
  ) extends AdaptiveRateLimiter[F] {

    @volatile private var _failureRatio: Double = 0.0

    override def consume: F[Boolean] = rateLimiter.consume()

    override def recordSuccess: F[Unit] = measurements.recordSuccess

    override def recordFailure: F[Unit] = measurements.recordFailure

    override def rate: F[Rate] = rateLimiter.refillRate

    override def failureRatio: F[Double] = Sync[F].delay(_failureRatio)

    def setFailureRatio(newValue: Double): F[Unit] = Sync[F].delay {
      _failureRatio = newValue
    }
  }
}
