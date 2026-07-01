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
      failureLevels: NonEmptyList[HysteresisBand]
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
      rateDecreaseBy = rateDecreaseBy
    )
  }

  object Config {

    /** Convenience builder for RPS-shaped configurations. Capacity is set to `maxRps`, the bucket window covers
      * `timeRangeForMeasurementInSeconds` one-second slots, and `minNumberOfMeasurements` equals the slot count.
      */
    def fromRps(
        minRps: Int,
        maxRps: Int,
        rpsIncreaseRate: Rate,
        rpsDecrease: Double,
        timeRangeForMeasurementInSeconds: Int,
        failureLevels: NonEmptyList[HysteresisBand]
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
        failureLevels = failureLevels
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
      onRateChange = (_: Rate) => Async[F].unit,
      onError = (_: Throwable) => Async[F].unit
    )

  /** @param onFailureCategoryChange
    *   callback fired on each [[FailureGradient]] event (per band crossed on worsening or recovery)
    * @param onRateChange
    *   callback fired whenever the AIMD updates its estimated rate for the protected sink
    * @param onError
    *   callback fired when the AIMD control loop stream encounters an error; the stream restarts automatically after
    *   invoking this callback
    */
  def start[F[_]: Async](
      config: Config,
      onFailureCategoryChange: FailureGradient => F[Unit],
      onRateChange: Rate => F[Unit],
      onError: Throwable => F[Unit]
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

      stream = failureRates
        .evalTap(adaptiveRateLimiter.setFailureRatio)
        .through(failureRateCategorizer)
        .evalTap(onFailureCategoryChange)
        .through(aimdRateController)
        .evalTap(onRateChange)
        .evalMap(rateLimiter.setRefillRate)
        .compile
        .drain

      _ <- Spawn[F].background {
        stream.handleErrorWith(e => onError(e).attempt.void *> stream)
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

    private[adaptiveratelimiter] def createProducerAndConsumer[F[_]: Async](
        config: Config
    ): F[(SampledMeasurements[F], fs2.Stream[F, Double])] =
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
          .unNone
      )

  }

  private[adaptiveratelimiter] object FailureRateCategorizer {

    private val NoChange = Chunk.empty[FailureGradient]

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

    def apply[F[_]: ApplicativeThrow](config: Config): F[Pipe[F, Double, FailureGradient]] =
      for {
        _ <- ApplicativeThrow[F].fromEither(config.validate.leftMap(new IllegalArgumentException(_)))
      } yield failureRateCategorizer(bands = config.failureLevels)

    private def failureRateCategorizer[F[_]](
        bands: NonEmptyList[HysteresisBand]
    ): Pipe[F, Double, FailureGradient] = {
      /*
      Bands are ordered least-severe first (band 0 is the least severe). Hysteresis prevents flapping: a band engages
      at `start` and releases at `exit`. Every state transition emits one [[FailureGradient]] event per band crossed.
       */
      val bandsArr = bands.toList.toArray // already sorted in validation above

      def worsening(currentLevel: Int, nextLevel: Int): Chunk[FailureGradient] =
        Chunk.from(((currentLevel + 1) to nextLevel).map(FailureGradient.Worsening(_)))

      def recovering(currentLevel: Int, nextLevel: Int): Chunk[FailureGradient] =
        Chunk.from((currentLevel until nextLevel by -1).map {
          // Fully releasing band 0 means the backend is healthy again.
          case 0     => FailureGradient.Recovered
          case level => FailureGradient.Recovering(fromLevel = level)
        })

      _.scan((FailureState.Healthy: FailureState, NoChange)) { case ((current, _), failureRate) =>
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
        rateDecreaseBy: Double
    )

    private case object Tick

    private[adaptiveratelimiter] def apply[F[_]: Temporal](
        config: AimdRateController.Config
    ): F[Pipe[F, FailureGradient, Rate]] =
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
            multiplicativeDecrease = multiplicativeDecrease
          )
        }

    private def aimdRateController[F[_]: Temporal](
        initialRate: Rate,
        minRate: Rate,
        maxRate: Rate,
        rateIncreaseBy: AimdRateIncrease,
        multiplicativeDecrease: Double
    ): Pipe[F, FailureGradient, Rate] = { failureSignals =>
      val ticks =
        fs2.Stream
          .awakeEvery[F](period = rateIncreaseBy.tickInterval)
          .map(_ => Tick.asLeft[FailureGradient])

      failureSignals
        .map(_.asRight[Tick.type])
        .merge(ticks)
        .scan(initialRate) {
          case (rate, Right(FailureGradient.Worsening(_))) =>
            rate.scaleBy(multiplicativeDecrease).max(minRate)
          case (rate, Right(_)) =>
            rate
          case (rate, Left(Tick)) =>
            Either
              .catchOnly[IllegalArgumentException]((rate + rateIncreaseBy.rate).min(maxRate))
              .getOrElse((rate.normalizedTo(rateIncreaseBy.rate.period) + rateIncreaseBy.rate).min(maxRate))
        }
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
