package io.mienks.resilience.adaptiveratelimiter

import cats.data.NonEmptyList
import cats.effect.std.Queue
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._
import io.mienks.resilience.Rate
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.FailureGradient._
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.{Config, FailureGradient, HysteresisBand}
import munit.CatsEffectSuite

import scala.concurrent.duration._

private final case class LimiterWithEffects(
    limiter: AdaptiveRateLimiter[IO],
    categoryChanges: Queue[IO, FailureGradient],
    minObservedRate: Ref[IO, Rate]
)

class AdaptiveRateLimiterTests extends CatsEffectSuite {

  private val Bands: NonEmptyList[HysteresisBand] = NonEmptyList.of(
    HysteresisBand(exit = 0.1, start = 0.3), // level 0: minor degradation
    HysteresisBand(exit = 0.4, start = 0.7)  // level 1: severe degradation
  )

  private val MeasurementPeriod: FiniteDuration = 10.millis
  private val MeasurementBuckets: Int           = 4
  private val MeasurementWindow: FiniteDuration = MeasurementPeriod * MeasurementBuckets
  private val MinNumberOfMeasurements: Int      = 5
  // Keep Larger than `minNumberOfMeasurements` so the window is initialized, and failure ratio is whole number.
  private val BackendHitsPerRound: Int = MinNumberOfMeasurements * 4 // 20

  private val Capacity: Int     = 10
  private val InitialRate: Rate = Rate(requests = 2, period = 1.second)
  private val MinRate: Rate     = Rate(requests = 1, period = 1.second)
  private val MaxRate: Rate     = Rate(requests = 10, period = 1.second)

  // Coarse additive step so a healthy backend climbs from initial to max in a handful of ticks.
  private val RateIncreaseBy: Rate               = Rate(requests = 2, period = 1.second)
  private val RateIncreasePeriod: FiniteDuration = 20.millis

  private val BaseConfig: Config =
    Config(
      capacity = Capacity,
      initialRate = InitialRate,
      minRate = MinRate,
      maxRate = MaxRate,
      rateIncreaseBy = RateIncreaseBy,
      rateIncreasePeriod = RateIncreasePeriod,
      rateDecreaseBy = 0.5,
      numberOfSlotsForMeasurements = MeasurementBuckets,
      slotDuration = MeasurementPeriod,
      measurementPeriod = MeasurementPeriod,
      minNumberOfMeasurements = MeasurementBuckets,
      failureLevels = Bands
    )

  test("no traffic: noop") {
    startLimiter().use { case LimiterWithEffects(limiter, categoryChanges, minObservedRate) =>
      for {
        _                  <- IO.sleep(MeasurementPeriod * 3)
        ratio              <- limiter.failureRatio
        _                  <- IO(assertEquals(ratio, 0.0))
        category           <- categoryChanges.tryTake
        _                  <- IO(assertEquals(category, none[FailureGradient]))
        lowestObservedRate <- minObservedRate.get
        _                  <- IO(assert(lowestObservedRate === InitialRate, clue = lowestObservedRate))
      } yield ()
    }
  }

  test("healthy backend: rate climbs to max and throttles at that level") {
    require(BaseConfig.maxRate > BaseConfig.initialRate)
    startLimiter().use { case LimiterWithEffects(limiter, categoryChanges, minObservedRate) =>
      startHittingBackend(limiter, initialFailureRatio = 0.0).use { _ =>
        for {
          _                  <- assertRateConverges(limiter, MaxRate)
          category           <- categoryChanges.tryTake
          _                  <- IO(assertEquals(category, none[FailureGradient]))
          lowestObservedRate <- minObservedRate.get
          _                  <- IO(assert(lowestObservedRate === InitialRate, clue = lowestObservedRate))
          admitted           <- limiter.consume.replicateA(Capacity)
          _                  <- IO(assert(admitted.forall(identity), clue = s"expected to admit a burst of $Capacity"))
          denied             <- limiter.consume
          _                  <- IO(assert(!denied, clue = "further requests are throttled once the burst is exhausted"))
        } yield ()
      }
    }
  }

  test("flaky-but-healthy backend: errors oscillating below the first band never trips a decrease") {
    startLimiter().use { case LimiterWithEffects(limiter, categoryChanges, minObservedRate) =>
      startHittingBackend(limiter, initialFailureRatio = 0.2).use { ratioRef =>
        for {
          _                  <- hold(ratioRef, ratio = 0.2, duration = MeasurementWindow * 2)
          _                  <- hold(ratioRef, ratio = 0.1, duration = MeasurementWindow * 2)
          _                  <- hold(ratioRef, ratio = 0.2, duration = MeasurementWindow * 2)
          _                  <- assertRateConverges(limiter, MaxRate)
          category           <- categoryChanges.tryTake
          _                  <- IO(assertEquals(category, none[FailureGradient]))
          lowestObservedRate <- minObservedRate.get
          _                  <- IO(assert(lowestObservedRate === InitialRate, clue = lowestObservedRate))
        } yield ()
      }
    }
  }

  test("quick degradation: a spike promotes through both bands and cuts the rate") {
    startLimiter().use { case LimiterWithEffects(limiter, categoryChanges, minObservedRate) =>
      startHittingBackend(limiter, initialFailureRatio = 0.0).use { ratioRef =>
        for {
          _ <- assertRateConverges(limiter, MaxRate)
          _ <- ratioRef.set(0.8)
          _ <- categoryChanges.take.map(event => assertEquals(event, Worsening(toLevel = 0)))
          _ <- categoryChanges.take.map(event => assertEquals(event, Worsening(toLevel = 1)))
          _ <- poll(
            minObservedRate.get.map { rate =>
              assert(rate < Rate(requests = 3, period = 1.second), clue = rate)
              assert(rate > MinRate, clue = rate)
            }
          )
        } yield ()
      }
    }
  }

  test("degrade then recover: a recovered backend returns to Healthy and the rate climbs back to max") {
    startLimiter().use { case LimiterWithEffects(limiter, categoryChanges, _) =>
      startHittingBackend(limiter, initialFailureRatio = 0.0).use { ratioRef =>
        for {
          _ <- assertRateConverges(limiter, MaxRate)
          // degrade
          _ <- ratioRef.set(0.8)
          _ <- categoryChanges.take.map(event => assertEquals(event, Worsening(toLevel = 0)))
          _ <- categoryChanges.take.map(event => assertEquals(event, Worsening(toLevel = 1)))
          // recover
          _ <- ratioRef.set(0.0)
          _ <- categoryChanges.take.map(event => assertEquals(event, Recovering(fromLevel = 1)))
          _ <- categoryChanges.take.map(event => assertEquals(event, Recovered))
          _ <- assertRateConverges(limiter, MaxRate)
        } yield ()
      }
    }
  }

  test("slow recovery: errors clearing gradually hold the failing tier until fully below the exit band") {
    startLimiter().use { case LimiterWithEffects(limiter, categoryChanges, minObservedRate) =>
      startHittingBackend(limiter, initialFailureRatio = 0.0).use { ratioRef =>
        for {
          _ <- assertRateConverges(limiter, MaxRate)
          // a spike straight to the severe tier
          _ <- ratioRef.set(0.8)
          _ <- categoryChanges.take.map(event => assertEquals(event, Worsening(toLevel = 0)))
          _ <- categoryChanges.take.map(event => assertEquals(event, Worsening(toLevel = 1)))
          _ <- poll(minObservedRate.get.map(rate => assert(rate < Rate(requests = 3, period = 1.second), clue = rate)))
          rateAfterCut <- minObservedRate.get
          // recover gradually, lingering inside the hysteresis bands (above each exit)
          _                  <- hold(ratioRef, ratio = 0.5, duration = MeasurementWindow * 2) // still within band 1
          _                  <- hold(ratioRef, ratio = 0.2, duration = MeasurementWindow * 2) // demotes to band 0
          _                  <- categoryChanges.take.map(event => assertEquals(event, Recovering(fromLevel = 1)))
          rateWhileLingering <- minObservedRate.get
          _ <- IO(assert(rateWhileLingering === rateAfterCut, clue = "no fresh decrease is applied while recovering"))
          // only a full clear below the exit band returns to Recovered
          _ <- ratioRef.set(0.0)
          _ <- categoryChanges.take.map(event => assertEquals(event, Recovered))
          _ <- assertRateConverges(limiter, MaxRate)
        } yield ()
      }
    }
  }

  test("slow degradation: rising errors trip the bands one tier at a time") {
    // Slow the additive recovery so the first tier's cut is still visible when the second tier trips.
    val config = BaseConfig.copy(initialRate = MaxRate, rateIncreasePeriod = 1.second)

    startLimiter(config).use { case LimiterWithEffects(limiter, categoryChanges, minObservedRate) =>
      startHittingBackend(limiter, initialFailureRatio = 0.0).use { ratioRef =>
        for {
          _ <- assertRateConverges(limiter, MaxRate)
          // moderate degradation trips only the first tier
          _            <- ratioRef.set(0.4)
          _            <- categoryChanges.take.map(event => assertEquals(event, Worsening(toLevel = 0)))
          _            <- IO.sleep(MeasurementWindow * 2)
          noEscalation <- categoryChanges.tryTake
          _            <- IO(
            assertEquals(noEscalation, none[FailureGradient], clue = "moderate errors do not trip the severe tier")
          )
          _ <- poll(
            minObservedRate.get.map(rate => assert(rate === Rate(requests = 5, period = 1.second), clue = rate))
          )
          rateAfterFirstCut <- minObservedRate.get
          // severe degradation trips the second tier and compounds the rate cut
          _ <- ratioRef.set(0.8)
          _ <- categoryChanges.take.map(event => assertEquals(event, Worsening(toLevel = 1)))
          _ <- poll(minObservedRate.get.map(rate => assert(rate < rateAfterFirstCut, clue = rate)))
          _ <- minObservedRate.get.map(rate => assert(rate > MinRate, clue = rate))
        } yield ()
      }
    }
  }

  test("flapping backend: repeated degrade/recover cycles ratchet the rate down to the min floor") {
    // Slow the additive recovery so the rate barely climbs between flaps; compounding cuts then reach the floor.
    val config = BaseConfig.copy(initialRate = MaxRate, rateIncreasePeriod = 1.second)

    startLimiter(config).use { case LimiterWithEffects(limiter, _, minObservedRate) =>
      startHittingBackend(limiter, initialFailureRatio = 0.0).use { ratioRef =>
        val flap =
          hold(ratioRef, ratio = 0.8, duration = MeasurementWindow * 2) >>
            hold(ratioRef, ratio = 0.0, duration = MeasurementWindow * 2)

        for {
          _ <- flap.replicateA_(4)
          _ <- poll(minObservedRate.get.map(rate => assert(rate === MinRate, clue = rate)))
        } yield ()
      }
    }
  }

  test("partial recovery: new decreases apply when re-degrading without returning to healthy") {
    // Initial rate should be at least 2^6 larger than minRate, as there are multiple rate decreases (halvings).
    val config = BaseConfig.copy(
      initialRate = Rate(requests = 64, period = 1.second),
      minRate = Rate(requests = 1, period = 1.second),
      maxRate = Rate(requests = 64, period = 1.second),
      rateIncreasePeriod = 1.second,
      // Three bands so re-climbing from band 0 passes through Failing(1) (which differs from the last-emitted
      // Failing(2)) and survives the categorizer's consecutive-duplicate suppression. With only two bands a
      // Failing(1) -> Failing(0) -> Failing(1) round-trip would re-emit the same Failing(1) and be deduped away.
      failureLevels = NonEmptyList.of(
        HysteresisBand(exit = 0.1, start = 0.3), // level 0
        HysteresisBand(exit = 0.4, start = 0.6), // level 1
        HysteresisBand(exit = 0.7, start = 0.9)  // level 2
      )
    )

    startLimiter(config).use { case LimiterWithEffects(limiter, categoryChanges, minObservedRate) =>
      startHittingBackend(limiter, initialFailureRatio = 0.0).use { ratioRef =>
        for {
          _ <- assertRateConverges(limiter, config.initialRate)
          // spike straight to the most severe tier: promotes through every band and cuts three times
          _ <- ratioRef.set(0.95)
          _ <- categoryChanges.take.map(event => assertEquals(event, Worsening(toLevel = 0)))
          _ <- categoryChanges.take.map(event => assertEquals(event, Worsening(toLevel = 1)))
          _ <- categoryChanges.take.map(event => assertEquals(event, Worsening(toLevel = 2)))
          _ <- poll(minObservedRate.get.map(rate => assert(rate < Rate(requests = 16, period = 1.second), clue = rate)))
          rateAfterFirstCuts <- minObservedRate.get
          // partial recovery into band 0 (Failing(2) -> Failing(0))
          _ <- hold(ratioRef, ratio = 0.2, duration = MeasurementWindow * 2)
          _ <- categoryChanges.take.map(event => assertEquals(event, Recovering(fromLevel = 2)))
          _ <- categoryChanges.take.map(event => assertEquals(event, Recovering(fromLevel = 1)))
          // re-degrade: re-emits Worsening(1) and compounds the cut
          _ <- ratioRef.set(0.95)
          _ <- categoryChanges.take.map(event => assertEquals(event, Worsening(toLevel = 1)))
          _ <- poll(minObservedRate.get.map(rate => assert(rate < rateAfterFirstCuts, clue = rate)))
          _ <- minObservedRate.get.map(rate => assert(rate > MinRate, clue = rate))
        } yield ()
      }
    }
  }

  test("not enough measurements: failures below the window minimum never trip a decrease") {
    startLimiter().use { case LimiterWithEffects(limiter, categoryChanges, minObservedRate) =>
      for {
        // all errors, but fewer than the window minimum, so the window never initializes
        _                  <- limiter.recordFailure.replicateA_(BaseConfig.minNumberOfMeasurements - 1)
        _                  <- IO.sleep(MeasurementWindow)
        ratio              <- limiter.failureRatio
        _                  <- IO(assertEquals(ratio, 0.0))
        category           <- categoryChanges.tryTake
        _                  <- IO(assertEquals(category, none[FailureGradient]))
        lowestObservedRate <- minObservedRate.get
        _                  <- IO(assert(lowestObservedRate === InitialRate, clue = lowestObservedRate))
      } yield ()
    }
  }

  test("failureRatio reflects the sampled failure ratio of the backend") {
    startLimiter().use { case LimiterWithEffects(limiter, _, _) =>
      startHittingBackend(limiter, initialFailureRatio = 0.0).use { ratioRef =>
        for {
          _ <- poll(limiter.failureRatio.map(ratio => assert(ratio <= 0.05, clue = ratio)))
          _ <- ratioRef.set(0.8)
          _ <- poll(limiter.failureRatio.map(ratio => assert(math.abs(ratio - 0.8) <= 0.1, clue = ratio)))
          _ <- ratioRef.set(0.0)
          _ <- poll(limiter.failureRatio.map(ratio => assert(ratio <= 0.05, clue = ratio)))
        } yield ()
      }
    }
  }

  test("onError is invoked and the stream restarts when the control loop encounters an error") {
    for {
      errors    <- Ref[IO].of(List.empty[Throwable])
      callCount <- Ref[IO].of(0)
      _         <- AdaptiveRateLimiter
        .start[IO](
          config = BaseConfig,
          onFailureCategoryChange = (_: FailureGradient) =>
            callCount.updateAndGet(_ + 1).flatMap {
              case 1 => IO.raiseError(new RuntimeException("boom"))
              case _ => IO.unit
            },
          onRateChange = (_: Rate) => IO.unit,
          onError = (e: Throwable) => errors.update(e :: _)
        )
        .use { limiter =>
          startHittingBackend(limiter, initialFailureRatio = 0.8).use { ratioRef =>
            for {
              // Wait for the first category change to throw and trigger onError
              _ <- poll(errors.get.map(es => assert(es.nonEmpty, clue = "onError was never called")))
              // Switch to healthy traffic; the stream must still be running to observe recovery
              _ <- ratioRef.set(0.0)
              _ <- assertRateConverges(limiter, MaxRate)
            } yield ()
          }
        }
    } yield ()
  }

  private def startLimiter(config: Config = BaseConfig): Resource[IO, LimiterWithEffects] =
    for {
      categoryChanges <- Resource.eval(Queue.unbounded[IO, FailureGradient])
      minObservedRate <- Resource.eval(Ref[IO].of(config.initialRate))
      limiter         <- AdaptiveRateLimiter.start[IO](
        config = config,
        onFailureCategoryChange = (event: FailureGradient) => categoryChanges.offer(event),
        onRateChange = (rate: Rate) => minObservedRate.update(current => if (rate < current) rate else current),
        onError = (_: Throwable) => IO.unit
      )
    } yield LimiterWithEffects(limiter = limiter, categoryChanges = categoryChanges, minObservedRate = minObservedRate)

  /** A background process that simulates the protected backend: every measurement period it records
    * `BackendHitsPerRound` outcomes at the failure ratio currently held in the returned `Ref`. Mutating that `Ref`
    * switches the backend's health over the lifetime of the resource.
    */
  private def startHittingBackend(
      limiter: AdaptiveRateLimiter[IO],
      initialFailureRatio: Double
  ): Resource[IO, Ref[IO, Double]] =
    Resource.eval(Ref[IO].of(initialFailureRatio)).flatTap { ratioRef =>
      (ratioRef.get.flatMap { ratio =>
        val failures = math.round(BackendHitsPerRound * ratio).toInt
        limiter.recordFailure.replicateA_(failures) >>
          limiter.recordSuccess.replicateA_(BackendHitsPerRound - failures)
      } >> IO.sleep(MeasurementPeriod)).foreverM.background
    }

  private def hold(ratioRef: Ref[IO, Double], ratio: Double, duration: FiniteDuration): IO[Unit] =
    ratioRef.set(ratio) >> IO.sleep(duration)

  private def assertRateConverges(limiter: AdaptiveRateLimiter[IO], expected: Rate): IO[Unit] =
    poll(limiter.rate.map(rate => assert(rate === expected, clue = rate)))

  private def poll(check: IO[Unit]): IO[Unit] = {
    val ConvergenceTimeout: FiniteDuration = 2.seconds
    val PollInterval: FiniteDuration       = 5.millis

    IO.monotonic.flatMap { start =>
      def loop: IO[Unit] =
        check.handleErrorWith { error =>
          IO.monotonic.flatMap { now =>
            if (now - start >= ConvergenceTimeout) IO.raiseError(error)
            else IO.sleep(PollInterval) >> loop
          }
        }
      loop
    }
  }
}
