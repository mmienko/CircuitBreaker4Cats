package io.mienks.resilience.adaptiveratelimiter

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all._
import io.mienks.resilience.{Rate, RateMultiplier}
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.FailureGradient._
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.{AimdRateController, FailureGradient}
import munit.CatsEffectSuite

import scala.concurrent.duration._

/** Unit tests for [[AdaptiveRateLimiter.AimdRateController]]. */
class AimdRateControllerTests extends CatsEffectSuite {

  private val FailedSignal = Worsening(toLevel = 0)

  private val MinRate     = Rate(requests = 3, period = 1.second)
  private val MaxRate     = Rate(requests = 13, period = 1.second)
  private val InitialRate = Rate(requests = 10, period = 1.second)

  private val BaseConfig: AimdRateController.Config =
    AimdRateController.Config(
      initialRate = InitialRate,
      minRate = MinRate,
      maxRate = MaxRate,
      rateIncreaseBy =
        AimdRateController.AimdRateIncrease(rate = Rate(requests = 1, period = 1.second), tickInterval = 50.millis),
      rateDecreaseBy = 0.5
    )

  test("emits the initial rate") {
    TestControl.executeEmbed {
      run(config = BaseConfig, failureSignals = fs2.Stream.empty, take = 1)
        .map(assertRatesEquivalent(_, List(InitialRate)))
    }
  }

  test("ignores Recovered signals") {
    TestControl.executeEmbed {
      run(
        config = BaseConfig,
        failureSignals = fs2.Stream(Recovered).covary[IO],
        take = 2
      ).map(
        assertRatesEquivalent(
          _,
          List(
            InitialRate,
            Rate(requests = 11, period = 1.second)
          )
        )
      )
    }
  }

  test("ignores Recovering signals") {
    TestControl.executeEmbed {
      run(
        config = BaseConfig,
        failureSignals = fs2.Stream(Recovering(fromLevel = 1)).covary[IO],
        take = 2
      ).map(
        assertRatesEquivalent(
          _,
          List(
            InitialRate,
            Rate(requests = 11, period = 1.second)
          )
        )
      )
    }
  }

  test("applies one decrease per Worsening band crossed") {
    TestControl.executeEmbed {
      run(
        config = BaseConfig,
        failureSignals = fs2.Stream.emits(List(Worsening(toLevel = 0), Worsening(toLevel = 1))).covary[IO],
        take = 3
      ).map(
        assertRatesEquivalent(
          _,
          List(
            InitialRate,
            Rate(requests = 5, period = 1.second),
            MinRate
          )
        )
      )
    }
  }

  test("applies multiplicative decrease and clamps at min") {
    TestControl.executeEmbed {
      val maxRate = Rate(requests = 20, period = 1.second)
      val minRate = Rate(requests = 1, period = 2.seconds) // 0.5 / sec
      run(
        config = BaseConfig.copy(initialRate = maxRate, maxRate = maxRate, minRate = minRate),
        failureSignals = fs2.Stream.emits(List.fill(7)(FailedSignal)).covary[IO],
        take = 7
      ).map(
        assertRatesEquivalent(
          _,
          List(
            maxRate,
            Rate(requests = 10, period = 1.second),
            Rate(requests = 5, period = 1.second),
            Rate(requests = 5, period = 2.seconds), // 2.5 / sec
            Rate(requests = 5, period = 4.seconds), // 1.25 / sec
            Rate(requests = 5, period = 8.seconds), // 0.625 / sec
            minRate
          )
        )
      )
    }
  }

  test("a near-total decrease clamps to min") {
    TestControl.executeEmbed {
      run(
        config = BaseConfig.copy(rateDecreaseBy = 0.99), // close to 1 since 100% is invalid
        failureSignals = fs2.Stream.emit(FailedSignal).covary[IO],
        take = 2
      ).map(assertRatesEquivalent(_, List(InitialRate, MinRate)))
    }
  }

  test("accepts a zero decrease") {
    TestControl.executeEmbed {
      run(
        config = BaseConfig.copy(rateDecreaseBy = 0.0),
        failureSignals = fs2.Stream.empty,
        take = 1
      ).map(assertRatesEquivalent(_, List(InitialRate)))
    }
  }

  test("additively increases on ticks and clamps at max") {
    TestControl.executeEmbed {
      run(config = BaseConfig, failureSignals = fs2.Stream.empty, take = 4)
        .map(
          assertRatesEquivalent(
            _,
            List(
              InitialRate,
              Rate(requests = 11, period = 1.second),
              Rate(requests = 12, period = 1.second),
              MaxRate
            )
          )
        )
    }
  }

  test("clamps min and max rates by throughput across denominators") {
    TestControl.executeEmbed {
      run(
        config = AimdRateController.Config(
          initialRate = Rate(requests = 100, period = 1.second), // 6000 req/min
          minRate = Rate(requests = 3000, period = 1.minute),    // 3000 req/min
          maxRate = Rate(requests = 12, period = 60.millis),     // 12000 req/min
          rateIncreaseBy = AimdRateController.AimdRateIncrease(
            rate = Rate(requests = 2000, period = 1.minute),
            tickInterval = 3.minutes
          ),
          rateDecreaseBy = 0.9
        ),
        failureSignals = fs2.Stream.emit(FailedSignal).covary[IO],
        take = 7
      ).map(
        assertRatesEquivalent(
          _,
          List(
            Rate(requests = 6000, period = 1.minute),
            Rate(requests = 3000, period = 1.minute),
            Rate(requests = 5000, period = 1.minute),
            Rate(requests = 7000, period = 1.minute),
            Rate(requests = 9000, period = 1.minute),
            Rate(requests = 11000, period = 1.minute),
            Rate(requests = 12000, period = 1.minute)
          )
        )
      )
    }
  }

  test("additive increase is constant when multiplicative decrease crosses to lowest time scale") {
    TestControl.executeEmbed {
      run(
        config = AimdRateController.Config(
          initialRate = Rate(requests = 1, period = 30.seconds),
          minRate = Rate(requests = 1, period = 3.minutes),
          maxRate = Rate(requests = 1, period = 1.second),
          rateIncreaseBy = AimdRateController.AimdRateIncrease(
            rate = Rate(requests = 1, period = 30.seconds),
            tickInterval = 30.seconds
          ),
          rateDecreaseBy = 0.5
        ),
        failureSignals = fs2.Stream(FailedSignal, FailedSignal).covary[IO],
        take = 5
      ).map(
        assertRatesEquivalent(
          _,
          List(
            Rate(requests = 1, period = 30.seconds), // initial rate
            Rate(requests = 1, period = 1.minute),
            Rate(requests = 1, period = 2.minutes),
            Rate(requests = 5, period = 2.minutes),
            Rate(requests = 9, period = 2.minutes)
          )
        )
      )
    }
  }

  test("additive increase is constant when multiplicative decrease crosses time scales") {
    TestControl.executeEmbed {
      run(
        config = AimdRateController.Config(
          initialRate = Rate(requests = 1, period = 30.seconds),
          minRate = Rate(requests = 1, period = 3.minutes),
          maxRate = Rate(requests = 1, period = 1.second),
          rateIncreaseBy = AimdRateController.AimdRateIncrease(
            rate = Rate(requests = 1, period = 30.seconds),
            tickInterval = 30.seconds
          ),
          rateDecreaseBy = 0.5
        ),
        failureSignals = fs2.Stream(FailedSignal).covary[IO],
        take = 5
      ).map(
        assertRatesEquivalent(
          _,
          List(
            Rate(requests = 1, period = 30.seconds), // initial rate
            Rate(requests = 1, period = 1.minute),
            Rate(requests = 3, period = 1.minute),
            Rate(requests = 5, period = 1.minute),
            Rate(requests = 7, period = 1.minute)
          )
        )
      )
    }
  }

  test("clamps oversized additive increases at max") {
    TestControl.executeEmbed {
      run(
        config = BaseConfig.copy(rateIncreaseBy =
          AimdRateController.AimdRateIncrease(rate = Rate(requests = 100, period = 1.second), tickInterval = 50.millis)
        ),
        failureSignals = fs2.Stream.empty,
        take = 2
      ).map(
        assertRatesEquivalent(
          _,
          List(
            InitialRate,
            MaxRate
          )
        )
      )
    }
  }

  test("slow start grows the rate exponentially on Insufficient Data (None) signals") {
    TestControl.executeEmbed {
      runSignals(
        config = AimdRateController.Config(
          initialRate = Rate(requests = 1, period = 1.second),
          minRate = Rate(requests = 1, period = 1.second),
          maxRate = Rate(requests = 50, period = 1.second),
          // Park ticks far in the future so only slow-start moves the rate within the take.
          rateIncreaseBy =
            AimdRateController.AimdRateIncrease(rate = Rate(requests = 1, period = 1.second), tickInterval = 1.hour),
          rateDecreaseBy = 0.5,
          insufficientDataRateGrowthFactor = 2.0
        ),
        signals = fs2.Stream.emits(List.fill(7)(none[FailureGradient])).covary[IO],
        take = 7
      ).map(
        assertRatesEquivalent(
          _,
          List(
            Rate(requests = 1, period = 1.second),
            Rate(requests = 2, period = 1.second),
            Rate(requests = 4, period = 1.second),
            Rate(requests = 8, period = 1.second),
            Rate(requests = 16, period = 1.second),
            Rate(requests = 32, period = 1.second),
            Rate(requests = 50, period = 1.second) // would be 64, but clamped at max
          )
        )
      )
    }
  }

  test("slow start growth is capped at ssthresh (last good rate) after a Worsening") {
    TestControl.executeEmbed {
      runSignals(
        config = AimdRateController.Config(
          initialRate = Rate(requests = 32, period = 1.second),
          minRate = Rate(requests = 1, period = 1.second),
          maxRate = Rate(requests = 64, period = 1.second),
          rateIncreaseBy =
            AimdRateController.AimdRateIncrease(rate = Rate(requests = 1, period = 1.second), tickInterval = 1.hour),
          rateDecreaseBy = 0.5,
          insufficientDataRateGrowthFactor = 2.0
        ),
        signals = fs2.Stream
          .emits(List(FailedSignal.some, none[FailureGradient], none[FailureGradient]))
          .covary[IO],
        take = 3
      ).map(
        assertRatesEquivalent(
          _,
          List(
            Rate(requests = 32, period = 1.second), // initial
            Rate(requests = 16, period = 1.second), // Worsening halves; ssthresh captured at 32
            Rate(requests = 32, period = 1.second)  // slow start climbs back to ssthresh and stops below maxRate (64)
          )
        )
      )
    }
  }

  test("slow start rounds fractional fixed-point growth up") {
    TestControl.executeEmbed {
      runSignals(
        config = AimdRateController.Config(
          initialRate = Rate(perDay = 3L),
          minRate = Rate.Min,
          maxRate = Rate(perDay = 20L),
          rateIncreaseBy = AimdRateController.AimdRateIncrease(rate = Rate.Min, tickInterval = 1.hour),
          rateDecreaseBy = 0.5,
          insufficientDataRateGrowthFactor = 1.5
        ),
        signals = fs2.Stream.emit(none[FailureGradient]).covary[IO],
        take = 2
      ).map(assertRatesEquivalent(_, List(Rate(perDay = 3L), Rate(perDay = 5L))))
    }
  }

  test("rejects slow start growth factors not greater than one") {
    val roundsToOne = 1.0 + 1.0 / (RateMultiplier.Denominator.toDouble * 4.0)

    List(1.0, 0.5, roundsToOne).traverse_ { slowStartGrowthFactor =>
      assertInvalid(
        config = BaseConfig.copy(insufficientDataRateGrowthFactor = slowStartGrowthFactor),
        expectedMessage = "insufficientDataRateGrowthFactor > 1"
      )
    }
  }

  test("rejects non-finite or oversized slow start growth factors") {
    List(Double.PositiveInfinity, Rate.Max.perDay.toDouble + 1.0).traverse_ { slowStartGrowthFactor =>
      assertInvalid(
        config = BaseConfig.copy(insufficientDataRateGrowthFactor = slowStartGrowthFactor),
        expectedMessage = "failed to convert insufficientDataRateGrowthFactor to fixed point"
      )
    }
  }

  test("rejects non-positive additive increase tick intervals") {
    assertInvalid(
      config = BaseConfig.copy(rateIncreaseBy =
        AimdRateController.AimdRateIncrease(rate = Rate(requests = 1, period = 1.second), tickInterval = 0.seconds)
      ),
      expectedMessage = "rateIncreaseBy.tickInterval > 0ms"
    )
  }

  test("rejects invalid min initial max ordering") {
    assertInvalid(
      config = BaseConfig.copy(minRate = Rate(requests = 11, period = 1.second)),
      expectedMessage = "min rate <= initial rate <= max rate"
    )
  }

  test("rejects invalid decrease ratios") {
    List(-0.1, 1.0, 1.1).traverse_ { rateDecreaseBy =>
      assertInvalid(
        config = BaseConfig.copy(rateDecreaseBy = rateDecreaseBy),
        expectedMessage = "0 <= rateDecreaseBy < 1"
      )
    }
  }

  private def run(
      config: AimdRateController.Config,
      failureSignals: fs2.Stream[IO, FailureGradient],
      take: Long
  ): IO[List[Rate]] =
    runSignals(
      config = config,
      signals = failureSignals.map(_.some),
      take = take
    )

  private def runSignals(
      config: AimdRateController.Config,
      signals: fs2.Stream[IO, Option[FailureGradient]],
      take: Long
  ): IO[List[Rate]] =
    AdaptiveRateLimiter.AimdRateController[IO](config = config).flatMap { controller =>
      signals.through(controller).take(take).compile.toList
    }

  private def assertInvalid(config: AimdRateController.Config, expectedMessage: String): IO[Unit] =
    AdaptiveRateLimiter
      .AimdRateController[IO](config = config)
      .attempt
      .map { result =>
        assert(result.isLeft)
        val msg = result.left.toOption.fold("")(_.getMessage)
        assert(msg.contains(expectedMessage), clue = msg)
      }

  private def assertRatesEquivalent(obtained: List[Rate], expected: List[Rate]): Unit =
    assert(
      obtained.length == expected.length && obtained.zip(expected).forall { case (a, b) => a === b },
      clue = s"expected throughput-equivalent rates: $expected, got: $obtained"
    )

}
