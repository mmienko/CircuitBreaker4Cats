package io.mienks.resilience.adaptiveratelimiter

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all._
import io.mienks.resilience.Rate
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

  test("rejects non-positive additive increase rates") {
    assertInvalid(
      config = BaseConfig.copy(rateIncreaseBy =
        AimdRateController.AimdRateIncrease(rate = Rate.Zero, tickInterval = 1.second)
      ),
      expectedMessage = "rateIncreaseBy.rate must be nonzero"
    )
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
    List(-0.1, 1.1).traverse_ { rateDecreaseBy =>
      assertInvalid(
        config = BaseConfig.copy(rateDecreaseBy = rateDecreaseBy),
        expectedMessage = "0 <= rateDecreaseBy <= 1"
      )
    }
  }

  test("rejects zero AIMD rates") {
    List(
      BaseConfig.copy(initialRate = Rate.Zero) -> "initialRate must be nonzero",
      BaseConfig.copy(minRate = Rate.Zero)     -> "minRate must be nonzero",
      BaseConfig.copy(maxRate = Rate.Zero)     -> "maxRate must be nonzero"
    ).traverse_ { case (config, expectedMessage) =>
      assertInvalid(config = config, expectedMessage = expectedMessage)
    }
  }

  test("additive increase does not overflow Int when rate has non-standard period from prior scaleBy") {
    // Reproduces the load test crash. With rateDecreaseBy=0.7, each Worsening calls scaleBy(0.3) = ×(3/10).
    // If requests×3 divides by 10, only requests shrinks (period stays 1s):
    // Rate(10000,1s) → Rate(3000,1s) → Rate(900,1s) → Rate(270,1s) → Rate(81,1s)
    // At Rate(81,1s): 81×3=243, and 243%10≠0 so inReducedForm kicks in. Since gcd(243, 10^10)=1
    // (243=3^5 shares no factors with 10=2×5), the period must absorb the factor of 10:
    // Rate(81,1s) → Rate(243,10s) → Rate(729,100s) → ... → Rate(59049, 10^6 s)
    // After 10 decreases the rate is Rate(59049, 10^15 ns). 59049=3^10 is coprime with 10^15,
    // so it can never reduce back to a shorter period. Each additive tick re-expresses 100 rps in
    // this period (100 × 10^6 = 10^8 requests per 10^6 s), growing requests by ~100M per tick.
    // At tick 22, requests hits 2_200_059_049 > Int.MaxValue and Rate.inReducedForm throws
    // IllegalArgumentException, killing the AIMD background fiber.
    TestControl.executeEmbed {
      val config = AimdRateController.Config(
        initialRate = Rate(requests = 10000, period = 1.second),
        minRate = Rate(requests = 1, period = 10000.seconds),
        maxRate = Rate(requests = 10000, period = 1.second),
        rateIncreaseBy = AimdRateController.AimdRateIncrease(
          rate = Rate(requests = 100, period = 1.second),
          tickInterval = 1.second
        ),
        rateDecreaseBy = 0.7
      )
      // 10 Worsening signals each scale by 0.3: the rate drops to ~0.059 rps with a huge period.
      // Recovery ticks then grow requests by ~100M each; tick 22 overflows without the fix.
      // take=33: initial(1) + 10 decreases + 22 ticks (the 22nd is where the overflow occurs).
      val signals = fs2.Stream.emits(List.fill(10)(FailedSignal)).covary[IO]
      run(config = config, failureSignals = signals, take = 33).map { rates =>
        assert(rates.length == 33)
        assert(rates.forall(_ <= config.maxRate))
        assert(rates.forall(_ >= config.minRate))
      }
    }
  }

  private def run(
      config: AimdRateController.Config,
      failureSignals: fs2.Stream[IO, FailureGradient],
      take: Long
  ): IO[List[Rate]] =
    AdaptiveRateLimiter.AimdRateController[IO](config = config).flatMap { controller =>
      failureSignals.through(controller).take(take).compile.toList
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
