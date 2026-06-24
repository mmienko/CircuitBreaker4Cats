package io.mienks.resilience.adaptiveratelimiter

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all._
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.FailureGradient._
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.{
  FailureGradient,
  FailureRateCategorizer,
  HysteresisBand
}
import munit.CatsEffectSuite

class FailureRateCategorizerTests extends CatsEffectSuite {

  private def runSingleBand(failureRates: Double*): IO[List[FailureGradient]] =
    runWithBands(
      bands = NonEmptyList.one(HysteresisBand(exit = 0.2, start = 0.5)),
      failureRates = failureRates
    )

  /** Runs a timeseries of sampled failure rates and returns the failure-category gradients. */
  private def runWithBands(bands: NonEmptyList[HysteresisBand], failureRates: Seq[Double]): IO[List[FailureGradient]] =
    runSamples(bands = bands, samples = failureRates.map(_.some)).map(_.flatten)

  /** Runs a timeseries of raw samples (`None` = too few measurements) and returns the emitted signals. */
  private def runSamples(
      bands: NonEmptyList[HysteresisBand],
      samples: Seq[Option[Double]]
  ): IO[List[Option[FailureGradient]]] =
    FailureRateCategorizer[IO](config = FailureRateCategorizer.Config(failureLevels = bands)).flatMap { categorizer =>
      fs2.Stream
        .emits(samples.toList)
        .covary[IO]
        .through(categorizer)
        .compile
        .toList
    }

  test("rejects invalid hysteresis bands") {
    List(
      FailureRateCategorizer.Config(
        failureLevels = NonEmptyList.one(HysteresisBand(exit = 0.5, start = 0.1))
      ) -> "band[0]: start >= exit to contain hysteresis",
      FailureRateCategorizer.Config(
        failureLevels = NonEmptyList.of(
          HysteresisBand(exit = 0.1, start = 0.5),
          HysteresisBand(exit = 0.2, start = 0.4)
        )
      ) -> "bands must have monotonically increasing start",
      FailureRateCategorizer.Config(
        failureLevels = NonEmptyList.of(
          HysteresisBand(exit = 0.2, start = 0.3),
          HysteresisBand(exit = 0.1, start = 0.6)
        )
      ) -> "bands must have monotonically increasing exit",
      FailureRateCategorizer.Config(
        failureLevels = NonEmptyList.of(
          HysteresisBand(exit = 0.1, start = 0.5),
          HysteresisBand(exit = 0.4, start = 0.8)
        )
      ) -> "bands must not overlap",
      FailureRateCategorizer.Config(
        failureLevels = NonEmptyList.of(
          HysteresisBand(exit = 0.1, start = 0.5),
          HysteresisBand(exit = 0.5, start = 0.8)
        )
      ) -> "bands must not overlap"
    ).traverse_ { case (config, expectedMessage) =>
      FailureRateCategorizer[IO](config = config).attempt.map { result =>
        assert(result.isLeft)
        val msg = result.left.toOption.fold("")(_.getMessage)
        assert(msg.contains(expectedMessage), clue = msg)
      }
    }
  }

  test("single band: stays at zero -> no signals") {
    runSingleBand(0.0, 0.0, 0.0).map(assertEquals(_, List.empty[FailureGradient]))
  }

  test("single band: hovers below start -> no signals") {
    runSingleBand(0.0, 0.2, 0.4, 0.2, 0.0).map(assertEquals(_, List.empty[FailureGradient]))
  }

  test("single band: above start and stays there -> Worsening(0)") {
    runSingleBand(0.0, 0.6, 1.0, 1.0).map(assertEquals(_, List(Worsening(toLevel = 0))))
  }

  test("single band: above start then back into hysteresis band (no exit) -> Worsening(0)") {
    runSingleBand(0.0, 0.6, 0.4).map(assertEquals(_, List(Worsening(toLevel = 0))))
  }

  test("single band: above start and drifts higher to 100% -> Worsening(0)") {
    runSingleBand(0.0, 0.6, 0.8, 1.0).map(assertEquals(_, List(Worsening(toLevel = 0))))
  }

  test("single band: above start, drifts higher, then back into hysteresis band -> Worsening(0)") {
    runSingleBand(0.0, 0.6, 1.0, 0.4).map(assertEquals(_, List(Worsening(toLevel = 0))))
  }

  test("single band: above start, then below exit -> Worsening(0), Recovered") {
    runSingleBand(0.0, 1.0, 0.0).map(assertEquals(_, List(Worsening(toLevel = 0), Recovered)))
  }

  test(
    "single band: above start, below exit, then back into hysteresis band (no re-enter) -> Worsening(0), Recovered"
  ) {
    runSingleBand(0.0, 1.0, 0.0, 0.4).map(assertEquals(_, List(Worsening(toLevel = 0), Recovered)))
  }

  test("single band: above start, below exit, then above start again -> Worsening(0), Recovered, Worsening(0)") {
    runSingleBand(0.0, 1.0, 0.0, 0.6).map(
      assertEquals(_, List(Worsening(toLevel = 0), Recovered, Worsening(toLevel = 0)))
    )
  }

  private val MultipleBands: NonEmptyList[HysteresisBand] = NonEmptyList.of(
    HysteresisBand(exit = 0.1, start = 0.3),
    HysteresisBand(exit = 0.4, start = 0.6),
    HysteresisBand(exit = 0.7, start = 0.9)
  )

  private def runMultipleBands(failureRates: Double*): IO[List[FailureGradient]] =
    runWithBands(bands = MultipleBands, failureRates = failureRates)

  test("multiple bands: stays at zero -> no signals") {
    runMultipleBands(0.0, 0.0).map(assertEquals(_, List.empty[FailureGradient]))
  }

  test("multiple bands: hovers below band 0 start -> no signals") {
    runMultipleBands(0.0, 0.1, 0.2, 0.1, 0.0).map(assertEquals(_, List.empty[FailureGradient]))
  }

  test("multiple bands: Healthy -> Worsening(0) only") {
    runMultipleBands(0.0, 0.3).map(assertEquals(_, List(Worsening(toLevel = 0))))
  }

  test("multiple bands: Healthy -> Worsening(0), Worsening(1)") {
    runMultipleBands(0.0, 0.6).map(assertEquals(_, List(Worsening(toLevel = 0), Worsening(toLevel = 1))))
  }

  test("multiple bands: Healthy -> Worsening(0), Worsening(1), Worsening(2)") {
    runMultipleBands(0.0, 0.9)
      .map(assertEquals(_, List(Worsening(toLevel = 0), Worsening(toLevel = 1), Worsening(toLevel = 2))))
  }

  test("multiple bands: Worsening(0) -> Worsening(1) retransition") {
    runMultipleBands(0.0, 0.3, 0.6).map(assertEquals(_, List(Worsening(toLevel = 0), Worsening(toLevel = 1))))
  }

  test("multiple bands: Worsening(1) -> Worsening(2) retransition") {
    runMultipleBands(0.0, 0.6, 0.9)
      .map(assertEquals(_, List(Worsening(toLevel = 0), Worsening(toLevel = 1), Worsening(toLevel = 2))))
  }

  test("multiple bands: climbs band by band") {
    runMultipleBands(0.0, 0.3, 0.6, 0.9)
      .map(assertEquals(_, List(Worsening(toLevel = 0), Worsening(toLevel = 1), Worsening(toLevel = 2))))
  }

  test("multiple bands: drifts higher within top band -> Worsening(2)") {
    runMultipleBands(0.0, 0.9, 1.0)
      .map(assertEquals(_, List(Worsening(toLevel = 0), Worsening(toLevel = 1), Worsening(toLevel = 2))))
  }

  test("multiple bands: worsens then partially recovers -> Worsening(2), Recovering(2), Recovering(1)") {
    runMultipleBands(0.0, 0.9, 0.5, 0.2)
      .map(
        assertEquals(
          _,
          List(
            Worsening(toLevel = 0),
            Worsening(toLevel = 1),
            Worsening(toLevel = 2),
            Recovering(fromLevel = 2),
            Recovering(fromLevel = 1)
          )
        )
      )
  }

  test("multiple bands: recovery target uses exit thresholds") {
    runMultipleBands(0.91, 0.95, 0.8, 0.7, 0.2)
      .map(
        assertEquals(
          _,
          List(
            Worsening(toLevel = 0),
            Worsening(toLevel = 1),
            Worsening(toLevel = 2),
            Recovering(fromLevel = 2),
            Recovering(fromLevel = 1)
          )
        )
      )
  }

  test("multiple bands: full recovery from top band") {
    runMultipleBands(0.0, 0.9, 0.5, 0.2, 0.0)
      .map(
        assertEquals(
          _,
          List(
            Worsening(toLevel = 0),
            Worsening(toLevel = 1),
            Worsening(toLevel = 2),
            Recovering(fromLevel = 2),
            Recovering(fromLevel = 1),
            Recovered
          )
        )
      )
  }

  test("multiple bands: quick recovery from top band") {
    runMultipleBands(0.0, 0.9, 0.0)
      .map(
        assertEquals(
          _,
          List(
            Worsening(toLevel = 0),
            Worsening(toLevel = 1),
            Worsening(toLevel = 2),
            Recovering(fromLevel = 2),
            Recovering(fromLevel = 1),
            Recovered
          )
        )
      )
  }

  test("multiple bands: re-enters top band after full recovery") {
    runMultipleBands(0.0, 0.9, 0.5, 0.2, 0.0, 0.9)
      .map(
        assertEquals(
          _,
          List(
            Worsening(toLevel = 0),
            Worsening(toLevel = 1),
            Worsening(toLevel = 2),
            Recovering(fromLevel = 2),
            Recovering(fromLevel = 1),
            Recovered,
            Worsening(toLevel = 0),
            Worsening(toLevel = 1),
            Worsening(toLevel = 2)
          )
        )
      )
  }

  test("multiple bands: re-enters Worsening(1) and Worsening(2) after partial recovery") {
    runMultipleBands(0.0, 0.3, 0.6, 0.9, 0.6, 0.3, 0.6, 0.9, 0.6, 0.3)
      .map(
        assertEquals(
          _,
          List(
            Worsening(toLevel = 0),
            Worsening(toLevel = 1),
            Worsening(toLevel = 2),
            Recovering(fromLevel = 2),
            Recovering(fromLevel = 1),
            Worsening(toLevel = 1),
            Worsening(toLevel = 2),
            Recovering(fromLevel = 2),
            Recovering(fromLevel = 1)
          )
        )
      )
  }

  test("None samples pass through unchanged without advancing failure state") {
    runSamples(
      bands = NonEmptyList.one(HysteresisBand(exit = 0.2, start = 0.5)),
      samples = List(
        none[Double], // emitted, state held at Healthy
        0.6.some,     // worsens to band 0
        none[Double], // emitted, state held at Failing(0)
        0.6.some,     // still Failing(0) -> no change
        0.0.some      // recovers
      )
    ).map(
      assertEquals(
        _,
        List(
          none[FailureGradient],
          Worsening(toLevel = 0).some,
          none[FailureGradient],
          Recovered.some
        )
      )
    )
  }
}
