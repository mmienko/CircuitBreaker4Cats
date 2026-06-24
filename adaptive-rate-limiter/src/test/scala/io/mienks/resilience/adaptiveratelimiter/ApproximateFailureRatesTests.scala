package io.mienks.resilience.adaptiveratelimiter

import cats.effect.IO
import cats.effect.std.Queue
import cats.syntax.all._
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.ApproximateFailureRates
import munit.CatsEffectSuite

import scala.concurrent.duration._

/** Unit tests for [[AdaptiveRateLimiter.ApproximateFailureRates]]. */
class ApproximateFailureRatesTests extends CatsEffectSuite {

  private val MeasurementPeriod: FiniteDuration          = 10.millis
  private val BaseConfig: ApproximateFailureRates.Config =
    ApproximateFailureRates.Config(
      numberOfSlotsForMeasurements = 4,
      slotDuration = MeasurementPeriod,
      measurementPeriod = MeasurementPeriod,
      minNumberOfMeasurements = 4
    )

  test("samples measurements into approximate failure rates; keeps last value if no new measurements") {
    for {
      producerConsumer <- ApproximateFailureRates.createProducerAndConsumer[IO](config = BaseConfig)
      (measurements, failureRates) = producerConsumer
      samples <- Queue.unbounded[IO, Option[Double]]
      fiber   <- failureRates.evalMap(samples.offer).compile.drain.start
      waitForSampling = IO.sleep(MeasurementPeriod)

      // no measurements: the window is uninitialized, so the producer reports None
      _ <- waitForSampling
      _ <- pollUntil(samples)(assertEquals(_, none[Double]))

      // only errors
      _ <- measurements.recordFailure.replicateA_(4)
      _ <- waitForSampling
      _ <- pollUntilMeasured(samples)(ratio => assertEquals(ratio, 1.0))

      // mostly success
      _ <- measurements.recordSuccess.replicateA_(100)
      _ <- waitForSampling
      _ <- pollUntilMeasured(samples)(ratio => assert(ratio <= 0.1, clue = ratio))

      // no change
      _ <- waitForSampling
      _ <- pollUntilMeasured(samples)(ratio => assert(ratio <= 0.1, clue = ratio))

      // clear/waitForSampling until not enough measurements: window de-initializes back to None
      _ <- IO.sleep(BaseConfig.measurementWindow * 2)
      _ <- samples.tryTakeN(maxN = None)
      _ <- pollUntil(samples)(assertEquals(_, none[Double]))

      // mixed signals
      _ <- measurements.recordFailure.replicateA_(50)
      _ <- measurements.recordSuccess.replicateA_(50)
      _ <- samples.tryTakeN(maxN = None)
      _ <- pollUntilMeasured(samples)(ratio => assertEquals(ratio, 0.5))

      _ <- fiber.cancel
    } yield ()
  }

  test("rejects invalid measurement configuration") {
    List(
      BaseConfig.copy(numberOfSlotsForMeasurements = 0) -> "slots > 0",
      BaseConfig.copy(slotDuration = 0.seconds)         -> "slot duration > 0ms",
      BaseConfig.copy(measurementPeriod = 0.seconds)    -> "measurements period > 0ms",
      BaseConfig.copy(minNumberOfMeasurements = 0)      -> "min measurements > 0"
    ).traverse_ { case (config, expectedMessage) =>
      assertInvalid(config = config, expectedMessage = expectedMessage)
    }
  }

  private def assertInvalid(config: ApproximateFailureRates.Config, expectedMessage: String): IO[Unit] =
    ApproximateFailureRates.createProducerAndConsumer[IO](config = config).attempt.map { result =>
      assert(result.isLeft)
      val msg = result.left.toOption.fold("")(_.getMessage)
      assert(msg.contains(expectedMessage), clue = msg)
    }

  private def pollUntil(samples: Queue[IO, Option[Double]])(check: Option[Double] => Unit): IO[Unit] = {
    val PollTimeout = 2.seconds

    def loop: IO[Unit] =
      samples.take.flatMap { sample =>
        IO(check(sample)).handleErrorWith(_ => loop)
      }

    loop.timeout(PollTimeout)
  }

  /** Polls until a `Some(ratio)` satisfies `check`, skipping any `None` (too few measurements). */
  private def pollUntilMeasured(samples: Queue[IO, Option[Double]])(check: Double => Unit): IO[Unit] =
    pollUntil(samples) {
      case Some(ratio) => check(ratio)
      case None        => throw new AssertionError("expected a measured ratio, got: None")
    }
}
