package io.mienks.resilience.charts

import cats.effect.{IO, Ref}
import cats.syntax.all._
import io.mienks.resilience.Rate
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.FailureGradient
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.syntax._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

object SimulationRunner {

  // Number of client fibers hitting the backend roughly every ClientInterval. The offered load comfortably exceeds
  // maxRate and the admitted rate tracks the limiter's current refill rate.
  private val NumberOfClients: Int           = 4
  private val ClientInterval: FiniteDuration = 1.millis

  // Sampled finer than the slot duration so band crossings and rate cuts are visible in the timeseries.
  private val SamplePeriod: FiniteDuration = 50.millis

  // Fixed seed keeps the backend's probabilistic soft-ceiling failures reproducible across runs.
  private val Seed: Long = 42L

  def run(scenario: Scenario): IO[Result] =
    for {
      recorder <- Recorder.create
      simulation = for {
        backend <- Backend.start(schedule = scenario.phases, seed = Seed)

        limiter <- AdaptiveRateLimiter
          .start[IO](
            config = scenario.config,
            onFailureCategoryChange = (event: FailureGradient) => recorder.recordGradient(event),
            onRateChange = (rate: Rate) => recorder.recordRate(toRps(rate)),
            onError = (_: Throwable) => IO.unit
          )

        callBackend = limiter.protect(
          fa = recorder.countAdmitted >> backend.call,
          isError = (ok: Boolean) => !ok,
          orElse = true
        ) >> IO.sleep(ClientInterval)

        takeSample =
          for {
            observed <- limiter.failureRatio
            rate     <- limiter.rate
            capacity <- backend.baseCapacity
            _        <- recorder.recordSample(
              aimdRps = toRps(rate),
              backendCapacityRps = toRps(capacity),
              observedFailureRatio = observed
            )
          } yield ()

        _ <- callBackend.foreverM.background
          .replicateA_(NumberOfClients)

        _ <- (takeSample >> IO.sleep(SamplePeriod)).foreverM.background
      } yield ()

      _      <- simulation.surround(IO.sleep(scenario.totalDuration))
      result <- recorder.result(scenario)
    } yield result

  private def toRps(rate: Rate): Double =
    rate.requests.toDouble / rate.period.toUnit(TimeUnit.SECONDS)

  /** Mutable, in-flight recording for a single run: the sampled timeseries, the discrete control-loop events, and the
    * admitted-request counter used to derive throughput.
    */
  final class Recorder private (
      startNanos: FiniteDuration,
      samplesRef: Ref[IO, Vector[Sample]],
      rateEventsRef: Ref[IO, Vector[(Long, Double)]],
      gradientEventsRef: Ref[IO, Vector[(Long, FailureGradient)]],
      admittedRef: Ref[IO, Long],
      lastSampleRef: Ref[IO, (Long, Long)]
  ) {

    private val elapsedMillis: IO[Long] = IO.monotonic.map(now => (now - startNanos).toMillis)

    val countAdmitted: IO[Unit] = admittedRef.update(_ + 1L)

    def recordRate(rps: Double): IO[Unit] =
      elapsedMillis.flatMap(ms => rateEventsRef.update(_ :+ (ms, rps)))

    def recordGradient(event: FailureGradient): IO[Unit] =
      elapsedMillis.flatMap(ms => gradientEventsRef.update(_ :+ (ms, event)))

    /** Append a sample, deriving admitted throughput from the delta since the previous sample. */
    def recordSample(aimdRps: Double, backendCapacityRps: Double, observedFailureRatio: Double): IO[Unit] =
      for {
        now      <- IO.monotonic
        admitted <- admittedRef.get
        prev     <- lastSampleRef.getAndSet((now.toNanos, admitted))
        (prevNanos, prevAdmitted) = prev
        dtSeconds                 = (now.toNanos - prevNanos).toDouble / 1e9
        admittedRps               = if (dtSeconds > 0.0) (admitted - prevAdmitted).toDouble / dtSeconds else 0.0
        _ <- samplesRef.update(
          _ :+ Sample(
            elapsedMillis = (now - startNanos).toMillis,
            aimdRps = aimdRps,
            admittedRps = admittedRps,
            backendCapacityRps = backendCapacityRps,
            observedFailureRatio = observedFailureRatio
          )
        )
      } yield ()

    def result(scenario: Scenario): IO[Result] =
      for {
        samples        <- samplesRef.get
        rateEvents     <- rateEventsRef.get
        gradientEvents <- gradientEventsRef.get
      } yield Result(
        scenario = scenario,
        samples = samples,
        rateEvents = rateEvents,
        gradientEvents = gradientEvents
      )
  }

  object Recorder {
    val create: IO[Recorder] =
      for {
        startNanos        <- IO.monotonic
        samplesRef        <- Ref[IO].of(Vector.empty[Sample])
        rateEventsRef     <- Ref[IO].of(Vector.empty[(Long, Double)])
        gradientEventsRef <- Ref[IO].of(Vector.empty[(Long, FailureGradient)])
        admittedRef       <- Ref[IO].of(0L)
        lastSampleRef     <- Ref[IO].of((startNanos.toNanos, 0L))
      } yield new Recorder(
        startNanos = startNanos,
        samplesRef = samplesRef,
        rateEventsRef = rateEventsRef,
        gradientEventsRef = gradientEventsRef,
        admittedRef = admittedRef,
        lastSampleRef = lastSampleRef
      )
  }

  /** One sampled point of a closed-loop run.
    *
    * @param aimdRps
    *   the AIMD's current rate estimate (the controlled variable)
    * @param admittedRps
    *   the measured throughput the limiter actually admitted over the last sample interval
    * @param backendCapacityRps
    *   the backend's current hard-ceiling capacity (the bottleneck the AIMD is trying to discover)
    * @param observedFailureRatio
    *   the limiter's sampled failure ratio in `[0, 1]`
    */
  final case class Sample(
      elapsedMillis: Long,
      aimdRps: Double,
      admittedRps: Double,
      backendCapacityRps: Double,
      observedFailureRatio: Double
  )

  /** Everything a chart needs for one scenario: the dense sampled timeseries plus the discrete control-loop events. */
  final case class Result(
      scenario: Scenario,
      samples: Vector[Sample],
      rateEvents: Vector[(Long, Double)],
      gradientEvents: Vector[(Long, FailureGradient)]
  )
}
