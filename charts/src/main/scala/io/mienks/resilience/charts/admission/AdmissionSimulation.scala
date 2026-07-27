package io.mienks.resilience.charts.admission

import cats.effect.std.Random
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._
import io.mienks.resilience.Rate
import io.mienks.resilience.admissioncontroller.AdmissionController
import io.mienks.resilience.admissioncontroller.AdmissionController.syntax._
import io.mienks.resilience.charts.Backend

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

object AdmissionSimulation {

  // Client fibers each offer a call roughly every ClientInterval. The aggregate offered load (~800 rps) sits well
  // above the degraded capacities so the controller always has excess to shed during overload phases, and below the
  // Healthy capacity so the gate fully opens when the backend recovers.
  private val NumberOfClients: Int           = 8
  private val ClientInterval: FiniteDuration = 10.millis

  // Sampled finer than the slot duration so the shedding response is visible in the timeseries.
  private val SamplePeriod: FiniteDuration = 50.millis

  // Fixed seed keeps both the backend's overflow failures and the controller's admission draws reproducible.
  private val Seed: Long = 42L

  def run(scenario: AdmissionScenario): IO[Result] =
    for {
      recorder <- Recorder.create
      random   <- Random.scalaUtilRandomSeedLong[IO](Seed)
      simulation = for {
        backend    <- Backend.start(schedule = scenario.phases, seed = Seed)
        controller <- Resource.eval(AdmissionController[IO](config = scenario.config, random = random))

        offerCall = recorder.countOffered >> controller.protect(
          fa = recorder.countAdmitted >> backend.call.flatTap(accepted => recorder.countAccepted.whenA(accepted)),
          isFailure = (accepted: Boolean) => !accepted,
          orElse = false
        ) >> IO.sleep(ClientInterval)

        takeSample =
          for {
            probability  <- controller.rejectionProbability
            failureRatio <- controller.failureRatio
            capacity     <- backend.baseCapacity
            _            <- recorder.recordSample(
              capacityRps = toRps(capacity),
              rejectionProbability = probability,
              failureRatio = failureRatio
            )
          } yield ()

        _ <- offerCall.foreverM.background
          .replicateA_(NumberOfClients)

        _ <- (takeSample >> IO.sleep(SamplePeriod)).foreverM.background
      } yield ()

      _      <- simulation.surround(IO.sleep(scenario.totalDuration))
      result <- recorder.result(scenario)
    } yield result

  private def toRps(rate: Rate): Double =
    rate.eventsPer(unit = TimeUnit.SECONDS)

  /** Mutable, in-flight recording for a single run: the sampled timeseries plus the counters used to derive the offered
    * / admitted / accepted throughput.
    */
  final class Recorder private (
      startNanos: FiniteDuration,
      samplesRef: Ref[IO, Vector[Sample]],
      offeredRef: Ref[IO, Long],
      admittedRef: Ref[IO, Long],
      acceptedRef: Ref[IO, Long],
      lastSampleRef: Ref[IO, Recorder.Counters]
  ) {

    val countOffered: IO[Unit]  = offeredRef.update(_ + 1L)
    val countAdmitted: IO[Unit] = admittedRef.update(_ + 1L)
    val countAccepted: IO[Unit] = acceptedRef.update(_ + 1L)

    /** Append a sample, deriving each throughput from the counter delta since the previous sample. */
    def recordSample(capacityRps: Double, rejectionProbability: Double, failureRatio: Double): IO[Unit] =
      for {
        now      <- IO.monotonic
        offered  <- offeredRef.get
        admitted <- admittedRef.get
        accepted <- acceptedRef.get
        prev     <- lastSampleRef.getAndSet(Recorder.Counters(now.toNanos, offered, admitted, accepted))
        dtSeconds = (now.toNanos - prev.nanos).toDouble / 1e9
        perSecond = (current: Long, previous: Long) =>
          if (dtSeconds > 0.0) (current - previous).toDouble / dtSeconds else 0.0
        _ <- samplesRef.update(
          _ :+ Sample(
            elapsedMillis = (now - startNanos).toMillis,
            offeredRps = perSecond(offered, prev.offered),
            admittedRps = perSecond(admitted, prev.admitted),
            acceptedRps = perSecond(accepted, prev.accepted),
            capacityRps = capacityRps,
            rejectionProbability = rejectionProbability,
            failureRatio = failureRatio
          )
        )
      } yield ()

    def result(scenario: AdmissionScenario): IO[Result] =
      samplesRef.get.map(samples => Result(scenario = scenario, samples = samples))
  }

  object Recorder {

    private final case class Counters(nanos: Long, offered: Long, admitted: Long, accepted: Long)

    val create: IO[Recorder] =
      for {
        startNanos    <- IO.monotonic
        samplesRef    <- Ref[IO].of(Vector.empty[Sample])
        offeredRef    <- Ref[IO].of(0L)
        admittedRef   <- Ref[IO].of(0L)
        acceptedRef   <- Ref[IO].of(0L)
        lastSampleRef <- Ref[IO].of(Counters(startNanos.toNanos, 0L, 0L, 0L))
      } yield new Recorder(
        startNanos = startNanos,
        samplesRef = samplesRef,
        offeredRef = offeredRef,
        admittedRef = admittedRef,
        acceptedRef = acceptedRef,
        lastSampleRef = lastSampleRef
      )
  }

  /** One sampled point of a closed-loop run.
    *
    * @param offeredRps
    *   the throughput clients attempted (the load before the controller's gate)
    * @param admittedRps
    *   the throughput the controller let through to the backend
    * @param acceptedRps
    *   the backend's goodput: admitted calls that were not throttled
    * @param capacityRps
    *   the backend's current hard-ceiling capacity (the bottleneck)
    * @param rejectionProbability
    *   the controller's current shedding probability in `[0, 1]`
    * @param failureRatio
    *   the controller's windowed failure ratio in `[0, 1]` (unclamped by the dead zone, unlike rejectionProbability)
    */
  final case class Sample(
      elapsedMillis: Long,
      offeredRps: Double,
      admittedRps: Double,
      acceptedRps: Double,
      capacityRps: Double,
      rejectionProbability: Double,
      failureRatio: Double
  )

  /** Everything a chart needs for one scenario: the dense sampled timeseries. */
  final case class Result(
      scenario: AdmissionScenario,
      samples: Vector[Sample]
  )
}
