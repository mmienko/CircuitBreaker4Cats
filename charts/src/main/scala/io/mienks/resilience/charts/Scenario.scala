package io.mienks.resilience.charts

import cats.data.NonEmptyList
import io.mienks.resilience.Rate
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.{Config, HysteresisBand}

import scala.concurrent.duration._

/** A closed-loop run against an [[io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter]]: a workload offers
  * load through the limiter to a [[Backend]] whose capacity follows `phases`, and failures emerge from the limiter
  * over-driving that capacity.
  *
  * @param name
  *   slug used for output file names
  * @param description
  *   one-line caption rendered on the chart
  * @param config
  *   limiter configuration for the run
  * @param phases
  *   ordered backend-capacity phases (see [[Backend.Phase]]); the head phase's duration includes the warmup
  */
final case class Scenario(
    name: String,
    description: String,
    config: Config,
    phases: NonEmptyList[Backend.Phase]
) {
  def totalDuration: FiniteDuration = phases.toList.map(_.duration).reduce(_ + _)
}

object Scenario {

  private def rps(requests: Int): Rate = Rate(requests = requests, period = 1.second)

  // Millisecond time scale so a full run takes a few seconds. The rps magnitudes stay in the hundreds because the
  // closed loop derives the failure ratio from requests the limiter actually admits, so each 100ms measurement window
  // needs enough admitted requests (>= minNumberOfMeasurements) for the ratio to be meaningful.
  private val SlotDuration: FiniteDuration = 25.millis
  private val MeasurementBuckets: Int      = 4

  // Settle time spent in the first phase before the scripted capacity changes begin; folded into each head duration.
  private val Warmup: FiniteDuration = 500.millis

  private val Bands: NonEmptyList[HysteresisBand] = NonEmptyList.of(
    HysteresisBand(exit = 0.1, start = 0.3), // level 0: minor degradation
    HysteresisBand(exit = 0.4, start = 0.7)  // level 1: severe degradation
  )

  private val BaseConfig: Config =
    Config(
      capacity = 40,
      initialRate = rps(50),
      // Floor high enough that the 100ms window still collects >= minNumberOfMeasurements admitted requests.
      minRate = rps(50),
      maxRate = rps(400),
      // Additive increase of ~125 rps/s: a brisk climb that still shows clear sawtooth ramps.
      rateIncreaseBy = rps(10),
      rateIncreasePeriod = 80.millis,
      rateDecreaseBy = 0.5,
      numberOfSlotsForMeasurements = MeasurementBuckets,
      slotDuration = SlotDuration,
      measurementPeriod = SlotDuration,
      minNumberOfMeasurements = MeasurementBuckets,
      failureLevels = Bands
    )

  // A capacity comfortably above maxRate keeps the backend healthy (no overload), so the rate plateaus at max.
  private val Healthy: Rate = rps(500)

  // Slow-start config: start the estimate far below maxRate and stretch the measurement cadence (50ms slots x 4 =
  // 200ms window, sampled every 200ms) so the geometric doublings are legible. The starved lull offers fewer than
  // NumberOfClients requests inside any window, so minNumberOfMeasurements above that count keeps every window
  // insufficient and the limiter continuously in slow-start (a tiny additive step keeps doublings the visible driver).
  // A small burst capacity keeps the resume transition from dumping a large burst that would overwhelm the backend.
  private val SlowStartConfig: Config =
    BaseConfig.copy(
      capacity = 8,
      initialRate = rps(20),
      minRate = rps(20),
      rateIncreaseBy = rps(2),
      slotDuration = 50.millis,
      measurementPeriod = 200.millis,
      minNumberOfMeasurements = 16
    )

  // Re-entrance config: same stretched cadence and starvation floor as SlowStartConfig, but the estimate starts at
  // maxRate so the run opens on a healthy plateau. A degradation then cuts the rate and leaves the limiter unhealthy
  // (additive probing paused); once traffic stops, slow-start is the only thing that can move the rate.
  private val SlowStartReentranceConfig: Config =
    SlowStartConfig.copy(initialRate = rps(400))

  private val congestionSawtooth: Scenario =
    Scenario(
      name = "congestion-sawtooth",
      description = "Constant backend capacity below maxRate: AIMD oscillates around it in the classic TCP sawtooth.",
      config = BaseConfig,
      phases =
        NonEmptyList.one(Backend.Phase(hardCeiling = rps(200), softCeilings = Nil, duration = Warmup + 7.seconds))
    )

  private val quickDegradation: Scenario =
    Scenario(
      name = "quick-degradation",
      description = "A sudden capacity drop trips a band and cuts the rate into a lower sawtooth around the capacity.",
      config = BaseConfig,
      phases = NonEmptyList.of(
        Backend.Phase(hardCeiling = Healthy, softCeilings = Nil, duration = Warmup + 1.second),
        Backend.Phase(hardCeiling = rps(130), softCeilings = Nil, duration = 5.seconds)
      )
    )

  private val degradeThenRecover: Scenario =
    Scenario(
      name = "degrade-then-recover",
      description = "After a capacity collapse and rate cut, restored capacity returns the backend to healthy and max.",
      config = BaseConfig,
      phases = NonEmptyList.of(
        Backend.Phase(hardCeiling = Healthy, softCeilings = Nil, duration = Warmup + 1.second),
        Backend.Phase(hardCeiling = rps(130), softCeilings = Nil, duration = 4.seconds),
        Backend.Phase(hardCeiling = Healthy, softCeilings = Nil, duration = 5.seconds)
      )
    )

  private val slowRecovery: Scenario =
    Scenario(
      name = "slow-recovery",
      description = "Capacity recovers in steps; the limiter climbs back to a higher safe rate at each step.",
      config = BaseConfig,
      phases = NonEmptyList.of(
        Backend.Phase(hardCeiling = Healthy, softCeilings = Nil, duration = Warmup + 1.second),
        Backend.Phase(hardCeiling = rps(130), softCeilings = Nil, duration = 2500.millis), // severe
        Backend.Phase(hardCeiling = rps(200), softCeilings = Nil, duration = 2500.millis), // partial
        Backend.Phase(hardCeiling = rps(320), softCeilings = Nil, duration = 2500.millis), // more headroom
        Backend.Phase(hardCeiling = Healthy, softCeilings = Nil, duration = 3.seconds)     // full clear
      )
    )

  private val flapping: Scenario = {
    // Degraded windows longer than recovery windows so the cuts dominate and the rate ratchets down.
    val cycle = List(
      Backend.Phase(hardCeiling = rps(60), softCeilings = Nil, duration = 1500.millis),
      Backend.Phase(hardCeiling = Healthy, softCeilings = Nil, duration = 800.millis)
    )
    Scenario(
      name = "flapping",
      description = "Capacity flaps between healthy and degraded: compounding cuts ratchet the rate toward the floor.",
      config = BaseConfig,
      phases = NonEmptyList(
        Backend.Phase(hardCeiling = rps(60), softCeilings = Nil, duration = Warmup + 1500.millis),
        Backend.Phase(hardCeiling = Healthy, softCeilings = Nil, duration = 800.millis) :: List.fill(4)(cycle).flatten
      )
    )
  }

  private val slowDegradation: Scenario =
    Scenario(
      name = "slow-degradation",
      description = "Capacity steps down gradually; the limiter re-discovers a lower safe rate at each step.",
      config = BaseConfig,
      // Each step is gentle enough that the incoming rate stays in the mild band: a steep drop would throw the rate
      // into the severe band, where a single multiplicative cut can't clear the failure ratio and the rate creeps up.
      phases = NonEmptyList.of(
        Backend.Phase(hardCeiling = Healthy, softCeilings = Nil, duration = Warmup + 1.second),
        Backend.Phase(hardCeiling = rps(150), softCeilings = Nil, duration = 3.seconds), // mild sawtooth around it
        Backend.Phase(hardCeiling = rps(110), softCeilings = Nil, duration = 3.seconds), // lower sawtooth
        Backend.Phase(hardCeiling = rps(90), softCeilings = Nil, duration = 3.seconds)   // tighter sawtooth
      )
    )

  private val gradedDegradation: Scenario =
    Scenario(
      name = "graded-degradation",
      description = "A two-tier backend (soft + hard ceiling) produces two distinct failure levels across the bands.",
      config = BaseConfig,
      // Soft ceiling (half the hard ceiling) fails 50% of its overflow; over the hard ceiling, calls always fail.
      phases = NonEmptyList.of(
        Backend.Phase(
          hardCeiling = Healthy,
          softCeilings = List(Backend.SoftCeiling(capacity = rps(250), failProbability = 0.5)),
          duration = Warmup + 1.second
        ),
        Backend.Phase(
          hardCeiling = rps(80),
          softCeilings = List(Backend.SoftCeiling(capacity = rps(40), failProbability = 0.5)),
          duration = 5.seconds // soft then hard ceiling trip both bands
        ),
        Backend.Phase(
          hardCeiling = Healthy,
          softCeilings = List(Backend.SoftCeiling(capacity = rps(250), failProbability = 0.5)),
          duration = 4.seconds // restored: the backend recovers and the rate climbs back
        )
      )
    )

  private val slowStart: Scenario =
    Scenario(
      name = "slow-start",
      description =
        "Offered load starved below the sampling floor: with no measurements the limiter blindly doubles the rate " +
          "(TCP slow-start) until load returns and the window fills.",
      config = SlowStartConfig,
      // The backend stays healthy throughout; the story is the offered load, not the capacity. The lull offers 10 rps
      // (below the 20 rps floor) so every window is starved and slow-start drives the estimate up to maxRate while the
      // admitted throughput stays pinned at the offered load. Restoring full load fills the window and the estimate
      // settles at maxRate.
      phases = NonEmptyList.of(
        Backend.Phase(hardCeiling = Healthy, softCeilings = Nil, duration = Warmup + 2.seconds, offeredLoad = rps(10)),
        Backend.Phase(hardCeiling = Healthy, softCeilings = Nil, duration = 2.seconds)
      )
    )

  private val slowStartReentrance: Scenario =
    Scenario(
      name = "slow-start-reentrance",
      description =
        "A degradation cuts the rate and pauses probing; then traffic stops, so slow-start re-enters and re-probes " +
          "back up to the last good rate (ssthresh) instead of staying stuck at the floor.",
      config = SlowStartReentranceConfig,
      // Start healthy at maxRate. The mid phase degrades the backend enough to trip the first band once (a single cut
      // to ~half rate) and hold the failure ratio inside that band, so the limiter stays unhealthy and additive
      // probing is paused (a flat bottom). The final phase stops the traffic (offered load below the floor): every
      // window starves, so slow-start re-enters and doubles the rate back up to ssthresh (the pre-degrade rate) where
      // it caps, rather than blindly overshooting.
      phases = NonEmptyList.of(
        Backend.Phase(hardCeiling = Healthy, softCeilings = Nil, duration = Warmup + 1.second),
        Backend.Phase(hardCeiling = rps(150), softCeilings = Nil, duration = 2.seconds),
        Backend.Phase(hardCeiling = rps(150), softCeilings = Nil, duration = 2500.millis, offeredLoad = rps(10))
      )
    )

  val all: List[Scenario] = List(
    congestionSawtooth,
    quickDegradation,
    degradeThenRecover,
    slowRecovery,
    flapping,
    slowDegradation,
    gradedDegradation,
    slowStart,
    slowStartReentrance
  )
}
