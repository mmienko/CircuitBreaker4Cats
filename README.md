# Resilience4Cats

[![Latest Release](https://img.shields.io/github/v/release/mmienko/resilience4cats?sort=semver)](https://github.com/mmienko/resilience4cats/releases)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.mmienko/resilience4cats_2.13)](https://central.sonatype.com/artifact/io.github.mmienko/resilience4cats_2.13)

Resilience structures not included in Cats Effect standard library, such as `CircuitBreaker`, `RateLimiter`,
`DynamicRateLimiter`, `AdaptiveRateLimiter`, and `AdmissionController`.

## Installation

Add the following to your `build.sbt`:

```scala
// All modules
libraryDependencies += "io.github.mmienko" %% "resilience4cats" % "<version>"

// Or individual modules
libraryDependencies += "io.github.mmienko" %% "circuit-breaker" % "<version>"
libraryDependencies += "io.github.mmienko" %% "rate-limiter" % "<version>"
libraryDependencies += "io.github.mmienko" %% "adaptive-rate-limiter" % "<version>"
libraryDependencies += "io.github.mmienko" %% "admission-controller" % "<version>"
```

## Rate-Limiter
The `rate-limiter` uses the GCRA algorithm, an equivalent to the token-bucket algorithm, to track the rate of requests.

### Usage
Rate-Limiters are build with a total capacity, an initial capacity that may be different than the total
capacity, and a linear refill rate. The refill rate has syntax extensions for a more human-readable format.

```scala
def apply[F[_]: Sync](
      capacity: Int,
      initialCapacity: Int,
      rate: RefillRate
  ): F[RateLimiter[F]]

final case class RefillRate(requests: Int, period: FiniteDuration)
```

For convenience, you can create a full rate limiter, with the initial capacity equal to the total capacity,
or an empty rate limiter, with the initial capacity equal to 0.

```scala
def empty[F[_]: Sync](capacity: Int, rate: RefillRate): F[RateLimiter[F]]

def full[F[_]: Sync](capacity: Int, rate: RefillRate): F[RateLimiter[F]]

```

Example with syntax extension for rate config:
```scala
import cats.effect._
import io.mienks.resilience.ratelimiter.RateLimiter
import io.mienks.resilience.ratelimiter.RateLimiter.syntax._
import scala.concurrent.duration._
for {
  rl <- RateLimiter.full[IO](capacity = 10, rate = rate"1 request / 1 second")
  consumed <- rl.consume()
  _ <- if (consumed) IO.println("Request allowed")
       else IO.println("Request rejected")
} yield ()
```

### DynamicRateLimiter
`DynamicRateLimiter` extends `RateLimiter` with runtime reconfiguration. The capacity and refill rate
can be atomically updated.

```scala
trait DynamicRateLimiter[F[_]] extends RateLimiter[F] {
  def setCapacity(capacity: Int): F[Unit]
  def setRefillRate(rate: RefillRate): F[Unit]
  def update(config: Config): F[Unit]
}
```

- `setCapacity` Atomically clamps the bucket to the new size when shrinking and never grants phantom tokens when growing.
- `setRefillRate` Atomically replace the refill rate. The number of currently-available tokens are preserved across the rate change.
- `update` combines both semantics atomically; `Config.initialCapacity` has no effect — only `capacity` and `refillRate`.

#### Usage

Builders mirror the `RateLimiter` builder API via `DynamicRateLimiter` or `RateLimiter.Dynamic`:

```scala
def apply[F[_]: Sync](config: Config): F[DynamicRateLimiter[F]]

def empty[F[_]: Sync](capacity: Int, rate: RefillRate): F[DynamicRateLimiter[F]]

def full[F[_]: Sync](capacity: Int, rate: RefillRate): F[DynamicRateLimiter[F]]
```

Example — adjusting the refill rate at runtime to implement an AIMD-style congestion control loop:
```scala
import cats.effect._
import io.mienks.resilience.ratelimiter.{DynamicRateLimiter, RateLimiter}
import io.mienks.resilience.ratelimiter.RateLimiter.RefillRate
import io.mienks.resilience.ratelimiter.RateLimiter.syntax._
import scala.concurrent.duration._

for {
  rl <- DynamicRateLimiter.full[IO](capacity = 100, rate = rate"10 requests / 1 second")

  // Additive increase: bump the rate after a successful probe window
  _ <- rl.setRefillRate(rate"20 requests / 1 second")

  // Multiplicative decrease: halve the rate when congestion is detected
  _ <- rl.setRefillRate(rate"10 requests / 1 second")

  // Update capacity + rate
  _ <- rl.update(RateLimiter.Config(capacity = 50, initialCapacity = 0, refillRate = rate"5 requests / 1 second"))
} yield ()
```

## Adaptive-Rate-Limiter
The `adaptive-rate-limiter` estimates and self-tunes the estimated rate at which a downstream resource (the
"protected sink") can be invoked. It wraps a `DynamicRateLimiter` whose refill rate is driven by an AIMD (Additive
Increase / Multiplicative Decrease) control loop reacting to observed failure rates.

Outcomes are recorded on a hot path with `recordSuccess` and `recordFailure` (cheap, lock-free). A background fiber
samples those counters over a time-based sliding window, categorizes the failure ratio into hysteresis bands to avoid
flapping, and feeds the result into the AIMD loop. Additive increase runs on a fixed tick; multiplicative decrease
fires when the categorizer reports a worsening signal.

```scala
trait AdaptiveRateLimiter[F[_]] {
  def consume: F[Boolean]
  def recordSuccess: F[Unit]
  def recordFailure: F[Unit]
  def rate: F[Rate]
  def failureRatio: F[Double]
}
```

### Usage

`AdaptiveRateLimiter.start` returns a `Resource` that owns the background control loop. Call `consume` before invoking
the protected sink; record the outcome afterward (or use the `protect` syntax extension to do both in one step).

```scala
import cats.data.NonEmptyList
import cats.effect._
import io.mienks.resilience.Rate
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.syntax._
import scala.concurrent.duration._

val failureLevels = NonEmptyList.of(
  AdaptiveRateLimiter.HysteresisBand(exit = 0.1, start = 0.3), // minor degradation
  AdaptiveRateLimiter.HysteresisBand(exit = 0.4, start = 0.7), // severe degradation
)

val config = AdaptiveRateLimiter.Config.fromRps(
  minRps = 10,
  maxRps = 100,
  rpsIncreaseRate = Rate(requests = 5, period = 1.second),
  rpsDecrease = 0.5,
  timeRangeForMeasurementInSeconds = 60,
  failureLevels = failureLevels,
)

AdaptiveRateLimiter.start[IO](config = config).use { limiter =>
  limiter.protect(
    fa = callProtectedSink,
    isError = _.isLeft,
    orElse = Left("rate limited"),
  )
}
```

When the limiter is exhausted, `protect` returns `orElse` without recording an outcome. Use `protectF` when the
failure classifier itself is effectful.

### Configuration

`Config` ties together the rate limiter, measurement window, hysteresis bands, and AIMD parameters. The rate-shaped
fields (`initialRate`, `minRate`, `maxRate`, `rateIncreaseBy`) describe the AIMD's estimate of sustainable throughput;
the underlying `DynamicRateLimiter` is always configured at that estimate.

- `capacity` — maximum burst size of the underlying rate limiter
- `initialRate` / `minRate` / `maxRate` — starting estimate and AIMD floor/ceiling
- `rateIncreaseBy` / `rateIncreasePeriod` — additive increase step and tick interval
- `rateDecreaseBy` — multiplicative decrease in `[0, 1]`; on each `FailureGradient.Worsening` band crossed the estimate becomes `(1 - rateDecreaseBy) * current`
- `numberOfSlotsForMeasurements` / `slotDuration` — time-bucket slots in the failure-rate sliding window
- `measurementPeriod` — how often the background fiber samples counters
- `minNumberOfMeasurements` — minimum samples before a failure ratio is reported
- `insufficientDataRateGrowthFactor` — slow-start multiplier (`> 1`); while the window holds fewer than `minNumberOfMeasurements` samples the estimate is grown by this factor each window to re-initialize measurements (defaults to `2.0`, TCP-style doubling)
- `failureLevels` — ordered (least- to most-severe) hysteresis bands on failure ratio

Each `HysteresisBand` has an `exit` and `start` threshold (`start >= exit`). A band engages when the failure ratio
rises to `start` and releases only after it falls to `exit` or below, which prevents oscillation around a single
point.

`Config.fromRps` is a convenience builder for RPS-shaped setups: capacity is set to `maxRps`, slots are one second
wide, and `minNumberOfMeasurements` equals the slot count.

Optional callbacks fire on control-loop events:

```scala
AdaptiveRateLimiter.start[IO](
  config = config,
  onFailureCategoryChange = (event: AdaptiveRateLimiter.FailureGradient) => IO.println(s"gradient: $event"),
  onRateChange = (rate: Rate) => IO.println(s"rate: $rate"),
)
```

The categorizer emits `FailureGradient.Worsening(level)` (one event per band crossed on worsening),
`FailureGradient.Recovering(fromLevel)` on partial recovery, and `FailureGradient.Recovered` on full recovery. The AIMD
controller multiplicatively decreases on every `Worsening` and additively increases on a fixed tick, but the additive
increase is gated on health: probing pauses once a band trips and resumes only after `Recovered`. When the window
holds too few samples to report a ratio, the controller has no signal to act on, so it runs TCP-style slow-start —
growing the estimate by `insufficientDataRateGrowthFactor` each window — to escape the low-rate live-lock and
re-initialize measurements.

### Behavior

Driven against a simulated backend whose failure rate depends on the offered load, the AIMD loop produces the classic
TCP-style sawtooth: additive increase probes for more throughput until the backend overloads, then a multiplicative
decrease backs off.

![congestion-sawtooth](docs/images/adaptive-rate-limiter/congestion-sawtooth.png)

If the offered load falls below the sampling floor, no window collects enough samples to report a failure ratio. Rather
than deadlock at a low rate (additive increase is paused while unhealthy), the limiter runs TCP-style slow-start,
blindly doubling the estimate each window until load returns or it reaches `maxRate`.

![slow-start](docs/images/adaptive-rate-limiter/slow-start.png)

See [`charts/`](charts/README.md) for how these charts are generated and for more scenarios (degradation, recovery,
flapping, and graded multi-tier backends).

## Admission-Controller
The `admission-controller` implements probabilistic client-side throttling from the
[SRE adaptive throttling pattern](https://sre.google/sre-book/handling-overload/). It tracks recent requests and
backend accepts over a sliding window, then locally sheds a proportional share of traffic when the backend is
rejecting too many requests.

```scala
trait AdmissionController[F[_]] {
  def allow: F[Boolean]
  def record(isFailure: Boolean): F[Measurements.Snapshot]
  def rejectionProbability: F[Double]
}
```

### Usage
Use `allow` and `record` directly, or import syntax to gate and record in one step. Local rejections are recorded as
failures so the controller sees them as non-accepts and stabilizes through the sliding window.

```scala
import cats.effect._
import io.mienks.resilience.admissioncontroller.AdmissionController
import io.mienks.resilience.admissioncontroller.AdmissionController.syntax._

AdmissionController[IO]().flatMap { controller =>
  controller.protect(
    fa = callProtectedSink,
    isFailure = _.isThrottledByBackend,
    orElse = RejectedLocally,
  )
}
```

### Behavior
These charts drive the controller through a simulated backend (see [`charts`](charts)). Under overload it sheds the
excess so the client allowed rate holds near `k * capacity` and the backend actual rate (goodput) stays near capacity,
and on recovery the rejection probability returns to zero. On the right axis the dotted **failure ratio** keeps rising
with throttling while the **rejection probability** stays at zero inside the dead zone — the gap between them is the
dead zone.

### Tuning
Each request is rejected with probability `max(0, (requests - k * accepts) / (requests + 1))` over the window. The `k`
parameter (default `2.0`) tunes how aggressively load is shed: higher `k` sheds less, lower `k` sheds more. At `k = 1`
this is just the plain failure ratio, but `k = 1` is only marginally stable and can stay latched while shedding even
after the backend recovers; `k > 1` guarantees the controller fully reopens once the backend is healthy and adds a dead
zone so transient failures don't trigger shedding. Configure `k` and the sliding window via `AdmissionController.Config`.

The same constant-overload run at three values of `k` shows the trade-off: the admitted (client allowed) plateau lands
around `k * capacity`, so higher `k` admits more while lower `k` sheds harder. At `k = 1` the loop is only marginally
stable and under-utilizes the backend; `k = 2` keeps goodput at capacity with headroom to keep probing.

`k = 1` (plain failure ratio - jittery, under-utilizes):

![steady-overload-k1](docs/images/admission-controller/steady-overload-k1.png)

`k = 1.5` (middle ground):

![steady-overload-k1.5](docs/images/admission-controller/steady-overload-k1.5.png)

`k = 2` (default - goodput at capacity, smooth):

![steady-overload](docs/images/admission-controller/steady-overload.png)

Shedding and full reopen on recovery (`k = 2`):

![drop-then-recover](docs/images/admission-controller/drop-then-recover.png)

With `k = 1`, the gate is slow to reopen: after capacity is restored the rejection probability lingers instead of
snapping back to zero (the marginal-stability latching that motivates the `k > 1` default):

![drop-then-recover-k1](docs/images/admission-controller/drop-then-recover-k1.png)

## Circuit-Breaker
The `circuit-breaker` models a concurrent state machine used to provide stability and prevent cascading failures in
distributed systems. 

It can be in any of these 3 states:

1. `CircuitBreaker.Closed`: The starting state, all effects are evaluated. Outcomes are recorded over a sliding
   window. When the failure rate reaches the `failureRateThreshold`, the breaker is tripped into `Open` state.
1. `CircuitBreaker.Open`: The state where all tasks are rejected with `CircuitBreaker.RejectedExecution` until
     the `resetTimeout` has passed. The next call to the circuit breaker will move the state into `Half-Open`.
1. `CircuitBreaker.HalfOpen`: The state which allows `numberOfHalfOpenCalls` tasks to go through as a way of
     testing the protected resource. If all those tasks succeed, then the circuit breaker is set to `Closed` and
     counters reset. If there are any failures, then the circuit breaker is reset back to `Open` with another reset
     timeout according to `backoff` policy, but no longer than `maxResetTimeout`.

### Usage

```scala
import cats.effect._
import io.mienks.resilience.circuitbreaker
import scala.concurrent.duration._

def isLessThanPointOne(d: Double): Boolean = d < 0.1
for {
    cb <- CircuitBreaker[IO]()
    intOrFail = IO {
      val i = Math.random()
      if (i > 0.5) throw new RuntimeException("error")
      else i
    }
    _ <- cb.protect(intOrFail)
    _ <- cb.protectIf(isError = isLessThanPointOne)(intOrFail)
} yield ()
```

You can fully configure the circuit breaker like so

```scala
CircuitBreaker.of[IO](
    measurementStrategy = MeasurementStrategy.FixedSlidingWindow[IO](numberOfMeasurements = 100),
    failureRateThreshold = 1.0,
    resetTimeout = 10.seconds,
    numberOfHalfOpenCalls = 1,
    backoff = Backoff.exponential,
    maxResetTimeout = 1.minute,
    exceptionFilter = Function.const(true),
    onRejected = IO.unit,
    onOpen = IO.unit,
    onHalfOpen = IO.unit,
    onClosed = IO.unit,
)
```

You can choose between a count-based sliding window, `CircuitBreaker.MeasurementStrategy.CountBasedSlidingWindow`,
a time-based sliding window, `CircuitBreaker.MeasurementStrategy.TimeBasedSlidingWindow`, or a custom window,
`CircuitBreaker.MeasurementStrategy.Custom`, for the `measurementStrategy`. The count-based sliding window
aggregates the outcome of the last N calls. The time-based sliding window aggregates the outcome over the last
specified duration.

In the sample above, we attempt to restest the protected resource after 10 seconds, then after 20, 40 and so on, a
delay that keeps increasing until the configurable maximum of 1 minute.

It's important that the task passed to the `protect` and `protectIf` methods timeout, and specifically timeout
before the `resetTimeout`.

## Credits
Inspired by Christopher Davenport's [circuit library](https://github.com/ChristopherDavenport/circuit) and
resilience4j's [Circuit Breaker](https://resilience4j.readme.io/docs/circuitbreaker).
