package io.mienks.resilience.ratelimiter

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all._
import io.mienks.resilience.ratelimiter.RateLimiter.{Config, RefillRate}

import scala.concurrent.duration._

class DynamicGCRATests extends RateLimiterTests {

  override protected def buildFullRateLimiter(capacity: Int, refillRate: RefillRate): IO[RateLimiter[IO]] =
    DynamicGCRA.full[IO](capacity, refillRate)

  override protected def buildEmptyRateLimiter(capacity: Int, refillRate: RefillRate): IO[RateLimiter[IO]] =
    DynamicGCRA.empty[IO](capacity, refillRate)

  // --- input validation ---

  test("consume rejects zero tokens") {
    for {
      rl <- DynamicGCRA.full[IO](capacity = 5, RefillRate(1, 1.second))
      _  <- interceptIO[IllegalArgumentException](rl.consume(0))
    } yield ()
  }

  test("consume rejects negative tokens") {
    for {
      rl <- DynamicGCRA.full[IO](capacity = 5, RefillRate(1, 1.second))
      _  <- interceptIO[IllegalArgumentException](rl.consume(-1))
    } yield ()
  }

  test("apply validates config") {
    interceptIO[IllegalArgumentException](
      DynamicGCRA[IO](capacity = 0, initialCapacity = 0, RefillRate(1, 1.second))
    )
  }

  // --- mutation validation ---

  test("setCapacity validates positive capacity") {
    for {
      rl <- DynamicGCRA.full[IO](capacity = 5, RefillRate(1, 1.second))
      _  <- interceptIO[IllegalArgumentException](rl.setCapacity(0))
      _  <- interceptIO[IllegalArgumentException](rl.setCapacity(-1))
    } yield ()
  }

  test("setRefillRate rejects rate that overflows emission * capacity") {
    for {
      rl <- DynamicGCRA.full[IO](capacity = Int.MaxValue, RefillRate(1, 1.second))
      _  <- interceptIO[ArithmeticException](rl.setRefillRate(RefillRate(1, 24.hours)))
    } yield ()
  }

  test("setCapacity rejects capacity that overflows emission * capacity") {
    for {
      rl <- DynamicGCRA.full[IO](capacity = 1, RefillRate(1, 24.hours))
      _  <- interceptIO[ArithmeticException](rl.setCapacity(Int.MaxValue))
    } yield ()
  }

  test("update rejects invalid config") {
    for {
      rl <- DynamicGCRA.full[IO](capacity = 5, RefillRate(1, 1.second))
      _  <- interceptIO[IllegalArgumentException](
        rl.update(Config(capacity = 0, initialCapacity = 0, refillRate = RefillRate(1, 1.second)))
      )
    } yield ()
  }

  // --- mutations ---

  // setCapacity matrix: {identical, shrink, grow} x {full, empty, partial}

  test("setCapacity identical: full bucket preserves state") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.full[IO](capacity = 10, RefillRate(1, 1.second))
        _  <- rl.requests.map(assertEquals(_, 10))
        _  <- rl.setCapacity(10)
        _  <- rl.capacity.map(assertEquals(_, 10))
        _  <- rl.requests.map(assertEquals(_, 10))

        _ <- IO.sleep(10.seconds)
        _ <- rl.requests.map(assertEquals(_, 10))
      } yield ()
    }
  }

  test("setCapacity identical: empty bucket preserves state") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.empty[IO](capacity = 10, RefillRate(1, 1.second))
        _  <- rl.requests.map(assertEquals(_, 0))
        _  <- rl.setCapacity(10)
        _  <- rl.capacity.map(assertEquals(_, 10))
        _  <- rl.requests.map(assertEquals(_, 0))

        _ <- IO.sleep(10.seconds)
        _ <- rl.requests.map(assertEquals(_, 10))
      } yield ()
    }
  }

  test("setCapacity identical: partial bucket preserves state") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.full[IO](capacity = 10, RefillRate(1, 1.second))
        _  <- rl.consume(5).map(assertEquals(_, true))
        _  <- rl.requests.map(assertEquals(_, 5))
        _  <- rl.setCapacity(10)
        _  <- rl.capacity.map(assertEquals(_, 10))
        _  <- rl.requests.map(assertEquals(_, 5))

        _ <- IO.sleep(10.seconds)
        _ <- rl.requests.map(assertEquals(_, 10))
      } yield ()
    }
  }

  test("setCapacity shrink: full bucket clamps to newCap") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.full[IO](capacity = 10, RefillRate(1, 1.second))
        _  <- rl.requests.map(assertEquals(_, 10))
        _  <- rl.setCapacity(3)
        _  <- rl.capacity.map(assertEquals(_, 3))
        _  <- rl.requests.map(assertEquals(_, 3))

        _ <- IO.sleep(3.seconds)
        _ <- rl.requests.map(assertEquals(_, 3))
      } yield ()
    }
  }

  test("setCapacity shrink: empty bucket clamps to newCap") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.empty[IO](capacity = 10, RefillRate(1, 1.second))
        _  <- rl.requests.map(assertEquals(_, 0))
        _  <- rl.setCapacity(3)
        _  <- rl.capacity.map(assertEquals(_, 3))
        _  <- rl.requests.map(assertEquals(_, 0))

        _ <- IO.sleep(3.seconds)
        _ <- rl.requests.map(assertEquals(_, 3))
      } yield ()
    }
  }

  test("setCapacity shrink: partial bucket clamps to newCap") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.full[IO](capacity = 10, RefillRate(1, 1.second))
        _  <- rl.consume(5).map(assertEquals(_, true))
        _  <- rl.requests.map(assertEquals(_, 5))
        _  <- rl.setCapacity(3)
        _  <- rl.capacity.map(assertEquals(_, 3))
        _  <- rl.requests.map(assertEquals(_, 3))

        _ <- IO.sleep(3.seconds)
        _ <- rl.requests.map(assertEquals(_, 3))
      } yield ()
    }
  }

  test("setCapacity grow: full bucket preserves availability") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.full[IO](capacity = 10, RefillRate(1, 1.second))
        _  <- rl.requests.map(assertEquals(_, 10))
        _  <- rl.setCapacity(50)
        _  <- rl.capacity.map(assertEquals(_, 50))
        _  <- rl.requests.map(assertEquals(_, 10))

        _ <- IO.sleep(50.seconds)
        _ <- rl.requests.map(assertEquals(_, 50))
      } yield ()
    }
  }

  test("setCapacity grow: empty bucket preserves availability") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.empty[IO](capacity = 10, RefillRate(1, 1.second))
        _  <- rl.requests.map(assertEquals(_, 0))
        _  <- rl.setCapacity(50)
        _  <- rl.capacity.map(assertEquals(_, 50))
        _  <- rl.requests.map(assertEquals(_, 0))

        _ <- IO.sleep(50.seconds)
        _ <- rl.requests.map(assertEquals(_, 50))
      } yield ()
    }
  }

  test("setCapacity grow: partial bucket preserves availability") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.full[IO](capacity = 10, RefillRate(1, 1.second))
        _  <- rl.consume(5).map(assertEquals(_, true))
        _  <- rl.requests.map(assertEquals(_, 5))
        _  <- rl.setCapacity(50)
        _  <- rl.capacity.map(assertEquals(_, 50))
        _  <- rl.requests.map(assertEquals(_, 5))

        _ <- IO.sleep(50.seconds)
        _ <- rl.requests.map(assertEquals(_, 50))
      } yield ()
    }
  }

  // setRefillRate matrix: {same, increase, decrease} x {full, empty, partial}

  test("setRefillRate same: full bucket preserves state") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.full[IO](capacity = 10, RefillRate(1, 1.second))
        _  <- rl.requests.map(assertEquals(_, 10))
        _  <- rl.setRefillRate(RefillRate(1, 1.second))
        _  <- rl.capacity.map(assertEquals(_, 10))
        _  <- rl.requests.map(assertEquals(_, 10))

        _ <- IO.sleep(10.seconds)
        _ <- rl.requests.map(assertEquals(_, 10))
      } yield ()
    }
  }

  test("setRefillRate same: empty bucket fills at unchanged rate") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.empty[IO](capacity = 10, RefillRate(1, 1.second))
        _  <- rl.requests.map(assertEquals(_, 0))
        _  <- rl.setRefillRate(RefillRate(1, 1.second))
        _  <- rl.capacity.map(assertEquals(_, 10))
        _  <- rl.requests.map(assertEquals(_, 0))

        _ <- IO.sleep(0.9.second)
        _ <- rl.requests.map(assertEquals(_, 0))
        _ <- IO.sleep(1.second)
        _ <- rl.requests.map(assertEquals(_, 1))
        _ <- IO.sleep(9.seconds)
        _ <- rl.requests.map(assertEquals(_, 10))
        _ <- IO.sleep(1.second)
        _ <- rl.requests.map(assertEquals(_, 10))
      } yield ()
    }
  }

  test("setRefillRate same: partial bucket fills at unchanged rate") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.full[IO](capacity = 10, RefillRate(1, 1.second))
        _  <- rl.consume(5).map(assertEquals(_, true))
        _  <- rl.requests.map(assertEquals(_, 5))
        _  <- rl.setRefillRate(RefillRate(1, 1.second))
        _  <- rl.capacity.map(assertEquals(_, 10))
        _  <- rl.requests.map(assertEquals(_, 5))

        _ <- IO.sleep(1.second)
        _ <- rl.requests.map(assertEquals(_, 6))
        _ <- IO.sleep(4.seconds)
        _ <- rl.requests.map(assertEquals(_, 10))
      } yield ()
    }
  }

  test("setRefillRate increase: full bucket preserves state") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.full[IO](capacity = 10, RefillRate(1, 1.second))
        _  <- rl.requests.map(assertEquals(_, 10))
        _  <- rl.setRefillRate(RefillRate(1, 500.millis))
        _  <- rl.capacity.map(assertEquals(_, 10))
        _  <- rl.requests.map(assertEquals(_, 10))

        _ <- IO.sleep(5.seconds)
        _ <- rl.requests.map(assertEquals(_, 10))
      } yield ()
    }
  }

  test("setRefillRate increase: empty bucket fills at faster rate") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.empty[IO](capacity = 10, RefillRate(1, 1.second))
        _  <- rl.requests.map(assertEquals(_, 0))
        _  <- rl.setRefillRate(RefillRate(1, 500.millis))
        _  <- rl.capacity.map(assertEquals(_, 10))
        _  <- rl.requests.map(assertEquals(_, 0))

        _ <- IO.sleep(500.millis)
        _ <- rl.requests.map(assertEquals(_, 1))
        _ <- IO.sleep(4500.millis)
        _ <- rl.requests.map(assertEquals(_, 10))
      } yield ()
    }
  }

  test("setRefillRate increase: partial bucket fills at faster rate") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.full[IO](capacity = 10, RefillRate(1, 1.second))
        _  <- rl.consume(5).map(assertEquals(_, true))
        _  <- rl.requests.map(assertEquals(_, 5))
        _  <- rl.setRefillRate(RefillRate(1, 500.millis))
        _  <- rl.capacity.map(assertEquals(_, 10))
        _  <- rl.requests.map(assertEquals(_, 5))

        _ <- IO.sleep(500.millis)
        _ <- rl.requests.map(assertEquals(_, 6))
        _ <- IO.sleep(2.seconds)
        _ <- rl.requests.map(assertEquals(_, 10))
      } yield ()
    }
  }

  test("setRefillRate decrease: full bucket preserves state") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.full[IO](capacity = 10, RefillRate(1, 1.second))
        _  <- rl.requests.map(assertEquals(_, 10))
        _  <- rl.setRefillRate(RefillRate(1, 2.seconds))
        _  <- rl.capacity.map(assertEquals(_, 10))
        _  <- rl.requests.map(assertEquals(_, 10))

        _ <- IO.sleep(20.seconds)
        _ <- rl.requests.map(assertEquals(_, 10))
      } yield ()
    }
  }

  test("setRefillRate decrease: empty bucket fills at slower rate") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.empty[IO](capacity = 10, RefillRate(1, 1.second))
        _  <- rl.requests.map(assertEquals(_, 0))
        _  <- rl.setRefillRate(RefillRate(1, 2.seconds))
        _  <- rl.capacity.map(assertEquals(_, 10))
        _  <- rl.requests.map(assertEquals(_, 0))

        _ <- IO.sleep(2.seconds)
        _ <- rl.requests.map(assertEquals(_, 1))
        _ <- IO.sleep(18.seconds)
        _ <- rl.requests.map(assertEquals(_, 10))
      } yield ()
    }
  }

  test("setRefillRate decrease: partial bucket fills at slower rate") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.full[IO](capacity = 10, RefillRate(1, 1.second))
        _  <- rl.consume(5).map(assertEquals(_, true))
        _  <- rl.requests.map(assertEquals(_, 5))
        _  <- rl.setRefillRate(RefillRate(1, 2.seconds))
        _  <- rl.capacity.map(assertEquals(_, 10))
        _  <- rl.requests.map(assertEquals(_, 5))

        _ <- IO.sleep(2.seconds)
        _ <- rl.requests.map(assertEquals(_, 6))
        _ <- IO.sleep(8.seconds)
        _ <- rl.requests.map(assertEquals(_, 10))
      } yield ()
    }
  }

  test("setRefillRate churn: each transition refills at the new rate") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.empty[IO](capacity = 100, RefillRate(1, 1.second))

        _ <- IO.sleep(1.second)
        _ <- rl.consumeRemaining().map(assertEquals(_, 1))

        _ <- rl.setRefillRate(RefillRate(2, 1.second))
        _ <- IO.sleep(1.second)
        _ <- rl.consumeRemaining().map(assertEquals(_, 2))

        _ <- rl.setRefillRate(RefillRate(4, 1.second))
        _ <- IO.sleep(1.second)
        _ <- rl.consumeRemaining().map(assertEquals(_, 4))

        _ <- rl.setRefillRate(RefillRate(2, 1.second))
        _ <- IO.sleep(1.second)
        _ <- rl.consumeRemaining().map(assertEquals(_, 2))

        _ <- rl.setRefillRate(RefillRate(1, 1.second))
        _ <- IO.sleep(1.second)
        _ <- rl.consumeRemaining().map(assertEquals(_, 1))

        _ <- rl.capacity.map(assertEquals(_, 100))
      } yield ()
    }
  }

  // single update test

  test("update applies both capacity and rate atomically") {
    TestControl.executeEmbed {
      for {
        rl <- DynamicGCRA.full[IO](capacity = 10, RefillRate(1, 1.second))
        _  <- rl.consume(5).map(assertEquals(_, true))
        _  <- rl.requests.map(assertEquals(_, 5))
        _  <- rl.update(Config(capacity = 20, initialCapacity = 0, refillRate = RefillRate(1, 100.millis)))
        _  <- rl.capacity.map(assertEquals(_, 20))
        _  <- rl.requests.map(assertEquals(_, 5))
      } yield ()
    }
  }

  // --- concurrent ---

  private val isConsumed: Boolean => Boolean = identity

  test("concurrent burst consumes at most the bucket capacity") {
    val n = 200
    for {
      rl      <- DynamicGCRA.full[IO](capacity = n, RefillRate(1, 1.minute))
      results <- rl.consume().parReplicateA(n * 2)
    } yield assertEquals(results.count(isConsumed), n)
  }

  test("concurrent consumeRemaining burst consumes at most the bucket capacity") {
    val n = 200
    for {
      rl      <- DynamicGCRA.full[IO](capacity = n, RefillRate(1, 1.minute))
      results <- rl.consumeRemaining().parReplicateA(20)
    } yield assertEquals(results.sum, n)
  }

  test("concurrent consume during repeated setCapacity shrinks, is bounded by starting capacity") {
    val n = 200
    for {
      rl <- DynamicGCRA.full[IO](capacity = n, RefillRate(1, 1.hour))
      consumer = rl.consume().parReplicateA(n * 4)
      flipper  = rl.setCapacity(n / 2).replicateA_(50)
      pair <- (consumer, flipper).parTupled
      (results, _) = pair
      tokens       = results.count(isConsumed)
    } yield assert(
      tokens <= n,
      s"granted $tokens tokens; expected at most $n (max capacity ever set)"
    )
  }

  test("concurrent consume during repeated setRefillRate grows, is bounded by starting capacity") {
    val n = 100
    for {
      rl <- DynamicGCRA.full[IO](capacity = n, RefillRate(1, 1.hour))
      consumer = rl.consume().parReplicateA(n * 4)
      flipper  = rl
        .setRefillRate(RefillRate(1, 30.minutes))
        .replicateA_(50)
      pair <- (consumer, flipper).parTupled
      (results, _) = pair
      tokens       = results.count(isConsumed)
    } yield assert(
      tokens <= n,
      s"granted $tokens tokens; expected at most $n (capacity)"
    )
  }

  test("concurrent consume during oscillating setRefillRate is bounded by starting capacity") {
    val n = 100
    for {
      rl <- DynamicGCRA.full[IO](capacity = n, RefillRate(1, 1.hour))
      consumer = rl.consume().parReplicateA(n * 4)
      flipper  = (
        rl.setRefillRate(RefillRate(4, 1.hour)) >>
          rl.setRefillRate(RefillRate(1, 1.hour))
      ).replicateA_(25)
      pair <- (consumer, flipper).parTupled
      (results, _) = pair
      tokens       = results.count(isConsumed)
    } yield assert(
      tokens <= n,
      s"granted $tokens tokens; expected at most $n (capacity)"
    )
  }

}
