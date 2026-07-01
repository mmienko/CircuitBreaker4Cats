package io.mienks.resilience.charts

import cats.effect.{IO, Sync}
import cats.syntax.all._
import io.mienks.resilience.adaptiveratelimiter.AdaptiveRateLimiter.FailureGradient

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

/** One row of the manifest the Python renderer iterates over. */
final case class ManifestEntry(
    scenario: String,
    description: String,
    samplesFile: String,
    eventsFile: String
)

/** Writes the timeseries of a run as plain CSV plus a manifest, so the matplotlib renderer needs no Scala knowledge. */
object CsvWriter {

  private val SamplesHeader: String =
    "elapsed_ms,aimd_rps,admitted_rps,backend_capacity_rps,observed_failure_ratio,slow_start_active"
  private val EventsHeader: String   = "elapsed_ms,kind,detail"
  private val ManifestHeader: String = "scenario,description,samples_file,events_file"

  /** Write `<scenario>-samples.csv` and `<scenario>-events.csv` into `dataDir`, returning the manifest entry. */
  def writeScenario(result: SimulationRunner.Result, dataDir: Path): IO[ManifestEntry] = {
    val samplesFile = s"${result.scenario.name}-samples.csv"
    val eventsFile  = s"${result.scenario.name}-events.csv"

    val samplesContent =
      (SamplesHeader +: result.samples.map { sample =>
        s"${sample.elapsedMillis.toString}," +
          s"${sample.aimdRps.toString}," +
          s"${sample.admittedRps.toString}," +
          s"${sample.backendCapacityRps.toString}," +
          s"${sample.observedFailureRatio.toString}," +
          (if (sample.slowStartActive) "1" else "0")
      }).mkString("\n")

    val rateRows =
      result.rateEvents.map { case (ms, rps) => (ms, "rate_change", rps.toString) }
    val gradientRows =
      result.gradientEvents.map { case (ms, gradient) =>
        val (kind, detail) = describe(gradient)
        (ms, kind, detail)
      }
    val eventsContent =
      (EventsHeader +: (rateRows ++ gradientRows).sortBy(_._1).map { case (ms, kind, detail) =>
        s"${ms.toString},$kind,$detail"
      }).mkString("\n")

    writeFile(dataDir.resolve(samplesFile), samplesContent) >>
      writeFile(dataDir.resolve(eventsFile), eventsContent).as(
        ManifestEntry(
          scenario = result.scenario.name,
          description = result.scenario.description,
          samplesFile = samplesFile,
          eventsFile = eventsFile
        )
      )
  }

  /** Write `manifest.csv` listing every scenario; returns its path. */
  def writeManifest(entries: List[ManifestEntry], dataDir: Path): IO[Path] = {
    val manifest = dataDir.resolve("manifest.csv")
    val content  =
      (ManifestHeader +: entries.map { entry =>
        s"${escape(entry.scenario)},${escape(entry.description)},${escape(entry.samplesFile)},${escape(entry.eventsFile)}"
      }).mkString("\n")
    writeFile(manifest, content).as(manifest)
  }

  private def describe(gradient: FailureGradient): (String, String) =
    gradient match {
      case FailureGradient.Worsening(toLevel)    => ("worsening", toLevel.toString)
      case FailureGradient.Recovering(fromLevel) => ("recovering", fromLevel.toString)
      case FailureGradient.Recovered             => ("recovered", "")
    }

  private def writeFile(path: Path, content: String): IO[Unit] =
    Sync[IO].blocking {
      Files.write(path, content.getBytes(StandardCharsets.UTF_8))
      ()
    }

  /** Minimal RFC-4180 escaping: quote fields containing a comma, quote, or newline. */
  private def escape(field: String): String =
    if (field.exists(c => c == ',' || c == '"' || c == '\n' || c == '\r'))
      "\"" + field.replace("\"", "\"\"") + "\""
    else field
}
