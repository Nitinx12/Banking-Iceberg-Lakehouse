// dbt/monitor/dbt-report.scala — dbt run_results.json to console report + Pushgateway metrics
// (Architecture 12.1: pipeline metrics reach Prometheus via Pushgateway)
//
// Run after a dbt build (run_results.json lands in <project>/target/):
//   scala-cli run dbt/monitor/dbt-report.scala -- dbt/banking_dbt/target/run_results.json
// Optional push (monitoring profile):
//   PUSHGATEWAY_URL=http://localhost:9091 scala-cli run dbt/monitor/dbt-report.scala -- <path>
//
// Exits non-zero if any model errored or failed, so it can gate an Airflow BashOperator.

//> using scala 3.3.3
//> using dep com.lihaoyi::upickle:3.3.0

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.{Duration, OffsetDateTime}
import scala.io.Source
import scala.util.Using

@main def dbtReport(args: String*): Unit =
  val path = args.headOption.getOrElse("dbt/banking_dbt/target/run_results.json")
  val raw =
    Using.resource(Source.fromFile(path))(_.mkString) // fails loudly if dbt hasn't run yet
  val json = ujson.read(raw)

  val invocationId = json("metadata")("invocation_id").strOpt.getOrElse("unknown")
  val results = json("results").arr.toSeq

  case class Row(model: String, resource: String, status: String, seconds: Double)

  val rows = results.map { r =>
    val uid = r("unique_id").str
    val (resource, name) = uid.split('.') match
      case Array(kind, _, n) => (kind, n)
      case _                 => ("unknown", uid)
    val seconds = r("timing").arrOpt
      .flatMap(_.find(t => t("name").strOpt.contains("execute")))
      .flatMap { t =>
        for
          s <- t("started_at").strOpt
          e <- t("completed_at").strOpt
          d = Duration.between(OffsetDateTime.parse(s), OffsetDateTime.parse(e))
        yield d.toMillis / 1000.0
      }
      .getOrElse(0.0)
    Row(name, resource, r("status").strOpt.getOrElse("unknown"), seconds)
  }

  // Console report — quick triage without opening Data Docs
  println(s"dbt run $invocationId — ${rows.size} results")
  val width = rows.foldLeft(5)((m, r) => math.max(m, r.model.length))
  for r <- rows.sortBy(_.seconds)(Ordering.Double.TotalOrdering).reverse do
    println(f"  ${r.model.padTo(width, ' ')} ${r.status}%-8s ${r.seconds}%8.2fs")

  val failed = rows.filter(r => r.status == "error" || r.status == "fail")
  if failed.nonEmpty then
    println(s"[FAIL] ${failed.size} model(s) failed: ${failed.map(_.model).mkString(", ")}")

  // Optional metrics push — text exposition format, one PUT per job
  sys.env.get("PUSHGATEWAY_URL").foreach { base =>
    val body = rows
      .flatMap { r =>
        List(
          s"""dbt_model_duration_seconds{model="${r.model}",resource="${r.resource}",status="${r.status}"} ${r.seconds}""",
          s"""dbt_model_status{model="${r.model}",resource="${r.resource}",status="${r.status}"} 1"""
        )
      }
      .appended(s"""dbt_run_failures{invocation="$invocationId"} ${failed.size}""")
      .mkString("\n", "\n", "\n")

    val req = HttpRequest
      .newBuilder(URI.create(s"$base/metrics/job/dbt/invocation/$invocationId"))
      .PUT(HttpRequest.BodyPublishers.ofString(body))
      .timeout(Duration.ofSeconds(10))
      .build()
    val resp = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.discarding())
    println(s"pushed metrics to $base — HTTP ${resp.statusCode()}")
  }

  if failed.nonEmpty then sys.exit(1)
