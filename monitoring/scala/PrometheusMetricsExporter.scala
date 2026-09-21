//> using scala 3.3.3

// PrometheusMetricsExporter — Scala push helper used by heavy Scala jobs and SLO publisher
// Mirrors jobs/common/metrics.py but JVM-native. Emits text exposition format directly
// to Pushgateway so Grafana dashboards (01-pipeline … 05-error-budget) light up without Python.

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

object PrometheusMetricsExporter {
  private val gateway = sys.env.getOrElse("PUSHGATEWAY_URL", "http://localhost:19091")
  private val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()

  // push any gauge map — labels are Prometheus label set
  def push(job: String, gauges: Map[String, Seq[(Map[String, String], Double)]], runId: String = ""): Boolean = {
    if (gauges.isEmpty) return false
    val body = gauges.flatMap { case (name, series) =>
      val header = s"# TYPE $name gauge"
      val lines = series.map { case (labels, v) =>
        val all = if (runId.nonEmpty) labels + ("run_id" -> runId) else labels
        val lbl = if (all.isEmpty) "" else all.map { case (k, vv) => s"""$k="$vv"""" }.mkString("{", ",", "}")
        s"$name$lbl $v"
      }
      header +: lines
    }.mkString("\n") + "\n"
    try {
      val req = HttpRequest.newBuilder(URI.create(s"$gateway/metrics/job/$job"))
        .POST(HttpRequest.BodyPublishers.ofString(body)).header("Content-Type", "text/plain")
        .timeout(Duration.ofSeconds(5)).build()
      val r = client.send(req, HttpResponse.BodyHandlers.discarding())
      println(s"[ok] push $job http=${r.statusCode()} gauges=${gauges.size}")
      r.statusCode() / 100 == 2
    } catch { case e: Exception => println(s"[warn] pushgateway down: ${e.getMessage}"); false }
  }
}

@main def demoMetrics(args: String*): Unit = {
  // demo push — used to prove Grafana panels light up end-to-end
  val pushed = PrometheusMetricsExporter.push(
    "demo-scala",
    Map(
      "data_freshness_seconds" -> Seq(Map("table" -> "fct_transactions") -> 1234.0),
      "sla_breach_total"       -> Seq(Map.empty[String, String] -> 0.0),
      "dq_gate_pass_pct"       -> Seq(Map.empty[String, String] -> 99.2)
    ),
    runId = "scala-demo"
  )
  println(s"demo push result: $pushed")
}
