package jobs.transform.scala

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration

// Pushgateway helper for heavy Scala jobs — mirrors jobs/common/metrics.py (Architecture 12.1)
// Best-effort: missing gateway never fails the data run. Metrics land as job="silver_*"
// and feed dashboards 01-pipeline-overview / 03-data-quality.
object PipelineMetrics {
  private val gateway = sys.env.getOrElse("PUSHGATEWAY_URL", "http://localhost:19091")
  private val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()

  // single metric push — labels are Prom label set, value is gauge
  def push(job: String, metrics: Map[String, Seq[(Map[String, String], Double)]], runId: String = ""): Boolean = {
    if (metrics.isEmpty) return false
    val body = metrics.flatMap { case (name, series) =>
      val header = s"# TYPE $name gauge"
      val lines = series.map { case (labels, v) =>
        val all = if (runId.nonEmpty) labels + ("run_id" -> runId) else labels
        val labelStr = if (all.isEmpty) "" else all.map { case (k, vv) => s"""$k="$vv"""" }.mkString("{", ",", "}")
        s"$name$labelStr $v"
      }
      header +: lines
    }.mkString("\n") + "\n"

    try {
      val req = HttpRequest.newBuilder(URI.create(s"$gateway/metrics/job/$job"))
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .header("Content-Type", "text/plain")
        .timeout(Duration.ofSeconds(5))
        .build()
      val resp = client.send(req, HttpResponse.BodyHandlers.discarding())
      println(s"[ok] pushgateway job=$job http=${resp.statusCode()} metrics=${metrics.size}")
      resp.statusCode() / 100 == 2
    } catch {
      case e: Exception =>
        println(s"[warn] pushgateway unavailable (${e.getMessage}) - metrics skipped")
        false
    }
  }

  // convenience for row counts per collection — feeds panel "Bronze rows ingested per run"
  def pushRowsWritten(job: String, byCollection: Map[String, Long], runId: String = ""): Boolean =
    push(job, Map("rows_written_total" -> byCollection.map { case (c, n) => Map("collection" -> c) -> n.toDouble }.toSeq), runId)

  // dq gate metric — feeds "DQ gate pass rate (%)" gauge (98% warn, 100% ok)
  def pushDqGate(passPct: Double, runId: String = ""): Boolean =
    push("dq_checks", Map("dq_gate_pass_pct" -> Seq(Map.empty[String, String] -> passPct)), runId)

  // pipeline_runs_total — feeds "Pipeline runs by stage (24h)"
  def pushPipelineRun(stage: String, status: String, runId: String = ""): Boolean =
    push("pipeline", Map("pipeline_runs_total" -> Seq(Map("stage" -> stage, "status" -> status) -> 1.0)), runId)

  // ops.pipeline_runs row — same contract as PySpark _record_run (bronze.py:366)
  // Best-effort JDBC insert; never fails the data run if DB is down.
  def pushRun(runId: String, batchId: String, stage: String, status: String, rowsWritten: Long, durationMs: Long): Boolean = {
    try {
      val url = s"jdbc:postgresql://${sys.env.getOrElse("POSTGRES_HOST","postgres")}:${sys.env.getOrElse("POSTGRES_PORT","5432")}/${sys.env.getOrElse("POSTGRES_WAREHOUSE_DB","banking_dw")}"
      val user = sys.env.getOrElse("POSTGRES_USER","postgres")
      val pw = sys.env.getOrElse("POSTGRES_PASSWORD","")
      val conn = java.sql.DriverManager.getConnection(url, user, pw)
      try {
        val ps = conn.prepareStatement(
          "INSERT INTO ops.pipeline_runs (run_id,batch_id,stage,status,rows_written,duration_ms,started_at,finished_at) VALUES (?,?,?,?,?,?, now(), now()) ON CONFLICT (run_id,stage) DO UPDATE SET batch_id=EXCLUDED.batch_id, status=EXCLUDED.status, rows_written=EXCLUDED.rows_written, duration_ms=EXCLUDED.duration_ms, finished_at=now()")
        ps.setString(1, runId); ps.setString(2, batchId); ps.setString(3, stage); ps.setString(4, status); ps.setLong(5, rowsWritten); ps.setLong(6, durationMs)
        ps.executeUpdate(); ps.close()
      } finally conn.close()
      pushPipelineRun(stage, status, runId)
      true
    } catch { case e: Exception => println(s"[warn] ops.pipeline_runs insert failed: ${e.getMessage}"); false }
  }
}
