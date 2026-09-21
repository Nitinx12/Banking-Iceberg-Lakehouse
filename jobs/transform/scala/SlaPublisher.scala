package jobs.transform.scala

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.sql.{Connection, DriverManager}
import java.time.Duration

// Scala SLA publisher — mirrors jobs/observability/sla.py (Architecture 13.2)
// Measures freshness per serving table, writes ops.freshness_metrics / ops.sla_events,
// pushes data_freshness_seconds + sla_breach_total to Pushgateway for Grafana SLO panels.
// Best-effort on push, fail-closed on measurement: breach rows always land in Postgres.

object SlaPublisher {
  // SLO 26h = 93600s per docs/runbooks and monitoring/prometheus/rules/data-slo.yml
  private val sloSeconds = sys.env.getOrElse("FRESHNESS_SLO_SECONDS", "93600").toInt
  // Freshness is defined on fact tables only (_loaded_at); dims have no load timestamp
  private val servingTables = Seq("fct_transactions", "fct_card_transactions")

  private def pgUrl(): String = {
    val host = sys.env.getOrElse("POSTGRES_HOST", "localhost")
    val port = sys.env.getOrElse("POSTGRES_PORT", "5433")
    val db   = sys.env.getOrElse("POSTGRES_WAREHOUSE_DB", "banking_dw")
    s"jdbc:postgresql://$host:$port/$db"
  }

  private def pgUser = sys.env.getOrElse("POSTGRES_USER", "postgres")
  private def pgPass = sys.env.getOrElse("POSTGRES_PASSWORD", sys.env.getOrElse("POSTGRES_STREAMLIT_READER_PASSWORD", ""))

  private def withConn[T](f: Connection => T): T = {
    val c = DriverManager.getConnection(pgUrl(), pgUser, pgPass)
    try f(c) finally c.close()
  }

  // freshness = now() - max(_loaded_at) in seconds, plus max timestamp for audit
  private def measure(table: String): Option[(Int, java.sql.Timestamp)] = withConn { conn =>
    // GREATEST(0, ...) clamps host/container clock skew (negative freshness is noise)
    val ps = conn.prepareStatement(s"SELECT GREATEST(0, COALESCE(EXTRACT(EPOCH FROM (now() - max(_loaded_at)))::bigint,0)), max(_loaded_at) FROM serving.$table")
    val rs = ps.executeQuery()
    if (rs.next()) {
      val freshness = rs.getInt(1)
      val ts = rs.getTimestamp(2)
      if (rs.wasNull() || ts == null) None else Some((freshness, ts))
    } else None
  }

  private def record(table: String, freshness: Int, ts: java.sql.Timestamp): Boolean = withConn { conn =>
    val breached = freshness > sloSeconds
    conn.setAutoCommit(false)
    try {
      val ins1 = conn.prepareStatement("INSERT INTO ops.freshness_metrics (table_name, freshness_seconds, max_loaded_at) VALUES (?,?,?)")
      ins1.setString(1, table); ins1.setInt(2, freshness); ins1.setTimestamp(3, ts); ins1.executeUpdate()
      if (breached) {
        val ins2 = conn.prepareStatement("INSERT INTO ops.sla_events (table_name, target_name, target_seconds, actual_seconds, breached) VALUES (?,?,?,?,true)")
        ins2.setString(1, table); ins2.setString(2, "freshness_gold"); ins2.setInt(3, sloSeconds); ins2.setInt(4, freshness); ins2.executeUpdate()
      }
      conn.commit(); breached
    } catch { case e: Exception => conn.rollback(); throw e }
  }

  def run(runId: String = "sla-scala"): (Int, Int) = {
    var checked = 0; var breaches = 0
    var freshByTable = Map.empty[String, Int]
    for (tbl <- servingTables) {
      measure(tbl) match {
        case None => println(s"[skip] $tbl empty")
        case Some((fresh, ts)) =>
          freshByTable += (tbl -> fresh)
          checked += 1
          val breached = record(tbl, fresh, ts)
          if (breached) breaches += 1
          println(s"[ok] $tbl freshness=${fresh}s breached=$breached max=$ts")
      }
    }
    // push to gateway — feeds Grafana panels "Freshness per Gold table" + "SLA breach events"
    val gateway = sys.env.getOrElse("PUSHGATEWAY_URL", "http://localhost:9091")
    val body = freshByTable.map { case (t, v) => s"""data_freshness_seconds{table="$t"} $v""" }.mkString("\n") +
      s"\n# TYPE sla_breach_total counter\nsla_breach_total $breaches\n"
    try {
      val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()
      val req = HttpRequest.newBuilder(URI.create(s"$gateway/metrics/job/sla_monitor/instance/scala"))
        .PUT(HttpRequest.BodyPublishers.ofString(body)).header("Content-Type","text/plain").timeout(Duration.ofSeconds(5)).build()
      val resp = client.send(req, HttpResponse.BodyHandlers.discarding())
      println(s"[ok] pushed ${freshByTable.size} freshness series + breach=$breaches http=${resp.statusCode()}")
    } catch { case e: Exception => println(s"[warn] pushgateway unavailable (${e.getMessage})") }
    (checked, breaches)
  }

  def main(args: Array[String]): Unit = {
    val runId = if (args.nonEmpty) args(0) else "sla-scala"
    val (checked, breaches) = run(runId)
    println(s"sla Scala: $checked tables checked, $breaches breach(s) run_id=$runId")
    if (breaches > 0) println(s"[warn] $breaches table(s) breach SLO ${sloSeconds}s — check ops.sla_events")
  }
}
