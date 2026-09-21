package jobs.transform.scala

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

// Heavy table: transactions 2M — Scala for efficient shuffle (Architecture 7.1)
// Mirrors jobs/transform/silver_transactions.py: explicit schema, _id tiebreaker, MERGE, quarantine
object SilverTransactions {
  val txnSchema: StructType = StructType(Seq(
    StructField("transaction_id", IntegerType, nullable = false),
    StructField("account_id", IntegerType, nullable = true),
    StructField("txn_date", StringType, nullable = true),
    StructField("txn_type", StringType, nullable = true),
    StructField("amount", DecimalType(18, 2), nullable = true),
    StructField("channel", StringType, nullable = true),
    StructField("merchant_category", StringType, nullable = true),
    StructField("created_at", TimestampType, nullable = true)
  ))

  def run(spark: SparkSession, batchId: Option[String] = None): Long = {
    var bronze = spark.table("banking.bronze.transactions")
    batchId.foreach(id => bronze = bronze.filter(F.col("_batch_id") === id))

    val parsed = bronze
      .withColumn("parsed", F.from_json(F.col("_doc"), txnSchema))
      .selectExpr("_id", "_batch_id", "_source_ts", "_ingested_at", "_doc_hash", "parsed.*")

    // dedupe by latest _source_ts with _id tiebreaker — same created_at per profiling.md:65
    val w = Window.partitionBy("transaction_id").orderBy(F.col("_source_ts").desc, F.col("_id").desc)
    val deduped = parsed.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") === 1).drop("rn")

    // audit cols per Architecture 7.3
    val withAudit = deduped
      .withColumn("silver_loaded_at", F.current_timestamp())
      .withColumn("_bronze_batch_id", F.col("_batch_id"))
      .withColumn("_bronze_doc_hash", F.col("_doc_hash"))

    val quarantine = withAudit
      .filter(F.col("transaction_id").isNull || F.col("amount").isNull || F.col("amount") <= 0)
      .withColumn("_dq_rule", F.lit("not_null_or_amount_check"))
      .withColumn("_quarantined_at", F.current_timestamp())
    val clean = withAudit.filter(F.col("transaction_id").isNotNull && F.col("amount").isNotNull && F.col("amount") > 0)

    // MERGE upsert by business key — idempotent per Architecture 6.6 (not append)
    clean.createOrReplaceTempView("clean_transactions")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS banking.silver")
    if (!spark.catalog.tableExists("banking.silver.transactions")) {
      clean.limit(0).write.mode("append").saveAsTable("banking.silver.transactions")
    }
    spark.sql("""
      MERGE INTO banking.silver.transactions t USING clean_transactions s
      ON t.transaction_id = s.transaction_id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)

    if (quarantine.count() > 0) {
      quarantine.write.mode("append").saveAsTable("banking.quarantine.transactions")
    }

    clean.count()
  }

  def main(args: Array[String]): Unit = {
    // Architecture 7.1 contract: --run-id <id> --batch-id <id> --env <local|dev|...>
    var runId: Option[String] = None; var batchId: Option[String] = None; var env = "local"
    var i = 0; while (i < args.length) { args(i) match {
      case "--run-id" if i+1 < args.length => runId = Some(args(i+1)); i+=2
      case "--batch-id" if i+1 < args.length => batchId = Some(args(i+1)); i+=2
      case "--env" if i+1 < args.length => env = args(i+1); i+=2
      case other if batchId.isEmpty && !other.startsWith("--") => batchId = Some(other); i+=1
      case _ => i+=1
    }}
    val spark = SparkSession.builder.appName("silver_txn_scala").getOrCreate()
    val t0 = System.nanoTime()
    val cnt = run(spark, batchId)
    val ms = (System.nanoTime() - t0) / 1e6
    // emit JSON summary and ops.pipeline_runs row — same contract as PySpark jobs (Architecture 12.1)
    val summary = s"""{"run_id":"${runId.getOrElse("")}","batch_id":"${batchId.getOrElse("")}","env":"$env","stage":"silver_transactions_scala","rows_written":$cnt,"duration_ms":${ms.toLong}}"""
    println(summary)
    runId.foreach { rid => try { PipelineMetrics.pushRun(rid, batchId.getOrElse(rid), "silver_transactions_scala", "success", cnt, ms.toLong) } catch { case _: Throwable => () } }
    spark.stop()
  }
}
