package jobs.transform.scala

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

// Heavy table: card_transactions 3M — Scala optimized shuffle (Architecture 7.1)
// Mirrors jobs/transform/silver_card_transactions.py pattern: explicit schema, _id tiebreaker, MERGE, quarantine
object SilverCardTransactions {
  val schema: StructType = StructType(Seq(
    StructField("card_txn_id", IntegerType, nullable = false),
    StructField("card_id", IntegerType, nullable = true),
    StructField("txn_date", StringType, nullable = true),
    StructField("merchant_category", StringType, nullable = true),
    StructField("amount", DecimalType(18, 2), nullable = true),
    StructField("is_fraud", IntegerType, nullable = true),
    StructField("created_at", TimestampType, nullable = true)
  ))

  def run(spark: SparkSession, batchId: Option[String] = None): Long = {
    var bronze = spark.table("banking.bronze.card_transactions")
    batchId.foreach(id => bronze = bronze.filter(F.col("_batch_id") === id))

    val parsed = bronze
      .withColumn("parsed", F.from_json(F.col("_doc"), schema))
      .selectExpr("_id", "_batch_id", "_source_ts", "_ingested_at", "_doc_hash", "parsed.*")

    // dedupe by latest _source_ts with _id tiebreaker — same created_at per profiling.md:65
    val w = Window.partitionBy("card_txn_id").orderBy(F.col("_source_ts").desc, F.col("_id").desc)
    val deduped = parsed.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") === 1).drop("rn")

    // audit cols per Architecture 7.3
    val withAudit = deduped
      .withColumn("silver_loaded_at", F.current_timestamp())
      .withColumn("_bronze_batch_id", F.col("_batch_id"))
      .withColumn("_bronze_doc_hash", F.col("_doc_hash"))

    val quarantine = withAudit
      .filter(F.col("card_txn_id").isNull || F.col("amount").isNull || F.col("amount") <= 0)
      .withColumn("_dq_rule", F.lit("not_null_or_amount_check"))
      .withColumn("_quarantined_at", F.current_timestamp())
    val clean = withAudit.filter(F.col("card_txn_id").isNotNull && F.col("amount").isNotNull && F.col("amount") > 0)

    // skew-aware write: repartition by card_id for 3M shuffle, then MERGE (idempotent per 6.6)
    // For hot keys, consider salting: F.concat(F.col("card_id"), F.lit("_"), (F.rand()*10).cast("int"))
    val cleanRepart = clean.repartition(F.col("card_id"))

    cleanRepart.createOrReplaceTempView("clean_card_transactions")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS banking.silver")
    if (!spark.catalog.tableExists("banking.silver.card_transactions")) {
      cleanRepart.limit(0).write.mode("append").saveAsTable("banking.silver.card_transactions")
    }
    spark.sql("""
      MERGE INTO banking.silver.card_transactions t USING clean_card_transactions s
      ON t.card_txn_id = s.card_txn_id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)

    if (quarantine.count() > 0) {
      quarantine.write.mode("append").saveAsTable("banking.quarantine.card_transactions")
    }

    clean.count()
  }

  def main(args: Array[String]): Unit = {
    var runId: Option[String] = None; var batchId: Option[String] = None; var env = "local"
    var i = 0; while (i < args.length) { args(i) match {
      case "--run-id" if i+1 < args.length => runId = Some(args(i+1)); i+=2
      case "--batch-id" if i+1 < args.length => batchId = Some(args(i+1)); i+=2
      case "--env" if i+1 < args.length => env = args(i+1); i+=2
      case other if batchId.isEmpty && !other.startsWith("--") => batchId = Some(other); i+=1
      case _ => i+=1
    }}
    val spark = SparkSession.builder.appName("silver_cardtxn_scala").getOrCreate()
    val t0 = System.nanoTime(); val cnt = run(spark, batchId)
    val ms = (System.nanoTime() - t0) / 1e6
    val summary = s"""{"run_id":"${runId.getOrElse("")}","batch_id":"${batchId.getOrElse("")}","env":"$env","stage":"silver_cardtxn_scala","rows_written":$cnt,"duration_ms":${ms.toLong}}"""
    println(summary)
    runId.foreach { rid => try { PipelineMetrics.pushRun(rid, batchId.getOrElse(rid), "silver_cardtxn_scala", "success", cnt, ms.toLong) } catch { case _: Throwable => () } }
    spark.stop()
  }
}
