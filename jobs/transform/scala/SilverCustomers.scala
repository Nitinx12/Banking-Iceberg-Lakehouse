// jobs/transform/scala/SilverCustomers.scala — Scala Silver for customers (Architecture 7.1-7.3)
// Mirrors jobs/transform/silver_customers.py: explicit schema, _id tiebreaker, HMAC masking, MERGE, quarantine lineage
package jobs.transform.scala

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.UserDefinedFunction
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.util.Base64

object SilverCustomers {

  val customerSchema: StructType = StructType(Seq(
    StructField("customer_id", IntegerType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("gender", StringType, nullable = true),
    StructField("date_of_birth", StringType, nullable = true),
    StructField("city", StringType, nullable = true),
    StructField("state", StringType, nullable = true),
    StructField("phone", StringType, nullable = true),
    StructField("email", StringType, nullable = true),
    StructField("occupation", StringType, nullable = true),
    StructField("annual_income", IntegerType, nullable = true),
    StructField("join_date", StringType, nullable = true),
    StructField("credit_score", IntegerType, nullable = true),
    StructField("created_at", TimestampType, nullable = true)
  ))

  private def hmacSha256(value: String, secret: String, salt: String): String = {
    if (value == null) return null
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec((secret + salt).getBytes("UTF-8"), "HmacSHA256"))
    mac.doFinal(value.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }

  def hmacUdf(secret: String, salt: String): UserDefinedFunction =
    F.udf((v: String) => hmacSha256(v, secret, salt))

  def run(spark: SparkSession, batchId: Option[String] = None): Long = {
    import spark.implicits._
    val secret = sys.env.getOrElse("PII_HMAC_SECRET", "")
    val salt = sys.env.getOrElse("PII_HMAC_SALT", "")

    var bronze = spark.table("banking.bronze.customers")
    batchId.foreach(id => bronze = bronze.filter(F.col("_batch_id") === id))

    val parsed = bronze
      .withColumn("parsed", F.from_json(F.col("_doc"), customerSchema))
      .selectExpr("_id", "_batch_id", "_source_ts", "_ingested_at", "_doc_hash", "parsed.*")

    // dedupe by latest _source_ts with _id tiebreaker (profiling.md: same created_at)
    val w = Window.partitionBy("customer_id").orderBy(F.col("_source_ts").desc, F.col("_id").desc)
    val deduped = parsed.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") === 1).drop("rn")

    // mask PII + trim + audit cols per Architecture 7.3 / 14.2
    val hmac = hmacUdf(secret, salt)
    val silver = deduped
      .withColumn("email_hmac", hmac(F.col("email")))
      .withColumn("phone_hmac", hmac(F.col("phone")))
      .withColumn("name", F.trim(F.col("name")))
      .withColumn("silver_loaded_at", F.current_timestamp())
      .withColumn("_bronze_batch_id", F.col("_batch_id"))
      .withColumn("_bronze_doc_hash", F.col("_doc_hash"))
      .drop("email")
      .drop("phone")

    val quarantine = silver
      .filter(F.col("customer_id").isNull)
      .withColumn("_dq_rule", F.lit("not_null customer_id"))
      .withColumn("_quarantined_at", F.current_timestamp())
    val clean = silver.filter(F.col("customer_id").isNotNull)

    // MERGE upsert by business key — idempotent per Architecture 6.6
    clean.createOrReplaceTempView("clean_customers")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS banking.silver")
    spark.sql(
      "CREATE TABLE IF NOT EXISTS banking.silver.customers (customer_id INT, name STRING, gender STRING, date_of_birth STRING, city STRING, state STRING, email_hmac STRING, phone_hmac STRING, occupation STRING, annual_income INT, join_date STRING, credit_score INT, created_at TIMESTAMP, silver_loaded_at TIMESTAMP, _bronze_batch_id STRING, _bronze_doc_hash STRING) USING iceberg"
    )
    spark.sql("""
      MERGE INTO banking.silver.customers t USING clean_customers s
      ON t.customer_id = s.customer_id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)

    if (quarantine.count() > 0)
      quarantine.write.mode("append").saveAsTable("banking.quarantine.customers")

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
    val spark = SparkSession.builder.appName("silver_customers_scala").getOrCreate()
    val t0 = System.nanoTime(); val cnt = run(spark, batchId)
    val ms = (System.nanoTime() - t0) / 1e6
    val summary = s"""{"run_id":"${runId.getOrElse("")}","batch_id":"${batchId.getOrElse("")}","env":"$env","stage":"silver_customers_scala","rows_written":$cnt,"duration_ms":${ms.toLong}}"""
    println(summary)
    runId.foreach { rid => try { PipelineMetrics.pushRun(rid, batchId.getOrElse(rid), "silver_customers_scala", "success", cnt, ms.toLong) } catch { case _: Throwable => () } }
    spark.stop()
  }
}
