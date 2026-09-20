// jobs/transform/scala/SilverCustomers.scala — Scala Silver for customers (Architecture 7.1)
// Use when PySpark UDFs become heavy or for type-safe Decimal handling. Compile with sbt + spark-submit.
package jobs.transform.scala

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object SilverCustomers {
  val customerSchema = StructType(Seq(
    StructField("customer_id", IntegerType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("email", StringType, nullable = true),
    StructField("phone", StringType, nullable = true),
    StructField("city", StringType, nullable = true),
    StructField("state", StringType, nullable = true),
    StructField("credit_score", IntegerType, nullable = true),
    StructField("created_at", TimestampType, nullable = true)
  ))

  def run(spark: SparkSession, batchId: Option[String] = None): Long = {
    import spark.implicits._
    var bronze = spark.table("banking.bronze.customers")
    batchId.foreach(id => bronze = bronze.filter(F.col("_batch_id") === id))

    val parsed = bronze
      .withColumn("parsed", F.from_json(F.col("_doc"), customerSchema))
      .selectExpr("_id", "_batch_id", "_source_ts", "_ingested_at", "_doc_hash", "parsed.*")

    val w = Window.partitionBy("customer_id").orderBy(F.col("_source_ts").desc)
    val deduped = parsed.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

    val clean = deduped.filter("customer_id IS NOT NULL")
    val quarantine = deduped.filter("customer_id IS NULL")

    clean.write.mode("append").saveAsTable("banking.silver.customers")
    if (quarantine.count() > 0) quarantine.withColumn("_dq_rule", F.lit("not_null")).write.mode("append").saveAsTable("banking.quarantine.customers")

    clean.count()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("silver_customers_scala").getOrCreate()
    val batchId = if (args.nonEmpty) Some(args(0)) else None
    println(s"wrote ${run(spark, batchId)} rows")
    spark.stop()
  }
}
