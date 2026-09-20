package jobs.transform.scala
import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
object SilverCardTransactions {
  val schema = StructType(Seq(
    StructField("card_txn_id", IntegerType, false),
    StructField("card_id", IntegerType, true),
    StructField("amount", DecimalType(18,2), true),
    StructField("is_fraud", IntegerType, true),
    StructField("created_at", TimestampType, true)
  ))
  def run(spark: SparkSession, batchId: Option[String]=None): Long = {
    var b = spark.table("banking.bronze.card_transactions")
    batchId.foreach(id => b = b.filter(F.col("_batch_id")===id))
    val p = b.withColumn("parsed", F.from_json(F.col("_doc"), schema)).selectExpr("_id","_batch_id","_source_ts","_ingested_at","_doc_hash","parsed.*")
    val w = Window.partitionBy("card_txn_id").orderBy(F.col("_source_ts").desc)
    val d = p.withColumn("rn", F.row_number.over(w)).filter("rn=1").drop("rn")
    val clean = d.filter("card_txn_id IS NOT NULL AND amount > 0")
    // Scala optimized for heavy 3M shuffle: repartition by card_id, handle skew
    clean.repartition(F.col("card_id")).write.mode("append").saveAsTable("banking.silver.card_transactions")
    clean.count()
  }
  def main(args: Array[String]): Unit = { val s=SparkSession.builder.appName("silver_cardtxn_scala").getOrCreate(); println(run(s, args.headOption)); s.stop() }
}
