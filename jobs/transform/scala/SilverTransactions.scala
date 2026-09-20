package jobs.transform.scala
import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
// Heavy table: transactions 2M + card_transactions 3M — Scala handles large shuffle + complex windows efficiently
object SilverTransactions {
  val txnSchema = StructType(Seq(
    StructField("transaction_id", IntegerType, false),
    StructField("account_id", IntegerType, true),
    StructField("txn_type", StringType, true),
    StructField("amount", DecimalType(18,2), true),
    StructField("channel", StringType, true),
    StructField("created_at", TimestampType, true)
  ))
  def run(spark: SparkSession, batchId: Option[String]=None): Long = {
    var b = spark.table("banking.bronze.transactions")
    batchId.foreach(id => b = b.filter(F.col("_batch_id")===id))
    val p = b.withColumn("parsed", F.from_json(F.col("_doc"), txnSchema)).selectExpr("_id","_batch_id","_source_ts","_ingested_at","_doc_hash","parsed.*")
    val w = Window.partitionBy("transaction_id").orderBy(F.col("_source_ts").desc)
    val d = p.withColumn("rn", F.row_number.over(w)).filter("rn=1").drop("rn")
    val clean = d.filter("transaction_id IS NOT NULL AND amount > 0")
    clean.write.mode("append").saveAsTable("banking.silver.transactions")
    clean.count()
  }
  def main(args: Array[String]): Unit = { val s=SparkSession.builder.appName("silver_txn_scala").getOrCreate(); println(run(s, args.headOption)); s.stop() }
}
