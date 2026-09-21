// jobs/transform/scala/build.sbt — Phase 7 (Flink/streaming stretch) scaffold placeholder
// Heavy tables (transactions 2M, card_transactions 3M) run via Scala for JVM shuffle (Architecture 7.1).
// CE proven path is Python fallback: `make ingest_py` / `uv run python -m jobs.transform.silver_*`
// This build.sbt is intentionally thin until Phase 7 wires full Scala jobs (ADR 006 known gap).
// `make ingest` / `make silver` / `make gold` (sbt) will use this project; until deps are
// scaffolded they are expected to require sbt + full build definition. Python fallback remains live.

ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "jobs-transform-scala",
    libraryDependencies ++= Seq(
      // Pinned to match jobs/common/spark.py (Spark 3.5.5, Iceberg 1.5.2, Hadoop 3.3.4 — BulkDelete compat)
      "org.apache.spark" %% "spark-sql" % "3.5.5" % Provided,
      "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % "1.5.2",
      "org.postgresql" % "postgresql" % "42.7.4",
      "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
      "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.780"
    )
  )
