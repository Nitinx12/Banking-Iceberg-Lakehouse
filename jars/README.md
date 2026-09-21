# jars/ — offline Spark/Iceberg JARs for stable connection

Pinned versions match `jobs/common/spark.py:70` and `jobs/transform/scala/build.sbt`
(Spark 3.5.5 bundled Hadoop 3.3.4 — avoid 3.4 BulkDelete mismatch):

- `iceberg-spark-runtime-3.5_2.12-1.5.2.jar` (Iceberg 1.5.2)
- `postgresql-42.7.4.jar` (JDBC catalog)
- `hadoop-aws-3.3.4.jar` (S3A)
- `aws-java-sdk-bundle-1.12.780.jar` (S3A bundle)

Download once for offline/stable runs:

```bash
make jars          # or: bash scripts/sh/download_jars.sh
tasks.bat jars     # Windows
```

This pre-warms `~/.ivy2/cache` via Ivy and drops the 4 jars into `jars/` so
`jobs/common/spark.py` can use `spark.jars` (local files) instead of
`spark.jars.packages` (needs Maven Central on first run). After `make jars`,
`make ingest_py` and `make silver` work without internet (Ivy cached + local jars).

Jars are **not committed** (`/.gitignore: jars/*.jar`), only `jars/README.md` is.
Regenerate anytime with `make jars`.
