#!/usr/bin/env bash
# scripts/sh/download_jars.sh — download pinned Spark/Iceberg JARs for stable offline runs
# Pinned to match jobs/common/spark.py:70 + jobs/transform/scala/build.sbt (Spark 3.5.5, Iceberg 1.5.2, Hadoop 3.3.4)
set -euo pipefail
source "$(dirname "$0")/lib.sh"

STAGE="jars"
JARS_DIR="jars"
mkdir -p "$JARS_DIR"

# versions — keep in sync with jobs/common/spark.py and jobs/transform/scala/build.sbt
ICEBERG="1.5.2"
PG="42.7.4"
HADOOP_AWS="3.3.4"
AWS_BUNDLE="1.12.780"

BASE="https://repo1.maven.org/maven2"

# main 4 jars — transitive deps come from Ivy cache on first spark-submit
declare -A JARS=(
  ["iceberg-spark-runtime-3.5_2.12-${ICEBERG}.jar"]="${BASE}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG}/iceberg-spark-runtime-3.5_2.12-${ICEBERG}.jar"
  ["postgresql-${PG}.jar"]="${BASE}/org/postgresql/postgresql/${PG}/postgresql-${PG}.jar"
  ["hadoop-aws-${HADOOP_AWS}.jar"]="${BASE}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS}/hadoop-aws-${HADOOP_AWS}.jar"
  ["aws-java-sdk-bundle-${AWS_BUNDLE}.jar"]="${BASE}/com/amazonaws/aws-java-sdk-bundle/${AWS_BUNDLE}/aws-java-sdk-bundle-${AWS_BUNDLE}.jar"
)

log "downloading 4 pinned jars to ${JARS_DIR}/ (idempotent, --dry-run to preview)"

DRY_RUN=""
if [[ "${1:-}" == "--dry-run" ]]; then DRY_RUN=1; log "dry-run: would fetch"; fi

for name in "${!JARS[@]}"; do
  url="${JARS[$name]}"
  dest="${JARS_DIR}/${name}"
  if [[ -f "$dest" ]]; then
    log "exists ${dest} — skip (remove to re-download)"
    continue
  fi
  if [[ -n "$DRY_RUN" ]]; then
    log "would download ${url} -> ${dest}"
  else
    log "fetch ${url}"
    if command -v curl >/dev/null 2>&1; then
      curl -fL --retry 3 -o "$dest" "$url"
    elif command -v wget >/dev/null 2>&1; then
      wget -O "$dest" "$url"
    else
      die "curl or wget required"
    fi
    log "saved ${dest} ($(du -h "$dest" | cut -f1))"
  fi
done

if [[ -z "$DRY_RUN" ]]; then
  log "warming Ivy cache via sbt update (if sbt available) — makes next spark run stable without Maven Central"
  if command -v sbt >/dev/null 2>&1 && [[ -f "jobs/transform/scala/build.sbt" ]]; then
    (cd jobs/transform/scala && sbt update) || warn "sbt update failed — jars still downloaded, Spark will use spark.jars.packages fallback"
  else
    log "sbt not found or no build.sbt — skip Ivy warm (jars/ still usable via spark.jars)"
  fi
  # also warm Python Spark Ivy once if network allowed
  if command -v uv >/dev/null 2>&1; then
    log "warming Python Spark Ivy (one-time) — requires network, skips if offline"
    uv run python -c "from jobs.common.spark import get_spark; s=get_spark('jars-warm'); s.stop(); print('spark warm ok')" 2>&1 | tail -5 || warn "spark warm failed — still usable offline after first successful run"
  fi
fi

log "done — jars in ${JARS_DIR}/: $(ls -1 ${JARS_DIR}/*.jar 2>/dev/null | wc -l) files"
ls -lh "${JARS_DIR}/" 2>/dev/null || true
log "use: SPARK_JARS=jars/*.jar make ingest_py  — or just make ingest_py (auto-picks jars/*.jar if present)"
