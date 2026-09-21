#!/usr/bin/env bash
# scripts/sh/healthcheck.sh — service + pipeline freshness check (Architecture 12-13)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

STAGE="healthcheck"
LOG_FILE="${LOG_DIR}/${STAGE}.log"

FAIL=0
check() {
  local name="$1"; shift
  if "$@" >/dev/null 2>&1; then log "[ok] ${name}"; else warn "[FAIL] ${name}"; FAIL=1; fi
}

log "healthcheck start"

# Prefer the containers' own client tools — the host may not have psql/pg_isready/
# mongosh installed, and command-not-found must not read as "service down".
PG_CONTAINER="$(docker ps --filter name=banking_postgres -q | head -1)"
MONGO_CONTAINER="$(docker ps --filter name=banking_mongo -q | head -1)"

pg_alive() {
  if [[ -n "${PG_CONTAINER}" ]]; then
    docker exec "${PG_CONTAINER}" pg_isready -U "${POSTGRES_USER:-postgres}" >/dev/null 2>&1
  elif command -v pg_isready >/dev/null 2>&1; then
    pg_isready -h "${POSTGRES_HOST:-localhost}" -p "${POSTGRES_PORT:-5433}" -U "${POSTGRES_USER:-postgres}" >/dev/null 2>&1
  else
    (exec 3<>"/dev/tcp/${POSTGRES_HOST:-localhost}/${POSTGRES_PORT:-5433}") 2>/dev/null
  fi
}

mongo_alive() {
  if [[ -n "${MONGO_CONTAINER}" ]]; then
    docker exec "${MONGO_CONTAINER}" mongosh --quiet --eval 'db.adminCommand("ping").ok' >/dev/null 2>&1
  elif command -v mongosh >/dev/null 2>&1; then
    mongosh --host "${MONGO_HOST:-localhost}:${MONGO_PORT:-27018}" --quiet --eval 'db.adminCommand("ping").ok' >/dev/null 2>&1
  else
    (exec 3<>"/dev/tcp/${MONGO_HOST:-localhost}/${MONGO_PORT:-27018}") 2>/dev/null
  fi
}

check "postgres" pg_alive
check "mongo" mongo_alive
check "minio" curl -f "http://localhost:9000/minio/health/live" 2>/dev/null || curl -f "${S3_ENDPOINT:-http://localhost:9000}/minio/health/live" 2>/dev/null || true

# ops tables freshness — prefer docker exec (psql ships in the container; the host
# may not have psql installed, and command-not-found must not read as "db down")
PG_CONTAINER="$(docker ps --filter name=banking_postgres -q | head -1)"
psql_exec() {
  if [[ -n "${PG_CONTAINER}" ]]; then
    docker exec "${PG_CONTAINER}" psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_WAREHOUSE_DB:-banking_dw}" "$@"
  elif command -v psql >/dev/null 2>&1; then
    PGPASSWORD="${POSTGRES_PASSWORD:-}" psql -h "${POSTGRES_HOST:-localhost}" -p "${POSTGRES_PORT:-5433}" -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_WAREHOUSE_DB:-banking_dw}" "$@"
  else
    return 1
  fi
}

if psql_exec -c "SELECT count(*) FROM ops.pipeline_runs;" >/dev/null 2>&1; then
  log "[ok] ops.pipeline_runs reachable"
  psql_exec -c "SELECT stage, status, finished_at FROM ops.pipeline_runs ORDER BY finished_at DESC LIMIT 5;" 2>&1 | tee -a "${LOG_FILE}" || true
else
  warn "ops.pipeline_runs not reachable"
fi

if [[ "${FAIL}" -eq 0 ]]; then log "healthcheck: all ok"; exit 0; else warn "healthcheck: some checks failed — see ${LOG_FILE}"; exit 1; fi
