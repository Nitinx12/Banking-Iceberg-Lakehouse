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

check "postgres" pg_isready -h "${POSTGRES_HOST:-localhost}" -p "${POSTGRES_PORT:-5433}" -U "${POSTGRES_USER:-postgres}"
check "mongo" mongosh --host "${MONGO_HOST:-localhost}:${MONGO_PORT:-27018}" -u "${MONGO_INITDB_ROOT_USERNAME:-admin}" -p "${MONGO_INITDB_ROOT_PASSWORD:-admin}" --authenticationDatabase admin --eval 'db.adminCommand("ping")' --quiet 2>/dev/null || mongosh --quiet --eval 'db.adminCommand("ping").ok' 2>/dev/null || true
check "minio" curl -f "http://localhost:9000/minio/health/live" 2>/dev/null || curl -f "${S3_ENDPOINT:-http://localhost:9000}/minio/health/live" 2>/dev/null || true

# ops tables freshness (if pg reachable)
if PGPASSWORD="${POSTGRES_PASSWORD:-}" psql -h "${POSTGRES_HOST:-localhost}" -p "${POSTGRES_PORT:-5433}" -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_WAREHOUSE_DB:-banking_dw}" -c "SELECT count(*) FROM ops.pipeline_runs;" >/dev/null 2>&1; then
  log "[ok] ops.pipeline_runs reachable"
  PGPASSWORD="${POSTGRES_PASSWORD:-}" psql -h "${POSTGRES_HOST:-localhost}" -p "${POSTGRES_PORT:-5433}" -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_WAREHOUSE_DB:-banking_dw}" -c "SELECT stage, status, finished_at FROM ops.pipeline_runs ORDER BY finished_at DESC LIMIT 5;" 2>&1 | tee -a "${LOG_FILE}" || true
else
  warn "ops.pipeline_runs not reachable"
fi

if [[ "${FAIL}" -eq 0 ]]; then log "healthcheck: all ok"; exit 0; else warn "healthcheck: some checks failed — see ${LOG_FILE}"; exit 1; fi
