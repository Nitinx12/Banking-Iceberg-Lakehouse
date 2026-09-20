#!/usr/bin/env bash
# scripts/sh/backup_postgres.sh — idempotent pg_dump with rotation (Architecture 19)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

STAGE="backup"
LOG_FILE="${LOG_DIR}/${STAGE}.log"
DRY_RUN=false
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true
BACKUP_DIR="${BACKUP_DIR:-backups}"
RETENTION="${RETENTION:-7}"

trap 'warn "backup failed at line $LINENO"' ERR

require_env POSTGRES_WAREHOUSE_DB
require_env POSTGRES_USER
HOST="${POSTGRES_HOST:-postgres}"
PORT="${POSTGRES_PORT:-5432}"
DB="${POSTGRES_WAREHOUSE_DB}"
TS="$(date -u +%Y%m%d-%H%M%S)"
FILE="${BACKUP_DIR}/${DB}-${TS}.sql.gz"

mkdir -p "${BACKUP_DIR}"

if [[ "${DRY_RUN}" == true ]]; then log "[dry-run] would: pg_dump -h ${HOST} -p ${PORT} -U ${POSTGRES_USER} ${DB} | gzip > ${FILE}"; exit 0; fi

log "backup ${DB} -> ${FILE}"
retry pg_dump -h "${HOST}" -p "${PORT}" -U "${POSTGRES_USER}" "${DB}" 2>&1 | gzip > "${FILE}" || die "pg_dump failed"

# retain last N backups
if [[ "${RETENTION}" -gt 0 ]]; then
  ls -t "${BACKUP_DIR}/${DB}-"*.sql.gz 2>/dev/null | tail -n +$((RETENTION+1)) | xargs -r rm -f
  log "retention: kept last ${RETENTION} backups"
fi

log "backup done: ${FILE}"
