#!/usr/bin/env bash
# scripts/sh/restore_postgres.sh — restore from backup (destructive — requires --dry-run preview)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

STAGE="restore"
LOG_FILE="${LOG_DIR}/${STAGE}.log"
DRY_RUN=false
FILE=""

while [[ $# -gt 0 ]]; do case "$1" in
  --dry-run) DRY_RUN=true; shift;;
  --file) FILE="$2"; shift 2;;
  *) FILE="$1"; shift;;
esac; done

trap 'warn "restore failed at line $LINENO"' ERR

[[ -z "${FILE}" ]] && die "usage: $0 [--dry-run] --file <backup.sql.gz>"
[[ ! -f "${FILE}" ]] && die "backup not found: ${FILE}"

require_env POSTGRES_WAREHOUSE_DB
HOST="${POSTGRES_HOST:-postgres}"
PORT="${POSTGRES_PORT:-5432}"
DB="${POSTGRES_WAREHOUSE_DB}"
USER="${POSTGRES_USER:-postgres}"

if [[ "${DRY_RUN}" == true ]]; then
  log "[dry-run] would: gunzip -c ${FILE} | psql -h ${HOST} -p ${PORT} -U ${USER} ${DB}"
  log "[dry-run] aborting — destructive. Re-run without --dry-run to restore."
  exit 0
fi

log "restore ${FILE} -> ${DB}@${HOST}:${PORT} (confirm destructive)"
read -r -p "Restore ${FILE} into ${DB}? type YES to confirm: " ans
[[ "${ans}" != "YES" ]] && die "restore cancelled"

retry gunzip -c "${FILE}" | psql -h "${HOST}" -p "${PORT}" -U "${USER}" "${DB}" 2>&1 | tee -a "${LOG_FILE}"
log "restore done"
