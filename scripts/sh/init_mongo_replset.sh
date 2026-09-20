#!/usr/bin/env bash
# scripts/sh/init_mongo_replset.sh — idempotent replicaSet init (Architecture 4.2, 5.1)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

STAGE="mongo_init"
LOG_FILE="${LOG_DIR}/${STAGE}.log"
DRY_RUN=false
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true

require_env MONGO_INITDB_ROOT_USERNAME
require_env MONGO_INITDB_ROOT_PASSWORD
MONGO_HOST="${MONGO_HOST:-mongo}"
MONGO_PORT="${MONGO_PORT:-27017}"

if [[ "${DRY_RUN}" == true ]]; then log "[dry-run] would rs.initiate rs0 on ${MONGO_HOST}:${MONGO_PORT}"; exit 0; fi

log "init replicaSet rs0 on ${MONGO_HOST}:${MONGO_PORT}"
retry mongosh --host "${MONGO_HOST}:${MONGO_PORT}" -u "${MONGO_INITDB_ROOT_USERNAME}" -p "${MONGO_INITDB_ROOT_PASSWORD}" --authenticationDatabase admin --eval '
  try { rs.status(); print("rs already initiated"); }
  catch(e) { rs.initiate({_id:"rs0", members:[{_id:0, host:"mongo:27017"}]}); print("rs initiated"); }
' 2>&1 | tee -a "${LOG_FILE}"

# wait for PRIMARY
for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
  if mongosh --host "${MONGO_HOST}:${MONGO_PORT}" -u "${MONGO_INITDB_ROOT_USERNAME}" -p "${MONGO_INITDB_ROOT_PASSWORD}" --authenticationDatabase admin --quiet --eval 'rs.status().ok' 2>/dev/null | grep -q "1"; then
    log "mongo RS ready"
    exit 0
  fi
  sleep 2
done
die "mongo RS init timeout"
