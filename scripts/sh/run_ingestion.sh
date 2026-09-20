#!/usr/bin/env bash
# scripts/sh/run_ingestion.sh — batch Bronze ingestion (Architecture 5.2, Phase 1)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

STAGE="ingest"
LOG_FILE="${LOG_DIR}/${STAGE}.log"
DRY_RUN=false
BATCH_ID=""
RUN_ID=""

while [[ $# -gt 0 ]]; do case "$1" in
  --dry-run) DRY_RUN=true; shift;;
  --batch-id) BATCH_ID="$2"; shift 2;;
  --run-id) RUN_ID="$2"; shift 2;;
  *) shift;;
esac; done

trap 'warn "ingestion failed at line $LINENO"' ERR

ARGS=(--all)
[[ -n "${BATCH_ID}" ]] && ARGS+=(--batch-id "${BATCH_ID}")
[[ -n "${RUN_ID}" ]] && ARGS+=(--run-id "${RUN_ID}")
[[ "${DRY_RUN}" == true ]] && ARGS+=(--dry-run)

log "run_ingestion start args=${ARGS[*]}"
if [[ "${DRY_RUN}" == true ]]; then
  log "[dry-run] would: uv run python -m jobs.ingestion.bronze ${ARGS[*]}"
  exit 0
fi

# idempotent: бронза deletes by _batch_id before write (jobs/ingestion/bronze.py:143)
retry uv run python -m jobs.ingestion.bronze "${ARGS[@]}" 2>&1 | tee -a "${LOG_FILE}"
log "ingestion done — watermark advanced after commit"
