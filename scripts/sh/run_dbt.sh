#!/usr/bin/env bash
# scripts/sh/run_dbt.sh — dbt build wrapper (Architecture 7.2, Phase 2)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

STAGE="dbt"
LOG_FILE="${LOG_DIR}/${STAGE}.log"
SELECTOR="${1:-all}"
DRY_RUN=false
[[ "${2:-}" == "--dry-run" ]] && DRY_RUN=true

DBT_DIR="dbt/banking_dbt"
log "dbt build selector=${SELECTOR} dry_run=${DRY_RUN}"

if [[ ! -f "${DBT_DIR}/dbt_project.yml" ]]; then die "dbt project not found at ${DBT_DIR}"; fi

if [[ "${DRY_RUN}" == true ]]; then log "[dry-run] would: dbt build --project-dir ${DBT_DIR} --select ${SELECTOR}"; exit 0; fi

# prefer uv, fallback to system dbt
if command -v uv >/dev/null 2>&1; then
  uv run dbt build --project-dir "${DBT_DIR}" --select "${SELECTOR}" 2>&1 | tee -a "${LOG_FILE}"
else
  dbt build --project-dir "${DBT_DIR}" --select "${SELECTOR}" 2>&1 | tee -a "${LOG_FILE}"
fi
log "dbt build done"
