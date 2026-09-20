#!/usr/bin/env bash
# scripts/sh/run_dq.sh — Great Expectations checkpoints (Architecture 11, Phase 3)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

STAGE="dq"
LOG_FILE="${LOG_DIR}/${STAGE}.log"
DRY_RUN=false
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true

log "dq checks start dry_run=${DRY_RUN}"

if [[ ! -f "gx/great_expectations.yml" ]]; then die "GX config not found at gx/great_expectations.yml"; fi

if [[ "${DRY_RUN}" == true ]]; then log "[dry-run] would: uv run great_expectations checkpoint run bronze && silver && gold"; exit 0; fi

# run suites — fail-closed: critical failure stops downstream
if command -v uv >/dev/null 2>&1; then
  uv run python -m jobs.quality.checks 2>&1 | tee -a "${LOG_FILE}" || die "DQ checks failed — see ${LOG_FILE}"
else
  python -m jobs.quality.checks 2>&1 | tee -a "${LOG_FILE}" || die "DQ checks failed"
fi

# also run dbt tests as part of DQ (silver/gold contracts)
if [[ -f "dbt/banking_dbt/dbt_project.yml" ]]; then
  log "also running dbt test"
  if command -v uv >/dev/null 2>&1; then uv run dbt test --project-dir dbt/banking_dbt 2>&1 | tee -a "${LOG_FILE}"; else dbt test --project-dir dbt/banking_dbt 2>&1 | tee -a "${LOG_FILE}"; fi
fi

log "dq checks done — results in ops.dq_results and gx/uncommitted/data_docs"
