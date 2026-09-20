#!/usr/bin/env bash
# scripts/sh/init_buckets.sh — idempotent MinIO bucket init (Architecture 6.2)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

STAGE="buckets"
LOG_FILE="${LOG_DIR}/${STAGE}.log"
DRY_RUN=false
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true

require_env AWS_ACCESS_KEY_ID
require_env AWS_SECRET_ACCESS_KEY
S3_ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
S3_BUCKET="${S3_BUCKET:-banking-lakehouse}"

if [[ "${DRY_RUN}" == true ]]; then log "[dry-run] would mc mb local/${S3_BUCKET} at ${S3_ENDPOINT}"; exit 0; fi

require_cmd mc
log "init bucket ${S3_BUCKET} at ${S3_ENDPOINT}"
retry mc alias set local "${S3_ENDPOINT}" "${AWS_ACCESS_KEY_ID}" "${AWS_SECRET_ACCESS_KEY}" 2>&1 | tee -a "${LOG_FILE}"
retry mc mb --ignore-existing "local/${S3_BUCKET}" 2>&1 | tee -a "${LOG_FILE}"
log "bucket ${S3_BUCKET} ready — warehouse prefixes per Architecture 6.2"
