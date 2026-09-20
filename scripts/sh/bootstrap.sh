#!/usr/bin/env bash
# scripts/sh/bootstrap.sh — Phase 0 one-time setup (Architecture 16.2)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/sh/lib.sh
source "${SCRIPT_DIR}/lib.sh"

STAGE="bootstrap"
LOG_FILE="${LOG_DIR}/${STAGE}.log"
DRY_RUN=false
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=true

trap 'warn "bootstrap failed at line $LINENO"' ERR

require_cmd uv
require_cmd git

log "bootstrap start (dry_run=${DRY_RUN})"

if [[ ! -f .env ]]; then
  if [[ "${DRY_RUN}" == true ]]; then log "[dry-run] would copy .env.example -> .env"; else
    cp .env.example .env; log "created .env from .env.example — EDIT secrets"
  fi
else
  log ".env exists"
fi

if [[ "${DRY_RUN}" == true ]]; then log "[dry-run] would run: uv sync --group dev && git config core.hooksPath .githooks"; else
  uv sync --group dev || log "uv sync failed — install uv from https://docs.astral.sh/uv/"
  git config core.hooksPath .githooks
  log "hooks enabled (.githooks)"
fi

log "bootstrap done — next: make up PROFILE=core"
