#!/usr/bin/env bash
# scripts/sh/wait_for_services.sh — poll compose health (Architecture 16.2)
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib.sh"

STAGE="wait"
LOG_FILE="${LOG_DIR}/${STAGE}.log"
RETRIES="${1:-30}"
SLEEP_S="${2:-5}"

log "waiting for core services (retries=${RETRIES}, sleep=${SLEEP_S}s)"

retry_cmd() {
  docker compose --profile core ps --format json 2>/dev/null | grep -q "healthy" || docker compose ps | grep -q "Up"
}

for i in $(seq 1 "${RETRIES}"); do
  if docker compose --profile core ps 2>/dev/null | grep -q "healthy" || pg_isready -h "${POSTGRES_HOST:-localhost}" -p "${POSTGRES_PORT:-5433}" -U "${POSTGRES_USER:-postgres}" >/dev/null 2>&1; then
    log "core services healthy (attempt $i)"
    exit 0
  fi
  log "not ready — attempt $i/${RETRIES} sleeping ${SLEEP_S}s"
  sleep "${SLEEP_S}"
done

die "services not healthy after ${RETRIES} attempts — check: docker compose --profile core ps; docker compose logs"
