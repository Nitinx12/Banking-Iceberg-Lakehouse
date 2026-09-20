#!/usr/bin/env bash
# scripts/sh/lib.sh — shared helpers for all shell scripts (Architecture 16.2)
# Usage: source "$(dirname "$0")/lib.sh"
set -euo pipefail

# Logging with stage scope — one file per stage (Architecture 12.1)
# LOG_DIR defaults to logs/, STAGE defaults to generic
LOG_DIR="${LOG_DIR:-logs}"
STAGE="${STAGE:-generic}"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/${STAGE}.log"

log()  { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] [$STAGE] $*" | tee -a "$LOG_FILE"; }
warn() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] [$STAGE] WARN: $*" | tee -a "$LOG_FILE" >&2; }
die()  { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] [$STAGE] ERROR: $*" | tee -a "$LOG_FILE" >&2; exit 1; }

require_env() {
  local var="$1"
  if [ -z "${!var:-}" ]; then die "required env $var is not set"; fi
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command $1 not found"
}

# Retry helper: retry 3 <cmd>
retry() {
  local n=3; local delay=2; local i=0
  while [ $i -lt $n ]; do
    if "$@"; then return 0; fi
    i=$((i+1)); warn "attempt $i/$n failed: $* — retrying in ${delay}s"; sleep $delay; delay=$((delay*2))
  done
  die "all $n attempts failed: $*"
}

# Load .env if present
if [ -f ".env" ]; then set -a; source .env 2>/dev/null || true; set +a; fi
