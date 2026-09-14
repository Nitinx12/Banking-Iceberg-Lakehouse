#!/usr/bin/env bash
# =====================================================================
# health-check.sh — pre-flight environment check for StreamFlix Lakehouse
#
# Verifies everything the pipeline and CI need before you run them:
#   tooling (uv, python, java, gh), dependencies (.env, lockfile),
#   data (landing/), and local state (.spark/).
#
# Usage:  bash scripts/health-check.sh
# Exit:   0 if all critical checks pass, 1 otherwise. Warnings don't fail.
# =====================================================================
set -uo pipefail

cd "$(dirname "$0")/.."

PASS=0
FAIL=0
WARN=0

ok()   { printf '  [PASS] %s\n' "$1"; PASS=$((PASS + 1)); }
bad()  { printf '  [FAIL] %s\n' "$1"; FAIL=$((FAIL + 1)); }
warn() { printf '  [WARN] %s\n' "$1"; WARN=$((WARN + 1)); }

section() { printf '\n== %s ==\n' "$1"; }

# ---------------------------------------------------------------- tooling
section "Tooling"

if command -v uv >/dev/null 2>&1; then
  ok "uv $(uv --version 2>/dev/null | awk '{print $2}')"
else
  bad "uv not found — install from https://docs.astral.sh/uv/"
fi

if uv run python -c 'import sys; sys.exit(0 if sys.version_info >= (3, 13) else 1)' 2>/dev/null; then
  ok "python $(uv run python --version 2>/dev/null | awk '{print $2}') (>= 3.13)"
else
  bad "python < 3.13 (pyproject requires >= 3.13)"
fi

if java -version 2>&1 | grep -q 'version "17'; then
  ok "java 17 ($(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d. -f1-2))"
elif java -version >/dev/null 2>&1; then
  warn "java found but not 17 — pyspark 4.x officially targets 17+"
else
  bad "java not found — Spark needs a JDK"
fi

if command -v gh >/dev/null 2>&1; then
  ok "gh CLI available (used by scripts/monitor-ci.sh)"
else
  warn "gh CLI not found — scripts/monitor-ci.sh will not work"
fi

# ------------------------------------------------------------ dependencies
section "Dependencies"

if [ -f uv.lock ]; then
  ok "uv.lock present"
else
  bad "uv.lock missing — run 'uv lock'"
fi

if uv lock --check >/dev/null 2>&1; then
  ok "lockfile in sync with pyproject.toml"
else
  bad "lockfile out of sync — run 'uv lock'"
fi

if [ -f .env ]; then
  ok ".env present"
  # never print values — only that the keys exist
  for key in DATABRICKS_HOST DATABRICKS_TOKEN RAW_DATA_PATH; do
    if grep -q "^${key}=" .env; then
      ok "  ${key} set"
    else
      warn "  ${key} missing (needed for push / CE runs)"
    fi
  done
else
  warn ".env missing — local pipeline runs work, push/CE runs will not"
fi

if uv run python -c "import pyspark, delta, rich, faker" >/dev/null 2>&1; then
  ok "core deps importable (pyspark, delta, rich, faker)"
else
  bad "core deps not importable — run 'uv sync'"
fi

# -------------------------------------------------------------------- data
section "Landing data"

if [ -d landing ] && [ -n "$(ls -A landing 2>/dev/null)" ]; then
  n_tables=$(find landing -mindepth 1 -maxdepth 1 -type d | wc -l)
  n_files=$(find landing -name '*.json' | wc -l)
  ok "landing/ has ${n_tables} sources, ${n_files} JSON files"
  [ "${n_tables}" -eq 11 ] || warn "expected 11 sources, found ${n_tables} — run 'main.py generate'"
  # every source dir should have at least one non-empty file
  for d in landing/*/; do
    name=$(basename "$d")
    if find "$d" -name '*.json' -size +0c | grep -q .; then
      :
    else
      bad "landing/${name} has no non-empty JSON — regenerate"
    fi
  done
else
  warn "landing/ empty — run 'uv run python main.py generate'"
fi

# ------------------------------------------------------------ local state
section "Local pipeline state (.spark/)"

if [ -d .spark/warehouse ]; then
  size=$(du -sh .spark/warehouse 2>/dev/null | cut -f1)
  n_dbs=$(find .spark/warehouse -mindepth 1 -maxdepth 1 -type d | wc -l)
  ok "warehouse present: ${size}, ${n_dbs} schema dirs"
  warn "state is disposable — 'Remove-Item -Recurse -Force .spark' (PowerShell) resets it"
else
  warn "no local warehouse yet — run 'uv run python main.py pipeline'"
fi

if [ -d .spark/quarantine ] && [ -n "$(ls -A .spark/quarantine 2>/dev/null)" ]; then
  warn "quarantine dir not empty — inspect with scripts/monitor-pipeline.sh"
fi

# -------------------------------------------------------------------- git
section "Repo"

if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  branch=$(git branch --show-current)
  dirty=$(git status --porcelain | wc -l | tr -d ' ')
  ok "on branch '${branch}', ${dirty} uncommitted change(s)"
  [ "${dirty}" -gt 0 ] && warn "uncommitted changes — CI runs what is pushed, not what is local"
else
  bad "not a git repository"
fi

# ------------------------------------------------------------------ result
printf '\n'
printf 'health-check: %d passed, %d warnings, %d failed\n' "$PASS" "$WARN" "$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
