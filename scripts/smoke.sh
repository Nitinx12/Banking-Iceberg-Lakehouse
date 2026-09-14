#!/usr/bin/env bash
# =====================================================================
# smoke.sh — run the nightly E2E workflow locally
#
# Replicates .github/workflows/nightly-e2e.yml on your machine, with the
# same step order and the same small data volumes:
#   1. ruff check (ci.yml parity)
#   2. generate small landing data
#   3. GX suites resolve
#   4. full pytest suite
#
# WARNING: step 2 overwrites today's landing/<table>/*.json with the small
# nightly volumes (same as the CI runner). Skip it with SMOKE_GENERATE=0
# if you want to keep your current local data.
#
# Usage:  bash scripts/smoke.sh
#         SMOKE_GENERATE=0 bash scripts/smoke.sh   # keep existing landing data
# =====================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

GENERATE="${SMOKE_GENERATE:-1}"
START=$(date +%s)

step() { printf '\n==> [%ss] %s\n' "$(( $(date +%s) - START ))" "$1"; }
fail() {
  echo ""
  echo "SMOKE FAILED at: $1" >&2
  echo "This is what the nightly workflow would report at 02:30 UTC." >&2
  exit 1
}

echo "StreamFlix local smoke — $(date)"
echo "======================================================"

# ------------------------------------------------------------- 1. lint
step "ruff check"
uv run ruff check . || fail "ruff check"

# ---------------------------------------------------------- 2. generate
if [ "$GENERATE" = "1" ]; then
  step "Generating small landing data (nightly volumes)"
  # identical flags to nightly-e2e.yml so local == CI
  uv run python main.py generate \
    --content 500 --users 200 --watch 5000 --billing 1000 \
    --devices 200 --profiles 300 --promotions 50 --redemptions 500 \
    --tickets 300 --cdn 2000 --ratings 500 \
    || fail "generate"
else
  step "Skipping generate (SMOKE_GENERATE=0) — using existing landing data"
fi

# ---------------------------------------------------------- 3. GX suites
step "GX suites resolve"
uv run python main.py gx --list || fail "gx --list"
ls gx/expectations/*.json >/dev/null 2>&1 || fail "no GX suites found"

# ------------------------------------------------------------- 4. tests
step "Full test suite"
uv run pytest -q || fail "pytest"

# --------------------------------------------------------------- result
echo ""
echo "======================================================"
echo "SMOKE PASSED in $(( $(date +%s) - START ))s — all four steps green."
echo "The nightly workflow should pass on the next 02:30 UTC run."
