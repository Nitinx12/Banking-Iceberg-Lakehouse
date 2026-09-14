#!/usr/bin/env bash
# =====================================================================
# reset.sh — wipe local pipeline state and optionally rebuild fresh
#
# Deletes the disposable local state (.spark/ warehouse, checkpoints,
# quarantine, spark-warehouse/) and can immediately rebuild with a fresh
# generate + pipeline run. Silver MERGEs make re-runs safe, but after
# schema or config changes a clean slate is the honest reset.
#
# Usage:  bash scripts/reset.sh            # wipe only (asks first)
#         bash scripts/reset.sh --rebuild  # wipe + generate + pipeline
#         bash scripts/reset.sh --yes      # skip the confirmation
# =====================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

REBUILD=0
ASSUME_YES=0
for arg in "$@"; do
  case "$arg" in
    --rebuild) REBUILD=1 ;;
    --yes|-y)  ASSUME_YES=1 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

# never touch anything that is not disposable state
TARGETS=(".spark" "spark-warehouse")

echo "StreamFlix local state reset"
echo "======================================================"
echo "This will permanently delete:"
for t in "${TARGETS[@]}"; do
  if [ -d "$t" ]; then
    size=$(du -sh "$t" 2>/dev/null | cut -f1)
    echo "  $t/  ($size)"
  fi
done

if [ "$ASSUME_YES" -ne 1 ]; then
  echo ""
  read -r -p "Proceed? [y/N] " answer
  case "$answer" in
    [yY]|[yY][eE][sS]) ;;
    *) echo "aborted — nothing deleted"; exit 0 ;;
  esac
fi

for t in "${TARGETS[@]}"; do
  rm -rf "$t"
  echo "removed $t/"
done

if [ "$REBUILD" -eq 1 ]; then
  echo ""
  echo "==> Rebuilding: generate (default volumes) + pipeline"
  uv run python main.py generate
  uv run python main.py pipeline
  echo ""
  echo "==> Post-rebuild monitor"
  bash scripts/monitor-pipeline.sh || true
else
  echo ""
  echo "State wiped. Rebuild with:"
  echo "  uv run python main.py generate"
  echo "  uv run python main.py pipeline"
  echo "or in one go:  bash scripts/reset.sh --rebuild --yes"
fi
