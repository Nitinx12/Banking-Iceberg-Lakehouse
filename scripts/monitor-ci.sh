#!/usr/bin/env bash
# =====================================================================
# monitor-ci.sh — GitHub Actions monitor for this repository
#
# Reports on the workflows that gate every push:
#   1. last 10 runs across all workflows (status, conclusion, branch)
#   2. per-workflow latest status for the required set
#   3. failed runs in the last 7 days, with URLs
#   4. oldest still-green nightly E2E (environment rot canary)
#
# Usage:   bash scripts/monitor-ci.sh
#          bash scripts/monitor-ci.sh --report   # also write .reports/ci-<ts>.txt
# Requires: gh CLI authenticated ('gh auth login')
# Exit:    0 always — a monitor, not a gate.
# =====================================================================
set -uo pipefail

cd "$(dirname "$0")/.."

REPORT=""
if [ "${1:-}" = "--report" ]; then
  mkdir -p .reports
  REPORT=".reports/ci-$(date +%Y%m%d-%H%M%S).txt"
  exec > >(tee "$REPORT") 2>&1
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "gh CLI not found — install it and run 'gh auth login'" >&2
  exit 1
fi

if ! gh auth status >/dev/null 2>&1; then
  echo "gh not authenticated — run 'gh auth login'" >&2
  exit 1
fi

REPO=$(gh repo view --json nameWithOwner --jq .nameWithOwner)
echo "CI monitor — ${REPO} — $(date)"
echo "======================================================"

echo ""
echo "[1] Last 10 runs (all workflows)"
echo "--------------------------------------------------------------"
gh run list --limit 10 \
  --json displayTitle,workflowName,status,conclusion,createdAt,headBranch \
  --template '{{range .}}{{printf "%-28s %-18s %-10s %-10s %s\n" (truncate 28 .workflowName) (truncate 18 .headBranch) .status (or .conclusion "-") (truncate 40 .displayTitle)}}{{end}}'

echo ""
echo "[2] Required workflows — latest status each"
echo "--------------------------------------------------------------"
for wf in ci.yml nightly-e2e.yml commitlint.yml codacy.yml label.yml label-sync.yml greetings.yml summary.yml; do
  latest=$(gh run list --workflow "$wf" --limit 1 \
    --json status,conclusion,createdAt \
    --jq '.[0] | "\(.createdAt)  \(.status)  \(.conclusion // "-")"')
  if [ -n "$latest" ]; then
    printf '%-18s %s\n' "$wf" "$latest"
  else
    printf '%-18s %s\n' "$wf" "never run"
  fi
done

echo ""
echo "[3] Failed runs in the last 7 days"
echo "--------------------------------------------------------------"
since=$(date -u -d '7 days ago' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
  || date -u -v-7d +%Y-%m-%dT%H:%M:%SZ)
fails=$(gh run list --status failure --created ">=${since}" \
  --json databaseId,workflowName,conclusion,url \
  --jq '.[] | "\(.workflowName)\t\(.url)"')
if [ -n "$fails" ]; then
  echo "$fails" | column -t -s $'\t'
  n=$(echo "$fails" | wc -l | tr -d ' ')
  echo "-> ${n} failure(s) — check the URLs above"
else
  echo "none — all runs green in the last 7 days"
fi

echo ""
echo "[4] Nightly E2E — last 5 (environment-rot canary)"
echo "--------------------------------------------------------------"
gh run list --workflow nightly-e2e.yml --limit 5 \
  --json createdAt,conclusion,status \
  --template '{{range .}}{{printf "%s  %-10s %s\n" .createdAt .status (or .conclusion "-")}}{{end}}'

echo ""
echo "monitor complete${REPORT:+ — report written to ${REPORT}}"
