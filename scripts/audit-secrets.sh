#!/usr/bin/env bash
# =====================================================================
# audit-secrets.sh — pre-push secret hygiene check
#
# Answers three questions before anything leaves the machine:
#   1. is .env present but NOT tracked by git?
#   2. do any TRACKED files contain credential patterns?
#   3. does anything STAGED for commit contain credential patterns?
#
# Complements .githooks/pre-commit (which blocks .env itself) by scanning
# file *contents*, not just paths. Report files under .reports/ are also
# checked — they must never capture secrets.
#
# Usage:  bash scripts/audit-secrets.sh
# Exit:   0 clean, 1 if anything suspicious is found.
# =====================================================================
set -uo pipefail

cd "$(dirname "$0")/.."

FAIL=0

# Credential patterns — token formats with enough structure to avoid
# matching prose. This script's own source is excluded from the scan.
PATTERNS=(
  'dapi[0-9a-f]{20,}'                                  # Databricks PAT
  'ghp_[A-Za-z0-9]{20,}'                               # GitHub PAT (classic)
  'github_pat_[A-Za-z0-9_]{20,}'                       # GitHub PAT (fine-grained)
  'AKIA[0-9A-Z]{16}'                                   # AWS access key id
  'xox[baprs]-[A-Za-z0-9-]{10,}'                       # Slack token
  'sk-[A-Za-z0-9]{20,}'                                # generic API key
  '-----BEGIN [A-Z ]*PRIVATE KEY-----'                 # private key material
)

echo "Secret audit — $(date)"
echo "======================================================"

# --------------------------------------------------- 1. .env must be local-only
echo ""
echo "[1] .env tracked status"
if [ -f .env ]; then
  if git ls-files --error-unmatch .env >/dev/null 2>&1; then
    echo "  [FAIL] .env IS TRACKED by git — remove it: git rm --cached .env"
    FAIL=1
  elif git check-ignore -q .env; then
    echo "  [PASS] .env exists locally and is gitignored"
  else
    echo "  [FAIL] .env exists but is NOT in .gitignore"
    FAIL=1
  fi
else
  echo "  [PASS] no .env present (nothing to leak)"
fi

# --------------------------------------------------------- 2. tracked files
echo ""
echo "[2] Tracked file contents"
TRACKED_HITS=0
for pattern in "${PATTERNS[@]}"; do
  # exclude this script (it contains the patterns) and the .env template
  while IFS= read -r line; do
    echo "  [FAIL] $line"
    TRACKED_HITS=1
  done < <(git grep -nIE "$pattern" -- . \
    ':(exclude)scripts/audit-secrets.sh' ':(exclude).env.example' 2>/dev/null)
done
if [ "$TRACKED_HITS" -eq 0 ]; then
  echo "  [PASS] no credential patterns in tracked files"
else
  FAIL=1
fi

# ----------------------------------------------------------- 3. staged diff
echo ""
echo "[3] Staged changes"
STAGED_HITS=0
if git diff --cached --quiet 2>/dev/null; then
  echo "  [PASS] nothing staged — skipping"
else
  for pattern in "${PATTERNS[@]}"; do
    while IFS= read -r line; do
      echo "  [FAIL] staged: $line"
      STAGED_HITS=1
    done < <(git diff --cached | grep -nE "$pattern" 2>/dev/null \
      | sed 's/^/line /')
  done
  if [ "$STAGED_HITS" -eq 0 ]; then
    echo "  [PASS] no credential patterns in staged changes"
  else
    FAIL=1
  fi
fi

# ------------------------------------------------------------------- result
echo ""
if [ "$FAIL" -eq 0 ]; then
  echo "audit-secrets: clean — nothing credential-shaped found."
else
  echo "audit-secrets: SUSPICIOUS FINDINGS — resolve before pushing." >&2
  echo "If a hit is a false positive (docs, test fixtures), narrow the" >&2
  echo "pattern's exclude pathset in scripts/audit-secrets.sh." >&2
fi
exit "$FAIL"
