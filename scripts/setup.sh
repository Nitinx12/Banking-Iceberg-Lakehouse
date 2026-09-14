#!/usr/bin/env bash
# =====================================================================
# setup.sh — one-shot bootstrap for new contributors
#
# Does everything CONTRIBUTING.md § Setup lists, in order:
#   1. verify uv is installed
#   2. uv sync (from the frozen lockfile)
#   3. create .env from .env.example if missing
#   4. install local git hooks (commit-msg lint)
#   5. make scripts/ executable
#   6. run the environment health check
#
# Usage:  bash scripts/setup.sh
# =====================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

step() { printf '\n==> %s\n' "$1"; }

# ------------------------------------------------------------------- uv
step "Checking uv"
if ! command -v uv >/dev/null 2>&1; then
  echo "uv not found." >&2
  echo "Install it first:  https://docs.astral.sh/uv/getting-started/installation/" >&2
  exit 1
fi
echo "uv $(uv --version | awk '{print $2}')"

# ------------------------------------------------------------ dependencies
step "Syncing dependencies (uv sync)"
uv sync

# ------------------------------------------------------------------ .env
step "Configuring .env"
if [ -f .env ]; then
  echo ".env already exists — leaving it alone"
else
  if [ -f .env.example ]; then
    cp .env.example .env
    echo "created .env from .env.example"
    echo "  -> fill in DATABRICKS_HOST / DATABRICKS_TOKEN for CE runs"
    echo "     (local pipeline runs work without them)"
  else
    echo "no .env.example found — skipping"
  fi
fi

# ----------------------------------------------------------------- hooks
step "Installing git hooks"
if [ -d .githooks ]; then
  git config core.hooksPath .githooks
  echo "core.hooksPath = .githooks (Conventional Commits enforced locally)"
else
  echo "no .githooks/ directory — skipping"
fi

# --------------------------------------------------------------- scripts
step "Making scripts executable"
chmod +x scripts/*.sh 2>/dev/null || true
echo "done"

# ----------------------------------------------------------- health check
step "Running environment health check"
if bash scripts/health-check.sh; then
  :
else
  echo ""
  echo "health check reported failures — fix them, then re-run:" >&2
  echo "  bash scripts/health-check.sh" >&2
  exit 1
fi

# -------------------------------------------------------------- next steps
cat <<'EOF'

Setup complete. Next steps:

  uv run pytest                        # full test suite (~2-3 min)
  uv run python main.py generate       # synthetic landing data
  uv run python main.py pipeline       # bronze -> silver -> gold, locally

See CONTRIBUTING.md for ground rules and docs/STATUS.md for open issues.
EOF
