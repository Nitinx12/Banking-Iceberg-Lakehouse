#!/bin/bash
# format-python.sh
#
# PostToolUse hook (matcher: Edit|Write). After Claude edits or writes a .py
# file, format and lint it with ruff via uv, so every notebook/src file stays
# consistent with the same `uv run ruff check .` step CI runs (README §4).
#
# Runs quietly and never blocks the turn: if `uv`/`ruff` aren't set up yet
# (e.g. before the first `uv sync`), this just no-ops instead of erroring.

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

if [[ "$FILE_PATH" != *.py ]]; then
  exit 0
fi

if [ ! -f "$FILE_PATH" ]; then
  exit 0
fi

uv run ruff format "$FILE_PATH" >/dev/null 2>&1
uv run ruff check --fix "$FILE_PATH" >/dev/null 2>&1

exit 0
