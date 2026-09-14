#!/bin/bash
# protect-files.sh
#
# PreToolUse hook (matcher: Edit|Write). Blocks edits to files that should
# never be hand-edited in this project:
#   - .env              (secrets stand-in, see README §3)
#   - uv.lock            (managed by `uv sync` / `uv add`, not by hand)
#   - anything under a landing/raw data folder (simulates an upstream source
#     system that Bronze should treat as read-only input)
#
# Exit 2 blocks the tool call and feeds the stderr message back to Claude as
# feedback, so it can pick a different approach instead of just failing.

INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')

# Normalize Windows backslash separators so the patterns below match.
FILE_PATH="${FILE_PATH//\\//}"

PROTECTED_PATTERNS=(".env" "uv.lock" "/landing/" "/raw/")

for pattern in "${PROTECTED_PATTERNS[@]}"; do
  if [[ "$FILE_PATH" == *"$pattern"* ]]; then
    echo "Blocked: $FILE_PATH matches protected pattern '$pattern'. This file is generated or dependency-managed, not hand-edited — run the matching uv/data-generator command instead." >&2
    exit 2
  fi
done

exit 0
