#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_ROOT="${SW_BLOCK_ARTIFACT_DIR:-}"
SEARCH_ROOT="${SW_BLOCK_SEARCH_ROOT:-$ROOT/results}"
EXPECT_COMMIT="${SW_BLOCK_EXPECT_COMMIT:-$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || true)}"
SWBLOCK_CMD="${SWBLOCK_CMD:-swblock}"
SWBLOCK_RUNNER_ROOT="${SWBLOCK_RUNNER_ROOT:-}"
JSON_FLAG="${SW_BLOCK_JSON:-0}"

to_bash_path() {
  local p="$1"
  if command -v cygpath >/dev/null 2>&1 && [[ "$p" =~ ^[A-Za-z]:\\ ]]; then
    cygpath -u "$p"
    return
  fi
  printf '%s\n' "$p"
}

find_latest_protocol_gate_artifact() {
  local root="$1"
  [[ -d "$root" ]] || {
    printf 'search root does not exist: %s\n' "$root" >&2
    return 1
  }
  find "$root" -type f -name result.json -printf '%T@ %p\n' 2>/dev/null |
    sort -nr |
    while read -r _ path; do
      if python3 - "$path" <<'PY'
import json, sys
try:
    with open(sys.argv[1], encoding="utf-8") as f:
        doc = json.load(f)
except Exception:
    raise SystemExit(1)
raise SystemExit(0 if doc.get("scenario") == "protocol-release-gate-suite" else 1)
PY
      then
        dirname "$path"
        return 0
      fi
    done
}

if [[ -n "$SWBLOCK_RUNNER_ROOT" ]]; then
  SWBLOCK_RUNNER_ROOT="$(to_bash_path "$SWBLOCK_RUNNER_ROOT")"
fi
if [[ -z "$ARTIFACT_ROOT" ]]; then
  ARTIFACT_ROOT="$(find_latest_protocol_gate_artifact "$(to_bash_path "$SEARCH_ROOT")")"
fi

args=(validate-bundle --profile protocol-release-gate)
if [[ -n "$EXPECT_COMMIT" ]]; then
  args+=(--expect-commit "$EXPECT_COMMIT")
fi
if [[ "$JSON_FLAG" == "1" || "$JSON_FLAG" == "true" ]]; then
  args+=(--json)
fi
args+=("$(to_bash_path "$ARTIFACT_ROOT")")

printf '[protocol-gate-validate] artifact_root=%s\n' "$ARTIFACT_ROOT"
printf '[protocol-gate-validate] expect_commit=%s\n' "$EXPECT_COMMIT"
if [[ -n "$SWBLOCK_RUNNER_ROOT" ]]; then
  (cd "$SWBLOCK_RUNNER_ROOT" && go run ./cmd/swblock "${args[@]}")
else
  "$SWBLOCK_CMD" "${args[@]}"
fi
