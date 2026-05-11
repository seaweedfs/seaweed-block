#!/usr/bin/env bash
set -euo pipefail

RUNNER_ROOT="${SW_TEST_RUNNER_ROOT:-}"
OUTPUT_DIR=""
REPO_URL="${SW_TEST_RUNNER_REPO_URL:-https://github.com/pingqiu/sw-test-runner.git}"
NO_CLONE=0

usage() {
  cat <<'USAGE'
usage: internal/tools/build-swblock.sh [--runner-root PATH] [--output-dir PATH] [--repo-url URL] [--no-clone]

Builds the sw-test-runner V3 binary (swblock) and prints the resulting path.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runner-root)
      RUNNER_ROOT="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --repo-url)
      REPO_URL="$2"
      shift 2
      ;;
    --no-clone)
      NO_CLONE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PRODUCT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
if [[ -z "$OUTPUT_DIR" ]]; then
  OUTPUT_DIR="$PRODUCT_ROOT/.tools"
fi

has_swblock_source() {
  [[ -n "$1" && -f "$1/cmd/swblock/main.go" ]]
}

RESOLVED_RUNNER=""
for candidate in \
  "$RUNNER_ROOT" \
  "$(cd "$PRODUCT_ROOT/.." && pwd)/sw-test-runner" \
  "/c/work/sw-test-runner" \
  "/c/work/sw-test-runner-standalone" \
  "/c/work/seaweedfs/learn/sw-test-runner-standalone"
do
  if has_swblock_source "$candidate"; then
    RESOLVED_RUNNER="$(cd "$candidate" && pwd)"
    break
  fi
done

if [[ -z "$RESOLVED_RUNNER" ]]; then
  if [[ "$NO_CLONE" == "1" ]]; then
    echo "sw-test-runner checkout not found. Set --runner-root or SW_TEST_RUNNER_ROOT." >&2
    exit 2
  fi
  command -v git >/dev/null 2>&1 || { echo "missing required command: git" >&2; exit 2; }
  CLONE_TARGET="${RUNNER_ROOT:-$(cd "$PRODUCT_ROOT/.." && pwd)/sw-test-runner}"
  if [[ -e "$CLONE_TARGET" ]]; then
    echo "candidate runner path exists but lacks cmd/swblock/main.go: $CLONE_TARGET" >&2
    exit 2
  fi
  echo "[swblock-build] cloning $REPO_URL -> $CLONE_TARGET"
  git clone "$REPO_URL" "$CLONE_TARGET"
  RESOLVED_RUNNER="$(cd "$CLONE_TARGET" && pwd)"
fi

command -v go >/dev/null 2>&1 || { echo "missing required command: go" >&2; exit 2; }
mkdir -p "$OUTPUT_DIR"

OUT_PATH="$OUTPUT_DIR/swblock"
echo "[swblock-build] runner_root=$RESOLVED_RUNNER"
echo "[swblock-build] output=$OUT_PATH"
(cd "$RESOLVED_RUNNER" && go build -o "$OUT_PATH" ./cmd/swblock)
printf '%s\n' "$OUT_PATH" >"$OUTPUT_DIR/swblock.path"
printf '%s\n' "$OUT_PATH"
