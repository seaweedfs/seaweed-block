#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
ARTIFACT_ROOT="${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-iscsi-compat-soak-${RUN_ID}}"
OS_ITERATIONS="${SW_BLOCK_P8_OS_ITERATIONS:-2}"
OS_FIO_RUNTIME="${SW_BLOCK_P8_OS_FIO_RUNTIME:-120}"
OS_FIO_SIZE="${SW_BLOCK_P8_OS_FIO_SIZE:-128m}"
RUN_K8S_FIO="${SW_BLOCK_P8_RUN_K8S_FIO:-0}"
RUN_ATTACH_LOOP="${SW_BLOCK_P8_RUN_ATTACH_LOOP:-0}"
ATTACH_ITERATIONS="${SW_BLOCK_P8_ATTACH_ITERATIONS:-3}"

mkdir -p "$ARTIFACT_ROOT"

log() {
  printf '[iscsi-soak] %s\n' "$*" | tee -a "$ARTIFACT_ROOT/run.log"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

require_cmd bash
require_cmd grep
require_cmd tee

log "run_id=$RUN_ID"
log "root=$ROOT"
log "artifact_root=$ARTIFACT_ROOT"
log "os_iterations=$OS_ITERATIONS"
log "os_fio_runtime=${OS_FIO_RUNTIME}s"
log "os_fio_size=$OS_FIO_SIZE"
log "run_k8s_fio=$RUN_K8S_FIO"
log "run_attach_loop=$RUN_ATTACH_LOOP"

summary="$ARTIFACT_ROOT/summary.md"
cat >"$summary" <<EOF
# iSCSI P8 Compatibility Soak

Run ID: \`$RUN_ID\`
Root: \`$ROOT\`

This is compatibility and soak evidence. It is not a product performance
claim.

| Step | Result | Artifact |
|---|---|---|
EOF

overall=0

run_step() {
  local name="$1"
  local artifact="$2"
  shift 2
  mkdir -p "$artifact"
  log "start ${name}"
  if "$@" >"$artifact/stdout.log" 2>"$artifact/stderr.log"; then
    log "${name}: PASS"
    printf '| `%s` | `PASS` | `%s` |\n' "$name" "$artifact" >>"$summary"
  else
    log "${name}: FAIL"
    printf '| `%s` | `FAIL` | `%s` |\n' "$name" "$artifact" >>"$summary"
    overall=1
  fi
}

run_step "os-fio-repeat" "$ARTIFACT_ROOT/os-fio-repeat" \
  env \
    SW_BLOCK_ARTIFACT_DIR="$ARTIFACT_ROOT/os-fio-repeat" \
    SW_BLOCK_ISCSI_ITERATIONS="$OS_ITERATIONS" \
    SW_BLOCK_ISCSI_STRESS=fio \
    SW_BLOCK_ISCSI_FIO_RUNTIME="$OS_FIO_RUNTIME" \
    SW_BLOCK_ISCSI_FIO_SIZE="$OS_FIO_SIZE" \
    bash "$ROOT/scripts/run-iscsi-os-smoke.sh" "$ROOT"

if [[ "$RUN_K8S_FIO" == "1" || "$RUN_K8S_FIO" == "true" ]]; then
  run_step "k8s-fio" "$ARTIFACT_ROOT/k8s-fio" \
    env \
      SW_BLOCK_ARTIFACT_DIR="$ARTIFACT_ROOT/k8s-fio" \
      bash "$ROOT/scripts/run-k8s-alpha-fio.sh" "$ROOT"
fi

if [[ "$RUN_ATTACH_LOOP" == "1" || "$RUN_ATTACH_LOOP" == "true" ]]; then
  run_step "k8s-attach-detach" "$ARTIFACT_ROOT/k8s-attach-detach" \
    env \
      SW_BLOCK_ARTIFACT_DIR="$ARTIFACT_ROOT/k8s-attach-detach" \
      SW_BLOCK_ATTACH_DETACH_ITERATIONS="$ATTACH_ITERATIONS" \
      bash "$ROOT/scripts/run-k8s-attach-detach-loop.sh" "$ROOT"
fi

log "summary=$summary"
if [[ "$overall" -ne 0 ]]; then
  log "FAIL: one or more soak steps failed"
  exit "$overall"
fi

log "PASS: compatibility soak completed"
