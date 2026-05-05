#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ITERATIONS="${SW_BLOCK_ATTACH_DETACH_ITERATIONS:-3}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
BASE_ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-attach-detach-${RUN_ID}}"
SUMMARY="$BASE_ARTIFACT_DIR/summary.log"

mkdir -p "$BASE_ARTIFACT_DIR"

log() {
  printf '[attach-loop] %s\n' "$*" | tee -a "$SUMMARY"
}

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

log "root=$ROOT"
log "artifact_dir=$BASE_ARTIFACT_DIR"
log "iterations=$ITERATIONS"
log "launcher_pvc_owner_ref=${SW_BLOCK_LAUNCHER_PVC_OWNER_REF:-1}"

for iter in $(seq 1 "$ITERATIONS"); do
  iter_dir="$BASE_ARTIFACT_DIR/iter-${iter}"
  log "iteration ${iter}: start"
  SW_BLOCK_ARTIFACT_DIR="$iter_dir" \
  SW_BLOCK_LAUNCHER_PVC_OWNER_REF="${SW_BLOCK_LAUNCHER_PVC_OWNER_REF:-1}" \
    bash "$ROOT/scripts/run-alpha-app-demo.sh" "$ROOT" \
    >"$iter_dir.stdout.log" 2>"$iter_dir.stderr.log"

  if ! grep -q "PASS: app pod wrote data" "$iter_dir/run.log"; then
    log "iteration ${iter}: missing PASS line"
    exit 1
  fi
  if [[ -f "$iter_dir/iscsi-sessions.after-delete.txt" ]] &&
     ! grep -q "No active sessions" "$iter_dir/iscsi-sessions.after-delete.txt"; then
    log "iteration ${iter}: dangling iSCSI session"
    cat "$iter_dir/iscsi-sessions.after-delete.txt" | tee -a "$SUMMARY"
    exit 1
  fi
  log "iteration ${iter}: PASS"
done

log "PASS: ${ITERATIONS} attach/detach app PVC cycles completed"
log "artifacts=$BASE_ARTIFACT_DIR"
