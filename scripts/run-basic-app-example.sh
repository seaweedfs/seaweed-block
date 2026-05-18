#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-basic-app-$(date -u +%Y%m%dT%H%M%SZ)}"
NAMESPACE="${SW_BLOCK_APP_NAMESPACE:-default}"
MASTER_PORT="${SW_BLOCK_MASTER_PORT_FORWARD_PORT:-}"
CLEANUP_REQUESTED="${SW_BLOCK_BASIC_APP_CLEANUP:-1}"
PVC_NAME="sw-block-example-pvc"
WRITER_POD="sw-block-example-writer"
READER_POD="sw-block-example-reader"
EXAMPLE_DIR="$ROOT/examples/kubernetes/basic-app"
FIRST_VOLUME_STATUS="ok"
FAILED_PHASE=""
CLEANUP_STATUS="external_to_script"
if [[ "$CLEANUP_REQUESTED" == "1" || "$CLEANUP_REQUESTED" == "true" ]]; then
  CLEANUP_STATUS="pending"
fi
PVC_PHASE_SNAPSHOT=""
PV_NAME_SNAPSHOT=""
VOLUME_ID_SNAPSHOT=""

mkdir -p "$ARTIFACT_DIR"

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

log() {
  printf '[basic-app] %s\n' "$*" | tee -a "$ARTIFACT_DIR/run.log"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

safe_capture() {
  local out="$1"
  shift
  "$@" >"$out" 2>&1 || true
}

capture_failure_diagnostics() {
  local phase="$1"
  local out="$ARTIFACT_DIR/diagnostics/$phase"
  mkdir -p "$out"
  safe_capture "$out/pods.txt" kubectl -n "$NAMESPACE" get pods -o wide
  safe_capture "$out/pvc.txt" kubectl -n "$NAMESPACE" get pvc "$PVC_NAME" -o wide
  safe_capture "$out/blockvolume-deployments.txt" kubectl -n "$NAMESPACE" get deploy -l app=sw-blockvolume -o wide
  safe_capture "$out/writer-describe.txt" kubectl -n "$NAMESPACE" describe pod "$WRITER_POD"
  safe_capture "$out/reader-describe.txt" kubectl -n "$NAMESPACE" describe pod "$READER_POD"
  safe_capture "$out/events.txt" kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp
}

sw_block_cmd() {
  if [[ -n "${SW_BLOCK_CLI:-}" ]]; then
    "$SW_BLOCK_CLI" "$@"
  elif command -v sw-block >/dev/null 2>&1; then
    sw-block "$@"
  else
    go run ./cmd/sw-block "$@"
  fi
}

find_free_port() {
  if command -v python3 >/dev/null 2>&1; then
    python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
    return
  fi
  for port in $(seq 29333 29433); do
    if ! (echo >"/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
      echo "$port"
      return
    fi
  done
  echo "9333"
}

wait_for_port() {
  local port="$1"
  for _ in $(seq 1 30); do
    if (echo >"/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

stop_port_forward() {
  if [[ -n "${PF_PID:-}" ]]; then
    kill "$PF_PID" >/dev/null 2>&1 || true
    wait "$PF_PID" >/dev/null 2>&1 || true
  fi
}

collect_status_evidence() {
  local out="$ARTIFACT_DIR/status"
  local rc=0
  mkdir -p "$out"
  kubectl -n kube-system port-forward deploy/sw-blockmaster "${MASTER_PORT}:9333" >"$out/blockmaster-port-forward.log" 2>&1 &
  PF_PID=$!
  trap stop_port_forward RETURN
  if ! wait_for_port "$MASTER_PORT"; then
    echo "status evidence failed: blockmaster port-forward did not become ready" >"$out/status-evidence-error.txt"
    rc=1
  fi
  if [[ "$rc" -eq 0 ]] && ! sw_block_cmd ops cluster --master-api "127.0.0.1:${MASTER_PORT}" --timeout 30s -o json >"$out/cluster-evidence.json" 2>"$out/cluster-evidence.stderr.txt"; then
    rc=1
  fi
  if [[ "$rc" -eq 0 ]] && ! sw_block_cmd ops inventory --namespace "$NAMESPACE" --master "127.0.0.1:${MASTER_PORT}" --out "$out/inventory" --timeout 30s >"$out/inventory.stdout.txt" 2>"$out/inventory.stderr.txt"; then
    rc=1
  fi
  if [[ ! -s "$out/cluster-evidence.json" || ! -s "$out/inventory/volume-inventory-summary.txt" ]]; then
    rc=1
  fi
  stop_port_forward
  unset PF_PID
  trap - RETURN
  return "$rc"
}

cleanup_basic_app() {
  local rc=0
  kubectl -n "$NAMESPACE" delete pod "$READER_POD" "$WRITER_POD" --ignore-not-found=true --wait=true --timeout=120s || rc=1
  kubectl -n "$NAMESPACE" delete pvc "$PVC_NAME" --ignore-not-found=true --wait=true --timeout=120s || rc=1
  kubectl delete storageclass sw-block-example --ignore-not-found=true --wait=true --timeout=120s || rc=1
  return "$rc"
}

snapshot_volume_state() {
  PVC_PHASE_SNAPSHOT="$(kubectl -n "$NAMESPACE" get pvc "$PVC_NAME" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  PV_NAME_SNAPSHOT="$(kubectl -n "$NAMESPACE" get pvc "$PVC_NAME" -o jsonpath='{.spec.volumeName}' 2>/dev/null || true)"
  VOLUME_ID_SNAPSHOT="$PV_NAME_SNAPSHOT"
}

write_summary() {
  local pvc_phase pv_name volume_id writer_ok reader_ok inventory_status
  pvc_phase="$PVC_PHASE_SNAPSHOT"
  pv_name="$PV_NAME_SNAPSHOT"
  volume_id="$VOLUME_ID_SNAPSHOT"
  if [[ -z "$pvc_phase" || -z "$pv_name" ]]; then
    pvc_phase="$(kubectl -n "$NAMESPACE" get pvc "$PVC_NAME" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    pv_name="$(kubectl -n "$NAMESPACE" get pvc "$PVC_NAME" -o jsonpath='{.spec.volumeName}' 2>/dev/null || true)"
    volume_id="$pv_name"
  fi
  writer_ok="false"
  reader_ok="false"
  grep -q '/data/demo.bin: OK' "$ARTIFACT_DIR/writer.log" 2>/dev/null && writer_ok="true"
  grep -q '/data/demo.bin: OK' "$ARTIFACT_DIR/reader.log" 2>/dev/null && reader_ok="true"
  inventory_status="$(sed -n 's/^inventory_status: //p' "$ARTIFACT_DIR/status/inventory/volume-inventory-summary.txt" 2>/dev/null | head -1)"
  {
    echo "first_volume_status=$FIRST_VOLUME_STATUS"
    if [[ -n "$FAILED_PHASE" ]]; then
      echo "failed_phase=$FAILED_PHASE"
    fi
    echo "namespace=$NAMESPACE"
    echo "pvc=$PVC_NAME"
    echo "pvc_phase=${pvc_phase:-unknown}"
    echo "pv=${pv_name:-unknown}"
    echo "volume_id=${volume_id:-unknown}"
    echo "writer_verified=$writer_ok"
    echo "reader_verified=$reader_ok"
    echo "inventory_status=${inventory_status:-unknown}"
    echo "status_evidence=status/cluster-evidence.json,status/inventory"
    echo "cluster_evidence=status/cluster-evidence.json"
    echo "inventory_bundle=status/inventory"
    echo "cleanup_status=$CLEANUP_STATUS"
  } >"$ARTIFACT_DIR/first-volume-summary.txt"
}

require_cmd kubectl
if [[ "$NAMESPACE" != "default" ]]; then
  echo "SW_BLOCK_APP_NAMESPACE=$NAMESPACE is not supported by the static basic-app manifests; use default" >&2
  exit 2
fi
if [[ -z "${SW_BLOCK_CLI:-}" ]] && ! command -v sw-block >/dev/null 2>&1; then
  require_cmd go
fi
if [[ -z "$MASTER_PORT" ]]; then
  MASTER_PORT="$(find_free_port)"
fi

log "artifact_dir=$ARTIFACT_DIR"
log "namespace=$NAMESPACE"
log "master_port=$MASTER_PORT"
log "apply StorageClass and PVC"
kubectl apply -f "$EXAMPLE_DIR/storageclass-pvc.yaml" | tee "$ARTIFACT_DIR/apply-storageclass-pvc.log"
kubectl -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Bound "pvc/${PVC_NAME}" --timeout=180s | tee "$ARTIFACT_DIR/wait-pvc-bound.log"

safe_capture "$ARTIFACT_DIR/pvc.after-bound.txt" kubectl -n "$NAMESPACE" get pvc "$PVC_NAME" -o wide
safe_capture "$ARTIFACT_DIR/blockvolume-deployments.after-bound.txt" kubectl get deploy -A -l app=sw-blockvolume -o wide

log "run writer pod"
kubectl apply -f "$EXAMPLE_DIR/writer-pod.yaml" | tee "$ARTIFACT_DIR/apply-writer.log"
if ! kubectl -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/${WRITER_POD}" --timeout=240s | tee "$ARTIFACT_DIR/wait-writer.log"; then
  capture_failure_diagnostics "writer"
  exit 1
fi
kubectl -n "$NAMESPACE" logs "$WRITER_POD" >"$ARTIFACT_DIR/writer.log"
grep -q '/data/demo.bin: OK' "$ARTIFACT_DIR/writer.log"

log "replace writer with reader pod"
kubectl -n "$NAMESPACE" delete pod "$WRITER_POD" --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-writer.log"
kubectl apply -f "$EXAMPLE_DIR/reader-pod.yaml" | tee "$ARTIFACT_DIR/apply-reader.log"
if ! kubectl -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/${READER_POD}" --timeout=240s | tee "$ARTIFACT_DIR/wait-reader.log"; then
  capture_failure_diagnostics "reader"
  exit 1
fi
kubectl -n "$NAMESPACE" logs "$READER_POD" >"$ARTIFACT_DIR/reader.log"
grep -q '/data/demo.bin: OK' "$ARTIFACT_DIR/reader.log"

log "collect status evidence"
if ! collect_status_evidence; then
  FIRST_VOLUME_STATUS="failed"
  FAILED_PHASE="status_evidence"
fi
snapshot_volume_state

if [[ "$CLEANUP_REQUESTED" == "1" || "$CLEANUP_REQUESTED" == "true" ]]; then
  log "cleanup basic app resources"
  if cleanup_basic_app; then
    CLEANUP_STATUS="ok"
  else
    CLEANUP_STATUS="failed"
    FIRST_VOLUME_STATUS="failed"
    if [[ -z "$FAILED_PHASE" ]]; then
      FAILED_PHASE="cleanup"
    fi
  fi
fi
write_summary

cat "$ARTIFACT_DIR/first-volume-summary.txt"
if [[ "$FIRST_VOLUME_STATUS" != "ok" ]]; then
  log "FAIL: basic app PVC writer/reader loop incomplete"
  exit 1
fi
log "PASS: basic app PVC writer/reader loop complete"
