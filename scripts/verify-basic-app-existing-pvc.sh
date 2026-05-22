#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-basic-app-existing-$(date -u +%Y%m%dT%H%M%SZ)}"
NAMESPACE="${SW_BLOCK_APP_NAMESPACE:-default}"
PVC_NAME="${SW_BLOCK_BASIC_APP_PVC:-sw-block-example-pvc}"
READER_POD="${SW_BLOCK_BASIC_APP_READER:-sw-block-example-reader}"
EXAMPLE_DIR="$ROOT/examples/kubernetes/basic-app"
BASIC_APP_NODE_SELECTOR="${SW_BLOCK_BASIC_APP_NODE_SELECTOR:-}"

mkdir -p "$ARTIFACT_DIR"

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

log() {
  printf '[basic-app-existing] %s\n' "$*" | tee -a "$ARTIFACT_DIR/run.log"
}

safe_capture() {
  local out="$1"
  shift
  "$@" >"$out" 2>&1 || true
}

render_pod_manifest() {
  local input="$1"
  local output="$2"
  if [[ -z "$BASIC_APP_NODE_SELECTOR" ]]; then
    cp "$input" "$output"
    return
  fi
  awk -v node="$BASIC_APP_NODE_SELECTOR" '
    /^spec:[[:space:]]*$/ {
      print
      print "  nodeSelector:"
      print "    kubernetes.io/hostname: \"" node "\""
      next
    }
    { print }
  ' "$input" >"$output"
}

if [[ "$NAMESPACE" != "default" ]]; then
  echo "SW_BLOCK_APP_NAMESPACE=$NAMESPACE is not supported by the static basic-app manifests; use default" >&2
  exit 2
fi

if [[ ! -f "$EXAMPLE_DIR/reader-pod.yaml" ]]; then
  echo "reader manifest missing: $EXAMPLE_DIR/reader-pod.yaml" >&2
  exit 2
fi

log "artifact_dir=$ARTIFACT_DIR"
log "namespace=$NAMESPACE"
log "pvc=$PVC_NAME"
log "app_node_selector=${BASIC_APP_NODE_SELECTOR:-none}"

safe_capture "$ARTIFACT_DIR/pvc.before-reader.txt" kubectl -n "$NAMESPACE" get pvc "$PVC_NAME" -o wide
pvc_phase="$(kubectl -n "$NAMESPACE" get pvc "$PVC_NAME" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
pv_name="$(kubectl -n "$NAMESPACE" get pvc "$PVC_NAME" -o jsonpath='{.spec.volumeName}' 2>/dev/null || true)"
if [[ "$pvc_phase" != "Bound" || -z "$pv_name" ]]; then
  {
    echo "existing_pvc_status=failed"
    echo "failed_phase=pvc_not_bound"
    echo "pvc=$PVC_NAME"
    echo "pvc_phase=${pvc_phase:-unknown}"
    echo "pv=${pv_name:-unknown}"
  } >"$ARTIFACT_DIR/existing-pvc-summary.txt"
  cat "$ARTIFACT_DIR/existing-pvc-summary.txt"
  exit 1
fi

reader_manifest="$ARTIFACT_DIR/reader-pod.rendered.yaml"
render_pod_manifest "$EXAMPLE_DIR/reader-pod.yaml" "$reader_manifest"

log "replace reader pod and verify existing data"
kubectl -n "$NAMESPACE" delete pod "$READER_POD" --ignore-not-found=true --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-reader.log"
kubectl apply -f "$reader_manifest" | tee "$ARTIFACT_DIR/apply-reader.log"
if ! kubectl -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/${READER_POD}" --timeout=240s | tee "$ARTIFACT_DIR/wait-reader.log"; then
  safe_capture "$ARTIFACT_DIR/reader-describe.txt" kubectl -n "$NAMESPACE" describe pod "$READER_POD"
  safe_capture "$ARTIFACT_DIR/events.txt" kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp
  exit 1
fi
kubectl -n "$NAMESPACE" logs "$READER_POD" >"$ARTIFACT_DIR/reader.log"
grep -q '/data/demo.bin: OK' "$ARTIFACT_DIR/reader.log"

{
  echo "existing_pvc_status=ok"
  echo "pvc=$PVC_NAME"
  echo "pvc_phase=$pvc_phase"
  echo "pv=$pv_name"
  echo "volume_id=$pv_name"
  echo "reader_verified=true"
} >"$ARTIFACT_DIR/existing-pvc-summary.txt"

cat "$ARTIFACT_DIR/existing-pvc-summary.txt"
log "PASS: existing PVC reader verified persisted data"
