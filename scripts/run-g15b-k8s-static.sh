#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
NAMESPACE="${G15B_NAMESPACE:-default}"
ARTIFACT_DIR="${G15B_ARTIFACT_DIR:-/tmp/g15b-k8s-$(date -u +%Y%m%dT%H%M%SZ)}"
IMAGE="${G15B_IMAGE:-sw-block:local}"
CSI_IMAGE="${G15B_CSI_IMAGE:-sw-block-csi:local}"

mkdir -p "$ARTIFACT_DIR"

log() {
  printf '[g15b] %s\n' "$*" | tee -a "$ARTIFACT_DIR/run.log"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

require_cmd kubectl

log "artifact_dir=$ARTIFACT_DIR"
log "root=$ROOT"
log "namespace=$NAMESPACE"
log "image=$IMAGE csi_image=$CSI_IMAGE"
kubectl version --client=true >"$ARTIFACT_DIR/kubectl-version.txt" 2>&1 || true
kubectl get nodes -o wide >"$ARTIFACT_DIR/nodes.before.txt"

cleanup() {
  set +e
  kubectl delete -f "$ROOT/deploy/k8s/g15b/static-pv-pvc-pod.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/g15b/csi-node.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/g15b/csi-controller.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/g15b/csi-driver.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/g15b/rbac.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/g15b/block-stack.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
}

collect_daemon_logs() {
  set +e
  kubectl -n "$NAMESPACE" describe pod sw-block-static-smoke >"$ARTIFACT_DIR/pod.describe.txt" 2>&1 || true
  kubectl -n "$NAMESPACE" logs sw-block-static-smoke >"$ARTIFACT_DIR/pod.log" 2>&1 || true
  kubectl -n kube-system logs deploy/sw-blockmaster -c blockmaster >"$ARTIFACT_DIR/blockmaster.log" 2>&1 || true
  kubectl -n kube-system logs deploy/sw-blockvolume-r1 -c blockvolume >"$ARTIFACT_DIR/blockvolume-r1.log" 2>&1 || true
  kubectl -n kube-system logs deploy/sw-blockvolume-r2 -c blockvolume >"$ARTIFACT_DIR/blockvolume-r2.log" 2>&1 || true
  kubectl -n kube-system logs deploy/sw-block-csi-controller -c block-csi >"$ARTIFACT_DIR/blockcsi-controller.log" 2>&1 || true
  kubectl -n kube-system get pods -o wide >"$ARTIFACT_DIR/kube-system-pods.txt" 2>&1 || true
  kubectl -n "$NAMESPACE" get pv,pvc,pod -o wide >"$ARTIFACT_DIR/app-pv-pvc-pod.txt" 2>&1 || true
}

trap 'collect_daemon_logs; cleanup' EXIT

log "preflight: apply product stack"
kubectl apply -f "$ROOT/deploy/k8s/g15b/block-stack.yaml" | tee "$ARTIFACT_DIR/apply-block-stack.log"
kubectl -n kube-system wait --for=condition=available deploy/sw-blockmaster --timeout=120s
kubectl -n kube-system wait --for=condition=available deploy/sw-blockvolume-r1 --timeout=120s
kubectl -n kube-system wait --for=condition=available deploy/sw-blockvolume-r2 --timeout=120s

log "apply CSI manifests"
kubectl apply -f "$ROOT/deploy/k8s/g15b/rbac.yaml" | tee "$ARTIFACT_DIR/apply-rbac.log"
kubectl apply -f "$ROOT/deploy/k8s/g15b/csi-driver.yaml" | tee "$ARTIFACT_DIR/apply-csidriver.log"
kubectl apply -f "$ROOT/deploy/k8s/g15b/csi-controller.yaml" | tee "$ARTIFACT_DIR/apply-csi-controller.log"
kubectl apply -f "$ROOT/deploy/k8s/g15b/csi-node.yaml" | tee "$ARTIFACT_DIR/apply-csi-node.log"
kubectl -n kube-system wait --for=condition=available deploy/sw-block-csi-controller --timeout=120s
kubectl -n kube-system rollout status ds/sw-block-csi-node --timeout=120s

log "apply static PV/PVC/pod"
kubectl apply -f "$ROOT/deploy/k8s/g15b/static-pv-pvc-pod.yaml" | tee "$ARTIFACT_DIR/apply-static.log"
kubectl -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Bound pvc/sw-block-static-v1 --timeout=120s

log "wait for pod completion"
for _ in $(seq 1 180); do
  phase="$(kubectl -n "$NAMESPACE" get pod sw-block-static-smoke -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  if [[ "$phase" == "Succeeded" ]]; then
    break
  fi
  if [[ "$phase" == "Failed" ]]; then
    echo "pod failed" >&2
    exit 1
  fi
  sleep 1
done

phase="$(kubectl -n "$NAMESPACE" get pod sw-block-static-smoke -o jsonpath='{.status.phase}')"
if [[ "$phase" != "Succeeded" ]]; then
  echo "pod did not succeed; phase=$phase" >&2
  exit 1
fi

collect_daemon_logs

log "PASS: static PV pod completed checksum write/read"
log "artifacts=$ARTIFACT_DIR"
