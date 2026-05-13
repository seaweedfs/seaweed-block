#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-uninstall-$(date -u +%Y%m%dT%H%M%SZ)}"
NAMESPACE="${SW_BLOCK_APP_NAMESPACE:-default}"
DELETE_ALL_BLOCKVOLUMES="${SW_BLOCK_UNINSTALL_DELETE_ALL_BLOCKVOLUMES:-0}"

mkdir -p "$ARTIFACT_DIR"

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

log() {
  printf '[alpha-uninstall] %s\n' "$*" | tee -a "$ARTIFACT_DIR/uninstall.log"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

require_cmd kubectl

log "artifact_dir=$ARTIFACT_DIR"
log "namespace=$NAMESPACE"
log "delete_all_blockvolumes=$DELETE_ALL_BLOCKVOLUMES"

DEMO_PVC_UID="$(kubectl -n "$NAMESPACE" get pvc sw-block-demo-pvc -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"

log "delete generated blockvolume workloads for demo PVC"
if [[ -n "$DEMO_PVC_UID" ]]; then
  DEMO_BLOCKVOLUME_SELECTOR="sw-block.seaweedfs.com/volume=pvc-${DEMO_PVC_UID}"
  kubectl -n "$NAMESPACE" delete deploy -l "$DEMO_BLOCKVOLUME_SELECTOR" --ignore-not-found=true --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-blockvolumes-demo-pvc.log"
  if [[ "$NAMESPACE" != "kube-system" ]]; then
    kubectl -n kube-system delete deploy -l "$DEMO_BLOCKVOLUME_SELECTOR" --ignore-not-found=true --wait=true --timeout=120s | tee -a "$ARTIFACT_DIR/delete-blockvolumes-demo-pvc.log"
  fi
else
  echo "demo PVC not found; no demo PVC-scoped blockvolume selector available" | tee "$ARTIFACT_DIR/delete-blockvolumes-demo-pvc.log"
fi

log "delete demo app resources"
kubectl delete -f "$ROOT/deploy/k8s/alpha/demo-app-reader-pod.yaml" --ignore-not-found=true --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-demo-reader.log"
kubectl delete -f "$ROOT/deploy/k8s/alpha/demo-app-pvc.yaml" --ignore-not-found=true --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-demo-writer-pvc.log"
kubectl -n "$NAMESPACE" delete pod sw-block-demo-reader sw-block-demo-writer --ignore-not-found=true --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-demo-pods.log"
kubectl -n "$NAMESPACE" delete pvc sw-block-demo-pvc --ignore-not-found=true --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-demo-pvc.log"

if [[ "$DELETE_ALL_BLOCKVOLUMES" == "1" || "$DELETE_ALL_BLOCKVOLUMES" == "true" ]]; then
  log "delete all generated blockvolume workloads (TestOps guardrail)"
  kubectl -n "$NAMESPACE" delete deploy -l app=sw-blockvolume --ignore-not-found=true --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-blockvolumes-app-namespace.log"
  kubectl -n kube-system delete deploy -l app=sw-blockvolume --ignore-not-found=true --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-blockvolumes.log"
else
  echo "skipped broad app=sw-blockvolume deletion; set SW_BLOCK_UNINSTALL_DELETE_ALL_BLOCKVOLUMES=1 for TestOps guardrail cleanup" | tee "$ARTIFACT_DIR/delete-blockvolumes-app-namespace.log"
  echo "skipped broad kube-system app=sw-blockvolume deletion; set SW_BLOCK_UNINSTALL_DELETE_ALL_BLOCKVOLUMES=1 for TestOps guardrail cleanup" | tee "$ARTIFACT_DIR/delete-blockvolumes.log"
fi

log "delete CSI components"
kubectl delete -f "$ROOT/deploy/k8s/alpha/csi-node.yaml" --ignore-not-found=true | tee "$ARTIFACT_DIR/delete-csi-node.log"
kubectl delete -f "$ROOT/deploy/k8s/alpha/csi-controller.yaml" --ignore-not-found=true | tee "$ARTIFACT_DIR/delete-csi-controller.log"
kubectl delete -f "$ROOT/deploy/k8s/alpha/csi-driver.yaml" --ignore-not-found=true | tee "$ARTIFACT_DIR/delete-csidriver.log"
kubectl delete -f "$ROOT/deploy/k8s/alpha/rbac.yaml" --ignore-not-found=true | tee "$ARTIFACT_DIR/delete-rbac.log"

log "delete blockmaster stack"
kubectl delete -f "$ROOT/deploy/k8s/alpha/block-stack.yaml" --ignore-not-found=true | tee "$ARTIFACT_DIR/delete-block-stack.log"

kubectl -n kube-system get pods,deploy -o wide >"$ARTIFACT_DIR/kube-system.after-delete.txt" 2>&1 || true
kubectl -n "$NAMESPACE" get pods,deploy,pvc,pv -o wide >"$ARTIFACT_DIR/app-namespace.after-delete.txt" 2>&1 || true
if command -v sudo >/dev/null 2>&1; then
  sudo iscsiadm -m session >"$ARTIFACT_DIR/iscsi-sessions.after-delete.txt" 2>&1 || true
elif command -v iscsiadm >/dev/null 2>&1; then
  iscsiadm -m session >"$ARTIFACT_DIR/iscsi-sessions.after-delete.txt" 2>&1 || true
else
  echo "iscsiadm unavailable" >"$ARTIFACT_DIR/iscsi-sessions.after-delete.txt"
fi

log "PASS: seaweed-block alpha stack uninstall requested"
log "artifacts=$ARTIFACT_DIR"
