#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-uninstall-$(date -u +%Y%m%dT%H%M%SZ)}"

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
log "delete generated blockvolume workloads"
kubectl -n kube-system delete deploy -l app=sw-blockvolume --ignore-not-found=true --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-blockvolumes.log"

log "delete CSI components"
kubectl delete -f "$ROOT/deploy/k8s/alpha/csi-node.yaml" --ignore-not-found=true | tee "$ARTIFACT_DIR/delete-csi-node.log"
kubectl delete -f "$ROOT/deploy/k8s/alpha/csi-controller.yaml" --ignore-not-found=true | tee "$ARTIFACT_DIR/delete-csi-controller.log"
kubectl delete -f "$ROOT/deploy/k8s/alpha/csi-driver.yaml" --ignore-not-found=true | tee "$ARTIFACT_DIR/delete-csidriver.log"
kubectl delete -f "$ROOT/deploy/k8s/alpha/rbac.yaml" --ignore-not-found=true | tee "$ARTIFACT_DIR/delete-rbac.log"

log "delete blockmaster stack"
kubectl delete -f "$ROOT/deploy/k8s/alpha/block-stack.yaml" --ignore-not-found=true | tee "$ARTIFACT_DIR/delete-block-stack.log"

kubectl -n kube-system get pods,deploy -o wide >"$ARTIFACT_DIR/kube-system.after-delete.txt" 2>&1 || true
if command -v sudo >/dev/null 2>&1; then
  sudo iscsiadm -m session >"$ARTIFACT_DIR/iscsi-sessions.after-delete.txt" 2>&1 || true
elif command -v iscsiadm >/dev/null 2>&1; then
  iscsiadm -m session >"$ARTIFACT_DIR/iscsi-sessions.after-delete.txt" 2>&1 || true
else
  echo "iscsiadm unavailable" >"$ARTIFACT_DIR/iscsi-sessions.after-delete.txt"
fi

log "PASS: seaweed-block alpha stack uninstall requested"
log "artifacts=$ARTIFACT_DIR"
