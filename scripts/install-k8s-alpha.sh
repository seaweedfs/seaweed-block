#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-install-$(date -u +%Y%m%dT%H%M%SZ)}"
IMAGE="${SW_BLOCK_IMAGE:-ghcr.io/seaweedfs/seaweed-block:alpha}"
CSI_IMAGE="${SW_BLOCK_CSI_IMAGE:-ghcr.io/seaweedfs/seaweed-block-csi:alpha}"
LAUNCHER_PVC_OWNER_REF="${SW_BLOCK_LAUNCHER_PVC_OWNER_REF:-1}"

mkdir -p "$ARTIFACT_DIR"

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

log() {
  printf '[alpha-install] %s\n' "$*" | tee -a "$ARTIFACT_DIR/install.log"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

sed_escape() {
  printf '%s' "$1" | sed 's/[\/&]/\\&/g'
}

require_cmd kubectl

NODE_NAME="$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')"
STACK_RENDERED="$ARTIFACT_DIR/block-stack.rendered.yaml"
CSI_CONTROLLER_RENDERED="$ARTIFACT_DIR/csi-controller.rendered.yaml"
CSI_NODE_RENDERED="$ARTIFACT_DIR/csi-node.rendered.yaml"
IMAGE_SED="$(sed_escape "$IMAGE")"
CSI_IMAGE_SED="$(sed_escape "$CSI_IMAGE")"

sed -e "s/__NODE_NAME__/${NODE_NAME}/g" \
  -e "s/sw-block:local/${IMAGE_SED}/g" \
  -e "s/imagePullPolicy: Never/imagePullPolicy: IfNotPresent/g" \
  "$ROOT/deploy/k8s/alpha/block-stack.yaml" >"$STACK_RENDERED"
if [[ "$LAUNCHER_PVC_OWNER_REF" == "1" || "$LAUNCHER_PVC_OWNER_REF" == "true" ]]; then
  awk '/--launcher-namespace=/{print; print "            - \"--launcher-pvc-owner-ref\""; next} {print}' "$STACK_RENDERED" >"$STACK_RENDERED.tmp"
  mv "$STACK_RENDERED.tmp" "$STACK_RENDERED"
  grep -q -- '--launcher-pvc-owner-ref' "$STACK_RENDERED" || { echo "failed to inject --launcher-pvc-owner-ref into $STACK_RENDERED" >&2; exit 1; }
fi
sed -e "s/sw-block-csi:local/${CSI_IMAGE_SED}/g" \
  -e "s/imagePullPolicy: Never/imagePullPolicy: IfNotPresent/g" \
  "$ROOT/deploy/k8s/alpha/csi-controller.yaml" >"$CSI_CONTROLLER_RENDERED"
if [[ "$LAUNCHER_PVC_OWNER_REF" == "1" || "$LAUNCHER_PVC_OWNER_REF" == "true" ]]; then
  awk '/--node-id=\$\(NODE_NAME\)/{print; print "            - \"--kubernetes-pvc-uid-lookup\""; next} {print}' "$CSI_CONTROLLER_RENDERED" >"$CSI_CONTROLLER_RENDERED.tmp"
  mv "$CSI_CONTROLLER_RENDERED.tmp" "$CSI_CONTROLLER_RENDERED"
  grep -q -- '--kubernetes-pvc-uid-lookup' "$CSI_CONTROLLER_RENDERED" || { echo "failed to inject --kubernetes-pvc-uid-lookup into $CSI_CONTROLLER_RENDERED" >&2; exit 1; }
fi
sed -e "s/sw-block-csi:local/${CSI_IMAGE_SED}/g" \
  -e "s/imagePullPolicy: Never/imagePullPolicy: IfNotPresent/g" \
  "$ROOT/deploy/k8s/alpha/csi-node.yaml" >"$CSI_NODE_RENDERED"

log "artifact_dir=$ARTIFACT_DIR"
log "root=$ROOT"
log "node=$NODE_NAME"
log "image=$IMAGE"
log "csi_image=$CSI_IMAGE"
log "launcher_pvc_owner_ref=$LAUNCHER_PVC_OWNER_REF"

kubectl version --client=true >"$ARTIFACT_DIR/kubectl-version.txt" 2>&1 || true
kubectl get nodes -o wide >"$ARTIFACT_DIR/nodes.before.txt"

log "apply seaweed-block control plane"
kubectl apply -f "$STACK_RENDERED" | tee "$ARTIFACT_DIR/apply-block-stack.log"
kubectl -n kube-system wait --for=condition=available deploy/sw-blockmaster --timeout=120s

log "apply CSI components"
kubectl apply -f "$ROOT/deploy/k8s/alpha/rbac.yaml" | tee "$ARTIFACT_DIR/apply-rbac.log"
kubectl apply -f "$ROOT/deploy/k8s/alpha/csi-driver.yaml" | tee "$ARTIFACT_DIR/apply-csidriver.log"
kubectl apply -f "$CSI_CONTROLLER_RENDERED" | tee "$ARTIFACT_DIR/apply-csi-controller.log"
kubectl apply -f "$CSI_NODE_RENDERED" | tee "$ARTIFACT_DIR/apply-csi-node.log"
kubectl -n kube-system wait --for=condition=available deploy/sw-block-csi-controller --timeout=120s
kubectl -n kube-system rollout status ds/sw-block-csi-node --timeout=120s

log "PASS: seaweed-block alpha stack installed"
log "next: create a PVC, then run scripts/apply-k8s-alpha-blockvolumes.sh"
log "artifacts=$ARTIFACT_DIR"
