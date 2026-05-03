#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
NAMESPACE="${G15D_NAMESPACE:-default}"
ARTIFACT_DIR="${G15D_ARTIFACT_DIR:-/tmp/g15d-k8s-$(date -u +%Y%m%dT%H%M%SZ)}"

mkdir -p "$ARTIFACT_DIR"

log() {
  printf '[g15d] %s\n' "$*" | tee -a "$ARTIFACT_DIR/run.log"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

require_cmd kubectl

NODE_NAME="$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')"
STACK_RENDERED="$ARTIFACT_DIR/block-stack.rendered.yaml"
sed "s/__NODE_NAME__/${NODE_NAME}/g" "$ROOT/deploy/k8s/g15d/block-stack.yaml" >"$STACK_RENDERED"

log "artifact_dir=$ARTIFACT_DIR"
log "root=$ROOT"
log "namespace=$NAMESPACE"
log "node=$NODE_NAME"
kubectl version --client=true >"$ARTIFACT_DIR/kubectl-version.txt" 2>&1 || true
kubectl get nodes -o wide >"$ARTIFACT_DIR/nodes.before.txt"

cleanup() {
  set +e
  kubectl -n kube-system delete deploy -l app=sw-blockvolume --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/g15d/dynamic-pvc-pod.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/g15b/csi-node.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/g15d/csi-controller.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/g15b/csi-driver.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/g15d/rbac.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$STACK_RENDERED" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
}

collect_daemon_logs() {
  set +e
  kubectl -n "$NAMESPACE" describe pod sw-block-dynamic-smoke >"$ARTIFACT_DIR/pod.describe.txt" 2>&1 || true
  kubectl -n "$NAMESPACE" logs sw-block-dynamic-smoke >"$ARTIFACT_DIR/pod.log" 2>&1 || true
  kubectl -n kube-system logs deploy/sw-blockmaster -c blockmaster >"$ARTIFACT_DIR/blockmaster.log" 2>&1 || true
  kubectl -n kube-system logs deploy/sw-block-csi-controller -c block-csi >"$ARTIFACT_DIR/blockcsi-controller.log" 2>&1 || true
  kubectl -n kube-system logs deploy/sw-block-csi-controller -c csi-provisioner >"$ARTIFACT_DIR/csi-provisioner.log" 2>&1 || true
  kubectl -n kube-system logs deploy/sw-block-csi-controller -c csi-attacher >"$ARTIFACT_DIR/csi-attacher.log" 2>&1 || true
  kubectl -n kube-system logs -l app=sw-blockvolume -c blockvolume --tail=-1 >"$ARTIFACT_DIR/blockvolume-generated.log" 2>&1 || true
  kubectl -n kube-system get pods,deploy -o wide >"$ARTIFACT_DIR/kube-system-pods-deploys.txt" 2>&1 || true
  kubectl -n "$NAMESPACE" get sc,pv,pvc,pod -o wide >"$ARTIFACT_DIR/app-storage.txt" 2>&1 || true
  kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c 'cat /manifests/*.yaml' >"$ARTIFACT_DIR/generated-blockvolume.yaml" 2>"$ARTIFACT_DIR/generated-blockvolume.err" || true
}

trap 'collect_daemon_logs; cleanup' EXIT

log "apply product stack without pre-created blockvolumes"
kubectl apply -f "$STACK_RENDERED" | tee "$ARTIFACT_DIR/apply-block-stack.log"
kubectl -n kube-system wait --for=condition=available deploy/sw-blockmaster --timeout=120s

log "apply CSI dynamic-provisioning manifests"
kubectl apply -f "$ROOT/deploy/k8s/g15d/rbac.yaml" | tee "$ARTIFACT_DIR/apply-rbac.log"
kubectl apply -f "$ROOT/deploy/k8s/g15b/csi-driver.yaml" | tee "$ARTIFACT_DIR/apply-csidriver.log"
kubectl apply -f "$ROOT/deploy/k8s/g15d/csi-controller.yaml" | tee "$ARTIFACT_DIR/apply-csi-controller.log"
kubectl apply -f "$ROOT/deploy/k8s/g15b/csi-node.yaml" | tee "$ARTIFACT_DIR/apply-csi-node.log"
kubectl -n kube-system wait --for=condition=available deploy/sw-block-csi-controller --timeout=120s
kubectl -n kube-system rollout status ds/sw-block-csi-node --timeout=120s

log "apply dynamic StorageClass/PVC/pod"
kubectl apply -f "$ROOT/deploy/k8s/g15d/dynamic-pvc-pod.yaml" | tee "$ARTIFACT_DIR/apply-dynamic.log"

log "wait for launcher-generated blockvolume manifest"
for _ in $(seq 1 180); do
  if kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c 'ls /manifests/*.yaml >/dev/null 2>&1'; then
    break
  fi
  sleep 1
done
if ! kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c 'ls /manifests/*.yaml >/dev/null 2>&1'; then
  echo "launcher did not write blockvolume manifests" >&2
  exit 1
fi
kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c 'cat /manifests/*.yaml' >"$ARTIFACT_DIR/generated-blockvolume.yaml"

log "apply generated blockvolume manifests"
kubectl apply -f "$ARTIFACT_DIR/generated-blockvolume.yaml" | tee "$ARTIFACT_DIR/apply-generated-blockvolume.log"
kubectl -n kube-system wait --for=condition=available deploy -l app=sw-blockvolume --timeout=120s

log "wait for dynamic pod completion"
for _ in $(seq 1 240); do
  phase="$(kubectl -n "$NAMESPACE" get pod sw-block-dynamic-smoke -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  if [[ "$phase" == "Succeeded" ]]; then
    break
  fi
  if [[ "$phase" == "Failed" ]]; then
    echo "pod failed" >&2
    exit 1
  fi
  sleep 1
done

phase="$(kubectl -n "$NAMESPACE" get pod sw-block-dynamic-smoke -o jsonpath='{.status.phase}')"
if [[ "$phase" != "Succeeded" ]]; then
  echo "pod did not succeed; phase=$phase" >&2
  exit 1
fi

collect_daemon_logs

log "PASS: dynamic PVC pod completed checksum write/read"
log "artifacts=$ARTIFACT_DIR"
