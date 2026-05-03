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
  (
  set +e
  kubectl -n kube-system delete deploy -l app=sw-blockvolume --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl -n kube-system delete deploy sw-blockvolume-r1 sw-blockvolume-r2 --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/g15d/dynamic-pvc-pod.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/g15b/csi-node.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/g15d/csi-controller.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/g15b/csi-driver.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/g15d/rbac.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$STACK_RENDERED" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  )
}

collect_daemon_logs() {
  set +e
  capture_once "$ARTIFACT_DIR/pod.describe.txt" kubectl -n "$NAMESPACE" describe pod sw-block-dynamic-smoke
  capture_once "$ARTIFACT_DIR/pod.log" kubectl -n "$NAMESPACE" logs sw-block-dynamic-smoke
  capture_once "$ARTIFACT_DIR/blockmaster.log" kubectl -n kube-system logs deploy/sw-blockmaster -c blockmaster
  capture_once "$ARTIFACT_DIR/blockcsi-controller.log" kubectl -n kube-system logs deploy/sw-block-csi-controller -c block-csi
  capture_once "$ARTIFACT_DIR/csi-provisioner.log" kubectl -n kube-system logs deploy/sw-block-csi-controller -c csi-provisioner
  capture_once "$ARTIFACT_DIR/csi-attacher.log" kubectl -n kube-system logs deploy/sw-block-csi-controller -c csi-attacher
  capture_once "$ARTIFACT_DIR/blockvolume-generated.log" kubectl -n kube-system logs -l sw-block.seaweedfs.com/volume -c blockvolume --tail=-1
  capture_once "$ARTIFACT_DIR/kube-system-pods-deploys.txt" kubectl -n kube-system get pods,deploy -o wide
  capture_once "$ARTIFACT_DIR/app-storage.txt" kubectl -n "$NAMESPACE" get sc,pv,pvc,pod -o wide
  if [[ ! -s "$ARTIFACT_DIR/generated-blockvolume.yaml" ]]; then
    kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c 'cat /manifests/*.yaml' >"$ARTIFACT_DIR/generated-blockvolume.yaml" 2>"$ARTIFACT_DIR/generated-blockvolume.err" || true
  fi
}

capture_once() {
  local path="$1"
  shift
  if [[ -s "$path" ]]; then
    return
  fi
  "$@" >"$path" 2>&1 || true
}

collect_post_delete_state() {
  set +e
  kubectl -n kube-system get pods,deploy -o wide >"$ARTIFACT_DIR/kube-system-pods-deploys.after-delete.txt" 2>&1 || true
  kubectl -n "$NAMESPACE" get sc,pv,pvc,pod -o wide >"$ARTIFACT_DIR/app-storage.after-delete.txt" 2>&1 || true
  if command -v sudo >/dev/null 2>&1; then
    sudo iscsiadm -m session >"$ARTIFACT_DIR/iscsi-sessions.after-delete.txt" 2>&1 || true
  elif command -v iscsiadm >/dev/null 2>&1; then
    iscsiadm -m session >"$ARTIFACT_DIR/iscsi-sessions.after-delete.txt" 2>&1 || true
  else
    echo "iscsiadm unavailable" >"$ARTIFACT_DIR/iscsi-sessions.after-delete.txt"
  fi
}

cleanup
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

log "dynamic PVC pod completed checksum write/read"

log "delete dynamic pod and PVC"
kubectl -n "$NAMESPACE" delete pod sw-block-dynamic-smoke --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-pod.log"
kubectl -n "$NAMESPACE" delete pvc sw-block-dynamic-v1 --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-pvc.log"

log "wait for launcher manifest cleanup after DeleteVolume"
for _ in $(seq 1 180); do
  if kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c '! ls /manifests/*.yaml >/dev/null 2>&1'; then
    break
  fi
  sleep 1
done
if ! kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c '! ls /manifests/*.yaml >/dev/null 2>&1'; then
  echo "launcher manifest still present after PVC delete" >&2
  exit 1
fi

log "delete generated blockvolume Deployment after manifest cleanup"
kubectl -n kube-system delete deploy -l app=sw-blockvolume --ignore-not-found=true --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-generated-blockvolume.log"

collect_post_delete_state

if kubectl -n kube-system get deploy -l app=sw-blockvolume --no-headers 2>/dev/null | grep -q .; then
  echo "generated blockvolume deployment still present" >&2
  exit 1
fi
if kubectl -n "$NAMESPACE" get pvc sw-block-dynamic-v1 >/dev/null 2>&1; then
  echo "dynamic PVC still present" >&2
  exit 1
fi
if grep -q 'iqn.2026-05.io.seaweedfs' "$ARTIFACT_DIR/iscsi-sessions.after-delete.txt" 2>/dev/null; then
  echo "dangling sw-block iSCSI session after delete" >&2
  exit 1
fi

log "PASS: dynamic PVC create/delete completed checksum write/read and cleanup"
log "artifacts=$ARTIFACT_DIR"
