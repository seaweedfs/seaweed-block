#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
NAMESPACE="${SW_BLOCK_APP_NAMESPACE:-default}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-app-demo-$(date -u +%Y%m%dT%H%M%SZ)}"
POLL_LOG="$ARTIFACT_DIR/poll.log"
IMAGE="${SW_BLOCK_IMAGE:-sw-block:local}"
CSI_IMAGE="${SW_BLOCK_CSI_IMAGE:-sw-block-csi:local}"
LAUNCHER_PVC_OWNER_REF="${SW_BLOCK_LAUNCHER_PVC_OWNER_REF:-0}"
BLOCKVOLUME_NAMESPACE="kube-system"
if [[ "$LAUNCHER_PVC_OWNER_REF" == "1" || "$LAUNCHER_PVC_OWNER_REF" == "true" ]]; then
  BLOCKVOLUME_NAMESPACE="$NAMESPACE"
fi

mkdir -p "$ARTIFACT_DIR"

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

log() {
  printf '[app-demo] %s\n' "$*" | tee -a "$ARTIFACT_DIR/run.log"
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

capture_once() {
  local path="$1"
  shift
  if [[ -s "$path" ]]; then
    return
  fi
  "$@" >"$path" 2>&1 || true
}

collect_logs() {
  set +e
  capture_once "$ARTIFACT_DIR/writer.log" kubectl -n "$NAMESPACE" logs sw-block-demo-writer
  capture_once "$ARTIFACT_DIR/reader.log" kubectl -n "$NAMESPACE" logs sw-block-demo-reader
  capture_once "$ARTIFACT_DIR/writer.describe.txt" kubectl -n "$NAMESPACE" describe pod sw-block-demo-writer
  capture_once "$ARTIFACT_DIR/reader.describe.txt" kubectl -n "$NAMESPACE" describe pod sw-block-demo-reader
  capture_once "$ARTIFACT_DIR/blockmaster.log" kubectl -n kube-system logs deploy/sw-blockmaster -c blockmaster
  capture_once "$ARTIFACT_DIR/blockcsi-controller.log" kubectl -n kube-system logs deploy/sw-block-csi-controller -c block-csi
  capture_once "$ARTIFACT_DIR/csi-provisioner.log" kubectl -n kube-system logs deploy/sw-block-csi-controller -c csi-provisioner
  capture_once "$ARTIFACT_DIR/csi-attacher.log" kubectl -n kube-system logs deploy/sw-block-csi-controller -c csi-attacher
  capture_once "$ARTIFACT_DIR/blockvolume-generated.log" kubectl -n "$BLOCKVOLUME_NAMESPACE" logs -l sw-block.seaweedfs.com/volume -c blockvolume --tail=-1
  capture_once "$ARTIFACT_DIR/kube-system-pods-deploys.txt" kubectl -n kube-system get pods,deploy -o wide
  capture_once "$ARTIFACT_DIR/blockvolume-namespace-pods-deploys.txt" kubectl -n "$BLOCKVOLUME_NAMESPACE" get pods,deploy -o wide
  capture_once "$ARTIFACT_DIR/app-storage.txt" kubectl -n "$NAMESPACE" get sc,pv,pvc,pod -o wide
  if [[ ! -s "$ARTIFACT_DIR/generated-blockvolume.yaml" ]]; then
    kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c 'cat /manifests/*.yaml' >"$ARTIFACT_DIR/generated-blockvolume.yaml" 2>"$ARTIFACT_DIR/generated-blockvolume.err" || true
  fi
}

collect_post_delete_state() {
  set +e
  kubectl -n kube-system get pods,deploy -o wide >"$ARTIFACT_DIR/kube-system-pods-deploys.after-delete.txt" 2>&1 || true
  kubectl -n "$BLOCKVOLUME_NAMESPACE" get pods,deploy -o wide >"$ARTIFACT_DIR/blockvolume-namespace-pods-deploys.after-delete.txt" 2>&1 || true
  kubectl -n "$NAMESPACE" get sc,pv,pvc,pod -o wide >"$ARTIFACT_DIR/app-storage.after-delete.txt" 2>&1 || true
  if command -v sudo >/dev/null 2>&1; then
    sudo iscsiadm -m session >"$ARTIFACT_DIR/iscsi-sessions.after-delete.txt" 2>&1 || true
  elif command -v iscsiadm >/dev/null 2>&1; then
    iscsiadm -m session >"$ARTIFACT_DIR/iscsi-sessions.after-delete.txt" 2>&1 || true
  else
    echo "iscsiadm unavailable" >"$ARTIFACT_DIR/iscsi-sessions.after-delete.txt"
  fi
}

cleanup() {
  (
  set +e
  kubectl delete -f "$ROOT/deploy/k8s/alpha/demo-app-reader-pod.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/alpha/demo-app-pvc.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl -n "$NAMESPACE" delete pod sw-block-demo-reader sw-block-demo-writer --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl -n "$NAMESPACE" delete pvc sw-block-demo-pvc --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl -n kube-system delete deploy -l app=sw-blockvolume --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl -n "$NAMESPACE" delete deploy -l app=sw-blockvolume --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$CSI_NODE_RENDERED" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$CSI_CONTROLLER_RENDERED" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/alpha/csi-driver.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/alpha/rbac.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$STACK_RENDERED" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  )
}

wait_pod_succeeded() {
  local pod="$1"
  local timeout_s="$2"
  for _ in $(seq 1 "$timeout_s"); do
    phase="$(kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if [[ "$phase" == "Succeeded" ]]; then
      return 0
    fi
    if [[ "$phase" == "Failed" ]]; then
      echo "pod $pod failed" >&2
      return 1
    fi
    sleep 1
  done
  echo "pod $pod did not complete before timeout" >&2
  return 1
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
if [[ "$BLOCKVOLUME_NAMESPACE" != "kube-system" ]]; then
  awk '/--launcher-namespace=/{print; print "            - \"--launcher-pvc-owner-ref\""; next} {print}' "$STACK_RENDERED" >"$STACK_RENDERED.tmp"
  mv "$STACK_RENDERED.tmp" "$STACK_RENDERED"
fi
sed -e "s/sw-block-csi:local/${CSI_IMAGE_SED}/g" \
  -e "s/imagePullPolicy: Never/imagePullPolicy: IfNotPresent/g" \
  "$ROOT/deploy/k8s/alpha/csi-controller.yaml" >"$CSI_CONTROLLER_RENDERED"
sed -e "s/sw-block-csi:local/${CSI_IMAGE_SED}/g" \
  -e "s/imagePullPolicy: Never/imagePullPolicy: IfNotPresent/g" \
  "$ROOT/deploy/k8s/alpha/csi-node.yaml" >"$CSI_NODE_RENDERED"

log "artifact_dir=$ARTIFACT_DIR"
log "root=$ROOT"
log "namespace=$NAMESPACE"
log "node=$NODE_NAME"
log "image=$IMAGE"
log "csi_image=$CSI_IMAGE"
log "blockvolume_namespace=$BLOCKVOLUME_NAMESPACE"
log "launcher_pvc_owner_ref=$LAUNCHER_PVC_OWNER_REF"
kubectl version --client=true >"$ARTIFACT_DIR/kubectl-version.txt" 2>&1 || true
kubectl get nodes -o wide >"$ARTIFACT_DIR/nodes.before.txt"

cleanup
trap 'collect_logs; cleanup' EXIT

log "apply seaweed-block service stack"
kubectl apply -f "$STACK_RENDERED" | tee "$ARTIFACT_DIR/apply-block-stack.log"
kubectl -n kube-system wait --for=condition=available deploy/sw-blockmaster --timeout=120s

log "apply CSI manifests"
kubectl apply -f "$ROOT/deploy/k8s/alpha/rbac.yaml" | tee "$ARTIFACT_DIR/apply-rbac.log"
kubectl apply -f "$ROOT/deploy/k8s/alpha/csi-driver.yaml" | tee "$ARTIFACT_DIR/apply-csidriver.log"
kubectl apply -f "$CSI_CONTROLLER_RENDERED" | tee "$ARTIFACT_DIR/apply-csi-controller.log"
kubectl apply -f "$CSI_NODE_RENDERED" | tee "$ARTIFACT_DIR/apply-csi-node.log"
kubectl -n kube-system wait --for=condition=available deploy/sw-block-csi-controller --timeout=120s
kubectl -n kube-system rollout status ds/sw-block-csi-node --timeout=120s

log "apply demo app PVC and writer pod"
kubectl apply -f "$ROOT/deploy/k8s/alpha/demo-app-pvc.yaml" | tee "$ARTIFACT_DIR/apply-demo-app.log"

log "wait for launcher-generated blockvolume manifest"
for _ in $(seq 1 180); do
  if kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c 'ls /manifests/*.yaml >/dev/null 2>&1' >>"$POLL_LOG" 2>&1; then
    break
  fi
  sleep 1
done
if ! kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c 'ls /manifests/*.yaml >/dev/null 2>&1' >>"$POLL_LOG" 2>&1; then
  echo "launcher did not write blockvolume manifests" >&2
  exit 1
fi
kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c 'cat /manifests/*.yaml' >"$ARTIFACT_DIR/generated-blockvolume.yaml"

log "apply generated blockvolume workload"
kubectl apply -f "$ARTIFACT_DIR/generated-blockvolume.yaml" | tee "$ARTIFACT_DIR/apply-generated-blockvolume.log"
kubectl -n "$BLOCKVOLUME_NAMESPACE" wait --for=condition=available deploy -l app=sw-blockvolume --timeout=120s

log "wait for app writer completion"
wait_pod_succeeded sw-block-demo-writer 240
kubectl -n "$NAMESPACE" logs sw-block-demo-writer | tee "$ARTIFACT_DIR/writer.log"

log "delete writer pod but keep PVC"
kubectl -n "$NAMESPACE" delete pod sw-block-demo-writer --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-writer.log"

log "start reader pod on the same PVC"
kubectl apply -f "$ROOT/deploy/k8s/alpha/demo-app-reader-pod.yaml" | tee "$ARTIFACT_DIR/apply-reader.log"
wait_pod_succeeded sw-block-demo-reader 240
kubectl -n "$NAMESPACE" logs sw-block-demo-reader | tee "$ARTIFACT_DIR/reader.log"

log "reader verified data written by previous app pod"

log "delete reader pod and PVC"
kubectl -n "$NAMESPACE" delete pod sw-block-demo-reader --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-reader.log"
kubectl -n "$NAMESPACE" delete pvc sw-block-demo-pvc --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-pvc.log"

log "wait for launcher manifest cleanup after DeleteVolume"
for _ in $(seq 1 180); do
  if kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c '! ls /manifests/*.yaml >/dev/null 2>&1' >>"$POLL_LOG" 2>&1; then
    break
  fi
  sleep 1
done
if ! kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c '! ls /manifests/*.yaml >/dev/null 2>&1' >>"$POLL_LOG" 2>&1; then
  echo "launcher manifest still present after PVC delete" >&2
  exit 1
fi

if [[ "$BLOCKVOLUME_NAMESPACE" == "kube-system" ]]; then
  log "delete generated blockvolume Deployment after manifest cleanup"
  kubectl -n kube-system delete deploy -l app=sw-blockvolume --ignore-not-found=true --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-generated-blockvolume.log"
else
  log "wait for Kubernetes GC to delete PVC-owned blockvolume Deployment"
  for _ in $(seq 1 180); do
    if ! kubectl -n "$BLOCKVOLUME_NAMESPACE" get deploy -l app=sw-blockvolume --no-headers 2>/dev/null | grep -q .; then
      break
    fi
    sleep 1
  done
fi

collect_post_delete_state

if kubectl -n "$BLOCKVOLUME_NAMESPACE" get deploy -l app=sw-blockvolume --no-headers 2>/dev/null | grep -q .; then
  echo "generated blockvolume deployment still present" >&2
  exit 1
fi
if kubectl -n "$NAMESPACE" get pvc sw-block-demo-pvc >/dev/null 2>&1; then
  echo "demo PVC still present" >&2
  exit 1
fi
if grep -q 'iqn.2026-05.io.seaweedfs' "$ARTIFACT_DIR/iscsi-sessions.after-delete.txt" 2>/dev/null; then
  echo "dangling sw-block iSCSI session after delete" >&2
  exit 1
fi

log "PASS: app pod wrote data, replacement app pod read it back through the same PVC, cleanup complete"
log "artifacts=$ARTIFACT_DIR"
