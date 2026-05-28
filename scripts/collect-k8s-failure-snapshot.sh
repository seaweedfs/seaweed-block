#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_FAILURE_SNAPSHOT_OUT:-${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-failure-snapshot-$(date -u +%Y%m%dT%H%M%SZ)}}"
NAMESPACE="${SW_BLOCK_APP_NAMESPACE:-default}"
HELM_NAMESPACE="${SW_BLOCK_HELM_NAMESPACE:-kube-system}"
HELM_RELEASE="${SW_BLOCK_HELM_RELEASE:-sw-block}"
IQN_SUBSTR="${SW_BLOCK_FAILURE_SNAPSHOT_IQN_SUBSTR:-io.seaweedfs}"

mkdir -p "$ARTIFACT_DIR"/{k8s,logs,host,helm}

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

capture_failures=0

capture() {
  local out="$1"
  shift
  if ! "$@" >"$out" 2>&1; then
    capture_failures=$((capture_failures + 1))
    {
      echo
      echo "[failure-snapshot] command failed: $*"
    } >>"$out"
  fi
}

capture_optional_sudo() {
  local out="$1"
  shift
  if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
    capture "$out" sudo -n "$@"
  else
    capture "$out" "$@"
  fi
}

capture "$ARTIFACT_DIR/helm/status.txt" helm status "$HELM_RELEASE" --namespace "$HELM_NAMESPACE"
capture "$ARTIFACT_DIR/helm/list.txt" helm list --all-namespaces
capture "$ARTIFACT_DIR/helm/values.txt" helm get values "$HELM_RELEASE" --namespace "$HELM_NAMESPACE" --all

capture "$ARTIFACT_DIR/k8s/nodes.txt" kubectl get nodes -o wide
capture "$ARTIFACT_DIR/k8s/kube-system.txt" kubectl -n "$HELM_NAMESPACE" get pods,deploy,daemonset,svc,configmap,secret -o wide
capture "$ARTIFACT_DIR/k8s/app-namespace.txt" kubectl -n "$NAMESPACE" get pods,deploy,pvc,pv -o wide
capture "$ARTIFACT_DIR/k8s/pods-all.yaml" kubectl get pods -A -o yaml
capture "$ARTIFACT_DIR/k8s/pvc-all.yaml" kubectl get pvc -A -o yaml
capture "$ARTIFACT_DIR/k8s/volumeattachments.yaml" kubectl get volumeattachments -o yaml
capture "$ARTIFACT_DIR/k8s/events-all.txt" kubectl get events -A --sort-by=.lastTimestamp
capture "$ARTIFACT_DIR/k8s/sw-block-resources.txt" bash -c "kubectl get deploy,daemonset,statefulset,pod,svc,pvc,pv,configmap,secret,serviceaccount -A -o name; kubectl get storageclass,csidriver,clusterrole,clusterrolebinding -o name"

capture "$ARTIFACT_DIR/k8s/blockvolume-deployments.yaml" kubectl -n "$NAMESPACE" get deploy -l app=sw-blockvolume -o yaml
capture "$ARTIFACT_DIR/k8s/blockvolume-pods.yaml" kubectl -n "$NAMESPACE" get pods -l app=sw-blockvolume -o yaml
capture "$ARTIFACT_DIR/k8s/app-pods-describe.txt" kubectl -n "$NAMESPACE" describe pods
capture "$ARTIFACT_DIR/k8s/kube-system-pods-describe.txt" kubectl -n "$HELM_NAMESPACE" describe pods

capture "$ARTIFACT_DIR/logs/blockmaster.current.log" kubectl -n "$HELM_NAMESPACE" logs deploy/sw-blockmaster --all-containers --tail=500
capture "$ARTIFACT_DIR/logs/blockmaster.previous.log" kubectl -n "$HELM_NAMESPACE" logs deploy/sw-blockmaster --all-containers --previous --tail=500
capture "$ARTIFACT_DIR/logs/csi-controller.current.log" kubectl -n "$HELM_NAMESPACE" logs deploy/sw-block-csi-controller --all-containers --tail=500
capture "$ARTIFACT_DIR/logs/csi-controller.previous.log" kubectl -n "$HELM_NAMESPACE" logs deploy/sw-block-csi-controller --all-containers --previous --tail=500
capture "$ARTIFACT_DIR/logs/csi-node.current.log" kubectl -n "$HELM_NAMESPACE" logs ds/sw-block-csi-node --all-containers --tail=500
capture "$ARTIFACT_DIR/logs/csi-node.previous.log" kubectl -n "$HELM_NAMESPACE" logs ds/sw-block-csi-node --all-containers --previous --tail=500
capture "$ARTIFACT_DIR/logs/blockvolume.current.log" kubectl -n "$NAMESPACE" logs -l app=sw-blockvolume --all-containers --tail=500
capture "$ARTIFACT_DIR/logs/blockvolume.previous.log" kubectl -n "$NAMESPACE" logs -l app=sw-blockvolume --all-containers --previous --tail=500

capture_optional_sudo "$ARTIFACT_DIR/host/iscsi-sessions.txt" iscsiadm -m session
capture_optional_sudo "$ARTIFACT_DIR/host/iscsi-nodes.txt" iscsiadm -m node
capture_optional_sudo "$ARTIFACT_DIR/host/multipath.txt" multipath -ll
capture_optional_sudo "$ARTIFACT_DIR/host/dmsetup.txt" dmsetup ls --tree
capture "$ARTIFACT_DIR/host/kubelet-mounts.txt" findmnt -R /var/lib/kubelet -o TARGET,SOURCE,FSTYPE,OPTIONS
capture "$ARTIFACT_DIR/host/processes.txt" ps -eo pid,args

{
  if [[ "$capture_failures" -eq 0 ]]; then
    echo "failure_snapshot_status=ok"
  else
    echo "failure_snapshot_status=partial"
  fi
  echo "capture_failure_count=$capture_failures"
  echo "namespace=$NAMESPACE"
  echo "helm_namespace=$HELM_NAMESPACE"
  echo "helm_release=$HELM_RELEASE"
  echo "iqn_substr=$IQN_SUBSTR"
  echo "read_only=true"
  echo "k8s_snapshot=k8s"
  echo "logs=logs"
  echo "host_snapshot=host"
} >"$ARTIFACT_DIR/failure-snapshot-summary.txt"

cat "$ARTIFACT_DIR/failure-snapshot-summary.txt"
