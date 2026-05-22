#!/usr/bin/env bash
set -euo pipefail

ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-helm-cleanup-$(date -u +%Y%m%dT%H%M%SZ)}"
HELM_RELEASE="${SW_BLOCK_HELM_RELEASE:-sw-block}"
HELM_NAMESPACE="${SW_BLOCK_HELM_NAMESPACE:-kube-system}"
IQN_SUBSTR="${SW_BLOCK_CLEANUP_IQN_SUBSTR:-io.seaweedfs}"
HOSTPATH_PREFIX="${SW_BLOCK_CLEANUP_HOSTPATH_PREFIX:-}"

mkdir -p "$ARTIFACT_DIR"

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

failures=0

log() {
  printf '[helm-cleanup] %s\n' "$*" | tee -a "$ARTIFACT_DIR/verify-cleanup.log"
}

capture() {
  local name="$1"
  shift
  "$@" >"$ARTIFACT_DIR/$name" 2>&1 || true
}

mark_fail() {
  local reason="$1"
  failures=$((failures + 1))
  echo "$reason" >>"$ARTIFACT_DIR/cleanup-failures.txt"
  log "failure=$reason"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    mark_fail "missing-command-$1"
    return 1
  fi
}

log "artifact_dir=$ARTIFACT_DIR"
log "helm_release=$HELM_RELEASE"
log "helm_namespace=$HELM_NAMESPACE"
log "iqn_substr=$IQN_SUBSTR"
log "hostpath_prefix=${HOSTPATH_PREFIX:-none}"

require_cmd kubectl || true
require_cmd iscsiadm || true

wait_for_k8s_cleanup() {
  command -v kubectl >/dev/null 2>&1 || return 0
  local deadline=$((SECONDS + ${SW_BLOCK_CLEANUP_WAIT_SECONDS:-60}))
  while (( SECONDS < deadline )); do
    local residue
    residue="$(
      {
        kubectl get deploy,daemonset,statefulset,pod,svc,pvc,pv,configmap,secret,serviceaccount -A -o name
        kubectl get storageclass,csidriver,clusterrole,clusterrolebinding -o name
      } 2>/dev/null | grep -E '(sw-block|seaweed-block|block\.csi\.seaweedfs\.com)' || true
    )"
    if [[ -z "$residue" ]]; then
      return 0
    fi
    sleep 2
  done
}

wait_for_k8s_cleanup

if command -v helm >/dev/null 2>&1; then
  capture "helm-status.after-cleanup.txt" helm status "$HELM_RELEASE" --namespace "$HELM_NAMESPACE"
  if helm status "$HELM_RELEASE" --namespace "$HELM_NAMESPACE" >/dev/null 2>&1; then
    mark_fail "helm_release_still_present"
  fi
  capture "helm-list.after-cleanup.txt" helm list --all-namespaces
else
  mark_fail "missing-command-helm"
fi

capture "k8s-resources.after-cleanup.txt" bash -c '
  kubectl get deploy,daemonset,statefulset,pod,svc,pvc,pv,configmap,secret,serviceaccount -A -o name
  kubectl get storageclass,csidriver,clusterrole,clusterrolebinding -o name
'
if grep -E '(sw-block|seaweed-block|block\.csi\.seaweedfs\.com)' "$ARTIFACT_DIR/k8s-resources.after-cleanup.txt" >"$ARTIFACT_DIR/k8s-residue.after-cleanup.txt"; then
  mark_fail "kubernetes_sw_block_resources_present"
else
  : >"$ARTIFACT_DIR/k8s-residue.after-cleanup.txt"
fi

if command -v sudo >/dev/null 2>&1; then
  capture "iscsi-sessions.after-cleanup.txt" sudo -n iscsiadm -m session
  capture "iscsi-nodes.after-cleanup.txt" sudo -n iscsiadm -m node
else
  capture "iscsi-sessions.after-cleanup.txt" iscsiadm -m session
  capture "iscsi-nodes.after-cleanup.txt" iscsiadm -m node
fi
if grep -q "$IQN_SUBSTR" "$ARTIFACT_DIR/iscsi-sessions.after-cleanup.txt"; then
  mark_fail "iscsi_sessions_present"
fi
if grep -q "$IQN_SUBSTR" "$ARTIFACT_DIR/iscsi-nodes.after-cleanup.txt"; then
  mark_fail "iscsi_node_records_present"
fi

if command -v multipath >/dev/null 2>&1; then
  if command -v sudo >/dev/null 2>&1; then
    capture "multipath.after-cleanup.txt" sudo -n multipath -ll
  else
    capture "multipath.after-cleanup.txt" multipath -ll
  fi
  if grep -Eiq '(io\.seaweedfs|SeaweedF|BlockVol)' "$ARTIFACT_DIR/multipath.after-cleanup.txt"; then
    mark_fail "multipath_maps_present"
  fi
else
  echo "multipath unavailable" >"$ARTIFACT_DIR/multipath.after-cleanup.txt"
fi

capture "processes.after-cleanup.txt" ps -eo pid,args
if grep -E '[/]blockmaster --|[/]blockvolume --|[/]blockcsi |[/]iscsi-target |^ *[0-9]+ blockmaster |^ *[0-9]+ blockvolume |^ *[0-9]+ blockcsi |^ *[0-9]+ iscsi-target ' "$ARTIFACT_DIR/processes.after-cleanup.txt" >"$ARTIFACT_DIR/process-residue.after-cleanup.txt"; then
  mark_fail "sw_block_processes_present"
else
  : >"$ARTIFACT_DIR/process-residue.after-cleanup.txt"
fi

if [[ -n "$HOSTPATH_PREFIX" ]]; then
  if command -v sudo >/dev/null 2>&1; then
    sudo -n find /var/lib/sw-block -maxdepth 1 -mindepth 1 -type d -name "$HOSTPATH_PREFIX*" >"$ARTIFACT_DIR/hostpath-residue.after-cleanup.txt" 2>/dev/null || true
  else
    find /var/lib/sw-block -maxdepth 1 -mindepth 1 -type d -name "$HOSTPATH_PREFIX*" >"$ARTIFACT_DIR/hostpath-residue.after-cleanup.txt" 2>/dev/null || true
  fi
  if [[ -s "$ARTIFACT_DIR/hostpath-residue.after-cleanup.txt" ]]; then
    mark_fail "hostpath_residue_present"
  fi
else
  : >"$ARTIFACT_DIR/hostpath-residue.after-cleanup.txt"
fi

{
  if [[ "$failures" -eq 0 ]]; then
    echo "cleanup_status=ok"
  else
    echo "cleanup_status=failed"
  fi
  echo "helm_release=$HELM_RELEASE"
  echo "helm_namespace=$HELM_NAMESPACE"
  echo "iqn_substr=$IQN_SUBSTR"
  echo "k8s_residue_count=$(wc -l <"$ARTIFACT_DIR/k8s-residue.after-cleanup.txt")"
  echo "process_residue_count=$(wc -l <"$ARTIFACT_DIR/process-residue.after-cleanup.txt")"
  echo "hostpath_residue_count=$(wc -l <"$ARTIFACT_DIR/hostpath-residue.after-cleanup.txt")"
  echo "failure_count=$failures"
} >"$ARTIFACT_DIR/cleanup-summary.txt"

cat "$ARTIFACT_DIR/cleanup-summary.txt"
if [[ "$failures" -ne 0 ]]; then
  exit 1
fi
