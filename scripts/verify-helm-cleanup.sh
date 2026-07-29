#!/usr/bin/env bash
set -euo pipefail

ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-helm-cleanup-$(date -u +%Y%m%dT%H%M%SZ)}"
HELM_RELEASE="${SW_BLOCK_HELM_RELEASE:-sw-block}"
HELM_NAMESPACE="${SW_BLOCK_HELM_NAMESPACE:-kube-system}"
IQN_SUBSTR="${SW_BLOCK_CLEANUP_IQN_SUBSTR:-io.seaweedfs}"
HOSTPATH_PREFIX="${SW_BLOCK_CLEANUP_HOSTPATH_PREFIX:-}"
MULTIPATH_ORPHAN_PATTERN="${SW_BLOCK_CLEANUP_MULTIPATH_ORPHAN_PATTERN:-^mpath[^[:space:]]*[[:space:]].*##,##}"
MULTIPATH_FLUSH="${SW_BLOCK_CLEANUP_MULTIPATH_FLUSH:-0}"

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

count_file_matches() {
  local path="$1"
  local pattern="$2"
  if [[ ! -f "$path" ]]; then
    echo 0
    return
  fi
  grep -c "$pattern" "$path" 2>/dev/null || true
}

log "artifact_dir=$ARTIFACT_DIR"
log "helm_release=$HELM_RELEASE"
log "helm_namespace=$HELM_NAMESPACE"
log "iqn_substr=$IQN_SUBSTR"
log "hostpath_prefix=${HOSTPATH_PREFIX:-none}"
log "multipath_orphan_pattern=$MULTIPATH_ORPHAN_PATTERN"
log "multipath_flush=$MULTIPATH_FLUSH"

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
        kubectl get volumeattachments.storage.k8s.io \
          -o custom-columns=NAME:.metadata.name,ATTACHER:.spec.attacher,PV:.spec.source.persistentVolumeName --no-headers
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
  kubectl get volumeattachments.storage.k8s.io \
    -o custom-columns=NAME:.metadata.name,ATTACHER:.spec.attacher,PV:.spec.source.persistentVolumeName --no-headers
'
if grep -E '(sw-block|seaweed-block|block\.csi\.seaweedfs\.com)' "$ARTIFACT_DIR/k8s-resources.after-cleanup.txt" >"$ARTIFACT_DIR/k8s-residue.after-cleanup.txt"; then
  mark_fail "kubernetes_sw_block_resources_present"
else
  : >"$ARTIFACT_DIR/k8s-residue.after-cleanup.txt"
fi

if [[ -d /sys/kernel/config/nvmet/subsystems ]]; then
  if command -v sudo >/dev/null 2>&1; then
    sudo -n find /sys/kernel/config/nvmet/subsystems -mindepth 1 -maxdepth 1 -type d \
      -name '*io.seaweedfs*' -printf '%f\n' >"$ARTIFACT_DIR/nvme-target-residue.after-cleanup.txt" 2>/dev/null || true
  else
    find /sys/kernel/config/nvmet/subsystems -mindepth 1 -maxdepth 1 -type d \
      -name '*io.seaweedfs*' -printf '%f\n' >"$ARTIFACT_DIR/nvme-target-residue.after-cleanup.txt" 2>/dev/null || true
  fi
else
  : >"$ARTIFACT_DIR/nvme-target-residue.after-cleanup.txt"
fi
if [[ -s "$ARTIFACT_DIR/nvme-target-residue.after-cleanup.txt" ]]; then
  mark_fail "nvme_target_subsystems_present"
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
ISCSI_RESIDUE_COUNT=$(( \
  $(count_file_matches "$ARTIFACT_DIR/iscsi-sessions.after-cleanup.txt" "$IQN_SUBSTR") + \
  $(count_file_matches "$ARTIFACT_DIR/iscsi-nodes.after-cleanup.txt" "$IQN_SUBSTR") \
))

capture_multipath_state() {
  if command -v multipath >/dev/null 2>&1; then
    if command -v sudo >/dev/null 2>&1; then
      capture "multipath.after-cleanup.txt" sudo -n multipath -ll
    else
      capture "multipath.after-cleanup.txt" multipath -ll
    fi
  else
    echo "multipath unavailable" >"$ARTIFACT_DIR/multipath.after-cleanup.txt"
  fi

  if command -v dmsetup >/dev/null 2>&1; then
    if command -v sudo >/dev/null 2>&1; then
      capture "dmsetup.after-cleanup.txt" sudo -n dmsetup ls --tree
    else
      capture "dmsetup.after-cleanup.txt" dmsetup ls --tree
    fi
  else
    echo "dmsetup unavailable" >"$ARTIFACT_DIR/dmsetup.after-cleanup.txt"
  fi
}

collect_multipath_residue() {
  local out="$1"
  : >"$out"
  grep -Ei "(io\.seaweedfs|SeaweedF|BlockVol|$MULTIPATH_ORPHAN_PATTERN)" "$ARTIFACT_DIR/multipath.after-cleanup.txt" >>"$out" 2>/dev/null || true
  grep -E '^mpath[^[:space:]]*[[:space:]]' "$ARTIFACT_DIR/dmsetup.after-cleanup.txt" >>"$out" 2>/dev/null || true
}

capture_multipath_state
collect_multipath_residue "$ARTIFACT_DIR/multipath-residue.before-flush.txt"

if [[ "$MULTIPATH_FLUSH" == "1" && -s "$ARTIFACT_DIR/multipath-residue.before-flush.txt" ]]; then
  log "multipath_flush_attempt=1"
  capture "kubelet-mounts.before-multipath-flush.txt" findmnt -R /var/lib/kubelet -o TARGET,SOURCE,FSTYPE,OPTIONS
  while read -r map rest; do
    [[ "$map" == mpath* ]] || continue
    dm="$(printf '%s\n' "${rest:-}" | grep -o 'dm-[0-9]\+' | head -1 || true)"
    log "multipath_flush_map=$map dm=${dm:-unknown}"
    {
      findmnt -rn -S "/dev/mapper/$map" -o TARGET 2>/dev/null || true
      if [[ "$dm" == dm-* ]]; then
        findmnt -rn -S "/dev/$dm" -o TARGET 2>/dev/null || true
      fi
      lsblk -nr -o MOUNTPOINT "/dev/mapper/$map" 2>/dev/null || \
        lsblk -nr -o MOUNTPOINTS "/dev/mapper/$map" 2>/dev/null | sed 's/\\x0a/\n/g' || true
    } | awk 'NF {print}' | sort -u | while read -r mountpoint; do
      [[ "$mountpoint" == /var/lib/kubelet/* ]] || continue
      log "unmount_stale_kubelet_path=$mountpoint"
      if command -v sudo >/dev/null 2>&1; then
        sudo -n umount -fl "$mountpoint" >>"$ARTIFACT_DIR/multipath-flush.log" 2>&1 || true
      else
        umount -fl "$mountpoint" >>"$ARTIFACT_DIR/multipath-flush.log" 2>&1 || true
      fi
    done
    if command -v sudo >/dev/null 2>&1; then
      sudo -n multipath -f "$map" >>"$ARTIFACT_DIR/multipath-flush.log" 2>&1 || true
      sudo -n dmsetup remove -f "$map" >>"$ARTIFACT_DIR/multipath-flush.log" 2>&1 || true
    else
      multipath -f "$map" >>"$ARTIFACT_DIR/multipath-flush.log" 2>&1 || true
      dmsetup remove -f "$map" >>"$ARTIFACT_DIR/multipath-flush.log" 2>&1 || true
    fi
  done <"$ARTIFACT_DIR/multipath-residue.before-flush.txt"
  if command -v sudo >/dev/null 2>&1; then
    sudo -n multipath -F >>"$ARTIFACT_DIR/multipath-flush.log" 2>&1 || true
    sudo -n udevadm settle >>"$ARTIFACT_DIR/multipath-flush.log" 2>&1 || true
  else
    multipath -F >>"$ARTIFACT_DIR/multipath-flush.log" 2>&1 || true
    udevadm settle >>"$ARTIFACT_DIR/multipath-flush.log" 2>&1 || true
  fi
  capture "kubelet-mounts.after-multipath-flush.txt" findmnt -R /var/lib/kubelet -o TARGET,SOURCE,FSTYPE,OPTIONS
  capture_multipath_state
fi

collect_multipath_residue "$ARTIFACT_DIR/multipath-residue.after-cleanup.txt"
if [[ -s "$ARTIFACT_DIR/multipath-residue.after-cleanup.txt" ]]; then
  mark_fail "multipath_maps_present"
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
  echo "nvme_target_residue_count=$(wc -l <"$ARTIFACT_DIR/nvme-target-residue.after-cleanup.txt")"
  echo "iscsi_residue_count=$ISCSI_RESIDUE_COUNT"
  echo "process_residue_count=$(wc -l <"$ARTIFACT_DIR/process-residue.after-cleanup.txt")"
  echo "multipath_residue_count=$(wc -l <"$ARTIFACT_DIR/multipath-residue.after-cleanup.txt")"
  echo "hostpath_residue_count=$(wc -l <"$ARTIFACT_DIR/hostpath-residue.after-cleanup.txt")"
  echo "failure_count=$failures"
  echo "cleanup_observed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"$ARTIFACT_DIR/cleanup-summary.txt"

cat "$ARTIFACT_DIR/cleanup-summary.txt"
if [[ "$failures" -ne 0 ]]; then
  exit 1
fi
