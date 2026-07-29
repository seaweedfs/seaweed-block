#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
RUN_ID="${SW_BLOCK_PHASE166_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase166-nvme-rdma-k8s-multipath-reconnect}"
SUMMARY="${ARTIFACT_DIR}/phase166-nvme-rdma-k8s-multipath-reconnect-summary.txt"
APP_NODE="${SW_BLOCK_PHASE166_APP_NODE:-}"
APP_SSH_ADDR="${SW_BLOCK_PHASE166_APP_SSH_ADDR:-}"
APP_SSH_USER="${SW_BLOCK_PHASE166_APP_SSH_USER:-testdev}"
APP_SSH_KEY="${SW_BLOCK_PHASE166_APP_SSH_KEY:-/opt/work/testdev_key}"
TARGET_REMOTE_SSH_ADDR="${SW_BLOCK_PHASE166_TARGET_REMOTE_SSH_ADDR:-192.168.1.181}"
TARGET_REMOTE_SSH_USER="${SW_BLOCK_PHASE166_TARGET_REMOTE_SSH_USER:-testdev}"
TARGET_REMOTE_SSH_KEY="${SW_BLOCK_PHASE166_TARGET_REMOTE_SSH_KEY:-${APP_SSH_KEY}}"
FRONTEND_IP_MAP="${SW_BLOCK_PHASE166_FRONTEND_IP_MAP:-m01=10.0.0.1,m02=10.0.0.3}"
OUTAGE_STATE_HOSTPATH="/var/lib/sw-block/testops-${RUN_ID}-phase166-outage"
ENDPOINT_STATE_HOSTPATH="/var/lib/sw-block/testops-${RUN_ID}-phase166-endpoint"

mkdir -p "${ARTIFACT_DIR}"/{build,local,outage,endpoint-change}
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

require_line() {
  local file="$1"
  local line="$2"
  grep -Fqx "${line}" "${file}"
}

cleanup_phase166_hostpaths() {
  local root
  for root in "${OUTAGE_STATE_HOSTPATH}" "${ENDPOINT_STATE_HOSTPATH}"; do
    case "${root}" in
      /var/lib/sw-block/testops-*phase166-*) ;;
      *) echo "refusing unsafe Phase 166 hostPath cleanup ${root}" >&2; return 1 ;;
    esac
    sudo -n rm -rf -- "${root}"
    ssh -i "${TARGET_REMOTE_SSH_KEY}" -o BatchMode=yes -o ConnectTimeout=10 \
      "${TARGET_REMOTE_SSH_USER}@${TARGET_REMOTE_SSH_ADDR}" "sudo rm -rf -- '${root}'"
  done
}

verify_phase166_hostpaths_removed() {
  local root
  for root in "${OUTAGE_STATE_HOSTPATH}" "${ENDPOINT_STATE_HOSTPATH}"; do
    sudo -n test ! -e "${root}"
    ssh -i "${TARGET_REMOTE_SSH_KEY}" -o BatchMode=yes -o ConnectTimeout=10 \
      "${TARGET_REMOTE_SSH_USER}@${TARGET_REMOTE_SSH_ADDR}" "sudo test ! -e '${root}'"
  done
}

require_disjoint_rdma_initiator() {
  [[ -n "${APP_NODE}" ]] || {
    echo "SW_BLOCK_PHASE166_APP_NODE must name a third RoCE-capable Kubernetes initiator" >&2
    return 1
  }
  [[ -n "${APP_SSH_ADDR}" ]] || {
    echo "SW_BLOCK_PHASE166_APP_SSH_ADDR must reach the third RoCE initiator" >&2
    return 1
  }
  local target_node
  IFS=',' read -r -a frontend_entries <<<"${FRONTEND_IP_MAP}"
  for target_node in "${frontend_entries[@]}"; do
    target_node="${target_node%%=*}"
    if [[ "${APP_NODE}" == "${target_node}" ]]; then
      echo "Phase 166 initiator ${APP_NODE} must be disjoint from RDMA target ${target_node}" >&2
      return 1
    fi
  done
  kubectl get node "${APP_NODE}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' | grep -q '^True$'
  ssh -i "${APP_SSH_KEY}" -o BatchMode=yes -o ConnectTimeout=10 \
    "${APP_SSH_USER}@${APP_SSH_ADDR}" \
    "ip -4 -o addr show | grep -q '/'; rdma link show | grep -q .; sudo -n nvme list-subsys -o json >/dev/null"
}

require_clean_shared_lab() {
  local residue
  residue="$({
    helm status sw-block --namespace kube-system 2>/dev/null && echo "helm/sw-block"
    kubectl get deploy,daemonset,statefulset,pod,svc,pvc,pv,configmap,secret,serviceaccount -A -o name 2>/dev/null \
      | grep -E '(sw-block|seaweed-block|block\.csi\.seaweedfs\.com)' || true
    kubectl get storageclass,csidriver,clusterrole,clusterrolebinding -o name 2>/dev/null \
      | grep -E '(sw-block|seaweed-block|block\.csi\.seaweedfs\.com)' || true
    kubectl get volumeattachments.storage.k8s.io \
      -o custom-columns=NAME:.metadata.name,ATTACHER:.spec.attacher,PV:.spec.source.persistentVolumeName --no-headers 2>/dev/null \
      | grep 'block\.csi\.seaweedfs\.com' || true
    kubectl get validatingadmissionpolicy,validatingadmissionpolicybinding -o name 2>/dev/null \
      | grep -E '(sw-block|seaweed-block|block\.seaweedfs\.com)' || true
    kubectl get swblockclusters.block.seaweedfs.com,swblockvolumes.block.seaweedfs.com -A -o name 2>/dev/null || true
    sudo find /sys/kernel/config/nvmet/subsystems -mindepth 1 -maxdepth 1 -type d \
      -name '*io.seaweedfs*' -printf 'nvmet/%f\n' 2>/dev/null || true
    for pid_file in /sys/block/nbd*/pid; do
      [[ -s "${pid_file}" ]] && printf 'active-nbd/%s\n' "$(basename "$(dirname "${pid_file}")")"
    done
  } | awk 'NF')"
  if [[ -n "${residue}" ]]; then
    printf 'refusing to replace images on a non-clean shared lab:\n%s\n' "${residue}" >&2
    return 1
  fi
  ssh -i "${TARGET_REMOTE_SSH_KEY}" -o BatchMode=yes -o ConnectTimeout=10 \
    "${TARGET_REMOTE_SSH_USER}@${TARGET_REMOTE_SSH_ADDR}" \
    "test -z \"\$(sudo find /sys/kernel/config/nvmet/subsystems -mindepth 1 -maxdepth 1 -type d -name '*io.seaweedfs*' -print -quit 2>/dev/null)\"; ! grep -qs . /sys/block/nbd*/pid"
  ssh -i "${APP_SSH_KEY}" -o BatchMode=yes -o ConnectTimeout=10 \
    "${APP_SSH_USER}@${APP_SSH_ADDR}" \
    "! sudo -n nvme list-subsys -o json 2>/dev/null | grep -q 'nqn.*io.seaweedfs'"
}

trap cleanup_phase166_hostpaths EXIT

require_disjoint_rdma_initiator
require_clean_shared_lab

write_summary "phase166_nvme_rdma_k8s_multipath_reconnect_status=running"

(
  cd "${ROOT}"
  go test ./core/csi ./cmd/blockcsi -count=1
) >"${ARTIFACT_DIR}/local/go-test-csi.txt" 2>&1
write_summary "source_contract_tests=ok"

(
  cd "${ROOT}"
  SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/build" \
  SW_BLOCK_IMPORT_K3S=1 \
  SW_BLOCK_IMPORT_K3S_NODES="${TARGET_REMOTE_SSH_ADDR},${APP_SSH_ADDR}" \
  SW_BLOCK_IMPORT_K3S_SSH_USER="${APP_SSH_USER}" \
  SW_BLOCK_IMPORT_K3S_SSH_KEY="${APP_SSH_KEY}" \
    bash scripts/build-alpha-images.sh "${ROOT}"
) >"${ARTIFACT_DIR}/build/build.log" 2>&1
require_line "${ARTIFACT_DIR}/build/alpha-images.env" "SW_BLOCK_IMAGE=sw-block:local"
require_line "${ARTIFACT_DIR}/build/alpha-images.env" "SW_BLOCK_CSI_IMAGE=sw-block-csi:local"
write_summary "fresh_matching_images=ok"

export SW_BLOCK_NVME_TRANSPORT=rdma
export SW_BLOCK_NVME_FRONTEND_IP_MAP="${FRONTEND_IP_MAP}"
export SW_BLOCK_NVME_FRONTEND_NETWORK_CLASS=100gbe_roce
export SW_BLOCK_NVME_RESTART_PERSISTENCE=hostpath
export SW_BLOCK_NVME_CLEANUP_STATE_HOSTPATH=1
export SW_BLOCK_NVME_REQUIRE_HOST_TRANSPORT=rdma
export SW_BLOCK_NVME_STRICT_CLEANUP_SCOPE=1
export SW_BLOCK_NVME_APP_NODE_SELECTOR="${APP_NODE}"
export SW_BLOCK_NVME_APP_HOST_SSH_ADDR="${APP_SSH_ADDR}"
export SW_BLOCK_NVME_APP_HOST_SSH_USER="${APP_SSH_USER}"
export SW_BLOCK_NVME_APP_HOST_SSH_KEY="${APP_SSH_KEY}"
export SW_BLOCK_NVME_MOUNTED_IO=1
export SW_BLOCK_NVME_RECONNECT_OWNER=1
export SW_BLOCK_NVME_RECONNECT_INTERVAL=1s
export SW_BLOCK_NVME_FORCE_STAGE2_MULTIPATH=1

OUTAGE_SUMMARY="${ARTIFACT_DIR}/outage/phase166-rdma-multipath-outage-summary.txt"
SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/outage" \
SW_BLOCK_NVME_PATH_LOSS_SUMMARY_NAME="$(basename "${OUTAGE_SUMMARY}")" \
SW_BLOCK_NVME_PATH_LOSS_PHASE_STATUS_KEY=phase166_rdma_multipath_outage_status \
SW_BLOCK_NVME_MOUNTED_POD=sw-block-phase166-rdma-outage \
SW_BLOCK_NVME_RESTORE_PATH=1 \
SW_BLOCK_NVME_HOST_PATH_DISCONNECT=0 \
SW_BLOCK_NVME_DESIRED_PATH_CHANGE=0 \
SW_BLOCK_NVME_STATE_HOSTPATH="${OUTAGE_STATE_HOSTPATH}" \
SW_BLOCK_MASTER_PORT_FORWARD_PORT=29661 \
  bash "${ROOT}/scripts/run-phase111-nvme-k8s-path-loss-crd-gate.sh" "${ROOT}"

require_line "${OUTAGE_SUMMARY}" "phase166_rdma_multipath_outage_status=ok"
require_line "${OUTAGE_SUMMARY}" "initial_live_rdma_path_count=2"
require_line "${OUTAGE_SUMMARY}" "degraded_live_rdma_path_count=1"
require_line "${OUTAGE_SUMMARY}" "outage_is_non_primary=true"
require_line "${OUTAGE_SUMMARY}" "mounted_pod_uid_preserved=true"
require_line "${OUTAGE_SUMMARY}" "mounted_io_after_path_loss=ok"
require_line "${OUTAGE_SUMMARY}" "restored_live_rdma_path_count=2"
require_line "${OUTAGE_SUMMARY}" "mounted_pod_uid_preserved_after_restore=true"
require_line "${OUTAGE_SUMMARY}" "mounted_io_after_restore=ok"
require_line "${OUTAGE_SUMMARY}" "crd_reason=nvme_multipath_path_missing"
require_line "${OUTAGE_SUMMARY}" "cleanup_status=ok"
cleanup_phase166_hostpaths
verify_phase166_hostpaths_removed
require_clean_shared_lab
write_summary "outage_initial_live_rdma_path_count=2"
write_summary "outage_degraded_live_rdma_path_count=1"
write_summary "outage_mounted_io=ok"
write_summary "outage_pod_uid_preserved=true"
write_summary "outage_is_non_primary=true"
write_summary "outage_negative_surface_reason=nvme_multipath_path_missing"
write_summary "outage_restored_live_rdma_path_count=2"
write_summary "outage_restore_mounted_io=ok"
write_summary "outage_tcp_fallback_observed=false"
write_summary "outage_cleanup_status=ok"

ENDPOINT_SUMMARY="${ARTIFACT_DIR}/endpoint-change/phase166-rdma-endpoint-change-summary.txt"
SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/endpoint-change" \
SW_BLOCK_NVME_PATH_LOSS_SUMMARY_NAME="$(basename "${ENDPOINT_SUMMARY}")" \
SW_BLOCK_NVME_PATH_LOSS_PHASE_STATUS_KEY=phase166_rdma_endpoint_change_status \
SW_BLOCK_NVME_MOUNTED_POD=sw-block-phase166-rdma-endpoint \
SW_BLOCK_NVME_RESTORE_PATH=0 \
SW_BLOCK_NVME_HOST_PATH_DISCONNECT=0 \
SW_BLOCK_NVME_DESIRED_PATH_CHANGE=1 \
SW_BLOCK_NVME_REQUIRE_STALE_PATH_PRUNE=1 \
SW_BLOCK_NVME_STATE_HOSTPATH="${ENDPOINT_STATE_HOSTPATH}" \
SW_BLOCK_MASTER_PORT_FORWARD_PORT=29662 \
  bash "${ROOT}/scripts/run-phase111-nvme-k8s-path-loss-crd-gate.sh" "${ROOT}"

require_line "${ENDPOINT_SUMMARY}" "phase166_rdma_endpoint_change_status=ok"
require_line "${ENDPOINT_SUMMARY}" "initial_live_rdma_path_count=2"
require_line "${ENDPOINT_SUMMARY}" "desired_path_set_changed=true"
require_line "${ENDPOINT_SUMMARY}" "endpoint_change_is_non_primary=true"
require_line "${ENDPOINT_SUMMARY}" "new_desired_path_connected=true"
require_line "${ENDPOINT_SUMMARY}" "desired_change_live_rdma_path_count=2"
require_line "${ENDPOINT_SUMMARY}" "stale_old_path_pruned=true"
require_line "${ENDPOINT_SUMMARY}" "pod_uid_preserved=true"
require_line "${ENDPOINT_SUMMARY}" "mounted_io_during_endpoint_change=ok"
require_line "${ENDPOINT_SUMMARY}" "mounted_io_after_reconnect=ok"
require_line "${ENDPOINT_SUMMARY}" "surviving_controller_preserved=true"
require_line "${ENDPOINT_SUMMARY}" "crd_status_agrees=true"
require_line "${ENDPOINT_SUMMARY}" "report_dashboard_agree=true"
require_line "${ENDPOINT_SUMMARY}" "explain_agrees=true"
require_line "${ENDPOINT_SUMMARY}" "cleanup_status=ok"
cleanup_phase166_hostpaths
verify_phase166_hostpaths_removed
require_clean_shared_lab
write_summary "endpoint_initial_live_rdma_path_count=2"
write_summary "desired_rdma_endpoint_changed=true"
write_summary "new_rdma_endpoint_connected=true"
write_summary "stale_rdma_endpoint_pruned=true"
write_summary "endpoint_live_rdma_path_count=2"
write_summary "endpoint_pod_uid_preserved=true"
write_summary "endpoint_change_is_non_primary=true"
write_summary "endpoint_surviving_controller_preserved=true"
write_summary "endpoint_transition_io=ok"
write_summary "endpoint_mounted_io=ok"
write_summary "endpoint_surfaces_agree=true"
write_summary "endpoint_tcp_fallback_observed=false"
write_summary "endpoint_cleanup_status=ok"
write_summary "hostpath_cleanup_nodes=m01,m02"

write_summary "cleanup_status=ok"
write_summary "phase166_nvme_rdma_k8s_multipath_reconnect_status=ok"
cat "${SUMMARY}"
