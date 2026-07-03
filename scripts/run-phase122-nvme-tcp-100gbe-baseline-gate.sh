#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase122-nvme-tcp-100gbe-baseline-gate}"
SUMMARY="${ARTIFACT_DIR}/phase122-nvme-tcp-100gbe-baseline-summary.txt"
INNER_DIR="${ARTIFACT_DIR}/phase120-inner"
FRONTEND_IP_MAP="${SW_BLOCK_PHASE122_FRONTEND_IP_MAP:-m01=10.0.0.1,m02=10.0.0.3}"
FRONTEND_NETWORK_CLASS="${SW_BLOCK_PHASE122_FRONTEND_NETWORK_CLASS:-100gbe_tcp}"
EXPECTED_ROUTE_DEV="${SW_BLOCK_PHASE122_EXPECTED_ROUTE_DEV:-enp1s0np0}"
EXPECTED_TARGET_PREFIX="${SW_BLOCK_PHASE122_EXPECTED_TARGET_PREFIX:-10.0.0.}"
IMPORT_K3S_NODES="${SW_BLOCK_IMPORT_K3S_NODES:-192.168.1.181,192.168.1.184}"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

summary_value() {
  local key="$1"
  awk -F= -v key="$key" '$1 == key {value = substr($0, length(key) + 2)} END {if (value != "") print value}' "${INNER_DIR}/phase120-nvme-tcp-performance-baseline-summary.txt"
}

require_value() {
  local key="$1"
  local value
  value="$(summary_value "$key")"
  if [[ -z "${value}" ]]; then
    echo "missing inner summary key: ${key}" >&2
    exit 1
  fi
  printf '%s' "${value}"
}

write_summary "phase122_nvme_tcp_100gbe_baseline_status=running"
write_summary "frontend_transport=tcp"
write_summary "frontend_ip_map=${FRONTEND_IP_MAP}"
write_summary "frontend_network_class=${FRONTEND_NETWORK_CLASS}"
write_summary "nvme_rdma_supported=false"
write_summary "roce_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"

SW_BLOCK_ARTIFACT_DIR="${INNER_DIR}" \
SW_BLOCK_FRONTEND_IP_MAP="${FRONTEND_IP_MAP}" \
SW_BLOCK_FRONTEND_NETWORK_CLASS="${FRONTEND_NETWORK_CLASS}" \
SW_BLOCK_EXPECTED_FRONTEND_ROUTE_DEV="${EXPECTED_ROUTE_DEV}" \
SW_BLOCK_IMPORT_K3S_NODES="${IMPORT_K3S_NODES}" \
SW_BLOCK_PHASE120_PVC_NAME="${SW_BLOCK_PHASE122_PVC_NAME:-sw-block-phase122-pvc}" \
SW_BLOCK_PHASE120_STORAGECLASS="${SW_BLOCK_PHASE122_STORAGECLASS:-sw-block-phase122}" \
SW_BLOCK_PHASE120_POD="${SW_BLOCK_PHASE122_POD:-sw-block-phase122-perf}" \
  bash "${ROOT}/scripts/run-phase120-nvme-tcp-performance-baseline-gate.sh" "${ROOT}"

INNER_STATUS="$(require_value phase120_nvme_tcp_performance_baseline_status)"
if [[ "${INNER_STATUS}" != "ok" ]]; then
  echo "inner phase120-compatible gate status=${INNER_STATUS}, want ok" >&2
  exit 1
fi

PUBLISH_TARGET="$(require_value publish_target)"
PUBLISH_TARGET_HOST="${PUBLISH_TARGET%:*}"
if [[ "${PUBLISH_TARGET_HOST}" != ${EXPECTED_TARGET_PREFIX}* ]]; then
  echo "publish_target=${PUBLISH_TARGET} does not match expected prefix ${EXPECTED_TARGET_PREFIX}" >&2
  exit 1
fi
if [[ "${PUBLISH_TARGET_HOST}" == 192.168.* ]]; then
  echo "publish_target=${PUBLISH_TARGET} still uses management LAN" >&2
  exit 1
fi

ROUTE_DEV="$(require_value publish_target_route_dev)"
if [[ "${ROUTE_DEV}" != "${EXPECTED_ROUTE_DEV}" ]]; then
  echo "publish target route dev=${ROUTE_DEV}, want ${EXPECTED_ROUTE_DEV}" >&2
  exit 1
fi

NETWORK_CLASS="$(require_value publish_target_network_class)"
if [[ "${NETWORK_CLASS}" != "${FRONTEND_NETWORK_CLASS}" ]]; then
  echo "publish target network class=${NETWORK_CLASS}, want ${FRONTEND_NETWORK_CLASS}" >&2
  exit 1
fi

write_summary "management_ip=$(require_value management_ip)"
write_summary "frontend_ip=$(require_value frontend_ip)"
write_summary "publish_target=${PUBLISH_TARGET}"
write_summary "publish_target_network_class=${NETWORK_CLASS}"
write_summary "publish_target_source=$(require_value publish_target_source)"
write_summary "publish_target_route_dev=${ROUTE_DEV}"
write_summary "internal_ip_not_reused_as_performance_target=$(require_value internal_ip_not_reused_as_performance_target)"
write_summary "managed_volume_status=$(require_value managed_volume_status)"
write_summary "managed_volume_reason=$(require_value managed_volume_reason)"
write_summary "seq_size_mib=$(require_value seq_size_mib)"
write_summary "seq_write_duration_ms=$(require_value seq_write_duration_ms)"
write_summary "seq_write_mibps=$(require_value seq_write_mibps)"
write_summary "seq_read_duration_ms=$(require_value seq_read_duration_ms)"
write_summary "seq_read_mibps=$(require_value seq_read_mibps)"
write_summary "small_write_ops=$(require_value small_write_ops)"
write_summary "small_write_block_bytes=$(require_value small_write_block_bytes)"
write_summary "small_write_duration_ms=$(require_value small_write_duration_ms)"
write_summary "small_write_iops=$(require_value small_write_iops)"
write_summary "small_write_mibps=$(require_value small_write_mibps)"
write_summary "final_data_verified=$(require_value final_data_verified)"
write_summary "cleanup_status=$(require_value cleanup_status)"
write_summary "phase122_nvme_tcp_100gbe_baseline_status=ok"
