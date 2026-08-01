#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase174-mounted-nvme-phase-profile-gate}"
BASELINE_DIR="${ARTIFACT_DIR}/phase120-mounted-profile"
SUMMARY="${ARTIFACT_DIR}/phase174-mounted-nvme-phase-profile-summary.txt"
SEQ_MIB="${SW_BLOCK_PHASE174_MOUNTED_SEQ_MIB:-256}"
PVC_SIZE="${SW_BLOCK_PHASE174_MOUNTED_PVC_SIZE:-1Gi}"
SOURCE_COMMIT="${SW_BLOCK_PHASE174_SOURCE_COMMIT:-$(git -C "${ROOT}" rev-parse HEAD 2>/dev/null || true)}"

mkdir -p "${ARTIFACT_DIR}" "${BASELINE_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

summary_value() {
  local file="$1"
  local key="$2"
  awk -F= -v key="${key}" '$1 == key {value = substr($0, length(key) + 2)} END {if (value != "") print value}' "${file}"
}

require_value() {
  local file="$1"
  local key="$2"
  local value
  value="$(summary_value "${file}" "${key}")"
  if [[ -z "${value}" ]]; then
    echo "missing ${key} in ${file}" >&2
    exit 1
  fi
  printf '%s' "${value}"
}

assert_equal() {
  local actual="$1"
  local expected="$2"
  local label="$3"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "${label}=${actual}, want ${expected}" >&2
    exit 1
  fi
}

write_summary "phase174_mounted_nvme_phase_profile_status=running"
write_summary "source_commit=${SOURCE_COMMIT}"
write_summary "initiator=linux_kernel_nvme_tcp"
write_summary "workload=mounted_ext4_sequential_write"
write_summary "seq_size_mib=${SEQ_MIB}"
write_summary "status_surface=/status/nvme"
write_summary "mounted_shape_comparable=false"
write_summary "fixed_work_throughput_ratio_allowed=false"

SW_BLOCK_ARTIFACT_DIR="${BASELINE_DIR}" \
SW_BLOCK_PHASE120_PROFILE_WRITE=true \
SW_BLOCK_PHASE120_SEQ_MIB="${SEQ_MIB}" \
SW_BLOCK_PHASE120_PVC_SIZE="${PVC_SIZE}" \
  bash "${ROOT}/scripts/run-phase120-nvme-tcp-performance-baseline-gate.sh" "${ROOT}" \
  >"${ARTIFACT_DIR}/phase120.stdout.txt" \
  2>"${ARTIFACT_DIR}/phase120.stderr.txt"

BASELINE_SUMMARY="${BASELINE_DIR}/phase120-nvme-tcp-performance-baseline-summary.txt"
assert_equal "$(require_value "${BASELINE_SUMMARY}" phase120_nvme_tcp_performance_baseline_status)" "ok" "phase120 status"
assert_equal "$(require_value "${BASELINE_SUMMARY}" phase120_nvme_phase_profile_status)" "ok" "NVMe phase profile status"
assert_equal "$(require_value "${BASELINE_SUMMARY}" mounted_nvme_phase_counter_reconciliation)" "true" "phase counter reconciliation"
assert_equal "$(require_value "${BASELINE_SUMMARY}" mounted_client_latency_reconciliation_available)" "false" "mounted client latency boundary"
assert_equal "$(require_value "${BASELINE_SUMMARY}" mounted_shape_comparable)" "false" "mounted shape boundary"
assert_equal "$(require_value "${BASELINE_SUMMARY}" cleanup_status)" "ok" "cleanup status"

WRITE_OPS="$(require_value "${BASELINE_SUMMARY}" mounted_nvme_write_ops)"
R2T_OPS="$(require_value "${BASELINE_SUMMARY}" mounted_nvme_r2t_write_ops)"
R2T_PHASE_OPS="$(require_value "${BASELINE_SUMMARY}" mounted_nvme_r2t_collection_ops)"
HANDLER_OPS="$(require_value "${BASELINE_SUMMARY}" mounted_nvme_handler_ops)"
COMPLETION_OPS="$(require_value "${BASELINE_SUMMARY}" mounted_nvme_completion_send_ops)"
assert_equal "${R2T_PHASE_OPS}" "${R2T_OPS}" "R2T phase operations"
assert_equal "${HANDLER_OPS}" "${WRITE_OPS}" "handler operations"
assert_equal "${COMPLETION_OPS}" "${WRITE_OPS}" "completion operations"

python3 - "${WRITE_OPS}" "${R2T_OPS}" "$(require_value "${BASELINE_SUMMARY}" mounted_nvme_server_phase_ns)" <<'PY'
import sys
write_ops, r2t_ops, phase_ns = map(int, sys.argv[1:])
if write_ops <= 0 or r2t_ops <= 0 or phase_ns <= 0:
    raise SystemExit(f"missing mounted NVMe work: writes={write_ops} r2t={r2t_ops} phase_ns={phase_ns}")
PY

for key in \
  seq_write_mibps \
  seq_read_mibps \
  mounted_nvme_write_ops \
  mounted_nvme_r2t_write_ops \
  mounted_nvme_write_bytes \
  mounted_nvme_capsule_receive_parse_ns_per_op \
  mounted_nvme_r2t_collection_ns_per_op \
  mounted_nvme_dispatch_wait_ns_per_op \
  mounted_nvme_handler_ns_per_op \
  mounted_nvme_completion_queue_wait_ns_per_op \
  mounted_nvme_completion_send_ns_per_op \
  mounted_nvme_server_phase_ns_per_op \
  mounted_nvme_dominant_phase; do
  write_summary "${key}=$(require_value "${BASELINE_SUMMARY}" "${key}")"
done

write_summary "mounted_nvme_phase_counter_reconciliation=true"
write_summary "mounted_client_latency_reconciliation_available=false"
write_summary "architecture_candidate_selected=false"
write_summary "cleanup_status=ok"
write_summary "phase174_mounted_nvme_phase_profile_status=ok"
