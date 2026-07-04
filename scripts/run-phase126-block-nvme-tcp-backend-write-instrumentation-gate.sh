#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase126-block-nvme-tcp-backend-write-instrumentation-gate}"
SUMMARY="${ARTIFACT_DIR}/phase126-block-nvme-tcp-backend-write-instrumentation-summary.txt"
PHASE125_DIR="${ARTIFACT_DIR}/phase125-profile"
SEQ_MIB="${SW_BLOCK_PHASE126_SEQ_MIB:-512}"

mkdir -p "${ARTIFACT_DIR}" "${PHASE125_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

summary_value() {
  local file="$1"
  local key="$2"
  awk -F= -v key="$key" '$1 == key {value = substr($0, length(key) + 2)} END {if (value != "") print value}' "$file"
}

require_summary_value() {
  local file="$1"
  local key="$2"
  local value
  value="$(summary_value "$file" "$key")"
  if [[ -z "${value}" ]]; then
    echo "missing summary key ${key} in ${file}" >&2
    exit 1
  fi
  printf '%s' "${value}"
}

assert_int_ge() {
  local actual="$1"
  local want="$2"
  local label="$3"
  python3 - "$actual" "$want" "$label" <<'PY'
import sys
actual = int(sys.argv[1])
want = int(sys.argv[2])
label = sys.argv[3]
if actual < want:
    raise SystemExit(f"{label}={actual}, want >= {want}")
PY
}

classify_write_path() {
  python3 - "$1" "$2" "$3" "$4" <<'PY'
import sys

def i(v):
    try:
        return int(v)
    except ValueError:
        return 0

target_ms = i(sys.argv[1])
backend_ms = i(sys.argv[2])
sync_ms = i(sys.argv[3])
sync_ops = i(sys.argv[4])
if target_ms <= 0 or backend_ms <= 0:
    print("unknown")
elif sync_ops > 0 and sync_ms >= max(1, backend_ms // 2):
    print("backend_sync")
elif backend_ms >= max(1, int(target_ms * 0.75)):
    print("backend_write")
elif target_ms > backend_ms * 2:
    print("target_protocol")
else:
    print("unknown")
PY
}

recommendation_for() {
  case "$1" in
    backend_sync)
      printf '%s' "phase127_reduce_or_batch_durable_sync"
      ;;
    backend_write)
      printf '%s' "phase127_durable_backend_write_batching"
      ;;
    target_protocol)
      printf '%s' "phase127_nvme_target_copy_profile"
      ;;
    *)
      printf '%s' "phase127_deeper_write_path_trace"
      ;;
  esac
}

write_summary "phase126_block_nvme_tcp_backend_write_instrumentation_status=running"
write_summary "frontend_transport=tcp"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "instrumentation_surface=status_durable_write_profile"

SW_BLOCK_ARTIFACT_DIR="${PHASE125_DIR}" \
SW_BLOCK_PHASE125_SEQ_MIB="${SEQ_MIB}" \
SW_BLOCK_PHASE125_BLOCK_PVC_NAME="${SW_BLOCK_PHASE126_BLOCK_PVC_NAME:-sw-block-phase126-block-pvc}" \
SW_BLOCK_PHASE125_BLOCK_STORAGECLASS="${SW_BLOCK_PHASE126_BLOCK_STORAGECLASS:-sw-block-phase126-block}" \
SW_BLOCK_PHASE125_BLOCK_POD="${SW_BLOCK_PHASE126_BLOCK_POD:-sw-block-phase126-block-perf}" \
SW_BLOCK_PHASE125_LOCAL_PVC_NAME="${SW_BLOCK_PHASE126_LOCAL_PVC_NAME:-sw-block-phase126-local-pvc}" \
SW_BLOCK_PHASE125_LOCAL_POD="${SW_BLOCK_PHASE126_LOCAL_POD:-sw-block-phase126-local-perf}" \
  bash "${ROOT}/scripts/run-phase125-block-nvme-tcp-write-path-profile-gate.sh" "${ROOT}" \
  >"${ARTIFACT_DIR}/phase125-profile.stdout.txt" \
  2>"${ARTIFACT_DIR}/phase125-profile.stderr.txt"

PHASE125_SUMMARY="${PHASE125_DIR}/phase125-block-nvme-tcp-write-path-profile-summary.txt"
BLOCK_SUMMARY="${PHASE125_DIR}/block-profile/phase120-nvme-tcp-performance-baseline-summary.txt"
if [[ "$(require_summary_value "${PHASE125_SUMMARY}" phase125_block_nvme_tcp_write_path_profile_status)" != "ok" ]]; then
  echo "phase125 profile dependency did not finish ok" >&2
  exit 1
fi
if [[ "$(require_summary_value "${BLOCK_SUMMARY}" phase120_write_profile_status)" != "ok" ]]; then
  echo "phase120 write profile did not finish ok" >&2
  exit 1
fi

SEQ_BYTES=$((SEQ_MIB * 1024 * 1024))
TARGET_OBSERVED="$(require_summary_value "${BLOCK_SUMMARY}" target_write_observed)"
TARGET_OPS="$(require_summary_value "${BLOCK_SUMMARY}" target_write_ops)"
TARGET_BYTES="$(require_summary_value "${BLOCK_SUMMARY}" target_write_bytes)"
TARGET_MS="$(require_summary_value "${BLOCK_SUMMARY}" target_write_duration_ms)"
BACKEND_OPS="$(require_summary_value "${BLOCK_SUMMARY}" backend_write_ops)"
BACKEND_BYTES="$(require_summary_value "${BLOCK_SUMMARY}" backend_write_bytes)"
BACKEND_MS="$(require_summary_value "${BLOCK_SUMMARY}" backend_write_duration_ms)"
BACKEND_STORAGE_WRITE_CALLS="$(require_summary_value "${BLOCK_SUMMARY}" backend_storage_write_calls)"
BACKEND_STORAGE_WRITE_BLOCKS="$(require_summary_value "${BLOCK_SUMMARY}" backend_storage_write_blocks)"
BACKEND_STORAGE_BATCH_CALLS="$(require_summary_value "${BLOCK_SUMMARY}" backend_storage_batch_calls)"
BACKEND_STORAGE_BATCH_BLOCKS="$(require_summary_value "${BLOCK_SUMMARY}" backend_storage_batch_blocks)"
WAL_COPY_OPS="$(require_summary_value "${BLOCK_SUMMARY}" wal_copy_ops)"
WAL_COPY_BYTES="$(require_summary_value "${BLOCK_SUMMARY}" wal_copy_bytes)"
WAL_COPY_MS="$(require_summary_value "${BLOCK_SUMMARY}" wal_copy_duration_ms)"
WAL_ENCODE_OPS="$(require_summary_value "${BLOCK_SUMMARY}" wal_encode_ops)"
WAL_ENCODE_BYTES="$(require_summary_value "${BLOCK_SUMMARY}" wal_encode_bytes)"
WAL_ENCODE_MS="$(require_summary_value "${BLOCK_SUMMARY}" wal_encode_duration_ms)"
WAL_CHECKSUM_OPS="$(require_summary_value "${BLOCK_SUMMARY}" wal_checksum_ops)"
WAL_CHECKSUM_BYTES="$(require_summary_value "${BLOCK_SUMMARY}" wal_checksum_bytes)"
WAL_CHECKSUM_MS="$(require_summary_value "${BLOCK_SUMMARY}" wal_checksum_duration_ms)"
WAL_APPEND_OPS="$(require_summary_value "${BLOCK_SUMMARY}" wal_append_ops)"
WAL_APPEND_BYTES="$(require_summary_value "${BLOCK_SUMMARY}" wal_append_bytes)"
WAL_APPEND_MS="$(require_summary_value "${BLOCK_SUMMARY}" wal_append_duration_ms)"
DIRTY_MAP_UPDATE_OPS="$(require_summary_value "${BLOCK_SUMMARY}" dirty_map_update_ops)"
DIRTY_MAP_UPDATE_MS="$(require_summary_value "${BLOCK_SUMMARY}" dirty_map_update_duration_ms)"
SYNC_OPS="$(require_summary_value "${BLOCK_SUMMARY}" backend_sync_ops)"
SYNC_MS="$(require_summary_value "${BLOCK_SUMMARY}" backend_sync_duration_ms)"

if [[ "${TARGET_OBSERVED}" != "true" ]]; then
  echo "target_write_observed=${TARGET_OBSERVED}, want true" >&2
  exit 1
fi
assert_int_ge "${TARGET_OPS}" 1 "target_write_ops"
assert_int_ge "${BACKEND_OPS}" 1 "backend_write_ops"
assert_int_ge "${TARGET_BYTES}" "${SEQ_BYTES}" "target_write_bytes"
assert_int_ge "${BACKEND_BYTES}" "${SEQ_BYTES}" "backend_write_bytes"
assert_int_ge "${BACKEND_STORAGE_WRITE_CALLS}" 1 "backend_storage_write_calls"
assert_int_ge "${BACKEND_STORAGE_WRITE_BLOCKS}" 1 "backend_storage_write_blocks"
assert_int_ge "${WAL_COPY_OPS}" 1 "wal_copy_ops"
assert_int_ge "${WAL_ENCODE_OPS}" 1 "wal_encode_ops"
assert_int_ge "${WAL_CHECKSUM_OPS}" 1 "wal_checksum_ops"
assert_int_ge "${WAL_APPEND_OPS}" 1 "wal_append_ops"
assert_int_ge "${DIRTY_MAP_UPDATE_OPS}" 1 "dirty_map_update_ops"

WRITE_OBSERVATION="$(classify_write_path "${TARGET_MS}" "${BACKEND_MS}" "${SYNC_MS}" "${SYNC_OPS}")"
NEXT_RECOMMENDATION="$(recommendation_for "${WRITE_OBSERVATION}")"

write_summary "network_baseline_mibps=$(require_summary_value "${PHASE125_SUMMARY}" network_baseline_mibps)"
write_summary "block_nvme_seq_write_mibps=$(require_summary_value "${PHASE125_SUMMARY}" block_nvme_seq_write_mibps)"
write_summary "block_nvme_seq_read_mibps=$(require_summary_value "${PHASE125_SUMMARY}" block_nvme_seq_read_mibps)"
write_summary "local_path_seq_write_mibps=$(require_summary_value "${PHASE125_SUMMARY}" local_path_seq_write_mibps)"
write_summary "local_path_seq_read_mibps=$(require_summary_value "${PHASE125_SUMMARY}" local_path_seq_read_mibps)"
write_summary "block_vs_local_write_ratio=$(require_summary_value "${PHASE125_SUMMARY}" block_vs_local_write_ratio)"
write_summary "block_vs_local_read_ratio=$(require_summary_value "${PHASE125_SUMMARY}" block_vs_local_read_ratio)"
write_summary "target_write_observed=${TARGET_OBSERVED}"
write_summary "target_write_bytes=${TARGET_BYTES}"
write_summary "target_write_ops=${TARGET_OPS}"
write_summary "target_write_duration_ms=${TARGET_MS}"
write_summary "backend_write_bytes=${BACKEND_BYTES}"
write_summary "backend_write_ops=${BACKEND_OPS}"
write_summary "backend_write_duration_ms=${BACKEND_MS}"
write_summary "backend_storage_write_calls=${BACKEND_STORAGE_WRITE_CALLS}"
write_summary "backend_storage_write_blocks=${BACKEND_STORAGE_WRITE_BLOCKS}"
write_summary "backend_storage_batch_calls=${BACKEND_STORAGE_BATCH_CALLS}"
write_summary "backend_storage_batch_blocks=${BACKEND_STORAGE_BATCH_BLOCKS}"
write_summary "wal_copy_ops=${WAL_COPY_OPS}"
write_summary "wal_copy_bytes=${WAL_COPY_BYTES}"
write_summary "wal_copy_duration_ms=${WAL_COPY_MS}"
write_summary "wal_encode_ops=${WAL_ENCODE_OPS}"
write_summary "wal_encode_bytes=${WAL_ENCODE_BYTES}"
write_summary "wal_encode_duration_ms=${WAL_ENCODE_MS}"
write_summary "wal_checksum_ops=${WAL_CHECKSUM_OPS}"
write_summary "wal_checksum_bytes=${WAL_CHECKSUM_BYTES}"
write_summary "wal_checksum_duration_ms=${WAL_CHECKSUM_MS}"
write_summary "wal_append_ops=${WAL_APPEND_OPS}"
write_summary "wal_append_bytes=${WAL_APPEND_BYTES}"
write_summary "wal_append_duration_ms=${WAL_APPEND_MS}"
write_summary "dirty_map_update_ops=${DIRTY_MAP_UPDATE_OPS}"
write_summary "dirty_map_update_duration_ms=${DIRTY_MAP_UPDATE_MS}"
write_summary "backend_sync_ops=${SYNC_OPS}"
write_summary "backend_sync_duration_ms=${SYNC_MS}"
write_summary "write_path_observation=${WRITE_OBSERVATION}"
write_summary "top_bottleneck=${WRITE_OBSERVATION}"
write_summary "next_recommendation=${NEXT_RECOMMENDATION}"
write_summary "cleanup_status=$(require_summary_value "${PHASE125_SUMMARY}" cleanup_status)"
write_summary "phase126_block_nvme_tcp_backend_write_instrumentation_status=ok"
