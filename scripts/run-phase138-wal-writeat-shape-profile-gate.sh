#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase138-wal-writeat-shape-profile-gate}"
SUMMARY="${ARTIFACT_DIR}/phase138-wal-writeat-shape-profile-summary.txt"
PHASE126_DIR="${ARTIFACT_DIR}/phase126-profile"
SEQ_MIB="${SW_BLOCK_PHASE138_SEQ_MIB:-512}"
PHASE137_WAL_APPEND_MS="${SW_BLOCK_PHASE137_WAL_APPEND_MS:-375}"

mkdir -p "${ARTIFACT_DIR}" "${PHASE126_DIR}"
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

classify_writeat_shape() {
  python3 - "$@" <<'PY'
import sys
calls = int(sys.argv[1])
avg_bytes = int(sys.argv[2])
wraps = int(sys.argv[3])
padding = int(sys.argv[4])
append_ms = int(sys.argv[5])
if append_ms <= 0:
    print("unknown")
elif wraps > 0 and padding > 0 and avg_bytes >= 65536:
    print("wal_wrap_padding")
elif avg_bytes > 0 and avg_bytes < 65536:
    print("wal_append_small_writes")
elif calls > 0:
    print("wal_append_syscall")
else:
    print("unknown")
PY
}

recommendation_for_shape() {
  case "$1" in
    wal_append_small_writes)
      printf '%s' "phase139_wal_append_batch_shape_coalescing"
      ;;
    wal_append_syscall)
      printf '%s' "phase139_wal_append_syscall_latency_profile"
      ;;
    wal_wrap_padding)
      printf '%s' "phase139_wal_wrap_padding_reduction"
      ;;
    wal_encode)
      printf '%s' "phase139_wal_encode_allocation_profile"
      ;;
    wal_checksum)
      printf '%s' "phase139_checksum_strategy_profile"
      ;;
    dirty_map)
      printf '%s' "phase139_dirty_map_update_profile"
      ;;
    *)
      printf '%s' "phase139_deeper_backend_trace"
      ;;
  esac
}

write_summary "phase138_wal_writeat_shape_profile_status=running"
write_summary "frontend_transport=tcp"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "profile_size_mib=${SEQ_MIB}"
write_summary "phase137_wal_append_duration_ms=${PHASE137_WAL_APPEND_MS}"

cd "${ROOT}"
go test ./core/storage ./core/frontend/durable \
  >"${ARTIFACT_DIR}/go-test-storage-durable.log" \
  2>&1
write_summary "unit_wal_writeat_shape_regression_passed=true"

SW_BLOCK_ARTIFACT_DIR="${PHASE126_DIR}" \
SW_BLOCK_PHASE126_SEQ_MIB="${SEQ_MIB}" \
  bash "${ROOT}/scripts/run-phase126-block-nvme-tcp-backend-write-instrumentation-gate.sh" "${ROOT}" \
  >"${ARTIFACT_DIR}/phase126-profile.stdout.txt" \
  2>"${ARTIFACT_DIR}/phase126-profile.stderr.txt"

PHASE126_SUMMARY="${PHASE126_DIR}/phase126-block-nvme-tcp-backend-write-instrumentation-summary.txt"
if [[ "$(require_summary_value "${PHASE126_SUMMARY}" phase126_block_nvme_tcp_backend_write_instrumentation_status)" != "ok" ]]; then
  echo "phase126 profile dependency did not finish ok" >&2
  exit 1
fi

TARGET_OBSERVED="$(require_summary_value "${PHASE126_SUMMARY}" target_write_observed)"
BACKEND_STORAGE_BATCH_CALLS="$(require_summary_value "${PHASE126_SUMMARY}" backend_storage_batch_calls)"
WAL_APPEND_OPS="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_ops)"
WAL_APPEND_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_bytes)"
WAL_APPEND_MS="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_duration_ms)"
WAL_APPEND_WRITEAT_CALLS="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_writeat_calls)"
WAL_APPEND_WRITEAT_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_writeat_bytes)"
WAL_APPEND_WRITEAT_MAX_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_writeat_max_bytes)"
WAL_APPEND_WRITEAT_AVG_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_writeat_avg_bytes)"
WAL_APPEND_WRAP_COUNT="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_wrap_count)"
WAL_APPEND_PADDING_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_padding_bytes)"
CLEANUP_STATUS="$(require_summary_value "${PHASE126_SUMMARY}" cleanup_status)"

if [[ "${TARGET_OBSERVED}" != "true" ]]; then
  echo "target_write_observed=${TARGET_OBSERVED}, want true" >&2
  exit 1
fi
assert_int_ge "${BACKEND_STORAGE_BATCH_CALLS}" 1 "backend_storage_batch_calls"
assert_int_ge "${WAL_APPEND_OPS}" 1 "wal_append_ops"
assert_int_ge "${WAL_APPEND_BYTES}" 1 "wal_append_bytes"
assert_int_ge "${WAL_APPEND_MS}" 1 "wal_append_duration_ms"
assert_int_ge "${WAL_APPEND_WRITEAT_CALLS}" 1 "wal_append_writeat_calls"
assert_int_ge "${WAL_APPEND_WRITEAT_BYTES}" 1 "wal_append_writeat_bytes"
assert_int_ge "${WAL_APPEND_WRITEAT_MAX_BYTES}" 1 "wal_append_writeat_max_bytes"
assert_int_ge "${WAL_APPEND_WRITEAT_AVG_BYTES}" 1 "wal_append_writeat_avg_bytes"
if [[ "${CLEANUP_STATUS}" != "ok" ]]; then
  echo "cleanup_status=${CLEANUP_STATUS}, want ok" >&2
  exit 1
fi

POST_PHASE138_BOTTLENECK="$(classify_writeat_shape "${WAL_APPEND_WRITEAT_CALLS}" "${WAL_APPEND_WRITEAT_AVG_BYTES}" "${WAL_APPEND_WRAP_COUNT}" "${WAL_APPEND_PADDING_BYTES}" "${WAL_APPEND_MS}")"
NEXT_RECOMMENDATION="$(recommendation_for_shape "${POST_PHASE138_BOTTLENECK}")"

for key in \
  target_write_observed \
  target_write_bytes \
  backend_write_bytes \
  backend_storage_batch_calls \
  backend_storage_batch_blocks \
  wal_copy_ops \
  wal_copy_bytes \
  wal_copy_duration_ms \
  wal_encode_ops \
  wal_encode_bytes \
  wal_encode_duration_ms \
  wal_checksum_ops \
  wal_checksum_bytes \
  wal_checksum_duration_ms \
  wal_append_ops \
  wal_append_bytes \
  wal_append_duration_ms \
  wal_append_writeat_calls \
  wal_append_writeat_bytes \
  wal_append_writeat_max_bytes \
  wal_append_writeat_avg_bytes \
  wal_append_wrap_count \
  wal_append_padding_bytes \
  dirty_map_update_ops \
  dirty_map_update_duration_ms; do
  write_summary "${key}=$(require_summary_value "${PHASE126_SUMMARY}" "${key}")"
done

write_summary "backend_storage_batching_effective=true"
write_summary "post_phase138_bottleneck=${POST_PHASE138_BOTTLENECK}"
write_summary "next_recommendation=${NEXT_RECOMMENDATION}"
write_summary "cleanup_status=${CLEANUP_STATUS}"
write_summary "phase138_wal_writeat_shape_profile_status=ok"
