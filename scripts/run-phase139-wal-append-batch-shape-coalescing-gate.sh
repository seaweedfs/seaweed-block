#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase139-wal-append-batch-shape-coalescing-gate}"
SUMMARY="${ARTIFACT_DIR}/phase139-wal-append-batch-shape-coalescing-summary.txt"
PHASE126_DIR="${ARTIFACT_DIR}/phase126-profile"
SEQ_MIB="${SW_BLOCK_PHASE139_SEQ_MIB:-512}"
PHASE138_WRITEAT_AVG_BYTES="${SW_BLOCK_PHASE138_WAL_APPEND_WRITEAT_AVG_BYTES:-33013}"
PHASE138_WRITEAT_MAX_BYTES="${SW_BLOCK_PHASE138_WAL_APPEND_WRITEAT_MAX_BYTES:-33072}"
PHASE138_WRITEAT_CALLS="${SW_BLOCK_PHASE138_WAL_APPEND_WRITEAT_CALLS:-17979}"

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

classify_shape_result() {
  python3 - "$@" <<'PY'
import sys
writeat_avg = int(sys.argv[1])
writeat_max = int(sys.argv[2])
writeat_calls = int(sys.argv[3])
phase138_avg = int(sys.argv[4])
phase138_max = int(sys.argv[5])
phase138_calls = int(sys.argv[6])
request_max = int(sys.argv[7])
batch_max = int(sys.argv[8])
if request_max <= 32768 and batch_max <= 8:
    print("frontend_request_limited")
elif writeat_avg > phase138_avg or writeat_max > phase138_max or writeat_calls < phase138_calls:
    print("improved")
else:
    print("blocked")
PY
}

classify_next_bottleneck() {
  python3 - "$@" <<'PY'
import sys
items = {
    "wal_copy": int(sys.argv[1]),
    "wal_encode": int(sys.argv[2]),
    "wal_checksum": int(sys.argv[3]),
    "wal_append_small_writes": int(sys.argv[4]),
    "dirty_map": int(sys.argv[5]),
}
name, value = max(items.items(), key=lambda kv: kv[1])
print(name if value > 0 else "unknown")
PY
}

recommendation_for_shape() {
  case "$1" in
    frontend_request_limited)
      printf '%s' "phase140_frontend_request_size_profile"
      ;;
    improved)
      printf '%s' "phase140_post_coalescing_retriage"
      ;;
    *)
      printf '%s' "phase140_wal_append_coalescing_design"
      ;;
  esac
}

write_summary "phase139_wal_append_batch_shape_coalescing_status=running"
write_summary "frontend_transport=tcp"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "profile_size_mib=${SEQ_MIB}"
write_summary "phase138_wal_append_writeat_avg_bytes=${PHASE138_WRITEAT_AVG_BYTES}"
write_summary "phase138_wal_append_writeat_max_bytes=${PHASE138_WRITEAT_MAX_BYTES}"
write_summary "phase138_wal_append_writeat_calls=${PHASE138_WRITEAT_CALLS}"

cd "${ROOT}"
go test ./core/storage ./core/frontend/durable \
  >"${ARTIFACT_DIR}/go-test-storage-durable.log" \
  2>&1
write_summary "unit_batch_shape_regression_passed=true"

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
BACKEND_WRITE_REQUEST_MAX_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" backend_write_request_max_bytes)"
BACKEND_WRITE_REQUEST_AVG_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" backend_write_request_avg_bytes)"
BACKEND_FULL_BLOCK_BATCH_MAX="$(require_summary_value "${PHASE126_SUMMARY}" backend_full_block_batch_max)"
BACKEND_FULL_BLOCK_BATCH_AVG="$(require_summary_value "${PHASE126_SUMMARY}" backend_full_block_batch_avg)"
WAL_COPY_MS="$(require_summary_value "${PHASE126_SUMMARY}" wal_copy_duration_ms)"
WAL_ENCODE_MS="$(require_summary_value "${PHASE126_SUMMARY}" wal_encode_duration_ms)"
WAL_CHECKSUM_MS="$(require_summary_value "${PHASE126_SUMMARY}" wal_checksum_duration_ms)"
WAL_APPEND_MS="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_duration_ms)"
WAL_APPEND_WRITEAT_CALLS="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_writeat_calls)"
WAL_APPEND_WRITEAT_AVG_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_writeat_avg_bytes)"
WAL_APPEND_WRITEAT_MAX_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_writeat_max_bytes)"
DIRTY_MAP_UPDATE_MS="$(require_summary_value "${PHASE126_SUMMARY}" dirty_map_update_duration_ms)"
CLEANUP_STATUS="$(require_summary_value "${PHASE126_SUMMARY}" cleanup_status)"

if [[ "${TARGET_OBSERVED}" != "true" ]]; then
  echo "target_write_observed=${TARGET_OBSERVED}, want true" >&2
  exit 1
fi
assert_int_ge "${BACKEND_STORAGE_BATCH_CALLS}" 1 "backend_storage_batch_calls"
assert_int_ge "${BACKEND_WRITE_REQUEST_MAX_BYTES}" 1 "backend_write_request_max_bytes"
assert_int_ge "${BACKEND_FULL_BLOCK_BATCH_MAX}" 1 "backend_full_block_batch_max"
assert_int_ge "${WAL_APPEND_WRITEAT_CALLS}" 1 "wal_append_writeat_calls"
assert_int_ge "${WAL_APPEND_WRITEAT_AVG_BYTES}" 1 "wal_append_writeat_avg_bytes"
assert_int_ge "${WAL_APPEND_WRITEAT_MAX_BYTES}" 1 "wal_append_writeat_max_bytes"
if [[ "${CLEANUP_STATUS}" != "ok" ]]; then
  echo "cleanup_status=${CLEANUP_STATUS}, want ok" >&2
  exit 1
fi

PHASE139_SHAPE_RESULT="$(classify_shape_result \
  "${WAL_APPEND_WRITEAT_AVG_BYTES}" \
  "${WAL_APPEND_WRITEAT_MAX_BYTES}" \
  "${WAL_APPEND_WRITEAT_CALLS}" \
  "${PHASE138_WRITEAT_AVG_BYTES}" \
  "${PHASE138_WRITEAT_MAX_BYTES}" \
  "${PHASE138_WRITEAT_CALLS}" \
  "${BACKEND_WRITE_REQUEST_MAX_BYTES}" \
  "${BACKEND_FULL_BLOCK_BATCH_MAX}")"
POST_PHASE139_BOTTLENECK="$(classify_next_bottleneck "${WAL_COPY_MS}" "${WAL_ENCODE_MS}" "${WAL_CHECKSUM_MS}" "${WAL_APPEND_MS}" "${DIRTY_MAP_UPDATE_MS}")"
NEXT_RECOMMENDATION="$(recommendation_for_shape "${PHASE139_SHAPE_RESULT}")"

for key in \
  target_write_observed \
  target_write_bytes \
  backend_write_bytes \
  backend_write_request_ops \
  backend_write_request_bytes \
  backend_write_request_max_bytes \
  backend_write_request_avg_bytes \
  backend_storage_batch_calls \
  backend_storage_batch_blocks \
  backend_full_block_batch_calls \
  backend_full_block_batch_blocks \
  backend_full_block_batch_max \
  backend_full_block_batch_avg \
  wal_append_ops \
  wal_append_bytes \
  wal_append_duration_ms \
  wal_append_writeat_calls \
  wal_append_writeat_bytes \
  wal_append_writeat_max_bytes \
  wal_append_writeat_avg_bytes \
  wal_copy_duration_ms \
  wal_encode_duration_ms \
  wal_checksum_duration_ms \
  dirty_map_update_duration_ms; do
  write_summary "${key}=$(require_summary_value "${PHASE126_SUMMARY}" "${key}")"
done

write_summary "backend_storage_batching_effective=true"
write_summary "phase139_shape_result=${PHASE139_SHAPE_RESULT}"
write_summary "post_phase139_bottleneck=${POST_PHASE139_BOTTLENECK}"
write_summary "next_recommendation=${NEXT_RECOMMENDATION}"
write_summary "cleanup_status=${CLEANUP_STATUS}"
write_summary "phase139_wal_append_batch_shape_coalescing_status=ok"
