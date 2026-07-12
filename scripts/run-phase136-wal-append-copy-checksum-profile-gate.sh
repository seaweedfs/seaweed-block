#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase136-wal-append-copy-checksum-profile-gate}"
SUMMARY="${ARTIFACT_DIR}/phase136-wal-append-copy-checksum-profile-summary.txt"
PHASE126_DIR="${ARTIFACT_DIR}/phase126-profile"
SEQ_MIB="${SW_BLOCK_PHASE136_SEQ_MIB:-512}"

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

classify_backend_internal_cost() {
  python3 - "$@" <<'PY'
import sys
items = {
    "wal_copy": int(sys.argv[1]),
    "wal_encode": int(sys.argv[2]),
    "wal_checksum": int(sys.argv[3]),
    "wal_append": int(sys.argv[4]),
    "dirty_map": int(sys.argv[5]),
}
name, value = max(items.items(), key=lambda kv: kv[1])
print(name if value > 0 else "unknown")
PY
}

recommendation_for_internal_cost() {
  case "$1" in
    wal_copy)
      printf '%s' "phase137_reduce_backend_data_copy"
      ;;
    wal_encode)
      printf '%s' "phase137_reduce_wal_record_encode_copy"
      ;;
    wal_checksum)
      printf '%s' "phase137_checksum_strategy_profile"
      ;;
    wal_append)
      printf '%s' "phase137_wal_writeat_shape_profile"
      ;;
    dirty_map)
      printf '%s' "phase137_dirty_map_update_profile"
      ;;
    *)
      printf '%s' "phase137_deeper_backend_trace"
      ;;
  esac
}

write_summary "phase136_wal_append_copy_checksum_profile_status=running"
write_summary "frontend_transport=tcp"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "profile_size_mib=${SEQ_MIB}"

cd "${ROOT}"
go test ./core/frontend/durable ./core/storage/... \
  >"${ARTIFACT_DIR}/go-test-durable-storage.log" \
  2>&1
write_summary "unit_wal_profile_regression_passed=true"

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
WAL_COPY_OPS="$(require_summary_value "${PHASE126_SUMMARY}" wal_copy_ops)"
WAL_COPY_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" wal_copy_bytes)"
WAL_COPY_MS="$(require_summary_value "${PHASE126_SUMMARY}" wal_copy_duration_ms)"
WAL_ENCODE_OPS="$(require_summary_value "${PHASE126_SUMMARY}" wal_encode_ops)"
WAL_ENCODE_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" wal_encode_bytes)"
WAL_ENCODE_MS="$(require_summary_value "${PHASE126_SUMMARY}" wal_encode_duration_ms)"
WAL_CHECKSUM_OPS="$(require_summary_value "${PHASE126_SUMMARY}" wal_checksum_ops)"
WAL_CHECKSUM_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" wal_checksum_bytes)"
WAL_CHECKSUM_MS="$(require_summary_value "${PHASE126_SUMMARY}" wal_checksum_duration_ms)"
WAL_APPEND_OPS="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_ops)"
WAL_APPEND_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_bytes)"
WAL_APPEND_MS="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_duration_ms)"
DIRTY_MAP_UPDATE_OPS="$(require_summary_value "${PHASE126_SUMMARY}" dirty_map_update_ops)"
DIRTY_MAP_UPDATE_MS="$(require_summary_value "${PHASE126_SUMMARY}" dirty_map_update_duration_ms)"
CLEANUP_STATUS="$(require_summary_value "${PHASE126_SUMMARY}" cleanup_status)"

if [[ "${TARGET_OBSERVED}" != "true" ]]; then
  echo "target_write_observed=${TARGET_OBSERVED}, want true" >&2
  exit 1
fi
assert_int_ge "${BACKEND_STORAGE_BATCH_CALLS}" 1 "backend_storage_batch_calls"
assert_int_ge "${WAL_COPY_OPS}" 1 "wal_copy_ops"
assert_int_ge "${WAL_COPY_BYTES}" 1 "wal_copy_bytes"
assert_int_ge "${WAL_COPY_MS}" 1 "wal_copy_duration_ms"
assert_int_ge "${WAL_ENCODE_OPS}" 1 "wal_encode_ops"
assert_int_ge "${WAL_ENCODE_BYTES}" 1 "wal_encode_bytes"
assert_int_ge "${WAL_ENCODE_MS}" 1 "wal_encode_duration_ms"
assert_int_ge "${WAL_CHECKSUM_OPS}" 1 "wal_checksum_ops"
assert_int_ge "${WAL_CHECKSUM_BYTES}" 1 "wal_checksum_bytes"
assert_int_ge "${WAL_CHECKSUM_MS}" 1 "wal_checksum_duration_ms"
assert_int_ge "${WAL_APPEND_OPS}" 1 "wal_append_ops"
assert_int_ge "${WAL_APPEND_BYTES}" 1 "wal_append_bytes"
assert_int_ge "${WAL_APPEND_MS}" 1 "wal_append_duration_ms"
assert_int_ge "${DIRTY_MAP_UPDATE_OPS}" 1 "dirty_map_update_ops"
assert_int_ge "${DIRTY_MAP_UPDATE_MS}" 1 "dirty_map_update_duration_ms"
if [[ "${CLEANUP_STATUS}" != "ok" ]]; then
  echo "cleanup_status=${CLEANUP_STATUS}, want ok" >&2
  exit 1
fi

POST_PHASE136_BOTTLENECK="$(classify_backend_internal_cost "${WAL_COPY_MS}" "${WAL_ENCODE_MS}" "${WAL_CHECKSUM_MS}" "${WAL_APPEND_MS}" "${DIRTY_MAP_UPDATE_MS}")"
NEXT_RECOMMENDATION="$(recommendation_for_internal_cost "${POST_PHASE136_BOTTLENECK}")"

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
  dirty_map_update_ops \
  dirty_map_update_duration_ms; do
  write_summary "${key}=$(require_summary_value "${PHASE126_SUMMARY}" "${key}")"
done

write_summary "backend_storage_batching_effective=true"
write_summary "post_phase136_bottleneck=${POST_PHASE136_BOTTLENECK}"
write_summary "next_recommendation=${NEXT_RECOMMENDATION}"
write_summary "cleanup_status=${CLEANUP_STATUS}"
write_summary "phase136_wal_append_copy_checksum_profile_status=ok"
