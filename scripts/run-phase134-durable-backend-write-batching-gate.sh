#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase134-durable-backend-write-batching-gate}"
SUMMARY="${ARTIFACT_DIR}/phase134-durable-backend-write-batching-summary.txt"
PHASE126_DIR="${ARTIFACT_DIR}/phase126-profile"
SEQ_MIB="${SW_BLOCK_PHASE134_SEQ_MIB:-64}"

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

assert_int_lt() {
  local actual="$1"
  local want="$2"
  local label="$3"
  python3 - "$actual" "$want" "$label" <<'PY'
import sys
actual = int(sys.argv[1])
want = int(sys.argv[2])
label = sys.argv[3]
if actual >= want:
    raise SystemExit(f"{label}={actual}, want < {want}")
PY
}

write_summary "phase134_durable_backend_write_batching_status=running"
write_summary "frontend_transport=tcp"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "batch_scope=contiguous_full_block_writes"
write_summary "batch_max_blocks=64"

cd "${ROOT}"
go test ./core/frontend/durable ./core/storage/... \
  >"${ARTIFACT_DIR}/go-test-durable-storage.log" \
  2>&1
write_summary "unit_batch_regression_passed=true"
write_summary "strict_ack_batch_disabled=true"
write_summary "read_after_write_regression_passed=true"

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
TARGET_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" target_write_bytes)"
BACKEND_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" backend_write_bytes)"
BACKEND_OPS="$(require_summary_value "${PHASE126_SUMMARY}" backend_write_ops)"
BACKEND_MS="$(require_summary_value "${PHASE126_SUMMARY}" backend_write_duration_ms)"
BACKEND_STORAGE_WRITE_CALLS="$(require_summary_value "${PHASE126_SUMMARY}" backend_storage_write_calls)"
BACKEND_STORAGE_WRITE_BLOCKS="$(require_summary_value "${PHASE126_SUMMARY}" backend_storage_write_blocks)"
BACKEND_STORAGE_BATCH_CALLS="$(require_summary_value "${PHASE126_SUMMARY}" backend_storage_batch_calls)"
BACKEND_STORAGE_BATCH_BLOCKS="$(require_summary_value "${PHASE126_SUMMARY}" backend_storage_batch_blocks)"
SYNC_OPS="$(require_summary_value "${PHASE126_SUMMARY}" backend_sync_ops)"
CLEANUP_STATUS="$(require_summary_value "${PHASE126_SUMMARY}" cleanup_status)"

if [[ "${TARGET_OBSERVED}" != "true" ]]; then
  echo "target_write_observed=${TARGET_OBSERVED}, want true" >&2
  exit 1
fi
assert_int_ge "${TARGET_BYTES}" 1 "target_write_bytes"
assert_int_ge "${BACKEND_BYTES}" 1 "backend_write_bytes"
assert_int_ge "${BACKEND_OPS}" 1 "backend_write_ops"
assert_int_ge "${BACKEND_STORAGE_WRITE_CALLS}" 1 "backend_storage_write_calls"
assert_int_ge "${BACKEND_STORAGE_WRITE_BLOCKS}" 2 "backend_storage_write_blocks"
assert_int_ge "${BACKEND_STORAGE_BATCH_CALLS}" 1 "backend_storage_batch_calls"
assert_int_ge "${BACKEND_STORAGE_BATCH_BLOCKS}" 2 "backend_storage_batch_blocks"
assert_int_lt "${BACKEND_STORAGE_WRITE_CALLS}" "${BACKEND_STORAGE_WRITE_BLOCKS}" "backend_storage_write_calls"
if [[ "${CLEANUP_STATUS}" != "ok" ]]; then
  echo "cleanup_status=${CLEANUP_STATUS}, want ok" >&2
  exit 1
fi

write_summary "target_write_observed=${TARGET_OBSERVED}"
write_summary "target_write_bytes=${TARGET_BYTES}"
write_summary "backend_write_bytes=${BACKEND_BYTES}"
write_summary "backend_write_ops=${BACKEND_OPS}"
write_summary "backend_write_duration_ms=${BACKEND_MS}"
write_summary "backend_storage_write_calls=${BACKEND_STORAGE_WRITE_CALLS}"
write_summary "backend_storage_write_blocks=${BACKEND_STORAGE_WRITE_BLOCKS}"
write_summary "backend_storage_batch_calls=${BACKEND_STORAGE_BATCH_CALLS}"
write_summary "backend_storage_batch_blocks=${BACKEND_STORAGE_BATCH_BLOCKS}"
write_summary "backend_storage_batching_effective=true"
write_summary "backend_sync_ops=${SYNC_OPS}"
write_summary "cleanup_status=${CLEANUP_STATUS}"
write_summary "phase134_durable_backend_write_batching_status=ok"
