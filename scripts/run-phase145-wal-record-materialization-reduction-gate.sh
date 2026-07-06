#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase145-wal-record-materialization-reduction-gate}"
SUMMARY="${ARTIFACT_DIR}/phase145-wal-record-materialization-reduction-summary.txt"
PHASE126_DIR="${ARTIFACT_DIR}/phase126-profile"
CANDIDATE_MAX_H2C_BYTES="${SW_BLOCK_PHASE145_CANDIDATE_MAX_H2C_BYTES:-65536}"
SEQ_MIB="${SW_BLOCK_PHASE145_SEQ_MIB:-512}"
MATERIALIZATION_CHANGE="${SW_BLOCK_PHASE145_MATERIALIZATION_CHANGE:-writebatch_value_entries}"

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

assert_int_eq() {
  local actual="$1"
  local want="$2"
  local label="$3"
  python3 - "$actual" "$want" "$label" <<'PY'
import sys
actual = int(sys.argv[1])
want = int(sys.argv[2])
label = sys.argv[3]
if actual != want:
    raise SystemExit(f"{label}={actual}, want {want}")
PY
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

float_gt_zero() {
  python3 - "$1" <<'PY'
import sys
try:
    v = float(sys.argv[1])
except ValueError:
    v = 0
print(str(v > 0).lower())
PY
}

write_summary "phase145_wal_record_materialization_reduction_status=running"
write_summary "frontend_transport=tcp"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "candidate_max_h2c_bytes=${CANDIDATE_MAX_H2C_BYTES}"
write_summary "wal_record_materialization_change=${MATERIALIZATION_CHANGE}"

cd "${ROOT}"
go test ./core/storage ./core/frontend/durable \
  >"${ARTIFACT_DIR}/go-test-record-compatibility.log" \
  2>&1
write_summary "unit_record_compatibility=pass"

SW_BLOCK_ARTIFACT_DIR="${PHASE126_DIR}" \
SW_BLOCK_PHASE126_SEQ_MIB="${SEQ_MIB}" \
SW_BLOCK_NVME_MAX_H2C_DATA_LENGTH="${CANDIDATE_MAX_H2C_BYTES}" \
  bash "${ROOT}/scripts/run-phase126-block-nvme-tcp-backend-write-instrumentation-gate.sh" "${ROOT}" \
  >"${ARTIFACT_DIR}/phase126-profile.stdout.txt" \
  2>"${ARTIFACT_DIR}/phase126-profile.stderr.txt"

PHASE126_SUMMARY="${PHASE126_DIR}/phase126-block-nvme-tcp-backend-write-instrumentation-summary.txt"
PHASE120_SUMMARY="${PHASE126_DIR}/phase125-profile/block-profile/phase120-nvme-tcp-performance-baseline-summary.txt"
if [[ "$(require_summary_value "${PHASE126_SUMMARY}" phase126_block_nvme_tcp_backend_write_instrumentation_status)" != "ok" ]]; then
  echo "phase126 profile dependency did not finish ok" >&2
  exit 1
fi
if [[ "$(require_summary_value "${PHASE120_SUMMARY}" phase120_nvme_tcp_performance_baseline_status)" != "ok" ]]; then
  echo "phase120 candidate profile did not finish ok" >&2
  exit 1
fi

HELM_H2C="$(require_summary_value "${PHASE120_SUMMARY}" nvme_max_h2c_data_length)"
SEQ_WRITE_MIBPS="$(require_summary_value "${PHASE120_SUMMARY}" seq_write_mibps)"
SEQ_READ_MIBPS="$(require_summary_value "${PHASE120_SUMMARY}" seq_read_mibps)"
WRITER_VERIFIED="$(float_gt_zero "${SEQ_WRITE_MIBPS}")"
READER_VERIFIED="$(float_gt_zero "${SEQ_READ_MIBPS}")"
TARGET_WRITE_REQUEST_MAX_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" target_write_request_max_bytes)"
BACKEND_WRITE_REQUEST_MAX_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" backend_write_request_max_bytes)"
WAL_ENCODE_MS="$(require_summary_value "${PHASE126_SUMMARY}" wal_encode_duration_ms)"
WAL_APPEND_MS="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_duration_ms)"
CLEANUP_STATUS="$(require_summary_value "${PHASE126_SUMMARY}" cleanup_status)"

assert_int_eq "${HELM_H2C}" "${CANDIDATE_MAX_H2C_BYTES}" "helm_candidate_max_h2c_data_length"
assert_int_eq "${TARGET_WRITE_REQUEST_MAX_BYTES}" "${CANDIDATE_MAX_H2C_BYTES}" "target_write_request_max_bytes"
assert_int_eq "${BACKEND_WRITE_REQUEST_MAX_BYTES}" "${CANDIDATE_MAX_H2C_BYTES}" "backend_write_request_max_bytes"
assert_int_ge "${WAL_ENCODE_MS}" 1 "wal_encode_duration_ms"
assert_int_ge "${WAL_APPEND_MS}" 1 "wal_append_duration_ms"
if [[ "${WRITER_VERIFIED}" != "true" ]]; then
  echo "writer_verified=${WRITER_VERIFIED}, want true" >&2
  exit 1
fi
if [[ "${READER_VERIFIED}" != "true" ]]; then
  echo "reader_verified=${READER_VERIFIED}, want true" >&2
  exit 1
fi
if [[ "${CLEANUP_STATUS}" != "ok" ]]; then
  echo "cleanup_status=${CLEANUP_STATUS}, want ok" >&2
  exit 1
fi

write_summary "helm_candidate_max_h2c_data_length=${HELM_H2C}"
write_summary "target_write_request_max_bytes=${TARGET_WRITE_REQUEST_MAX_BYTES}"
write_summary "backend_write_request_max_bytes=${BACKEND_WRITE_REQUEST_MAX_BYTES}"
write_summary "wal_encode_duration_ms=${WAL_ENCODE_MS}"
write_summary "wal_append_duration_ms=${WAL_APPEND_MS}"
write_summary "writer_verified=${WRITER_VERIFIED}"
write_summary "reader_verified=${READER_VERIFIED}"
write_summary "phase145_decision=keep_change"
write_summary "next_recommendation=phase146_wal_record_materialization_effectiveness_profile"
write_summary "cleanup_status=${CLEANUP_STATUS}"
write_summary "phase145_wal_record_materialization_reduction_status=ok"
