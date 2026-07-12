#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase140-frontend-request-size-profile-gate}"
SUMMARY="${ARTIFACT_DIR}/phase140-frontend-request-size-profile-summary.txt"
PHASE126_DIR="${ARTIFACT_DIR}/phase126-profile"
SEQ_MIB="${SW_BLOCK_PHASE140_SEQ_MIB:-512}"

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

extract_nvme_tcp_limit() {
  python3 - "${ROOT}/core/frontend/nvme/session.go" "${ROOT}/core/frontend/nvme/identify.go" <<'PY'
import re
import sys
session = open(sys.argv[1], encoding="utf-8").read()
identify = open(sys.argv[2], encoding="utf-8").read()
def parse_num(text):
    return int(text, 16) if text.lower().startswith("0x") else int(text)
session_match = re.search(r"MaxH2CDataLength:\s*(0x[0-9a-fA-F]+|\d+)", session)
identify_match = re.search(r"const\s+maxH2CDataLen\s+uint32\s*=\s*(0x[0-9a-fA-F]+|\d+)", identify)
if not session_match:
    raise SystemExit("session MaxH2CDataLength constant not found")
if not identify_match:
    raise SystemExit("identify maxH2CDataLen constant not found")
session_value = parse_num(session_match.group(1))
identify_value = parse_num(identify_match.group(1))
if session_value != identify_value:
    raise SystemExit(f"MaxH2CDataLength mismatch session={session_value} identify={identify_value}")
ioccsz = (64 + session_value) // 16
print(f"nvme_tcp_max_h2c_data_length_bytes={session_value}")
print(f"nvme_tcp_ioccsz_units={ioccsz}")
PY
}

classify_frontend_request_size() {
  python3 - "$@" <<'PY'
import sys
target_max = int(sys.argv[1])
backend_max = int(sys.argv[2])
max_h2c = int(sys.argv[3])
batch_max = int(sys.argv[4])
if target_max <= 0 or backend_max <= 0 or max_h2c <= 0:
    owner = "unknown"
elif backend_max < target_max:
    owner = "backend_limit"
elif target_max == max_h2c and backend_max == target_max:
    owner = "target_limit"
elif target_max < max_h2c and backend_max == target_max:
    owner = "host_nvme"
else:
    owner = "unknown"
shape = {
    "target_limit": "target_limited",
    "backend_limit": "backend_limited",
    "host_nvme": "host_limited",
}.get(owner, "unknown")
if owner == "target_limit" and target_max > 32768 and batch_max > 8:
    shape = "tunable"
print(owner)
print(shape)
PY
}

recommendation_for_shape() {
  case "$1" in
    target_limited)
      printf '%s' "phase141_nvme_tcp_max_h2c_boundary"
      ;;
    host_limited)
      printf '%s' "phase141_host_nvme_request_size_experiment"
      ;;
    backend_limited)
      printf '%s' "phase141_backend_request_batch_boundary"
      ;;
    tunable)
      printf '%s' "phase141_request_size_regression_retriage"
      ;;
    *)
      printf '%s' "phase141_frontend_request_size_deeper_trace"
      ;;
  esac
}

write_summary "phase140_frontend_request_size_profile_status=running"
write_summary "frontend_transport=tcp"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "profile_size_mib=${SEQ_MIB}"

cd "${ROOT}"
go test ./core/frontend/durable \
  >"${ARTIFACT_DIR}/go-test-durable.log" \
  2>&1
write_summary "unit_target_request_profile_passed=true"

extract_nvme_tcp_limit >"${ARTIFACT_DIR}/nvme-tcp-limit-summary.txt"
cat "${ARTIFACT_DIR}/nvme-tcp-limit-summary.txt" >>"${SUMMARY}"
NVME_TCP_MAX_H2C_DATA_LENGTH_BYTES="$(require_summary_value "${ARTIFACT_DIR}/nvme-tcp-limit-summary.txt" nvme_tcp_max_h2c_data_length_bytes)"
NVME_TCP_IOCCSZ_UNITS="$(require_summary_value "${ARTIFACT_DIR}/nvme-tcp-limit-summary.txt" nvme_tcp_ioccsz_units)"
assert_int_ge "${NVME_TCP_MAX_H2C_DATA_LENGTH_BYTES}" 1 "nvme_tcp_max_h2c_data_length_bytes"
assert_int_ge "${NVME_TCP_IOCCSZ_UNITS}" 1 "nvme_tcp_ioccsz_units"

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
TARGET_WRITE_REQUEST_MAX_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" target_write_request_max_bytes)"
TARGET_WRITE_REQUEST_AVG_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" target_write_request_avg_bytes)"
BACKEND_WRITE_REQUEST_MAX_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" backend_write_request_max_bytes)"
BACKEND_WRITE_REQUEST_AVG_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" backend_write_request_avg_bytes)"
BACKEND_STORAGE_BATCH_CALLS="$(require_summary_value "${PHASE126_SUMMARY}" backend_storage_batch_calls)"
BACKEND_FULL_BLOCK_BATCH_MAX="$(require_summary_value "${PHASE126_SUMMARY}" backend_full_block_batch_max)"
BACKEND_FULL_BLOCK_BATCH_AVG="$(require_summary_value "${PHASE126_SUMMARY}" backend_full_block_batch_avg)"
CLEANUP_STATUS="$(require_summary_value "${PHASE126_SUMMARY}" cleanup_status)"

if [[ "${TARGET_OBSERVED}" != "true" ]]; then
  echo "target_write_observed=${TARGET_OBSERVED}, want true" >&2
  exit 1
fi
assert_int_ge "${TARGET_WRITE_REQUEST_MAX_BYTES}" 1 "target_write_request_max_bytes"
assert_int_ge "${TARGET_WRITE_REQUEST_AVG_BYTES}" 1 "target_write_request_avg_bytes"
assert_int_ge "${BACKEND_WRITE_REQUEST_MAX_BYTES}" 1 "backend_write_request_max_bytes"
assert_int_ge "${BACKEND_WRITE_REQUEST_AVG_BYTES}" 1 "backend_write_request_avg_bytes"
assert_int_ge "${BACKEND_STORAGE_BATCH_CALLS}" 1 "backend_storage_batch_calls"
assert_int_ge "${BACKEND_FULL_BLOCK_BATCH_MAX}" 1 "backend_full_block_batch_max"
if [[ "${CLEANUP_STATUS}" != "ok" ]]; then
  echo "cleanup_status=${CLEANUP_STATUS}, want ok" >&2
  exit 1
fi

mapfile -t CLASSIFICATION < <(classify_frontend_request_size \
  "${TARGET_WRITE_REQUEST_MAX_BYTES}" \
  "${BACKEND_WRITE_REQUEST_MAX_BYTES}" \
  "${NVME_TCP_MAX_H2C_DATA_LENGTH_BYTES}" \
  "${BACKEND_FULL_BLOCK_BATCH_MAX}")
FRONTEND_REQUEST_SIZE_OWNER="${CLASSIFICATION[0]}"
PHASE140_SHAPE_RESULT="${CLASSIFICATION[1]}"
POST_PHASE140_BOTTLENECK="frontend_request_size"
if [[ "${PHASE140_SHAPE_RESULT}" == "backend_limited" ]]; then
  POST_PHASE140_BOTTLENECK="wal_append_small_writes"
elif [[ "${PHASE140_SHAPE_RESULT}" == "unknown" ]]; then
  POST_PHASE140_BOTTLENECK="unknown"
fi
NEXT_RECOMMENDATION="$(recommendation_for_shape "${PHASE140_SHAPE_RESULT}")"

for key in \
  target_write_observed \
  target_write_bytes \
  target_write_ops \
  target_write_request_max_bytes \
  target_write_request_avg_bytes \
  backend_write_bytes \
  backend_write_ops \
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
  wal_append_writeat_max_bytes \
  wal_append_writeat_avg_bytes; do
  write_summary "${key}=$(require_summary_value "${PHASE126_SUMMARY}" "${key}")"
done

write_summary "backend_storage_batching_effective=true"
write_summary "frontend_request_size_owner=${FRONTEND_REQUEST_SIZE_OWNER}"
write_summary "phase140_shape_result=${PHASE140_SHAPE_RESULT}"
write_summary "post_phase140_bottleneck=${POST_PHASE140_BOTTLENECK}"
write_summary "next_recommendation=${NEXT_RECOMMENDATION}"
write_summary "cleanup_status=${CLEANUP_STATUS}"
write_summary "phase140_frontend_request_size_profile_status=ok"
