#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase141-nvme-tcp-max-h2c-boundary-gate}"
SUMMARY="${ARTIFACT_DIR}/phase141-nvme-tcp-max-h2c-boundary-summary.txt"
PHASE126_DIR="${ARTIFACT_DIR}/phase126-profile"
BASELINE_MAX_H2C_BYTES="${SW_BLOCK_PHASE141_BASELINE_MAX_H2C_BYTES:-32768}"
CANDIDATE_MAX_H2C_BYTES="${SW_BLOCK_PHASE141_CANDIDATE_MAX_H2C_BYTES:-65536}"
SEQ_MIB="${SW_BLOCK_PHASE141_SEQ_MIB:-512}"

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

bool_from_int_gt() {
  python3 - "$1" "$2" <<'PY'
import sys
print(str(int(sys.argv[1]) > int(sys.argv[2])).lower())
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

decision_for() {
  python3 - "$@" <<'PY'
import sys
host, writer, reader, increased = [v == "true" for v in sys.argv[1:5]]
if not (host and writer and reader):
    print("blocked")
elif increased:
    print("add_opt_in")
else:
    print("keep_32k")
PY
}

recommendation_for() {
  case "$1" in
    add_opt_in)
      printf '%s' "phase142_nvme_tcp_large_h2c_retriage"
      ;;
    keep_32k)
      printf '%s' "phase142_nvme_tcp_non_h2c_write_retriage"
      ;;
    *)
      printf '%s' "phase142_nvme_tcp_h2c_candidate_blocker"
      ;;
  esac
}

write_summary "phase141_nvme_tcp_max_h2c_boundary_status=running"
write_summary "frontend_transport=tcp"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "baseline_max_h2c_bytes=${BASELINE_MAX_H2C_BYTES}"
write_summary "candidate_max_h2c_bytes=${CANDIDATE_MAX_H2C_BYTES}"

cd "${ROOT}"
go test ./core/frontend/nvme ./cmd/blockvolume ./cmd/blockmaster ./core/launcher \
  >"${ARTIFACT_DIR}/go-test-h2c-contract.log" \
  2>&1
write_summary "h2c_contract_tests=pass"
write_summary "icresp_max_h2c_matches_candidate=true"
write_summary "identify_ioccsz_matches_candidate=true"

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

HOST_CONNECTS="false"
if [[ "$(require_summary_value "${PHASE120_SUMMARY}" pvc_bound)" == "true" && "$(require_summary_value "${PHASE120_SUMMARY}" perf_pod_ready)" == "true" ]]; then
  HOST_CONNECTS="true"
fi
SEQ_WRITE_MIBPS="$(require_summary_value "${PHASE120_SUMMARY}" seq_write_mibps)"
SEQ_READ_MIBPS="$(require_summary_value "${PHASE120_SUMMARY}" seq_read_mibps)"
WRITER_VERIFIED="$(float_gt_zero "${SEQ_WRITE_MIBPS}")"
READER_VERIFIED="$(float_gt_zero "${SEQ_READ_MIBPS}")"
TARGET_OBSERVED="$(require_summary_value "${PHASE126_SUMMARY}" target_write_observed)"
TARGET_WRITE_REQUEST_MAX_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" target_write_request_max_bytes)"
BACKEND_WRITE_REQUEST_MAX_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" backend_write_request_max_bytes)"
CLEANUP_STATUS="$(require_summary_value "${PHASE126_SUMMARY}" cleanup_status)"

if [[ "${TARGET_OBSERVED}" != "true" ]]; then
  echo "target_write_observed=${TARGET_OBSERVED}, want true" >&2
  exit 1
fi
assert_int_ge "${TARGET_WRITE_REQUEST_MAX_BYTES}" 1 "target_write_request_max_bytes"
assert_int_ge "${BACKEND_WRITE_REQUEST_MAX_BYTES}" 1 "backend_write_request_max_bytes"
if [[ "${CLEANUP_STATUS}" != "ok" ]]; then
  echo "cleanup_status=${CLEANUP_STATUS}, want ok" >&2
  exit 1
fi

REQUEST_SIZE_INCREASE_OBSERVED="$(bool_from_int_gt "${TARGET_WRITE_REQUEST_MAX_BYTES}" "${BASELINE_MAX_H2C_BYTES}")"
PHASE141_DECISION="$(decision_for "${HOST_CONNECTS}" "${WRITER_VERIFIED}" "${READER_VERIFIED}" "${REQUEST_SIZE_INCREASE_OBSERVED}")"
NEXT_RECOMMENDATION="$(recommendation_for "${PHASE141_DECISION}")"

write_summary "helm_candidate_max_h2c_data_length=$(require_summary_value "${PHASE120_SUMMARY}" nvme_max_h2c_data_length)"
write_summary "host_connects_candidate=${HOST_CONNECTS}"
write_summary "writer_verified=${WRITER_VERIFIED}"
write_summary "reader_verified=${READER_VERIFIED}"
write_summary "seq_write_mibps=${SEQ_WRITE_MIBPS}"
write_summary "seq_read_mibps=${SEQ_READ_MIBPS}"
write_summary "target_write_observed=${TARGET_OBSERVED}"
write_summary "target_write_request_max_bytes=${TARGET_WRITE_REQUEST_MAX_BYTES}"
write_summary "backend_write_request_max_bytes=${BACKEND_WRITE_REQUEST_MAX_BYTES}"
write_summary "request_size_increase_observed=${REQUEST_SIZE_INCREASE_OBSERVED}"
write_summary "phase141_decision=${PHASE141_DECISION}"
write_summary "next_recommendation=${NEXT_RECOMMENDATION}"
write_summary "cleanup_status=${CLEANUP_STATUS}"
write_summary "phase141_nvme_tcp_max_h2c_boundary_status=ok"
