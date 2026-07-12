#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase142-nvme-tcp-large-h2c-retriage-gate}"
SUMMARY="${ARTIFACT_DIR}/phase142-nvme-tcp-large-h2c-retriage-summary.txt"
PHASE126_DIR="${ARTIFACT_DIR}/phase126-profile"
CANDIDATE_MAX_H2C_BYTES="${SW_BLOCK_PHASE142_CANDIDATE_MAX_H2C_BYTES:-65536}"
SEQ_MIB="${SW_BLOCK_PHASE142_SEQ_MIB:-512}"

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

classify_bottleneck() {
  python3 - "$@" <<'PY'
import sys

candidate = int(sys.argv[1])
target_max = int(sys.argv[2])
backend_max = int(sys.argv[3])
costs = {
    "wal_copy": int(sys.argv[4]),
    "wal_append": int(sys.argv[5]),
    "wal_encode": int(sys.argv[6]),
    "wal_checksum": int(sys.argv[7]),
    "dirty_map": int(sys.argv[8]),
}

if target_max < candidate or backend_max < candidate:
    print("frontend_request_size")
    raise SystemExit(0)

name, value = max(costs.items(), key=lambda item: item[1])
print(name if value > 0 else "unknown")
PY
}

decision_for() {
  python3 - "$@" <<'PY'
import sys
host, writer, reader, exact_h2c, cleanup = [v == "true" for v in sys.argv[1:6]]
bottleneck = sys.argv[6]
if not (host and writer and reader and exact_h2c and cleanup):
    print("blocked")
elif bottleneck in {"wal_copy", "wal_append", "wal_encode", "wal_checksum", "dirty_map", "frontend_request_size"}:
    print("continue_backend_work")
elif bottleneck == "unknown":
    print("broader_compat_gate")
else:
    print("blocked")
PY
}

recommendation_for() {
  case "$1" in
    wal_copy)
      printf '%s' "phase143_backend_copy_reduction"
      ;;
    wal_append)
      printf '%s' "phase143_wal_append_large_h2c_profile"
      ;;
    wal_encode)
      printf '%s' "phase143_wal_encode_reduction"
      ;;
    wal_checksum)
      printf '%s' "phase143_wal_checksum_strategy"
      ;;
    dirty_map)
      printf '%s' "phase143_dirty_map_update_reduction"
      ;;
    frontend_request_size)
      printf '%s' "phase143_frontend_request_size_followup"
      ;;
    *)
      printf '%s' "phase143_deeper_large_h2c_trace"
      ;;
  esac
}

write_summary "phase142_nvme_tcp_large_h2c_retriage_status=running"
write_summary "frontend_transport=tcp"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "candidate_max_h2c_bytes=${CANDIDATE_MAX_H2C_BYTES}"

cd "${ROOT}"
go test ./core/frontend/nvme ./core/frontend/durable ./core/storage ./cmd/blockvolume ./cmd/blockmaster ./core/launcher \
  >"${ARTIFACT_DIR}/go-test-large-h2c-profile.log" \
  2>&1
write_summary "phase142_contract_tests=pass"

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
BACKEND_FULL_BLOCK_BATCH_MAX="$(require_summary_value "${PHASE126_SUMMARY}" backend_full_block_batch_max)"
WAL_COPY_MS="$(require_summary_value "${PHASE126_SUMMARY}" wal_copy_duration_ms)"
WAL_APPEND_WRITEAT_MAX_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_writeat_max_bytes)"
WAL_APPEND_MS="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_duration_ms)"
WAL_ENCODE_MS="$(require_summary_value "${PHASE126_SUMMARY}" wal_encode_duration_ms)"
WAL_CHECKSUM_MS="$(require_summary_value "${PHASE126_SUMMARY}" wal_checksum_duration_ms)"
DIRTY_MAP_UPDATE_MS="$(require_summary_value "${PHASE126_SUMMARY}" dirty_map_update_duration_ms)"
CLEANUP_STATUS="$(require_summary_value "${PHASE126_SUMMARY}" cleanup_status)"

if [[ "${TARGET_OBSERVED}" != "true" ]]; then
  echo "target_write_observed=${TARGET_OBSERVED}, want true" >&2
  exit 1
fi
assert_int_eq "${HELM_H2C}" "${CANDIDATE_MAX_H2C_BYTES}" "helm_candidate_max_h2c_data_length"
assert_int_eq "${TARGET_WRITE_REQUEST_MAX_BYTES}" "${CANDIDATE_MAX_H2C_BYTES}" "target_write_request_max_bytes"
assert_int_eq "${BACKEND_WRITE_REQUEST_MAX_BYTES}" "${CANDIDATE_MAX_H2C_BYTES}" "backend_write_request_max_bytes"
assert_int_ge "${BACKEND_FULL_BLOCK_BATCH_MAX}" 1 "backend_full_block_batch_max"
assert_int_ge "${WAL_APPEND_WRITEAT_MAX_BYTES}" 1 "wal_append_writeat_max_bytes"
if [[ "${CLEANUP_STATUS}" != "ok" ]]; then
  echo "cleanup_status=${CLEANUP_STATUS}, want ok" >&2
  exit 1
fi

EXACT_H2C=true
CLEANUP_OK=true
BOTTLENECK="$(classify_bottleneck \
  "${CANDIDATE_MAX_H2C_BYTES}" \
  "${TARGET_WRITE_REQUEST_MAX_BYTES}" \
  "${BACKEND_WRITE_REQUEST_MAX_BYTES}" \
  "${WAL_COPY_MS}" \
  "${WAL_APPEND_MS}" \
  "${WAL_ENCODE_MS}" \
  "${WAL_CHECKSUM_MS}" \
  "${DIRTY_MAP_UPDATE_MS}")"
PHASE142_DECISION="$(decision_for "${HOST_CONNECTS}" "${WRITER_VERIFIED}" "${READER_VERIFIED}" "${EXACT_H2C}" "${CLEANUP_OK}" "${BOTTLENECK}")"
NEXT_RECOMMENDATION="$(recommendation_for "${BOTTLENECK}")"

write_summary "helm_candidate_max_h2c_data_length=${HELM_H2C}"
write_summary "host_connects_candidate=${HOST_CONNECTS}"
write_summary "writer_verified=${WRITER_VERIFIED}"
write_summary "reader_verified=${READER_VERIFIED}"
write_summary "seq_write_mibps=${SEQ_WRITE_MIBPS}"
write_summary "seq_read_mibps=${SEQ_READ_MIBPS}"
write_summary "target_write_observed=${TARGET_OBSERVED}"
write_summary "target_write_request_max_bytes=${TARGET_WRITE_REQUEST_MAX_BYTES}"
write_summary "backend_write_request_max_bytes=${BACKEND_WRITE_REQUEST_MAX_BYTES}"
write_summary "backend_full_block_batch_max=${BACKEND_FULL_BLOCK_BATCH_MAX}"
write_summary "wal_copy_duration_ms=${WAL_COPY_MS}"
write_summary "wal_append_writeat_max_bytes=${WAL_APPEND_WRITEAT_MAX_BYTES}"
write_summary "wal_append_duration_ms=${WAL_APPEND_MS}"
write_summary "wal_encode_duration_ms=${WAL_ENCODE_MS}"
write_summary "wal_checksum_duration_ms=${WAL_CHECKSUM_MS}"
write_summary "dirty_map_update_duration_ms=${DIRTY_MAP_UPDATE_MS}"
write_summary "phase142_bottleneck=${BOTTLENECK}"
write_summary "phase142_decision=${PHASE142_DECISION}"
write_summary "next_recommendation=${NEXT_RECOMMENDATION}"
write_summary "cleanup_status=${CLEANUP_STATUS}"
write_summary "phase142_nvme_tcp_large_h2c_retriage_status=ok"
