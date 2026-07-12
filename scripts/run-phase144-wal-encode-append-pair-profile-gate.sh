#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase144-wal-encode-append-pair-profile-gate}"
SUMMARY="${ARTIFACT_DIR}/phase144-wal-encode-append-pair-profile-summary.txt"
PHASE126_DIR="${ARTIFACT_DIR}/phase126-profile"
CANDIDATE_MAX_H2C_BYTES="${SW_BLOCK_PHASE144_CANDIDATE_MAX_H2C_BYTES:-65536}"
SEQ_MIB="${SW_BLOCK_PHASE144_SEQ_MIB:-512}"

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

classify_pair_shape() {
  python3 - "$@" <<'PY'
import sys

encode_ms = int(sys.argv[1])
append_ms = int(sys.argv[2])
encode_ops = int(sys.argv[3])
append_ops = int(sys.argv[4])

if encode_ms <= 0 or append_ms <= 0 or encode_ops <= 0 or append_ops <= 0:
    print("instrumentation_gap")
elif encode_ms * 100 >= append_ms * 85 and encode_ms * 100 <= append_ms * 115:
    print("encode_append_tied")
elif encode_ms > append_ms:
    print("encode_dominant")
elif append_ms > encode_ms:
    print("append_dominant")
else:
    print("unknown")
PY
}

decision_for() {
  case "$1" in
    encode_dominant|append_dominant|encode_append_tied)
      printf '%s' "continue_backend_work"
      ;;
    instrumentation_gap)
      printf '%s' "add_instrumentation"
      ;;
    *)
      printf '%s' "blocked"
      ;;
  esac
}

recommendation_for() {
  case "$1" in
    encode_dominant)
      printf '%s' "phase145_wal_encode_copy_reduction"
      ;;
    append_dominant)
      printf '%s' "phase145_wal_append_write_path_reduction"
      ;;
    encode_append_tied)
      printf '%s' "phase145_wal_record_materialization_reduction"
      ;;
    instrumentation_gap)
      printf '%s' "phase145_wal_pair_instrumentation"
      ;;
    *)
      printf '%s' "phase145_backend_profile_blocker"
      ;;
  esac
}

write_summary "phase144_wal_encode_append_pair_profile_status=running"
write_summary "frontend_transport=tcp"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "candidate_max_h2c_bytes=${CANDIDATE_MAX_H2C_BYTES}"

cd "${ROOT}"
go test ./core/frontend/durable ./core/storage ./cmd/blockvolume ./cmd/blockmaster \
  >"${ARTIFACT_DIR}/go-test-wal-pair-profile.log" \
  2>&1
write_summary "phase144_contract_tests=pass"

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
TARGET_WRITE_REQUEST_MAX_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" target_write_request_max_bytes)"
BACKEND_WRITE_REQUEST_MAX_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" backend_write_request_max_bytes)"
WAL_ENCODE_OPS="$(require_summary_value "${PHASE126_SUMMARY}" wal_encode_ops)"
WAL_ENCODE_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" wal_encode_bytes)"
WAL_ENCODE_MS="$(require_summary_value "${PHASE126_SUMMARY}" wal_encode_duration_ms)"
WAL_APPEND_OPS="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_ops)"
WAL_APPEND_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_bytes)"
WAL_APPEND_MS="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_duration_ms)"
WAL_APPEND_WRITEAT_CALLS="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_writeat_calls)"
WAL_APPEND_WRITEAT_AVG_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_writeat_avg_bytes)"
CLEANUP_STATUS="$(require_summary_value "${PHASE126_SUMMARY}" cleanup_status)"

assert_int_eq "${HELM_H2C}" "${CANDIDATE_MAX_H2C_BYTES}" "helm_candidate_max_h2c_data_length"
assert_int_eq "${TARGET_WRITE_REQUEST_MAX_BYTES}" "${CANDIDATE_MAX_H2C_BYTES}" "target_write_request_max_bytes"
assert_int_eq "${BACKEND_WRITE_REQUEST_MAX_BYTES}" "${CANDIDATE_MAX_H2C_BYTES}" "backend_write_request_max_bytes"
assert_int_ge "${WAL_ENCODE_OPS}" 1 "wal_encode_ops"
assert_int_ge "${WAL_ENCODE_BYTES}" 1 "wal_encode_bytes"
assert_int_ge "${WAL_ENCODE_MS}" 1 "wal_encode_duration_ms"
assert_int_ge "${WAL_APPEND_OPS}" 1 "wal_append_ops"
assert_int_ge "${WAL_APPEND_BYTES}" 1 "wal_append_bytes"
assert_int_ge "${WAL_APPEND_MS}" 1 "wal_append_duration_ms"
assert_int_ge "${WAL_APPEND_WRITEAT_CALLS}" 1 "wal_append_writeat_calls"
assert_int_ge "${WAL_APPEND_WRITEAT_AVG_BYTES}" 1 "wal_append_writeat_avg_bytes"
if [[ "${CLEANUP_STATUS}" != "ok" ]]; then
  echo "cleanup_status=${CLEANUP_STATUS}, want ok" >&2
  exit 1
fi

PAIR_SHAPE="$(classify_pair_shape "${WAL_ENCODE_MS}" "${WAL_APPEND_MS}" "${WAL_ENCODE_OPS}" "${WAL_APPEND_OPS}")"
PHASE144_DECISION="$(decision_for "${PAIR_SHAPE}")"
NEXT_RECOMMENDATION="$(recommendation_for "${PAIR_SHAPE}")"

write_summary "helm_candidate_max_h2c_data_length=${HELM_H2C}"
write_summary "target_write_request_max_bytes=${TARGET_WRITE_REQUEST_MAX_BYTES}"
write_summary "backend_write_request_max_bytes=${BACKEND_WRITE_REQUEST_MAX_BYTES}"
write_summary "wal_encode_ops=${WAL_ENCODE_OPS}"
write_summary "wal_encode_bytes=${WAL_ENCODE_BYTES}"
write_summary "wal_encode_duration_ms=${WAL_ENCODE_MS}"
write_summary "wal_append_ops=${WAL_APPEND_OPS}"
write_summary "wal_append_bytes=${WAL_APPEND_BYTES}"
write_summary "wal_append_duration_ms=${WAL_APPEND_MS}"
write_summary "wal_append_writeat_calls=${WAL_APPEND_WRITEAT_CALLS}"
write_summary "wal_append_writeat_avg_bytes=${WAL_APPEND_WRITEAT_AVG_BYTES}"
write_summary "phase144_pair_shape=${PAIR_SHAPE}"
write_summary "phase144_decision=${PHASE144_DECISION}"
write_summary "next_recommendation=${NEXT_RECOMMENDATION}"
write_summary "cleanup_status=${CLEANUP_STATUS}"
write_summary "phase144_wal_encode_append_pair_profile_status=ok"
