#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase151-wal-multiblock-mounted-nvme-profile-gate}"
SUMMARY="${ARTIFACT_DIR}/phase151-wal-multiblock-mounted-nvme-profile-summary.txt"
PHASE126_DIR="${ARTIFACT_DIR}/phase126-profile"
CANDIDATE_MAX_H2C_BYTES="${SW_BLOCK_PHASE151_CANDIDATE_MAX_H2C_BYTES:-65536}"
SEQ_MIB="${SW_BLOCK_PHASE151_SEQ_MIB:-512}"
EXTRA_VALUES_YAML=$'blockmaster:\n  durableWALMultiBlockRecords: true'

mkdir -p "${ARTIFACT_DIR}" "${PHASE126_DIR}" "${ARTIFACT_DIR}/helm"
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

write_summary "phase151_wal_multiblock_mounted_nvme_profile_status=running"
write_summary "frontend_transport=tcp"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "default_wal_format_unchanged=true"
write_summary "feature_gate_default=false"
write_summary "runtime_opt_in_name=durable-wal-multiblock-records"
write_summary "runtime_opt_in_enabled=true"
write_summary "candidate_max_h2c_bytes=${CANDIDATE_MAX_H2C_BYTES}"

cd "${ROOT}"
go test ./core/storage ./core/frontend/durable ./cmd/blockvolume ./cmd/blockmaster \
  >"${ARTIFACT_DIR}/go-test-compatibility.log" \
  2>&1
write_summary "unit_record_compatibility=pass"

helm template sw-block charts/seaweed-block --namespace kube-system \
  >"${ARTIFACT_DIR}/helm/default-template.yaml" \
  2>"${ARTIFACT_DIR}/helm/default-template.stderr.txt"
if grep -q -- '--launcher-durable-wal-multiblock-records' "${ARTIFACT_DIR}/helm/default-template.yaml"; then
  echo "default Helm render unexpectedly enables durable WAL multi-block records" >&2
  exit 1
fi
write_summary "helm_default_omits_opt_in=true"

helm template sw-block charts/seaweed-block --namespace kube-system \
  --set blockmaster.durableWALMultiBlockRecords=true \
  >"${ARTIFACT_DIR}/helm/explicit-template.yaml" \
  2>"${ARTIFACT_DIR}/helm/explicit-template.stderr.txt"
grep -q -- '--launcher-durable-wal-multiblock-records' "${ARTIFACT_DIR}/helm/explicit-template.yaml"
write_summary "helm_explicit_renders_opt_in=true"

SW_BLOCK_ARTIFACT_DIR="${PHASE126_DIR}" \
SW_BLOCK_PHASE120_EXTRA_VALUES_YAML="${EXTRA_VALUES_YAML}" \
SW_BLOCK_PHASE126_SEQ_MIB="${SEQ_MIB}" \
SW_BLOCK_NVME_MAX_H2C_DATA_LENGTH="${CANDIDATE_MAX_H2C_BYTES}" \
  bash "${ROOT}/scripts/run-phase126-block-nvme-tcp-backend-write-instrumentation-gate.sh" "${ROOT}" \
  >"${ARTIFACT_DIR}/phase126-profile.stdout.txt" \
  2>"${ARTIFACT_DIR}/phase126-profile.stderr.txt"

PHASE126_SUMMARY="${PHASE126_DIR}/phase126-block-nvme-tcp-backend-write-instrumentation-summary.txt"
PHASE120_SUMMARY="${PHASE126_DIR}/phase125-profile/block-profile/phase120-nvme-tcp-performance-baseline-summary.txt"
PHASE120_TEMPLATE="${PHASE126_DIR}/phase125-profile/block-profile/install/helm-template.yaml"
if [[ "$(require_summary_value "${PHASE126_SUMMARY}" phase126_block_nvme_tcp_backend_write_instrumentation_status)" != "ok" ]]; then
  echo "phase126 profile dependency did not finish ok" >&2
  exit 1
fi
if [[ "$(require_summary_value "${PHASE120_SUMMARY}" phase120_nvme_tcp_performance_baseline_status)" != "ok" ]]; then
  echo "phase120 candidate profile did not finish ok" >&2
  exit 1
fi

HELM_EXTRA_VALUES="$(require_summary_value "${PHASE120_SUMMARY}" helm_extra_values)"
HELM_H2C="$(require_summary_value "${PHASE120_SUMMARY}" nvme_max_h2c_data_length)"
SEQ_WRITE_MIBPS="$(require_summary_value "${PHASE120_SUMMARY}" seq_write_mibps)"
SEQ_READ_MIBPS="$(require_summary_value "${PHASE120_SUMMARY}" seq_read_mibps)"
WRITER_VERIFIED="$(float_gt_zero "${SEQ_WRITE_MIBPS}")"
READER_VERIFIED="$(float_gt_zero "${SEQ_READ_MIBPS}")"
TARGET_WRITE_REQUEST_MAX_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" target_write_request_max_bytes)"
BACKEND_WRITE_REQUEST_MAX_BYTES="$(require_summary_value "${PHASE126_SUMMARY}" backend_write_request_max_bytes)"
WAL_ENCODE_OPS="$(require_summary_value "${PHASE126_SUMMARY}" wal_encode_ops)"
WAL_APPEND_OPS="$(require_summary_value "${PHASE126_SUMMARY}" wal_append_ops)"
BACKEND_STORAGE_WRITE_CALLS="$(require_summary_value "${PHASE126_SUMMARY}" backend_storage_write_calls)"
BACKEND_STORAGE_WRITE_BLOCKS="$(require_summary_value "${PHASE126_SUMMARY}" backend_storage_write_blocks)"
BACKEND_STORAGE_BATCH_CALLS="$(require_summary_value "${PHASE126_SUMMARY}" backend_storage_batch_calls)"
BACKEND_FULL_BLOCK_BATCH_BLOCKS="$(require_summary_value "${PHASE126_SUMMARY}" backend_full_block_batch_blocks)"
CLEANUP_STATUS="$(require_summary_value "${PHASE126_SUMMARY}" cleanup_status)"

if [[ "${HELM_EXTRA_VALUES}" != "true" ]]; then
  echo "helm_extra_values=${HELM_EXTRA_VALUES}, want true" >&2
  exit 1
fi
grep -q -- '--launcher-durable-wal-multiblock-records' "${PHASE120_TEMPLATE}"
assert_int_eq "${HELM_H2C}" "${CANDIDATE_MAX_H2C_BYTES}" "helm_candidate_max_h2c_data_length"
assert_int_eq "${TARGET_WRITE_REQUEST_MAX_BYTES}" "${CANDIDATE_MAX_H2C_BYTES}" "target_write_request_max_bytes"
assert_int_eq "${BACKEND_WRITE_REQUEST_MAX_BYTES}" "${CANDIDATE_MAX_H2C_BYTES}" "backend_write_request_max_bytes"
assert_int_ge "${WAL_ENCODE_OPS}" 1 "wal_encode_ops"
assert_int_ge "${WAL_APPEND_OPS}" 1 "wal_append_ops"
assert_int_ge "${BACKEND_STORAGE_BATCH_CALLS}" 1 "backend_storage_batch_calls"
assert_int_ge "${BACKEND_FULL_BLOCK_BATCH_BLOCKS}" 1 "backend_full_block_batch_blocks"
python3 - "$WAL_ENCODE_OPS" "$BACKEND_STORAGE_WRITE_CALLS" "$BACKEND_STORAGE_WRITE_BLOCKS" <<'PY'
import sys
wal_encode_ops = int(sys.argv[1])
storage_write_calls = int(sys.argv[2])
storage_write_blocks = int(sys.argv[3])
if wal_encode_ops != storage_write_calls:
    raise SystemExit(f"wal_encode_ops={wal_encode_ops}, want storage write call count {storage_write_calls}")
if wal_encode_ops >= storage_write_blocks:
    raise SystemExit(f"wal_encode_ops={wal_encode_ops}, want less than storage write block count {storage_write_blocks}")
PY
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

write_summary "mounted_helm_extra_values=true"
write_summary "mounted_helm_renders_opt_in=true"
write_summary "helm_candidate_max_h2c_data_length=${HELM_H2C}"
write_summary "target_write_request_max_bytes=${TARGET_WRITE_REQUEST_MAX_BYTES}"
write_summary "backend_write_request_max_bytes=${BACKEND_WRITE_REQUEST_MAX_BYTES}"
write_summary "wal_encode_ops=${WAL_ENCODE_OPS}"
write_summary "wal_append_ops=${WAL_APPEND_OPS}"
write_summary "backend_storage_write_calls=${BACKEND_STORAGE_WRITE_CALLS}"
write_summary "backend_storage_write_blocks=${BACKEND_STORAGE_WRITE_BLOCKS}"
write_summary "backend_storage_batch_calls=${BACKEND_STORAGE_BATCH_CALLS}"
write_summary "backend_full_block_batch_blocks=${BACKEND_FULL_BLOCK_BATCH_BLOCKS}"
write_summary "multiblock_record_shape_observed=true"
write_summary "seq_write_mibps=${SEQ_WRITE_MIBPS}"
write_summary "seq_read_mibps=${SEQ_READ_MIBPS}"
write_summary "writer_verified=${WRITER_VERIFIED}"
write_summary "reader_verified=${READER_VERIFIED}"
write_summary "cleanup_status=${CLEANUP_STATUS}"
write_summary "phase151_decision=keep_opt_in"
write_summary "next_recommendation=phase152_wal_multiblock_recovery_compatibility_gate"
write_summary "phase151_wal_multiblock_mounted_nvme_profile_status=ok"
