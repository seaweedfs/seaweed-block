#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase135-nvme-tcp-post-batch-retriage-gate}"
SUMMARY="${ARTIFACT_DIR}/phase135-nvme-tcp-post-batch-retriage-summary.txt"
PHASE126_DIR="${ARTIFACT_DIR}/phase126-profile"
SEQ_MIB="${SW_BLOCK_PHASE135_SEQ_MIB:-512}"

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

classify_post_batch_recommendation() {
  case "$1" in
    backend_write)
      printf '%s' "phase136_wal_append_copy_checksum_profile"
      ;;
    backend_sync)
      printf '%s' "phase136_sync_flush_group_commit_profile"
      ;;
    target_protocol)
      printf '%s' "phase136_nvme_target_copy_queue_profile"
      ;;
    benchmark_shape)
      printf '%s' "phase136_benchmark_shape_control"
      ;;
    *)
      printf '%s' "phase136_deeper_write_path_trace"
      ;;
  esac
}

write_summary "phase135_nvme_tcp_post_batch_retriage_status=running"
write_summary "frontend_transport=tcp"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "profile_size_mib=${SEQ_MIB}"
if [[ "${SEQ_MIB}" == "512" ]]; then
  write_summary "profile_comparable_with_phase126=true"
else
  write_summary "profile_comparable_with_phase126=false"
fi

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
BACKEND_STORAGE_WRITE_CALLS="$(require_summary_value "${PHASE126_SUMMARY}" backend_storage_write_calls)"
BACKEND_STORAGE_WRITE_BLOCKS="$(require_summary_value "${PHASE126_SUMMARY}" backend_storage_write_blocks)"
BACKEND_STORAGE_BATCH_CALLS="$(require_summary_value "${PHASE126_SUMMARY}" backend_storage_batch_calls)"
BACKEND_STORAGE_BATCH_BLOCKS="$(require_summary_value "${PHASE126_SUMMARY}" backend_storage_batch_blocks)"
CLEANUP_STATUS="$(require_summary_value "${PHASE126_SUMMARY}" cleanup_status)"

if [[ "${TARGET_OBSERVED}" != "true" ]]; then
  echo "target_write_observed=${TARGET_OBSERVED}, want true" >&2
  exit 1
fi
assert_int_ge "${TARGET_BYTES}" 1 "target_write_bytes"
assert_int_ge "${BACKEND_BYTES}" 1 "backend_write_bytes"
assert_int_ge "${BACKEND_STORAGE_WRITE_CALLS}" 1 "backend_storage_write_calls"
assert_int_ge "${BACKEND_STORAGE_WRITE_BLOCKS}" 2 "backend_storage_write_blocks"
assert_int_ge "${BACKEND_STORAGE_BATCH_CALLS}" 1 "backend_storage_batch_calls"
assert_int_ge "${BACKEND_STORAGE_BATCH_BLOCKS}" 2 "backend_storage_batch_blocks"
if (( BACKEND_STORAGE_WRITE_CALLS >= BACKEND_STORAGE_WRITE_BLOCKS )); then
  echo "backend_storage_write_calls=${BACKEND_STORAGE_WRITE_CALLS}, want < backend_storage_write_blocks=${BACKEND_STORAGE_WRITE_BLOCKS}" >&2
  exit 1
fi
if [[ "${CLEANUP_STATUS}" != "ok" ]]; then
  echo "cleanup_status=${CLEANUP_STATUS}, want ok" >&2
  exit 1
fi

POST_BATCH_BOTTLENECK="$(require_summary_value "${PHASE126_SUMMARY}" write_path_observation)"
NEXT_RECOMMENDATION="$(classify_post_batch_recommendation "${POST_BATCH_BOTTLENECK}")"

for key in \
  network_baseline_mibps \
  block_nvme_seq_write_mibps \
  block_nvme_seq_read_mibps \
  local_path_seq_write_mibps \
  local_path_seq_read_mibps \
  block_vs_local_write_ratio \
  block_vs_local_read_ratio \
  target_write_observed \
  target_write_bytes \
  target_write_ops \
  target_write_duration_ms \
  backend_write_bytes \
  backend_write_ops \
  backend_write_duration_ms \
  backend_storage_write_calls \
  backend_storage_write_blocks \
  backend_storage_batch_calls \
  backend_storage_batch_blocks \
  backend_sync_ops \
  backend_sync_duration_ms; do
  write_summary "${key}=$(require_summary_value "${PHASE126_SUMMARY}" "${key}")"
done

write_summary "backend_storage_batching_effective=true"
write_summary "post_batch_bottleneck=${POST_BATCH_BOTTLENECK}"
write_summary "next_recommendation=${NEXT_RECOMMENDATION}"
write_summary "cleanup_status=${CLEANUP_STATUS}"
write_summary "phase135_nvme_tcp_post_batch_retriage_status=ok"
