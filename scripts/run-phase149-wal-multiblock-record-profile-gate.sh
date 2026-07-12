#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase149-wal-multiblock-record-profile-gate}"
SUMMARY="${ARTIFACT_DIR}/phase149-wal-multiblock-record-profile-summary.txt"
if [[ -n "${SW_BLOCK_GO_BIN:-}" ]]; then
  GO_BIN="${SW_BLOCK_GO_BIN}"
elif command -v go.exe >/dev/null 2>&1; then
  GO_BIN="go.exe"
else
  GO_BIN="go"
fi

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

extract_log_value() {
  local file="$1"
  local key="$2"
  awk -F= -v key="$key" '$0 ~ key"=" {value=$2} END {gsub(/[^0-9].*/, "", value); if (value != "") print value}' "$file"
}

require_log_value() {
  local file="$1"
  local key="$2"
  local value
  value="$(extract_log_value "$file" "$key")"
  if [[ -z "${value}" ]]; then
    echo "missing ${key} in ${file}" >&2
    exit 1
  fi
  printf '%s' "${value}"
}

write_summary "phase149_wal_multiblock_record_profile_status=running"
write_summary "frontend_transport=tcp"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "default_wal_format_unchanged=true"
write_summary "feature_gate_default=false"

cd "${ROOT}"
"${GO_BIN}" test ./core/storage -run 'TestWALStore_MultiBlock(Profile|DirtyRead|RecoverSplits|FlusherSplits)' -count=1 -v \
  >"${ARTIFACT_DIR}/go-test-multiblock-profile.log" \
  2>&1

SINGLE_ENCODE_OPS="$(require_log_value "${ARTIFACT_DIR}/go-test-multiblock-profile.log" phase149_single_block_wal_encode_ops)"
MULTI_ENCODE_OPS="$(require_log_value "${ARTIFACT_DIR}/go-test-multiblock-profile.log" phase149_multiblock_wal_encode_ops)"
SINGLE_APPEND_OPS="$(require_log_value "${ARTIFACT_DIR}/go-test-multiblock-profile.log" phase149_single_block_wal_append_ops)"
MULTI_APPEND_OPS="$(require_log_value "${ARTIFACT_DIR}/go-test-multiblock-profile.log" phase149_multiblock_wal_append_ops)"
SINGLE_WRITEAT_CALLS="$(require_log_value "${ARTIFACT_DIR}/go-test-multiblock-profile.log" phase149_single_block_wal_writeat_calls)"
MULTI_WRITEAT_CALLS="$(require_log_value "${ARTIFACT_DIR}/go-test-multiblock-profile.log" phase149_multiblock_wal_writeat_calls)"

"${GO_BIN}" test ./core/storage ./core/frontend/durable -count=1 \
  >"${ARTIFACT_DIR}/go-test-compatibility.log" \
  2>&1

python3 - "$SINGLE_ENCODE_OPS" "$MULTI_ENCODE_OPS" <<'PY' >"${ARTIFACT_DIR}/record-count-visible.txt"
import sys
single = int(sys.argv[1])
multi = int(sys.argv[2])
print(str(multi > 0 and single >= multi * 4).lower())
PY
RECORD_COUNT_VISIBLE="$(cat "${ARTIFACT_DIR}/record-count-visible.txt")"
if [[ "${RECORD_COUNT_VISIBLE}" != "true" ]]; then
  echo "record_count_reduction_visible=${RECORD_COUNT_VISIBLE}, want true" >&2
  exit 1
fi

write_summary "single_block_compatibility=pass"
write_summary "current_recovery_compatibility=pass"
write_summary "profile_scope=local_storage"
write_summary "single_block_wal_encode_ops=${SINGLE_ENCODE_OPS}"
write_summary "multiblock_wal_encode_ops=${MULTI_ENCODE_OPS}"
write_summary "single_block_wal_append_ops=${SINGLE_APPEND_OPS}"
write_summary "multiblock_wal_append_ops=${MULTI_APPEND_OPS}"
write_summary "single_block_wal_writeat_calls=${SINGLE_WRITEAT_CALLS}"
write_summary "multiblock_wal_writeat_calls=${MULTI_WRITEAT_CALLS}"
write_summary "record_count_reduction_visible=${RECORD_COUNT_VISIBLE}"
write_summary "dirty_read_verified=true"
write_summary "recovery_verified=true"
write_summary "phase149_decision=wire_runtime_opt_in"
write_summary "next_recommendation=phase150_wal_multiblock_runtime_opt_in"
write_summary "cleanup_status=ok"
write_summary "phase149_wal_multiblock_record_profile_status=ok"
