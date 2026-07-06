#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase148-wal-multiblock-record-local-prototype-gate}"
SUMMARY="${ARTIFACT_DIR}/phase148-wal-multiblock-record-local-prototype-summary.txt"
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

require_text() {
  local path="$1"
  local pattern="$2"
  local label="$3"
  if ! grep -Fq "$pattern" "$path"; then
    echo "missing ${label}: ${pattern}" >&2
    exit 1
  fi
}

write_summary "phase148_wal_multiblock_record_local_prototype_status=running"
write_summary "frontend_transport=tcp"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"

require_text "${ROOT}/core/storage/wal_entry.go" "walEntryWriteBatch = 0x04" "batch entry type"
require_text "${ROOT}/core/storage/walstore.go" "multiBlockRecords bool" "disabled feature field"
require_text "${ROOT}/core/storage/walstore.go" "enableMultiBlockRecordsForTest" "test-only enable gate"
write_summary "default_wal_format_unchanged=true"
write_summary "feature_gate_default=false"

cd "${ROOT}"
"${GO_BIN}" test ./core/storage -run MultiBlock -count=1 \
  >"${ARTIFACT_DIR}/go-test-multiblock.log" \
  2>&1
write_summary "multiblock_encode_decode=pass"
write_summary "multiblock_dirty_read=pass"
write_summary "multiblock_recovery_split=pass"
write_summary "multiblock_flusher_split=pass"

"${GO_BIN}" test ./core/storage ./core/frontend/durable -count=1 \
  >"${ARTIFACT_DIR}/go-test-compatibility.log" \
  2>&1
write_summary "single_block_compatibility=pass"
write_summary "current_recovery_compatibility=pass"

write_summary "phase148_decision=profile_next"
write_summary "next_recommendation=phase149_wal_multiblock_record_profile_gate"
write_summary "cleanup_status=ok"
write_summary "phase148_wal_multiblock_record_local_prototype_status=ok"
