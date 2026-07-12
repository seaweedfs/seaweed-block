#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase147-wal-multiblock-record-design-gate}"
SUMMARY="${ARTIFACT_DIR}/phase147-wal-multiblock-record-design-summary.txt"
DESIGN_DOC="${ROOT}/internal/docs/protocol/phase147-wal-multiblock-record-design.md"
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

require_source_text() {
  local rel="$1"
  local pattern="$2"
  local label="$3"
  require_text "${ROOT}/${rel}" "$pattern" "$label"
}

write_summary "phase147_wal_multiblock_record_design_status=running"
write_summary "frontend_transport=tcp"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"

require_source_text "core/storage/wal_entry.go" "walEntryHeaderSize = 38" "current WAL header size"
require_source_text "core/storage/wal_entry.go" "walEntryWrite   = 0x01" "current write entry type"
require_source_text "core/storage/superblock.go" "const WALStoreImplVersion uint32 = 1" "current WALStore impl version"
write_summary "current_wal_format_unchanged=true"

cd "${ROOT}"
"${GO_BIN}" test ./core/storage ./core/frontend/durable \
  >"${ARTIFACT_DIR}/go-test-recovery-compatibility.log" \
  2>&1
write_summary "current_recovery_compatibility=pass"

require_text "${DESIGN_DOC}" "candidate_design=multi_block_record" "candidate design"
require_text "${DESIGN_DOC}" "candidate_reduces_record_count=true" "record-count decision"
require_text "${DESIGN_DOC}" "candidate_reduces_write_calls=false" "write-call decision"
require_text "${DESIGN_DOC}" "INV-WAL-BATCH-FORMAT-VERSION" "format version invariant"
require_text "${DESIGN_DOC}" "INV-WAL-BATCH-CRC-ALL-OR-NOTHING" "durability invariant"
require_text "${DESIGN_DOC}" "INV-WAL-BATCH-RECOVERY-SPLIT" "recovery split invariant"
require_text "${DESIGN_DOC}" "INV-WAL-BATCH-FLUSH-SPLIT" "flush split invariant"
require_text "${DESIGN_DOC}" "phase147_decision=prototype_next" "phase decision"
require_text "${DESIGN_DOC}" "next_recommendation=phase148_wal_multiblock_record_local_prototype" "next recommendation"

write_summary "candidate_design=multi_block_record"
write_summary "candidate_reduces_record_count=true"
write_summary "candidate_reduces_write_calls=false"
write_summary "durability_invariant_documented=true"
write_summary "recovery_invariant_documented=true"
write_summary "phase147_decision=prototype_next"
write_summary "next_recommendation=phase148_wal_multiblock_record_local_prototype"
write_summary "cleanup_status=ok"
write_summary "phase147_wal_multiblock_record_design_status=ok"
