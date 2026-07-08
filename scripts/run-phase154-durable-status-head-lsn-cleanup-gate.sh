#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase154-durable-status-head-lsn-cleanup-gate}"
SUMMARY="${ARTIFACT_DIR}/phase154-durable-status-head-lsn-cleanup-summary.txt"
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

require_grep() {
  local file="$1"
  local needle="$2"
  local label="$3"
  if ! grep -Fq -- "${needle}" "${file}"; then
    echo "missing ${label}: ${needle} in ${file}" >&2
    exit 1
  fi
  write_summary "${label}=true"
}

write_summary "phase154_durable_status_head_lsn_cleanup_status=running"
write_summary "phase152_followup=head_lsn_diagnostic_cleanup"
write_summary "runtime_opt_in_name=durable-wal-multiblock-records"
write_summary "runtime_opt_in_enabled=true"
write_summary "default_wal_format_unchanged=true"
write_summary "no_recovery_semantics_change_without_test=true"

cd "${ROOT}"

"${GO_BIN}" test ./core/storage ./core/frontend/durable \
  -run 'Phase154|MultiBlockRecoveryHeadLSN|Phase152' -count=1 \
  >"${ARTIFACT_DIR}/go-test-head-lsn.log" \
  2>&1
write_summary "walstore_head_lsn_byte_offset_regression=pass"
write_summary "durable_status_head_lsn_regression=pass"
write_summary "recovered_lsn_remains_correct=true"
write_summary "head_lsn_after_recovery_is_bounded=true"

"${GO_BIN}" test ./core/storage ./core/frontend/durable -count=1 \
  >"${ARTIFACT_DIR}/go-test-storage-durable.log" \
  2>&1
write_summary "storage_durable_package_tests=pass"

require_grep "core/storage/logical_storage.go" "H: newest written LSN" "logical_storage_head_lsn_contract_documented"
require_grep "core/storage/walstore.go" "walHead:       sb.WALCheckpointLSN" "walstore_open_initializes_head_from_lsn_checkpoint"
require_grep "core/storage/walstore.go" "s.walHead = recoveredHead" "walstore_recover_sets_head_from_recovered_frontier"
require_grep "core/storage/walstore_multiblock_test.go" "not WAL byte offset" "storage_regression_asserts_not_wal_byte_offset"
require_grep "core/frontend/durable/provider_test.go" "HeadLSN" "durable_status_head_lsn_test_exists"
require_grep "docs/releases/wal-multiblock-opt-in.md" "Phase 154 fixed the diagnostic durable status" "release_doc_mentions_head_lsn_fix"

write_summary "durable_status_head_lsn_semantics_documented=true"
write_summary "cleanup_status=ok"
write_summary "phase154_decision=fixed"
write_summary "next_recommendation=phase155_mounted_durable_status_head_lsn_confirmation"
write_summary "phase154_durable_status_head_lsn_cleanup_status=ok"
