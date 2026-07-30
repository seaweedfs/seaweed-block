#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase169-segment-durability-gate}"
SUMMARY="${ARTIFACT_DIR}/phase169-segment-durability-summary.txt"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

run_test() {
  local name="$1"
  local pattern="$2"
  go test ./core/storage/parallelwal -run "${pattern}" -count=50 \
    >"${ARTIFACT_DIR}/${name}.log" 2>&1
  write_summary "${name}=pass"
}

write_summary "phase169_segment_durability_status=running"
write_summary "scope=publication_sync_dual_header_terminal_failure"

cd "${ROOT}"

run_test "dual_header_fallback" '^TestSegmentDurableHeaderFallsBackToPriorValidGeneration$'
run_test "durable_header_bounds" '^TestSegmentDurableHeaderRejectsResealedReservedAndManifestFields$'
run_test "trusted_prefix_recovery" '^TestSegmentDurableSyncPersistsOnlyTrustedPrefix$'
run_test "sync_waits_for_target" '^TestSegmentDurableSyncFencesAdmittedWrite$'
run_test "sync_excludes_future_admission" '^TestSegmentDurableSyncDoesNotWaitForFutureAdmission$'
run_test "segment_write_error_terminal" '^TestSegmentOwnerWriteErrorTerminallyFaultsOwner$'
run_test "data_sync_failure_terminal" '^TestSegmentDurableSyncFailureTerminallyFaultsOwner$'
run_test "failure_barrier_blocks_future_publish" '^TestSegmentDurableFailureBarrierBlocksFuturePublication$'
run_test "header_write_failure_terminal" '^TestSegmentDurableHeaderWriteFailureTerminallyFaultsOwner$'
run_test "header_sync_failure_terminal" '^TestSegmentDurableHeaderSyncFailureTerminallyFaultsOwner$'
run_test "external_failure_blocks_active_publish" '^TestSegmentOwnerExternalFailureCannotPublishActiveWrite$'
run_test "short_header_write_rejected" '^TestSegmentDurableHeaderRejectsShortWrite$'

go test ./core/storage/parallelwal ./core/storage ./core/recovery ./core/frontend/durable -count=1 \
  >"${ARTIFACT_DIR}/storage-regression.log" 2>&1
write_summary "storage_regression=pass"

if [[ "$(go env CGO_ENABLED)" == "1" ]]; then
  go test -race ./core/storage/parallelwal \
    -run 'TestSegmentDurable|TestSegmentOwnerExternal' \
    -count=20 >"${ARTIFACT_DIR}/segment-durability-race.log" 2>&1
  write_summary "segment_durability_race=pass"
else
  write_summary "segment_durability_race=not_run_cgo_disabled"
fi

GOOS=windows GOARCH=amd64 CGO_ENABLED=0 \
  go test -c ./core/storage/parallelwal \
  -o "${ARTIFACT_DIR}/parallelwal-windows.test.exe" \
  >"${ARTIFACT_DIR}/windows-compile.log" 2>&1
write_summary "windows_compile=pass"

if grep -R -n --include='*.go' --include='*.yaml' --include='*.tpl' \
  -E 'segmented-walstore|segment-walstore' \
  cmd charts core/storage >/dev/null 2>&1; then
  echo "Phase 169 D3 unexpectedly added a product selector" >&2
  exit 1
fi
write_summary "product_selector_added=false"

write_summary "sync_order=data_fsync_then_alternate_header_then_header_fsync"
write_summary "sync_target=highest_lsn_admitted_before_call"
write_summary "uncommitted_physical_tail_recovered=false"
write_summary "terminal_failure_allows_later_publish=false"
write_summary "phase169_segment_durability_status=ok"
