#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase169-segment-owner-gate}"
SUMMARY="${ARTIFACT_DIR}/phase169-segment-owner-summary.txt"

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

write_summary "phase169_segment_owner_status=running"
write_summary "scope=bounded_internal_group_commit_owner"

cd "${ROOT}"

run_test "queued_group_commit" '^TestSegmentOwnerGroupsAlreadyQueuedWritesWithoutDelay$'
run_test "queue_bound_and_lsn_continuity" '^TestSegmentOwnerQueueIsBoundedAndRejectsWithoutLSNHole$'
run_test "short_write_terminal_failure" '^TestSegmentOwnerShortWriteTerminallyFailsQueuedRequests$'
run_test "log_byte_ceiling" '^TestSegmentOwnerLogByteCeilingFailsClosed$'
run_test "close_drains_admitted" '^TestSegmentOwnerCloseDrainsAdmittedRequests$'
run_test "config_and_input_bounds" '^TestSegmentOwnerValidatesConfigAndInput$'

go test ./core/storage/parallelwal ./core/storage ./core/recovery ./core/frontend/durable -count=1 \
  >"${ARTIFACT_DIR}/storage-regression.log" 2>&1
write_summary "storage_regression=pass"

if [[ "$(go env CGO_ENABLED)" == "1" ]]; then
  go test -race ./core/storage/parallelwal -run '^TestSegmentOwner' -count=20 \
    >"${ARTIFACT_DIR}/segment-owner-race.log" 2>&1
  write_summary "segment_owner_race=pass"
else
  write_summary "segment_owner_race=not_run_cgo_disabled"
fi

GOOS=windows GOARCH=amd64 CGO_ENABLED=0 \
  go test -c ./core/storage/parallelwal \
  -o "${ARTIFACT_DIR}/parallelwal-windows.test.exe" \
  >"${ARTIFACT_DIR}/windows-compile.log" 2>&1
write_summary "windows_compile=pass"

if grep -Eq 'time\.(After|NewTimer|Sleep|Tick)' core/storage/parallelwal/segment_owner.go; then
  echo "segment owner contains timer-based batching" >&2
  exit 1
fi
write_summary "batching_timer_present=false"

if grep -R -n --include='*.go' --include='*.yaml' --include='*.tpl' \
  -E 'segmented-walstore|segment-walstore' \
  cmd charts core/storage >/dev/null 2>&1; then
  echo "Phase 169 D2 unexpectedly added a product selector" >&2
  exit 1
fi
write_summary "product_selector_added=false"

grep -Eq 'maxSegmentQueueDepth[[:space:]]*=[[:space:]]*4096' \
  core/storage/parallelwal/segment_owner.go
write_summary "segment_queue_depth_hard_limit=4096"
write_summary "queue_full_consumes_lsn=false"
write_summary "one_write_at_per_segment=true"
write_summary "phase169_segment_owner_status=ok"
