#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase169-segment-format-gate}"
SUMMARY="${ARTIFACT_DIR}/phase169-segment-format-summary.txt"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

run_test() {
  local name="$1"
  local pattern="$2"
  go test ./core/storage/parallelwal -run "${pattern}" -count=20 \
    >"${ARTIFACT_DIR}/${name}.log" 2>&1
  write_summary "${name}=pass"
}

write_summary "phase169_segment_format_status=running"
write_summary "scope=internal_format_and_committed_prefix_recovery"

cd "${ROOT}"

run_test "clean_encode_decode" '^TestSegmentEncodeDecodeAllowsOrderedSameLBAWrites$'
run_test "invalid_geometry_and_order" '^TestSegmentEncodeRejectsInvalidGeometryAndOrder$'
run_test "corruption_and_bounds" '^TestSegmentDecodeRejectsCorruptionAndBounds$'
run_test "uncommitted_tail_rule" '^TestRecoverCommittedSegmentsIgnoresOnlyUncommittedTail$'
run_test "committed_corruption_fail_closed" '^TestRecoverCommittedSegmentsFailsClosedOnCommittedCorruption$'
run_test "cross_segment_sequence_and_lsn" '^TestRecoverCommittedSegmentsRejectsSequenceAndLSNGaps$'
run_test "trusted_manifest_anchors" '^TestScanCommittedSegmentsRequiresManifestAnchors$'
run_test "same_lba_recovery_order" '^TestScanCommittedSegmentsReplaysSameLBAInLSNOrder$'
run_test "frozen_format_vector" '^TestSegmentFormatGoldenVector$'

go test ./core/storage/parallelwal ./core/storage ./core/recovery ./core/frontend/durable -count=1 \
  >"${ARTIFACT_DIR}/storage-regression.log" 2>&1
write_summary "storage_regression=pass"

if [[ "$(go env CGO_ENABLED)" == "1" ]]; then
  go test -race ./core/storage/parallelwal -count=1 \
    >"${ARTIFACT_DIR}/parallelwal-race.log" 2>&1
  write_summary "parallelwal_race=pass"
else
  write_summary "parallelwal_race=not_run_cgo_disabled"
fi

GOOS=windows GOARCH=amd64 CGO_ENABLED=0 \
  go test -c ./core/storage/parallelwal \
  -o "${ARTIFACT_DIR}/parallelwal-windows.test.exe" \
  >"${ARTIFACT_DIR}/windows-compile.log" 2>&1
write_summary "windows_compile=pass"

if grep -R -n --include='*.go' --include='*.yaml' --include='*.tpl' \
  -E 'segmented-walstore|segment-walstore' \
  cmd charts core/storage >/dev/null 2>&1; then
  echo "Phase 169 D1 unexpectedly added a product selector" >&2
  exit 1
fi
write_summary "product_selector_added=false"

grep -Eq 'segmentVersion[[:space:]]*=[[:space:]]*1' core/storage/parallelwal/segment.go
grep -Eq 'maxSegmentEntries[[:space:]]*=[[:space:]]*256' core/storage/parallelwal/segment.go
grep -Eq 'maxSegmentPayloadBytes[[:space:]]*=[[:space:]]*1[[:space:]]*<<[[:space:]]*20' \
  core/storage/parallelwal/segment.go
write_summary "segment_version=1"
write_summary "segment_max_entries=256"
write_summary "segment_max_payload_bytes=1048576"
write_summary "committed_corruption_policy=fail_closed"
write_summary "uncommitted_tail_policy=ignore_after_trusted_boundary"
write_summary "phase169_segment_format_status=ok"
