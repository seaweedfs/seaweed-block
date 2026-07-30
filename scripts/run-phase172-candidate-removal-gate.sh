#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${TMPDIR:-/tmp}/phase172-candidate-removal-gate}"
SUMMARY="${ARTIFACT_DIR}/phase172-candidate-removal-summary.txt"

if [[ -n "$(git -C "${ROOT}" status --porcelain)" ]]; then
  echo "Phase 172 candidate-removal gate requires a clean worktree" >&2
  exit 1
fi

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

cd "${ROOT}"
write_summary "phase172_candidate_removal_status=running"
write_summary "git_sha=$(git rev-parse HEAD)"
write_summary "git_dirty=false"
write_summary "go_version=$(go version | tr ' ' '_')"
write_summary "kernel=$(uname -srvmo | tr ' ' '_')"

if grep -R -E \
  'singleReadMaterialization|sharedRecordMaterialization|enableSingleReadMaterialization|enableSharedRecordMaterialization|SW_BLOCK_PHASE172_(MATERIALIZATION_MODE|SCOPED_PROBE|SHARED_PROBE)' \
  core/storage --include='*.go' >"${ARTIFACT_DIR}/forbidden-symbols.log"; then
  echo "rejected materialization candidate symbols remain" >&2
  exit 1
fi
write_summary "candidate_runtime_and_test_symbols=0"

if grep -E 'recordSize[[:space:]]+uint64|RecordSize[[:space:]]+uint64' \
  core/storage/dirty_map.go >"${ARTIFACT_DIR}/forbidden-geometry.log"; then
  echo "rejected dirty-map record geometry remains" >&2
  exit 1
fi
write_summary "candidate_dirty_geometry_fields=0"

if grep -E \
  'SnapshotUniqueWALRecords|SnapshotRecordReuseCandidates|MaterializationReadOps|MaterializationReadBytes|MaterializationRecordReuseHits' \
  core/storage/flusher_instrumentation.go \
  >"${ARTIFACT_DIR}/forbidden-instrumentation.log"; then
  echo "rejected materialization instrumentation remains" >&2
  exit 1
fi
write_summary "candidate_instrumentation_fields=0"

removed_files=(
  core/storage/walstore_record_geometry_test.go
  core/storage/walstore_single_read_materialization_test.go
  core/storage/walstore_shared_record_materialization_test.go
  core/storage/walstore_materialization_lifecycle_test.go
  scripts/run-phase172-wal-materialization-baseline-gate.sh
  scripts/run-phase172-single-read-correctness-gate.sh
  scripts/run-phase172-shared-record-correctness-gate.sh
  scripts/run-phase172-materialization-lifecycle-gate.sh
  scripts/run-phase172-materialization-performance-gate.sh
)
for file in "${removed_files[@]}"; do
  if [[ -e "${file}" ]]; then
    echo "rejected candidate file remains: ${file}" >&2
    exit 1
  fi
done
write_summary "candidate_files_removed=true"

header_reads="$(grep -c 'recordWALHeaderRead' core/storage/flusher.go)"
record_reads="$(grep -c 'recordWALRecordRead' core/storage/flusher.go)"
if [[ "${header_reads}" != "1" || "${record_reads}" != "1" ]]; then
  echo "default flusher read shape changed header=${header_reads} record=${record_reads}" >&2
  exit 1
fi
write_summary "default_flusher_header_read_path_present=true"
write_summary "default_flusher_record_read_path_present=true"

focused='TestWALStore(RecoverReplaysLegacyRangeTrim|RecoverRetainsMultiBlockSuffixAcrossRestart|RecoverRejectsOverflowingBatchGeometry|RecoverReconstructsLegacyWrappedRetainedWindow)$'
go test ./core/storage -run "${focused}" -count=20 \
  >"${ARTIFACT_DIR}/retained-correctness-repeat.log" 2>&1
write_summary "retained_correctness_repeat_20=pass"
write_summary "legacy_range_trim=pass"
write_summary "partial_multiblock_suffix_replay=pass"
write_summary "malformed_batch_typed_fail_closed=pass"
write_summary "legacy_wrapped_byte_boundaries=pass"

CGO_ENABLED=1 go test -race ./core/storage -run "${focused}" -count=10 \
  >"${ARTIFACT_DIR}/retained-correctness-race.log" 2>&1
write_summary "retained_correctness_race_repeat_10=pass"

go test ./core/storage -count=1 >"${ARTIFACT_DIR}/storage-regression.log" 2>&1
go test ./core/recovery -count=1 >"${ARTIFACT_DIR}/recovery-regression.log" 2>&1
go test ./core/replication -count=1 >"${ARTIFACT_DIR}/replication-regression.log" 2>&1
go test ./core/replication/component -count=1 \
  >"${ARTIFACT_DIR}/replication-component-regression.log" 2>&1
write_summary "storage_regression=pass"
write_summary "recovery_regression=pass"
write_summary "replication_regression=pass"
write_summary "replication_component_regression=pass"

go vet ./core/storage ./core/recovery ./core/replication ./core/replication/component \
  >"${ARTIFACT_DIR}/vet.log" 2>&1
write_summary "candidate_removal_vet=pass"
write_summary "d6_mounted_gate_run=false"
write_summary "default_materialization_path_unchanged=true"
write_summary "independent_recovery_fixes_retained=true"
write_summary "phase172_candidate_removal_status=ok"
