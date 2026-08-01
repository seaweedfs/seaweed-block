#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULT_DIR="${ROOT_DIR}/results/phase171-checkpoint-correctness-gate"
SUMMARY="${RESULT_DIR}/phase171-checkpoint-correctness-summary.txt"

rm -rf "${RESULT_DIR}"
mkdir -p "${RESULT_DIR}"
: >"${SUMMARY}"

write_summary() {
  printf '%s\n' "$1" | tee -a "${SUMMARY}"
}

write_summary "phase171_checkpoint_correctness_status=running"
write_summary "go_version=$(go version)"
write_summary "goos=$(go env GOOS)"
write_summary "goarch=$(go env GOARCH)"

if [[ "$(go env GOOS)" != "linux" ]]; then
  write_summary "phase171_checkpoint_correctness_status=unsupported"
  write_summary "unsupported_reason=linux_sigkill_required"
  exit 2
fi

go test ./core/storage \
  -run 'TestCheckpointPublicationCrashWindowsRemainRecoverable|TestFlusherCheckpointMetadataFailureDoesNotPublishOrRecycle|TestFlusherWALSlotMismatchFailsClosed|TestFlusherRejectsCorruptOrUnsupportedDirtyRecord|TestWALStoreClosePerformsFinalFlush|TestWALStoreCloseWaitsForInflightSync|TestWALStoreMutationAPIsRejectAfterClose|TestWALStoreCloseReturnsFinalMetadataFailure|TestWriteExtentDirect|TestRunningFlusherCannotOverwriteDirectBase|TestFlusherSnapshotCannotOverwriteLaterDirectBase' \
  -count=20 \
  >"${RESULT_DIR}/focused.log" 2>&1
write_summary "focused_repetitions=20"
write_summary "focused_status=pass"

go test ./core/storage \
  -run '^TestWALStoreCheckpointSIGKILLCrashWindows$' \
  -count=20 \
  >"${RESULT_DIR}/sigkill.log" 2>&1
write_summary "sigkill_windows=after_extent_sync,after_checkpoint_pwrite,after_checkpoint_sync,after_tail_publish"
write_summary "sigkill_repetitions=20"
write_summary "sigkill_status=pass"

go test -race ./core/storage \
  -run 'TestWALStoreCloseWaitsForInflightSync|TestRunningFlusherCannotOverwriteDirectBase|TestFlusher_ConcurrentWriteRaceLBAStaysFresh' \
  -count=10 \
  >"${RESULT_DIR}/race.log" 2>&1
write_summary "race_repetitions=10"
write_summary "race_status=pass"

go test ./core/storage ./core/recovery ./core/transport ./core/replication/... ./core/frontend/durable \
  -count=1 \
  >"${RESULT_DIR}/regression.log" 2>&1
write_summary "storage_recovery_transport_replication_regression=pass"

go vet ./core/storage >"${RESULT_DIR}/vet.log" 2>&1
write_summary "go_vet_storage=pass"
write_summary "checkpoint_metadata_durable_before_tail_reuse=true"
write_summary "stale_or_corrupt_dirty_record_fails_closed=true"
write_summary "close_lifecycle_fence=true"
write_summary "direct_base_ownership_restart_safe=true"
write_summary "phase171_checkpoint_correctness_status=ok"
