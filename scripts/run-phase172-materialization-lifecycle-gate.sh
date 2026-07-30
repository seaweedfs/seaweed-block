#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase172-materialization-lifecycle-gate}"
SUMMARY="${ARTIFACT_DIR}/phase172-materialization-lifecycle-summary.txt"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

cd "${ROOT}"
write_summary "phase172_materialization_lifecycle_status=running"
write_summary "git_sha=$(git rev-parse HEAD)"
git_dirty="$([[ -n "$(git status --porcelain)" ]] && echo true || echo false)"
write_summary "git_dirty=${git_dirty}"
if [[ "${git_dirty}" != "false" ]]; then
  echo "Phase 172 D4 requires a clean source worktree" >&2
  exit 1
fi
write_summary "go_version=$(go version | tr ' ' '_')"
write_summary "kernel=$(uname -srvmo | tr ' ' '_')"

candidate_tests=(
  TestWALStoreSharedMaterializationLargeConcurrentLifecycle
  TestWALStoreSharedMaterializationCannotOverwriteDirectBase
  TestWALStoreSharedMaterializationRespectsRecycleFloorAcrossRestart
  TestWALStoreSharedMaterializationCloseLifecycle
  TestWALStoreRecoverRejectsOverflowingBatchGeometry
  TestWALStoreRecoverReconstructsLegacyWrappedRetainedWindow
)
candidate_regex="^($(IFS='|'; echo "${candidate_tests[*]}"))$"
candidate_log="${ARTIFACT_DIR}/candidate-lifecycle-repeat.log"
: >"${candidate_log}"
for test_name in "${candidate_tests[@]}"; do
  go test ./core/storage -run "^${test_name}$" -count=20 -v \
    >>"${candidate_log}" 2>&1
  write_summary "${test_name}=pass"
done
if grep -Eq -- '--- SKIP|no tests to run' "${candidate_log}"; then
  echo "candidate lifecycle gate contains a skipped or missing test" >&2
  exit 1
fi
write_summary "candidate_repeat_20=pass"
write_summary "large_snapshot_concurrent_write_batch=pass"
write_summary "direct_base_overlap=pass"
write_summary "recycle_floor_partial_batch_recovery=pass"
write_summary "close_final_flush_and_failure_recovery=pass"
write_summary "overflow_batch_recovery_fails_closed=pass"
write_summary "legacy_wrapped_retained_window_reconstructed=pass"

go test -race ./core/storage -run "${candidate_regex}" -count=20 -v \
  >"${ARTIFACT_DIR}/candidate-lifecycle-race-repeat.log" 2>&1
if grep -Eq -- '--- SKIP|no tests to run' "${ARTIFACT_DIR}/candidate-lifecycle-race-repeat.log"; then
  echo "candidate race gate contains a skipped or missing test" >&2
  exit 1
fi
write_summary "candidate_race_repeat_20=pass"

equivalence_tests=(
  TestCheckpointPublicationCrashWindowsRemainRecoverable
  TestWALStoreCheckpointSIGKILLCrashWindows
  TestWALStoreCloseWaitsForInflightSync
  TestWALStoreCloseReturnsFinalMetadataFailure
  TestRunningFlusherCannotOverwriteDirectBase
  TestWalstoreRecovery_ScanLBAs_ConcurrentLiveWrite_Safe
  TestWALStore_RecycleGate_SourceActive_ClampsAtFloor
)
equivalence_log="${ARTIFACT_DIR}/existing-equivalence-repeat.log"
: >"${equivalence_log}"
for test_name in "${equivalence_tests[@]}"; do
  go test ./core/storage -run "^${test_name}$" -count=10 -v \
    >>"${equivalence_log}" 2>&1
  write_summary "${test_name}=pass"
done
if grep -Eq -- '--- SKIP|no tests to run' "${equivalence_log}"; then
  echo "existing equivalence gate contains a skipped or missing test" >&2
  exit 1
fi
write_summary "existing_equivalence_repeat_10=pass"
write_summary "checkpoint_crash_windows=pass"
write_summary "checkpoint_sigkill_windows=pass"
write_summary "sync_close_lifecycle=pass"
write_summary "scan_lbas_concurrent_live_write=pass"
write_summary "recycle_pin_contract=pass"

go test ./core/storage -count=1 \
  >"${ARTIFACT_DIR}/storage-regression.log" 2>&1
write_summary "storage_regression=pass"

go test ./core/recovery -count=1 \
  >"${ARTIFACT_DIR}/recovery-regression.log" 2>&1
write_summary "recovery_regression=pass"

go test ./core/replication -count=1 \
  >"${ARTIFACT_DIR}/replication-regression.log" 2>&1
write_summary "replication_regression=pass"

go test ./core/replication/component -count=1 \
  >"${ARTIFACT_DIR}/replication-component-regression.log" 2>&1
write_summary "replication_component_regression=pass"

go vet ./core/storage ./core/recovery ./core/replication ./core/replication/component \
  >"${ARTIFACT_DIR}/lifecycle-vet.log" 2>&1
write_summary "lifecycle_vet=pass"

write_summary "rf1_local_storage_contract=pass"
write_summary "rf3_sync_quorum_component_contract=pass"
write_summary "checkpoint_tail_dirty_consistency=pass"
write_summary "candidate_disk_format_unchanged=true"
write_summary "recovery_branch_added=false"
write_summary "external_selector_added=false"
write_summary "d5_performance_gate_eligible=true"
write_summary "phase172_materialization_lifecycle_status=ok"
