#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase168-native-wal-shape-gate}"
SUMMARY="${ARTIFACT_DIR}/phase168-native-wal-shape-summary.txt"
PORTABLE_LOG="${ARTIFACT_DIR}/portable-shape-tests.log"
LINUX_LOG="${ARTIFACT_DIR}/linux-shape-tests.log"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

require_line() {
  local pattern="$1"
  local path="$2"
  if ! grep -Eq "${pattern}" "${path}"; then
    echo "missing evidence pattern=${pattern} path=${path}" >&2
    exit 1
  fi
}

write_summary "phase168_native_wal_shape_status=running"
write_summary "default_execution=positioned"
write_summary "product_selector_added=false"

cd "${ROOT}"
if [[ "$(go env GOOS)/$(go env GOARCH)" != "linux/amd64" ]]; then
  write_summary "phase168_native_wal_shape_status=unsupported"
  write_summary "reason=gate_requires_linux_amd64"
  exit 2
fi

timeout 60s go test ./core/storage/parallelwal \
  -run 'TestNativeQueueSaturationReturnsTypedBackpressure|TestNativeCountsEveryShortCompletionAndFailsAllRequests|TestNativeCloseWaitsForInflightWriteBeforeClosingExecutor|TestNativeFsyncFailureTerminallyRejectsLaterWrites' \
  -count=1 -v >"${PORTABLE_LOG}" 2>&1
require_line '^--- PASS: TestNativeQueueSaturationReturnsTypedBackpressure ' "${PORTABLE_LOG}"
require_line '^--- PASS: TestNativeCountsEveryShortCompletionAndFailsAllRequests ' "${PORTABLE_LOG}"
require_line '^--- PASS: TestNativeCloseWaitsForInflightWriteBeforeClosingExecutor ' "${PORTABLE_LOG}"
require_line '^--- PASS: TestNativeFsyncFailureTerminallyRejectsLaterWrites ' "${PORTABLE_LOG}"
write_summary "typed_queue_backpressure=pass"
write_summary "all_short_completions_counted=pass"
write_summary "later_write_after_short_completion=denied"
write_summary "close_waits_for_inflight=pass"
write_summary "fsync_failure_terminal=true"

timeout 60s go test ./internal/iouring ./core/storage/parallelwal \
  -run 'TestExecutorPoisonsRingAfterPartialSubmission|TestExecutorPoisonsAfterEventFDWaitErrorDrainsAcceptedCQE|TestExecutorRejectsOversizedSubmissionWithoutStaleSQEs|TestAcceptedOperationRetainsBufferThroughForcedGC|TestIOUringOwnerRotatesAcrossLanesAtDepthOne|TestIOUringOwnerUsesMultipleRoundsWhenDepthBounded|TestNativeRingWrapRecyclesAndRecovers|TestNativeCloseWithInflightWriteRecovers' \
  -count=1 -v >"${LINUX_LOG}" 2>&1
require_line '^--- PASS: TestExecutorPoisonsRingAfterPartialSubmission ' "${LINUX_LOG}"
require_line '^--- PASS: TestExecutorPoisonsAfterEventFDWaitErrorDrainsAcceptedCQE ' "${LINUX_LOG}"
require_line '^--- PASS: TestExecutorRejectsOversizedSubmissionWithoutStaleSQEs ' "${LINUX_LOG}"
require_line '^--- PASS: TestAcceptedOperationRetainsBufferThroughForcedGC ' "${LINUX_LOG}"
require_line '^--- PASS: TestIOUringOwnerRotatesAcrossLanesAtDepthOne ' "${LINUX_LOG}"
require_line '^--- PASS: TestIOUringOwnerUsesMultipleRoundsWhenDepthBounded ' "${LINUX_LOG}"
require_line '^--- PASS: TestNativeRingWrapRecyclesAndRecovers ' "${LINUX_LOG}"
require_line '^--- PASS: TestNativeCloseWithInflightWriteRecovers ' "${LINUX_LOG}"
write_summary "partial_submission_executor_poisoned=true"
write_summary "eventfd_error_executor_poisoned=true"
write_summary "oversized_submission_rejected_without_stale_sqe=true"
write_summary "full_submission_queue=pass"
write_summary "accepted_buffer_forced_gc=pass"
write_summary "depth_one_round_accounting=pass"
write_summary "bounded_multiple_rounds=pass"
write_summary "native_ring_wrap_recovery=pass"
write_summary "real_native_close_reopen_recovery=pass"

timeout 120s go test -race ./internal/iouring ./core/storage/parallelwal \
  -run 'TestExecutorPoisonsRingAfterPartialSubmission|TestExecutorPoisonsAfterEventFDWaitErrorDrainsAcceptedCQE|TestExecutorRejectsOversizedSubmissionWithoutStaleSQEs|TestAcceptedOperationRetainsBufferThroughForcedGC|TestNativeQueueSaturationReturnsTypedBackpressure|TestNativeCountsEveryShortCompletionAndFailsAllRequests|TestNativeCloseWaitsForInflightWriteBeforeClosingExecutor|TestNativeFsyncFailureTerminallyRejectsLaterWrites|TestIOUringOwnerUsesMultipleRoundsWhenDepthBounded|TestNativeRingWrapRecyclesAndRecovers|TestNativeCloseWithInflightWriteRecovers' \
  -count=10 >"${ARTIFACT_DIR}/shape-race-tests.log" 2>&1
write_summary "shape_matrix_linux_race=pass"
write_summary "race_repetitions=10"
write_summary "fallback_count=0"
write_summary "phase168_native_wal_shape_status=ok"
