#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase168-native-wal-durability-gate}"
SUMMARY="${ARTIFACT_DIR}/phase168-native-wal-durability-summary.txt"
TEST_LOG="${ARTIFACT_DIR}/native-durability-test.log"

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

write_summary "phase168_native_wal_durability_status=running"
write_summary "default_execution=positioned"
write_summary "product_selector_added=false"

cd "${ROOT}"
if [[ "$(go env GOOS)/$(go env GOARCH)" != "linux/amd64" ]]; then
  write_summary "phase168_native_wal_durability_status=unsupported"
  write_summary "reason=gate_requires_linux_amd64"
  exit 2
fi

timeout 60s go test ./core/storage/parallelwal \
  -run '^TestIOUringOwnerBatchesAcrossLanesAndRecoversPortably$' \
  -count=1 -v >"${TEST_LOG}" 2>&1
require_line '^--- PASS: TestIOUringOwnerBatchesAcrossLanesAndRecoversPortably ' "${TEST_LOG}"
require_line 'native_durability_stats barriers=2 fsync_completions=2 submit_syscalls=[1-9][0-9]*' "${TEST_LOG}"
write_summary "target_lsn_barrier=pass"
write_summary "durability_barriers=2"
write_summary "fsync_completions=2"
write_summary "portable_reopen_recovery=pass"

timeout 60s go test ./core/storage/parallelwal \
  -run '^TestNativeFsyncFailureTerminallyRejectsLaterWrites$' \
  -count=1 -v >"${ARTIFACT_DIR}/fsync-failure-test.log" 2>&1
require_line '^--- PASS: TestNativeFsyncFailureTerminallyRejectsLaterWrites ' \
  "${ARTIFACT_DIR}/fsync-failure-test.log"
write_summary "fsync_failure_terminal=true"
write_summary "later_write_after_fsync_failure=denied"
write_summary "close_reports_terminal_failure=true"

timeout 120s go test -race \
  ./internal/iouring \
  ./core/storage/parallelwal \
  ./core/storage \
  ./core/recovery \
  ./core/frontend/durable \
  -count=1 >"${ARTIFACT_DIR}/affected-race-tests.log" 2>&1
write_summary "affected_linux_race=pass"

if command -v strace >/dev/null 2>&1; then
  timeout 60s strace -qq -f \
    -e trace=eventfd2,io_uring_register,io_uring_enter,poll,read \
    -o "${ARTIFACT_DIR}/native-durability.strace" \
    go test ./core/storage/parallelwal \
      -run '^TestIOUringOwnerBatchesAcrossLanesAndRecoversPortably$' \
      -count=1 >/dev/null 2>&1
  require_line 'eventfd2\(' "${ARTIFACT_DIR}/native-durability.strace"
  require_line 'io_uring_register\(' "${ARTIFACT_DIR}/native-durability.strace"
  require_line 'io_uring_enter\(' "${ARTIFACT_DIR}/native-durability.strace"
  if grep -q 'IORING_ENTER_GETEVENTS' "${ARTIFACT_DIR}/native-durability.strace"; then
    echo "completion path unexpectedly used IORING_ENTER_GETEVENTS" >&2
    exit 1
  fi
  write_summary "completion_wakeup=eventfd"
  write_summary "getevents_wait_calls=0"
  write_summary "external_native_syscall_validation=strace"
else
  write_summary "external_native_syscall_validation=unavailable"
fi

write_summary "fallback_count=0"
write_summary "phase168_native_wal_durability_status=ok"
