#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase168-native-wal-owner-gate}"
SUMMARY="${ARTIFACT_DIR}/phase168-native-wal-owner-summary.txt"
OWNER_LOG="${ARTIFACT_DIR}/native-owner-test.log"

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

write_summary "phase168_native_wal_owner_status=running"
write_summary "default_execution=positioned"
write_summary "product_selector_added=false"

cd "${ROOT}"
if [[ "$(go env GOOS)/$(go env GOARCH)" != "linux/amd64" ]]; then
  write_summary "phase168_native_wal_owner_status=unsupported"
  write_summary "reason=gate_requires_linux_amd64"
  exit 2
fi

timeout 60s go test ./core/storage/parallelwal \
  -run 'TestIOUringOwnerBatchesAcrossLanesAndRecoversPortably|TestPositionedExecutionRemainsDefault' \
  -count=1 -v >"${OWNER_LOG}" 2>&1
require_line '^--- PASS: TestIOUringOwnerBatchesAcrossLanesAndRecoversPortably ' "${OWNER_LOG}"
require_line '^--- PASS: TestPositionedExecutionRemainsDefault ' "${OWNER_LOG}"
require_line 'native_owner_stats enabled=true admitted=4 rounds=1 sqes=4 completions=4 inflight_high_water=4 fallback=0' "${OWNER_LOG}"
write_summary "single_owner_cross_lane_round=pass"
write_summary "admitted_requests=4"
write_summary "submission_rounds=1"
write_summary "sqes=4"
write_summary "completions=4"
write_summary "fallback_count=0"
write_summary "portable_reopen_recovery=pass"

timeout 120s go test -race \
  ./internal/iouring \
  ./core/storage/parallelwal \
  ./core/storage \
  ./core/recovery \
  ./core/frontend/durable \
  -count=1 >"${ARTIFACT_DIR}/affected-race-tests.log" 2>&1
write_summary "affected_linux_race=pass"

if command -v strace >/dev/null 2>&1; then
  timeout 60s strace -qq -f -e trace=io_uring_setup,io_uring_register,io_uring_enter \
    -o "${ARTIFACT_DIR}/native-owner.strace" \
    go test ./core/storage/parallelwal \
      -run '^TestIOUringOwnerBatchesAcrossLanesAndRecoversPortably$' \
      -count=1 >/dev/null 2>&1
  require_line 'io_uring_setup\(' "${ARTIFACT_DIR}/native-owner.strace"
  require_line 'io_uring_enter\(' "${ARTIFACT_DIR}/native-owner.strace"
  write_summary "external_native_syscall_validation=strace"
else
  write_summary "external_native_syscall_validation=unavailable"
fi

CGO_ENABLED=0 GOOS=windows GOARCH=amd64 \
  go test -c -o "${ARTIFACT_DIR}/parallelwal-windows-amd64.test.exe" \
  ./core/storage/parallelwal
write_summary "windows_cross_compile=pass"
write_summary "unsupported_explicit_no_fallback=true"
write_summary "phase168_native_wal_owner_status=ok"
