#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase168-io-uring-capability-gate}"
SUMMARY="${ARTIFACT_DIR}/phase168-io-uring-capability-summary.txt"
PROBE_LOG="${ARTIFACT_DIR}/io-uring-probe.txt"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

require_probe_line() {
  local pattern="$1"
  if ! grep -Eq "${pattern}" "${PROBE_LOG}"; then
    echo "missing probe evidence: ${pattern}" >&2
    exit 1
  fi
}

write_summary "phase168_io_uring_capability_status=running"
write_summary "product_selector_added=false"
write_summary "parallelwal_integration_added=false"

cd "${ROOT}"
if [[ "$(go env GOOS)/$(go env GOARCH)" != "linux/amd64" ]]; then
  write_summary "phase168_io_uring_capability_status=unsupported"
  write_summary "reason=gate_requires_linux_amd64"
  exit 2
fi

go test ./cmd/io-uring-probe -count=1 -v \
  >"${ARTIFACT_DIR}/probe-tests.log" 2>&1
write_summary "linux_probe_test=pass"

go run ./cmd/io-uring-probe | tee "${PROBE_LOG}" >/dev/null
require_probe_line '^io_uring_probe_status=ok$'
require_probe_line '^platform=linux/amd64$'
require_probe_line '^io_uring_supported=true$'
require_probe_line '^refusal_reason=-$'
require_probe_line '^queue_depth=[4-9][0-9]*$'
require_probe_line '^write_opcode_supported=true$'
require_probe_line '^fsync_opcode_supported=true$'
require_probe_line '^submitted_ops=4$'
require_probe_line '^write_completions=3$'
require_probe_line '^fsync_completions=1$'
require_probe_line '^completion_count=4$'
require_probe_line '^verified_bytes=12288$'
require_probe_line '^implementation=raw_linux_uapi$'
require_probe_line '^dependency=golang.org/x/sys/unix$'
require_probe_line '^cgo_required=false$'
write_summary "linux_write_fsync_reopen=pass"
write_summary "required_opcodes=write,fsync"
write_summary "dependency_added=false"
write_summary "cgo_required=false"

CGO_ENABLED=0 GOOS=windows GOARCH=amd64 \
  go build -o "${ARTIFACT_DIR}/io-uring-probe-windows-amd64.exe" ./cmd/io-uring-probe
write_summary "windows_cross_compile=pass"
write_summary "unsupported_platform_boundary=explicit"

write_summary "phase168_io_uring_capability_status=ok"
