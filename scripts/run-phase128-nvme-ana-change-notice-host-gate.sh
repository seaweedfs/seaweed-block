#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase128-nvme-ana-change-notice-host-gate}"
SUMMARY="${ARTIFACT_DIR}/phase128-nvme-ana-change-notice-host-summary.txt"
INNER_DIR="${ARTIFACT_DIR}/mounted-failover"
BIN_DIR="${SW_BLOCK_BIN_DIR:-${ARTIFACT_DIR}/bin}"
TRACE_FILE="${ARTIFACT_DIR}/nvme-async-event.trace"
HOST_AER_SUMMARY="${ARTIFACT_DIR}/host-aer-summary.txt"
RUN_ID="${RUN_ID:-phase128-$(date -u +%Y%m%dT%H%M%SZ)}"

mkdir -p "${ARTIFACT_DIR}" "${INNER_DIR}" "${BIN_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

summary_value() {
  local path="$1"
  local key="$2"
  awk -F= -v key="$key" '$1 == key {value = substr($0, length(key) + 2)} END {print value}' "$path"
}

detect_trace_root() {
  if [[ -d /sys/kernel/tracing/events/nvme/nvme_async_event ]]; then
    printf '%s\n' /sys/kernel/tracing
    return 0
  fi
  if [[ -d /sys/kernel/debug/tracing/events/nvme/nvme_async_event ]]; then
    printf '%s\n' /sys/kernel/debug/tracing
    return 0
  fi
  return 1
}

TRACE_ROOT=""

disable_trace() {
  if [[ -n "${TRACE_ROOT}" ]]; then
    sudo -n bash -c 'echo 0 > "$1/tracing_on" 2>/dev/null || true; echo 0 > "$1/events/nvme/nvme_async_event/enable" 2>/dev/null || true' _ "${TRACE_ROOT}" || true
  fi
}

enable_trace() {
  TRACE_ROOT="$(detect_trace_root || true)"
  if [[ -z "${TRACE_ROOT}" ]]; then
    write_summary "phase128_nvme_ana_change_notice_host_gate_status=blocked_host_observability"
    write_summary "host_aer_observed=false"
    write_summary "host_aer_observation=missing_nvme_async_event_tracepoint"
    exit 2
  fi
  sudo -n bash -c 'echo 0 > "$1/tracing_on"; : > "$1/trace"; echo 1 > "$1/events/nvme/nvme_async_event/enable"; echo 1 > "$1/tracing_on"' _ "${TRACE_ROOT}"
}

collect_trace() {
  if [[ -n "${TRACE_ROOT}" ]]; then
    sudo -n bash -c 'echo 0 > "$1/tracing_on"; echo 0 > "$1/events/nvme/nvme_async_event/enable"; cat "$1/trace"' _ "${TRACE_ROOT}" >"${TRACE_FILE}" 2>"${ARTIFACT_DIR}/trace-read.stderr" || true
  fi
}

parse_host_aer() {
  python3 - "${TRACE_FILE}" >"${HOST_AER_SUMMARY}" <<'PY'
import re
import sys

path = sys.argv[1]
try:
    body = open(path, errors="replace").read().splitlines()
except OSError:
    body = []

events = []
for line in body:
    m = re.search(r"NVME_AEN=0x([0-9a-fA-F]+)", line)
    if not m:
        continue
    result = int(m.group(1), 16)
    event_type = result & 0x7
    event_info = (result >> 8) & 0xFF
    log_page = (result >> 16) & 0xFF
    events.append((result, event_type, event_info, log_page, line.strip()))

match = None
for event in events:
    if event[1] == 0x02 and event[2] == 0x03 and event[3] == 0x0C:
        match = event
        break

print(f"host_aer_event_count={len(events)}")
if match is None:
    print("host_aer_observed=false")
    print("host_aer_event_type=none")
    print("host_aer_event_info=none")
    print("host_aer_log_page=none")
    print("host_aer_result=none")
    if events:
        print("host_aer_first_result=0x%08x" % events[0][0])
    sys.exit(0)

print("host_aer_observed=true")
print("host_aer_result=0x%08x" % match[0])
print("host_aer_event_type=notice")
print("host_aer_event_info=ana_change")
print("host_aer_log_page=ana")
print("host_aer_trace_line=" + match[4])
PY
}

require_cmd sudo
require_cmd nvme
require_cmd python3

write_summary "phase128_nvme_ana_change_notice_host_gate_status=running"
write_summary "nvme_transport=tcp"
write_summary "k8s_dynamic_reconnect_claim=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"

trap 'collect_trace; disable_trace' EXIT
enable_trace

(
  cd "${ROOT}"
  RUN_ID="${RUN_ID}" \
  SW_BLOCK_ARTIFACT_DIR="${INNER_DIR}" \
  SW_BLOCK_BIN_DIR="${BIN_DIR}" \
  SW_BLOCK_NVME_ANA_NOTICE_GATE=1 \
  SW_BLOCK_NVME_NQN="nqn.2026-05.io.seaweedfs:phase128-ana-change-notice" \
  bash scripts/run-nvme-mounted-failover-smoke.sh "${ROOT}"
)

collect_trace
disable_trace
trap - EXIT

INNER_SUMMARY="${INNER_DIR}/phase101-nvme-path-failure-summary.txt"
if [[ ! -f "${INNER_SUMMARY}" ]]; then
  echo "missing inner summary: ${INNER_SUMMARY}" >&2
  exit 1
fi
cp "${INNER_SUMMARY}" "${ARTIFACT_DIR}/phase101-inner-summary.txt"
parse_host_aer

cat "${HOST_AER_SUMMARY}" >>"${SUMMARY}"

inner_status="$(summary_value "${INNER_SUMMARY}" phase101_nvme_path_failure_status)"
oaes="$(summary_value "${INNER_SUMMARY}" oaes_ana_change_notice_advertised)"
before_count="$(summary_value "${INNER_SUMMARY}" ana_log_change_count_before)"
after_count="$(summary_value "${INNER_SUMMARY}" ana_log_change_count_after)"
count_advanced="$(summary_value "${INNER_SUMMARY}" ana_log_change_count_advanced)"
after_paths="$(summary_value "${INNER_SUMMARY}" after_path_count)"
mounted_io="$(summary_value "${INNER_SUMMARY}" mounted_io_after_notice)"
target_aer_parked_count="$(grep -Rhc "nvme: AER parked" "${INNER_DIR}"/blockvolume-*.log 2>/dev/null | awk '{s += $1} END {print s + 0}')"
target_aer_completed_count="$(grep -Rhc "nvme: AER completing" "${INNER_DIR}"/blockvolume-*.log 2>/dev/null | awk '{s += $1} END {print s + 0}')"
host_aer_observed="$(summary_value "${HOST_AER_SUMMARY}" host_aer_observed)"

write_summary "inner_phase101_status=${inner_status}"
write_summary "oaes_ana_change_notice_advertised=${oaes}"
write_summary "ana_log_change_count_before=${before_count}"
write_summary "ana_log_change_count_after=${after_count}"
write_summary "ana_log_change_count_advanced=${count_advanced}"
write_summary "host_path_state_refreshed=$([[ "${after_paths}" == "1" ]] && echo true || echo false)"
write_summary "mounted_io_after_notice=${mounted_io}"
write_summary "target_aer_parked_count=${target_aer_parked_count}"
write_summary "target_aer_completed_count=${target_aer_completed_count}"

if [[ "${inner_status}" != "ok" ]]; then
  write_summary "phase128_nvme_ana_change_notice_host_gate_status=failed_inner_gate"
  exit 1
fi
if [[ "${oaes}" != "true" ]]; then
  write_summary "phase128_nvme_ana_change_notice_host_gate_status=failed_oaes_not_advertised"
  exit 1
fi
if [[ "${count_advanced}" != "true" ]]; then
  write_summary "phase128_nvme_ana_change_notice_host_gate_status=failed_ana_change_count_not_advanced"
  exit 1
fi
if [[ "${mounted_io}" != "ok" ]]; then
  write_summary "phase128_nvme_ana_change_notice_host_gate_status=failed_mounted_io"
  exit 1
fi
if [[ "${host_aer_observed}" != "true" ]]; then
  if [[ "${target_aer_parked_count}" -gt 0 || "${target_aer_completed_count}" -gt 0 ]]; then
    write_summary "phase128_nvme_ana_change_notice_host_gate_status=blocked_host_observability"
  else
    write_summary "phase128_nvme_ana_change_notice_host_gate_status=blocked_host_aer_not_posted"
  fi
  write_summary "cleanup_status=ok"
  exit 2
fi

write_summary "cleanup_status=ok"
write_summary "phase128_nvme_ana_change_notice_host_gate_status=ok"
