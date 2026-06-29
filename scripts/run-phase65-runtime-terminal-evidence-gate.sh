#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase65-runtime-terminal-evidence-gate}"
SUMMARY="${ARTIFACT_DIR}/phase65-runtime-terminal-evidence-summary.txt"
TRANSPORT_LOG="${ARTIFACT_DIR}/core-transport-go-test.log"
REPLICATION_LOG="${ARTIFACT_DIR}/core-replication-go-test.log"
VOLUME_LOG="${ARTIFACT_DIR}/core-host-volume-go-test.log"
OPS_LOG="${ARTIFACT_DIR}/core-ops-go-test.log"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

require_log() {
  local name="$1"
  local pattern="$2"
  local file="$3"
  local found="false"
  if grep -Eq -- "${pattern}" "${file}"; then
    found="true"
  fi
  write_summary "${name}=${found}"
  if [ "${found}" != "true" ]; then
    echo "missing evidence ${name}: pattern ${pattern}" >&2
    return 1
  fi
}

write_summary "phase65_runtime_terminal_evidence_status=running"
write_summary "phase65_scope=runtime_started_to_caught_up_evidence"
write_summary "frontend_publication_allowed=false"
write_summary "failback_allowed=false"
write_summary "ack_eligibility_mutation_allowed=false"

TRANSPORT_PATTERN="TestTransport_Rebuild_RecordsTerminalSessionStatus"
REPLICATION_PATTERN="TestReplicationVolume_RuntimeRecoveryStatus_ReportsTerminalFrontier"
VOLUME_PATTERN="TestStatusServer_RuntimeRebuild_ReturnsTerminalEvidenceWithoutRestart|TestStatusServer_RuntimeRebuild_StartsRecoveryWithExactLineage"
OPS_PATTERN="TestAuthorityExecutorReconcilerTransitionsFromStartedToCaughtUpOnTerminalRuntimeEvidence|TestAuthorityExecutorReconcilerKeepsRunningWhenRuntimeOnlyStarts|TestAuthorityExecutorReconcilerExecutesRebuildRuntimeAndWritesCaughtUpStatus"

if (cd "${PRODUCT_ROOT}" && go test ./core/transport -run "${TRANSPORT_PATTERN}" -count=1 -v) >"${TRANSPORT_LOG}" 2>&1; then
  write_summary "core_transport_terminal_status_tests=pass"
else
  write_summary "core_transport_terminal_status_tests=fail"
  cat "${TRANSPORT_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./core/replication -run "${REPLICATION_PATTERN}" -count=1 -v) >"${REPLICATION_LOG}" 2>&1; then
  write_summary "core_replication_terminal_status_tests=pass"
else
  write_summary "core_replication_terminal_status_tests=fail"
  cat "${REPLICATION_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./core/host/volume -run "${VOLUME_PATTERN}" -count=1 -v) >"${VOLUME_LOG}" 2>&1; then
  write_summary "core_host_volume_terminal_endpoint_tests=pass"
else
  write_summary "core_host_volume_terminal_endpoint_tests=fail"
  cat "${VOLUME_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_terminal_transition_tests=pass"
else
  write_summary "core_ops_terminal_transition_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

require_log "transport_records_caught_up_session" "^--- PASS: TestTransport_Rebuild_RecordsTerminalSessionStatus" "${TRANSPORT_LOG}"
require_log "replication_reports_terminal_frontier" "^--- PASS: TestReplicationVolume_RuntimeRecoveryStatus_ReportsTerminalFrontier" "${REPLICATION_LOG}"
require_log "runtime_endpoint_returns_caught_up_without_restart" "^--- PASS: TestStatusServer_RuntimeRebuild_ReturnsTerminalEvidenceWithoutRestart" "${VOLUME_LOG}"
require_log "runtime_endpoint_still_starts_unknown_session" "^--- PASS: TestStatusServer_RuntimeRebuild_StartsRecoveryWithExactLineage" "${VOLUME_LOG}"
require_log "authority_executor_started_then_caught_up" "^--- PASS: TestAuthorityExecutorReconcilerTransitionsFromStartedToCaughtUpOnTerminalRuntimeEvidence" "${OPS_LOG}"

write_summary "runtime_terminal_status_recorded=true"
write_summary "runtime_terminal_frontier_reported=true"
write_summary "runtime_endpoint_terminal_caught_up=true"
write_summary "runtime_endpoint_terminal_does_not_restart=true"
write_summary "authority_executor_running_to_caught_up=true"
write_summary "runtime_start_without_terminal_still_running=true"
write_summary "phase65_runtime_terminal_evidence_status=ok"
