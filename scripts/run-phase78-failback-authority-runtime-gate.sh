#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase78-failback-authority-runtime-gate}"
SUMMARY="${ARTIFACT_DIR}/phase78-failback-authority-runtime-summary.txt"
AUTHORITY_LOG="${ARTIFACT_DIR}/core-authority-go-test.log"
OPS_LOG="${ARTIFACT_DIR}/core-ops-go-test.log"
CMD_LOG="${ARTIFACT_DIR}/cmd-sw-block-go-test.log"

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

write_summary "phase78_failback_authority_runtime_status=running"
write_summary "phase78_scope=failback_authority_runtime"
write_summary "default_failback_attempts=0"
write_summary "default_authority_mutation_allowed=false"
write_summary "default_frontend_publication_allowed=false"
write_summary "default_storage_mutation_allowed=false"

AUTHORITY_PATTERN="TestFailbackAuthorityRuntime"
OPS_PATTERN="TestFailbackExecutor|TestFailbackTargetOwner|TestHTTPFailbackRuntime|TestPhase46D2SwBlockVolumeReturnedReplicaSchema|TestPhase75SwBlockReplicaFailbackTargetSchema|TestKubernetesStatusClientCreatesSwBlockReplicaFailbackWithoutStatus"
CMD_PATTERN="TestOpsFailback"

if (cd "${PRODUCT_ROOT}" && go test ./core/authority -run "${AUTHORITY_PATTERN}" -count=1 -v) >"${AUTHORITY_LOG}" 2>&1; then
  write_summary "core_authority_failback_runtime_tests=pass"
else
  write_summary "core_authority_failback_runtime_tests=fail"
  cat "${AUTHORITY_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_failback_authority_runtime_tests=pass"
else
  write_summary "core_ops_failback_authority_runtime_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./cmd/sw-block -run "${CMD_PATTERN}" -count=1 -v) >"${CMD_LOG}" 2>&1; then
  write_summary "cmd_failback_authority_runtime_tests=pass"
else
  write_summary "cmd_failback_authority_runtime_tests=fail"
  cat "${CMD_LOG}" >&2 || true
  exit 1
fi

require_log "authority_failback_reassign_minted" "^--- PASS: TestFailbackAuthorityRuntime_ReassignsThroughPublisher" "${AUTHORITY_LOG}"
require_log "stale_expected_current_rejected" "^--- PASS: TestFailbackAuthorityRuntime_RejectsStaleExpectedCurrent" "${AUTHORITY_LOG}"
require_log "terminal_preconditions_required" "^--- PASS: TestFailbackAuthorityRuntime_RequiresTerminalPreconditions" "${AUTHORITY_LOG}"
require_log "failback_target_endpoint_fields" "^--- PASS: TestFailbackTargetOwnerCreatesTargetFromReadyContract" "${OPS_LOG}"
require_log "failback_target_expected_current_fields" "^--- PASS: TestFailbackExecutorInvokesRuntimeWhenExplicitlyEnabled" "${OPS_LOG}"
require_log "executable_failback_requires_authority_endpoint" "^--- PASS: TestFailbackExecutorMarksInvalidTargets" "${OPS_LOG}"
require_log "http_runtime_contract_includes_authority_fields" "^--- PASS: TestHTTPFailbackRuntimePostsRequestAndDecodesResult" "${OPS_LOG}"
require_log "swblockvolume_returned_replica_endpoint_schema" "^--- PASS: TestPhase46D2SwBlockVolumeReturnedReplicaSchema" "${OPS_LOG}"
require_log "failback_target_schema_authority_fields" "^--- PASS: TestPhase75SwBlockReplicaFailbackTargetSchema" "${OPS_LOG}"
require_log "target_writer_serializes_authority_fields" "^--- PASS: TestKubernetesStatusClientCreatesSwBlockReplicaFailbackWithoutStatus" "${OPS_LOG}"
require_log "cmd_default_executor_still_disabled" "^--- PASS: TestOpsFailbackExecutorWritesDisabledStatus" "${CMD_LOG}"
require_log "cmd_runtime_success_allows_authority_mutation" "^--- PASS: TestOpsFailbackExecutorRuntimeURLWritesFailedBackStatus" "${CMD_LOG}"

write_summary "authority_epoch_advanced=true"
write_summary "single_primary_after_failback=true"
write_summary "publish_target_swapped_after_failback=true"
write_summary "explicit_runtime_authority_mutation_allowed=true"
write_summary "storage_mutation_allowed=false"
write_summary "frontend_publication_allowed=false"
write_summary "phase78_failback_authority_runtime_status=ok"
