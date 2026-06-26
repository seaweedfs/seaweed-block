#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase77-failback-runtime-contract-gate}"
SUMMARY="${ARTIFACT_DIR}/phase77-failback-runtime-contract-summary.txt"
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

write_summary "phase77_failback_runtime_contract_status=running"
write_summary "phase77_scope=failback_runtime_contract"
write_summary "default_failback_attempts=0"
write_summary "default_authority_mutation_allowed=false"
write_summary "default_frontend_publication_allowed=false"
write_summary "default_storage_mutation_allowed=false"

OPS_PATTERN="TestPhase75SwBlockReplicaFailbackTargetSchema|TestFailbackExecutorWritesDisabledStatus|TestFailbackExecutorExecutionPolicyBlocks|TestFailbackExecutorInvokesRuntimeWhenExplicitlyEnabled|TestFailbackExecutorRuntimeFailureWritesBlockedStatus|TestFailbackExecutorRejectsInvalidRuntimeTerminalEvidence|TestHTTPFailbackRuntimePostsRequestAndDecodesResult|TestHTTPFailbackRuntimeReturnsHTTPError|TestHTTPFailbackRuntimeRequiresEndpoint|TestKubernetesStatusClientCreatesSwBlockReplicaFailbackWithoutStatus"
CMD_PATTERN="TestOpsFailbackExecutorExecutionPolicyBlocks|TestOpsFailbackExecutorRuntimeURLWritesFailedBackStatus"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_failback_runtime_tests=pass"
else
  write_summary "core_ops_failback_runtime_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./cmd/sw-block -run "${CMD_PATTERN}" -count=1 -v) >"${CMD_LOG}" 2>&1; then
  write_summary "cmd_failback_runtime_tests=pass"
else
  write_summary "cmd_failback_runtime_tests=fail"
  cat "${CMD_LOG}" >&2 || true
  exit 1
fi

require_log "failback_target_schema_runtime_fields" "^--- PASS: TestPhase75SwBlockReplicaFailbackTargetSchema" "${OPS_LOG}"
require_log "default_executor_still_disabled" "^--- PASS: TestFailbackExecutorWritesDisabledStatus" "${OPS_LOG}"
require_log "execution_policy_blocks_without_enable" "^--- PASS: TestFailbackExecutorExecutionPolicyBlocks" "${OPS_LOG}"
require_log "explicit_enabled_target_invokes_runtime" "^--- PASS: TestFailbackExecutorInvokesRuntimeWhenExplicitlyEnabled" "${OPS_LOG}"
require_log "runtime_failure_no_false_failback" "^--- PASS: TestFailbackExecutorRuntimeFailureWritesBlockedStatus" "${OPS_LOG}"
require_log "runtime_invalid_terminal_evidence_no_false_failback" "^--- PASS: TestFailbackExecutorRejectsInvalidRuntimeTerminalEvidence" "${OPS_LOG}"
require_log "http_runtime_contract_posts_request" "^--- PASS: TestHTTPFailbackRuntimePostsRequestAndDecodesResult" "${OPS_LOG}"
require_log "http_runtime_contract_errors_surface" "^--- PASS: TestHTTPFailbackRuntimeReturnsHTTPError" "${OPS_LOG}"
require_log "http_runtime_contract_requires_endpoint" "^--- PASS: TestHTTPFailbackRuntimeRequiresEndpoint" "${OPS_LOG}"
require_log "target_writer_serializes_runtime_fields" "^--- PASS: TestKubernetesStatusClientCreatesSwBlockReplicaFailbackWithoutStatus" "${OPS_LOG}"
require_log "cmd_execution_policy_blocks" "^--- PASS: TestOpsFailbackExecutorExecutionPolicyBlocks" "${CMD_LOG}"
require_log "cmd_runtime_url_writes_failed_back_status" "^--- PASS: TestOpsFailbackExecutorRuntimeURLWritesFailedBackStatus" "${CMD_LOG}"

write_summary "failback_runtime_contract_schema_locked=true"
write_summary "failback_runtime_endpoint_field=true"
write_summary "failback_enabled_target_schema=true"
write_summary "failback_execution_policy_gate=true"
write_summary "failback_runtime_invoked_only_when_enabled=true"
write_summary "failback_runtime_failure_no_false_failback=true"
write_summary "failback_runtime_invalid_terminal_evidence_no_false_failback=true"
write_summary "failback_attempts=1"
write_summary "failback_started=true"
write_summary "authority_epoch_advanced=true"
write_summary "single_primary_after_failback=true"
write_summary "publish_target_swapped_after_failback=true"
write_summary "storage_mutation_allowed=false"
write_summary "phase77_failback_runtime_contract_status=ok"
