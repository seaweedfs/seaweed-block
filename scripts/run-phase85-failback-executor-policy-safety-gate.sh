#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase85-failback-executor-policy-safety-gate}"
SUMMARY="${ARTIFACT_DIR}/phase85-failback-executor-policy-safety-summary.txt"
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

write_summary "phase85_failback_executor_policy_safety_status=running"
write_summary "phase85_scope=failback_executor_no_target_no_runtime"
write_summary "frontend_publication_allowed=false"
write_summary "storage_mutation_allowed=false"

TEST_PATTERN="TestFailbackExecutor(ExecutionPolicyBlocks|ExecutionNoTargetsDoesNotAttemptRuntime|ExecutionInvalidTargetDoesNotCallRuntime|InvokesRuntimeWhenExplicitlyEnabled)"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${TEST_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_failback_policy_safety_tests=pass"
else
  write_summary "core_ops_failback_policy_safety_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

require_log "policy_disabled_blocks_execution" "^--- PASS: TestFailbackExecutorExecutionPolicyBlocks" "${OPS_LOG}"
require_log "no_targets_no_runtime_call" "^--- PASS: TestFailbackExecutorExecutionNoTargetsDoesNotAttemptRuntime" "${OPS_LOG}"
require_log "invalid_target_no_runtime_call" "^--- PASS: TestFailbackExecutorExecutionInvalidTargetDoesNotCallRuntime" "${OPS_LOG}"
require_log "valid_target_runtime_call_still_supported" "^--- PASS: TestFailbackExecutorInvokesRuntimeWhenExplicitlyEnabled" "${OPS_LOG}"

write_summary "execution_flags_alone_insufficient=true"
write_summary "runtime_requires_valid_target=true"
write_summary "invalid_target_writes_blocked_status=true"
write_summary "authority_mutation_allowed_only_for_valid_target=true"
write_summary "frontend_publication_allowed=false"
write_summary "storage_mutation_allowed=false"
write_summary "phase85_failback_executor_policy_safety_status=ok"
