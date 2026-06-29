#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase92-failback-target-executor-handoff-gate}"
SUMMARY="${ARTIFACT_DIR}/phase92-failback-target-executor-handoff-summary.txt"
TEST_LOG="${ARTIFACT_DIR}/go-test-core-ops.log"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

require_text() {
  local name="$1"
  local pattern="$2"
  local file="$3"
  local found="false"
  if grep -Fq -- "${pattern}" "${file}"; then
    found="true"
  fi
  write_summary "${name}=${found}"
  if [ "${found}" != "true" ]; then
    echo "missing ${name}: ${pattern}" >&2
    return 1
  fi
}

write_summary "phase92_failback_target_executor_handoff_status=running"
write_summary "phase92_scope=target_owner_to_executor_handoff"
write_summary "frontend_publication_after_failback_claimed=false"
write_summary "storage_mutation_allowed=false"

TEST_RE='TestFailbackTargetOwnerExecutorHandoffUsesExpectedCurrentAuthority|TestFailbackTargetOwner|TestFailbackExecutorInvokesRuntimeWhenExplicitlyEnabled'
if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${TEST_RE}" -count=1 -v) >"${TEST_LOG}" 2>&1; then
  write_summary "go_test_core_ops_failback_handoff=pass"
else
  write_summary "go_test_core_ops_failback_handoff=fail"
  cat "${TEST_LOG}" >&2 || true
  exit 1
fi

require_text "handoff_test_present" "TestFailbackTargetOwnerExecutorHandoffUsesExpectedCurrentAuthority" "${PRODUCT_ROOT}/core/ops/failback_target_executor_handoff_test.go"
require_text "runtime_request_expected_replica_checked" "req.ExpectedCurrentReplicaID != \"r2\"" "${PRODUCT_ROOT}/core/ops/failback_target_executor_handoff_test.go"
require_text "runtime_request_expected_epoch_checked" "req.ExpectedCurrentEpoch != 7" "${PRODUCT_ROOT}/core/ops/failback_target_executor_handoff_test.go"
require_text "terminal_status_failed_back_checked" "status.State != FailbackStateFailedBack" "${PRODUCT_ROOT}/core/ops/failback_target_executor_handoff_test.go"
require_text "frontend_publication_remains_false_checked" "executorResult.FrontendPublicationAllowed" "${PRODUCT_ROOT}/core/ops/failback_target_executor_handoff_test.go"
require_text "storage_mutation_remains_false_checked" "executorResult.StorageMutationAllowed" "${PRODUCT_ROOT}/core/ops/failback_target_executor_handoff_test.go"

write_summary "target_owner_created_enabled_target=true"
write_summary "executor_consumed_target=true"
write_summary "runtime_request_expected_current_replica=r2"
write_summary "runtime_request_expected_current_epoch=7"
write_summary "executor_terminal_state=failed_back"
write_summary "phase92_failback_target_executor_handoff_status=ok"
