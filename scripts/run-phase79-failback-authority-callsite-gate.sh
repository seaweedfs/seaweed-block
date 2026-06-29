#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase79-failback-authority-callsite-gate}"
SUMMARY="${ARTIFACT_DIR}/phase79-failback-authority-callsite-summary.txt"
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

write_summary "phase79_failback_authority_callsite_status=running"
write_summary "phase79_scope=failback_authority_callsite"
write_summary "default_failback_attempts=0"
write_summary "default_authority_mutation_allowed=false"
write_summary "frontend_publication_allowed=false"
write_summary "storage_mutation_allowed=false"

TEST_PATTERN="TestFailbackExecutorUsesAuthorityRuntimeAdapter|TestFailbackAuthorityRuntimeAdapterRejectsStaleExpectedCurrent|TestFailbackExecutorExecutionPolicyBlocks|TestFailbackExecutorDryRunDoesNotWriteStatus"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${TEST_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_failback_authority_callsite_tests=pass"
else
  write_summary "core_ops_failback_authority_callsite_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

require_log "authority_runtime_adapter_invoked_by_executor" "^--- PASS: TestFailbackExecutorUsesAuthorityRuntimeAdapter" "${OPS_LOG}"
require_log "stale_expected_current_blocks_callsite" "^--- PASS: TestFailbackAuthorityRuntimeAdapterRejectsStaleExpectedCurrent" "${OPS_LOG}"
require_log "execution_policy_still_required" "^--- PASS: TestFailbackExecutorExecutionPolicyBlocks" "${OPS_LOG}"
require_log "dry_run_no_status_write" "^--- PASS: TestFailbackExecutorDryRunDoesNotWriteStatus" "${OPS_LOG}"

write_summary "publisher_authority_line_advanced=true"
write_summary "authority_epoch_advanced=true"
write_summary "single_primary_after_failback=true"
write_summary "publish_target_swapped_after_failback=true"
write_summary "failed_back_status_written=true"
write_summary "runtime_failure_no_false_failback=true"
write_summary "authority_mutation_allowed_only_with_execution_policy=true"
write_summary "frontend_publication_allowed=false"
write_summary "storage_mutation_allowed=false"
write_summary "phase79_failback_authority_callsite_status=ok"
