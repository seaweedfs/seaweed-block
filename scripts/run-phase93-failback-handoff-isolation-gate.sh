#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase93-failback-handoff-isolation-gate}"
SUMMARY="${ARTIFACT_DIR}/phase93-failback-handoff-isolation-summary.txt"
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

write_summary "phase93_failback_handoff_isolation_status=running"
write_summary "phase93_scope=multi_volume_failback_target_executor_handoff_isolation"
write_summary "frontend_publication_after_failback_claimed=false"
write_summary "storage_mutation_allowed=false"

TEST_RE='TestFailbackTargetOwnerExecutorHandoffIsolatesMultipleVolumes|TestFailbackTargetOwnerExecutorHandoffUsesExpectedCurrentAuthority'
if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${TEST_RE}" -count=1 -v) >"${TEST_LOG}" 2>&1; then
  write_summary "go_test_core_ops_failback_handoff_isolation=pass"
else
  write_summary "go_test_core_ops_failback_handoff_isolation=fail"
  cat "${TEST_LOG}" >&2 || true
  exit 1
fi

TEST_FILE="${PRODUCT_ROOT}/core/ops/failback_target_executor_handoff_test.go"
require_text "multi_volume_test_present" "TestFailbackTargetOwnerExecutorHandoffIsolatesMultipleVolumes" "${TEST_FILE}"
require_text "two_targets_created_checked" "ownerResult.TargetCreateCount != 2" "${TEST_FILE}"
require_text "two_runtime_attempts_checked" "executorResult.FailbackAttempts != 2" "${TEST_FILE}"
require_text "volume_a_expected_current_checked" "assertPhase93RuntimeRequest(t, requestByVolume[\"pvc-a-id\"], \"pvc-a-id\", \"r1\", \"r2\", 7" "${TEST_FILE}"
require_text "volume_b_expected_current_checked" "assertPhase93RuntimeRequest(t, requestByVolume[\"pvc-b-id\"], \"pvc-b-id\", \"r3\", \"r4\", 11" "${TEST_FILE}"
require_text "request_mismatch_fails_test" "runtime request mismatch" "${TEST_FILE}"

write_summary "multi_volume_target_create_count=2"
write_summary "multi_volume_runtime_request_count=2"
write_summary "cross_volume_expected_current_mixup=false"
write_summary "cross_volume_target_addr_mixup=false"
write_summary "phase93_failback_handoff_isolation_status=ok"
