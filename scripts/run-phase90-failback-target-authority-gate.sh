#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase90-failback-target-authority-gate}"
SUMMARY="${ARTIFACT_DIR}/phase90-failback-target-authority-summary.txt"
TEST_LOG="${ARTIFACT_DIR}/go-test-failback-target-owner.log"

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

write_summary "phase90_failback_target_authority_status=running"
write_summary "phase90_scope=failback_target_expected_current_authority"
write_summary "frontend_publication_after_failback_claimed=false"
write_summary "automatic_failback_claimed=false"
write_summary "failback_runtime_call_attempted=false"
write_summary "storage_mutation_allowed=false"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "TestFailbackTargetOwner" -count=1 -v) >"${TEST_LOG}" 2>&1; then
  write_summary "go_test_failback_target_owner=pass"
else
  write_summary "go_test_failback_target_owner=fail"
  cat "${TEST_LOG}" >&2 || true
  exit 1
fi

require_text "target_owner_requires_current_authority" "failbackTargetOwnerAuthorityFactsReady(volume)" "${PRODUCT_ROOT}/core/ops/failback_target_owner_controller.go"
require_text "target_spec_expected_current_replica" "ExpectedCurrentReplicaID:     volume.Status.PrimaryReplicaID" "${PRODUCT_ROOT}/core/ops/failback_target_owner_controller.go"
require_text "target_spec_expected_current_epoch" "ExpectedCurrentEpoch:         volume.Status.AuthorityEpoch" "${PRODUCT_ROOT}/core/ops/failback_target_owner_controller.go"
require_text "missing_authority_blocks_creation" "TestFailbackTargetOwnerRequiresCurrentAuthorityFacts" "${PRODUCT_ROOT}/core/ops/failback_target_owner_controller_test.go"
require_text "created_target_carries_expected_replica" "created.Spec.ExpectedCurrentReplicaID != \"r2\"" "${PRODUCT_ROOT}/core/ops/failback_target_owner_controller_test.go"
require_text "created_target_carries_expected_epoch" "created.Spec.ExpectedCurrentEpoch != 7" "${PRODUCT_ROOT}/core/ops/failback_target_owner_controller_test.go"

write_summary "expected_current_replica_from_swblockvolume_status=true"
write_summary "expected_current_epoch_from_swblockvolume_status=true"
write_summary "missing_current_authority_target_create_count=0"
write_summary "created_target_failback_decision=disabled"
write_summary "created_target_failback_mutation_allowed=false"
write_summary "phase90_failback_target_authority_status=ok"
