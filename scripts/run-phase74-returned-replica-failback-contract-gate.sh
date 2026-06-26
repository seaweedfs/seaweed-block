#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase74-returned-replica-failback-contract-gate}"
SUMMARY="${ARTIFACT_DIR}/phase74-returned-replica-failback-contract-summary.txt"
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

write_summary "phase74_returned_replica_failback_contract_status=running"
write_summary "phase74_scope=returned_replica_failback_contract"
write_summary "storage_mutation_allowed=false"
write_summary "frontend_publication_allowed=false"
write_summary "failback_execution_enabled=false"

OPS_PATTERN="TestEvaluateManagedVolumeAction_RejectsDisabledReturnedReplicaFailback|TestReturnedReplicaExecutorPreflight_FailbackReadyAfterAckEligibility|TestReturnedReplicaExecutorContract_DisablesFailbackAfterAckEligibility|TestManagedVolumeProjection_ReturnedReplicaFailbackActionAfterAckEligibility|TestOperatorStatusReconcilerWritesReturnedReplicaFailbackContract|TestObservationReportSummary_IncludesReturnedReplicaFailbackContract|TestReturnedReplicaExecutorContract_DisabledWhenPreflightReady|TestReturnedReplicaExecutorContract_DisablesRebuildTrafficWhenFrontierBehind"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_failback_contract_tests=pass"
else
  write_summary "core_ops_failback_contract_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

require_log "failback_action_policy_disabled" "^--- PASS: TestEvaluateManagedVolumeAction_RejectsDisabledReturnedReplicaFailback" "${OPS_LOG}"
require_log "failback_preflight_ready_after_ack" "^--- PASS: TestReturnedReplicaExecutorPreflight_FailbackReadyAfterAckEligibility" "${OPS_LOG}"
require_log "failback_contract_disabled" "^--- PASS: TestReturnedReplicaExecutorContract_DisablesFailbackAfterAckEligibility" "${OPS_LOG}"
require_log "failback_projection_visible_after_ack" "^--- PASS: TestManagedVolumeProjection_ReturnedReplicaFailbackActionAfterAckEligibility" "${OPS_LOG}"
require_log "failback_crd_contract_surface" "^--- PASS: TestOperatorStatusReconcilerWritesReturnedReplicaFailbackContract" "${OPS_LOG}"
require_log "failback_report_surface" "^--- PASS: TestObservationReportSummary_IncludesReturnedReplicaFailbackContract" "${OPS_LOG}"
require_log "ack_eligibility_contract_preserved" "^--- PASS: TestReturnedReplicaExecutorContract_DisabledWhenPreflightReady" "${OPS_LOG}"
require_log "rebuild_contract_preserved" "^--- PASS: TestReturnedReplicaExecutorContract_DisablesRebuildTrafficWhenFrontierBehind" "${OPS_LOG}"

write_summary "failback_allowed_mutation_class=failback"
write_summary "forbidden_mutation_classes=ack_eligibility,frontend_publication,rebuild_traffic"
write_summary "terminal_evidence_required=ack_eligible_true,frontend_fenced_before_failback,failback_authority_owner,authority_epoch_advanced,single_primary_after_failback,publish_target_swapped_after_failback,no_cross_volume_identity_change"
write_summary "failback_mutation_allowed=false"
write_summary "failback_runtime_invocations=0"
write_summary "frontend_publication_attempts=0"
write_summary "phase74_returned_replica_failback_contract_status=ok"
