#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase75-failback-target-owner-gate}"
SUMMARY="${ARTIFACT_DIR}/phase75-failback-target-owner-summary.txt"
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

write_summary "phase75_failback_target_owner_status=running"
write_summary "phase75_scope=returned_replica_failback_target_owner"
write_summary "failback_attempts=0"
write_summary "storage_mutation_allowed=false"
write_summary "frontend_publication_allowed=false"

OPS_PATTERN="TestFailbackTargetOwner|TestPhase75SwBlockReplicaFailbackTargetSchema|TestPhase75FailbackTargetOwnerPackagingIsNarrow"
CMD_PATTERN="TestOpsFailbackTargetOwner"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_failback_target_owner_tests=pass"
else
  write_summary "core_ops_failback_target_owner_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./cmd/sw-block -run "${CMD_PATTERN}" -count=1 -v) >"${CMD_LOG}" 2>&1; then
  write_summary "cmd_failback_target_owner_tests=pass"
else
  write_summary "cmd_failback_target_owner_tests=fail"
  cat "${CMD_LOG}" >&2 || true
  exit 1
fi

require_log "failback_target_owner_creates_target" "^--- PASS: TestFailbackTargetOwnerCreatesTargetFromReadyContract" "${OPS_LOG}"
require_log "failback_target_owner_dry_run_no_create" "^--- PASS: TestFailbackTargetOwnerDryRunDoesNotCreateTarget" "${OPS_LOG}"
require_log "failback_target_owner_rejects_non_failback_contract" "^--- PASS: TestFailbackTargetOwnerRejectsNonFailbackContract" "${OPS_LOG}"
require_log "failback_target_owner_requires_terminal_evidence" "^--- PASS: TestFailbackTargetOwnerRequiresTerminalEvidence" "${OPS_LOG}"
require_log "failback_target_owner_skips_existing_target" "^--- PASS: TestFailbackTargetOwnerSkipsExistingTarget" "${OPS_LOG}"
require_log "failback_target_crd_schema" "^--- PASS: TestPhase75SwBlockReplicaFailbackTargetSchema" "${OPS_LOG}"
require_log "failback_target_owner_chart_boundary" "^--- PASS: TestPhase75FailbackTargetOwnerPackagingIsNarrow" "${OPS_LOG}"
require_log "failback_target_owner_cli_creates_target" "^--- PASS: TestOpsFailbackTargetOwnerCreatesTarget" "${CMD_LOG}"
require_log "failback_target_owner_cli_dry_run_no_create" "^--- PASS: TestOpsFailbackTargetOwnerDryRunDoesNotCreateTarget" "${CMD_LOG}"

write_summary "failback_target_kind=SwBlockReplicaFailback"
write_summary "failback_target_owner_disabled_by_default=true"
write_summary "failback_target_owner_dry_run_default=true"
write_summary "failback_target_owner_rbac_create_only=true"
write_summary "failback_target_owner_status_rbac=false"
write_summary "failback_target_owner_finalizer_rbac=false"
write_summary "failback_terminal_evidence_required=ack_eligible_true,frontend_fenced_before_failback,durable_frontier_covered,no_cross_volume_identity_change"
write_summary "phase75_failback_target_owner_status=ok"
