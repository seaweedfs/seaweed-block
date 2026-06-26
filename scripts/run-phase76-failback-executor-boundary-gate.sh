#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase76-failback-executor-boundary-gate}"
SUMMARY="${ARTIFACT_DIR}/phase76-failback-executor-boundary-summary.txt"
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

write_summary "phase76_failback_executor_boundary_status=running"
write_summary "phase76_scope=failback_executor_status_boundary"
write_summary "failback_attempts=0"
write_summary "authority_mutation_allowed=false"
write_summary "frontend_publication_allowed=false"
write_summary "storage_mutation_allowed=false"
write_summary "authority_epoch_advanced=false"
write_summary "single_primary_after_failback=false"
write_summary "publish_target_swapped_after_failback=false"

OPS_PATTERN="TestPhase75SwBlockReplicaFailbackTargetSchema|TestPhase76FailbackExecutorPackagingIsStatusOnly|TestKubernetesStatusClientPatchesOnlyStatusSubresources|TestFailbackExecutorWritesDisabledStatus|TestFailbackExecutorDryRunDoesNotWriteStatus|TestFailbackExecutorMarksInvalidTargets"
CMD_PATTERN="TestOpsFailbackExecutorWritesDisabledStatus|TestOpsFailbackExecutorDryRunDoesNotWriteStatus"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_failback_executor_tests=pass"
else
  write_summary "core_ops_failback_executor_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./cmd/sw-block -run "${CMD_PATTERN}" -count=1 -v) >"${CMD_LOG}" 2>&1; then
  write_summary "cmd_failback_executor_tests=pass"
else
  write_summary "cmd_failback_executor_tests=fail"
  cat "${CMD_LOG}" >&2 || true
  exit 1
fi

require_log "failback_target_schema_locked" "^--- PASS: TestPhase75SwBlockReplicaFailbackTargetSchema" "${OPS_LOG}"
require_log "failback_executor_rbac_status_only" "^--- PASS: TestPhase76FailbackExecutorPackagingIsStatusOnly" "${OPS_LOG}"
require_log "kubernetes_writer_failback_status_subresource" "^--- PASS: TestKubernetesStatusClientPatchesOnlyStatusSubresources" "${OPS_LOG}"
require_log "failback_executor_writes_disabled_status" "^--- PASS: TestFailbackExecutorWritesDisabledStatus" "${OPS_LOG}"
require_log "failback_executor_dry_run_no_status_write" "^--- PASS: TestFailbackExecutorDryRunDoesNotWriteStatus" "${OPS_LOG}"
require_log "failback_executor_invalid_target_blocked" "^--- PASS: TestFailbackExecutorMarksInvalidTargets" "${OPS_LOG}"
require_log "cmd_failback_executor_writes_status" "^--- PASS: TestOpsFailbackExecutorWritesDisabledStatus" "${CMD_LOG}"
require_log "cmd_failback_executor_dry_run_no_status_write" "^--- PASS: TestOpsFailbackExecutorDryRunDoesNotWriteStatus" "${CMD_LOG}"

write_summary "failback_executor_status_writes=true"
write_summary "failback_executor_status=blocked"
write_summary "failback_executor_reason=failback_policy_disabled"
write_summary "failback_executor_status_mutation_allowed=true"
write_summary "failback_mutation_allowed=false"
write_summary "failback_started=false"
write_summary "failback_executor_rbac_status_only=true"
write_summary "phase76_failback_executor_boundary_status=ok"
