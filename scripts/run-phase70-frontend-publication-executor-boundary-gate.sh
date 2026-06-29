#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase70-frontend-publication-executor-boundary-gate}"
SUMMARY="${ARTIFACT_DIR}/phase70-frontend-publication-executor-boundary-summary.txt"
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

write_summary "phase70_frontend_publication_executor_boundary_status=running"
write_summary "phase70_scope=frontend_publication_executor_status_boundary"
write_summary "frontend_publication_attempts=0"
write_summary "failback_attempts=0"
write_summary "storage_mutation_allowed=false"
write_summary "frontend_published=false"
write_summary "failback_started=false"

OPS_PATTERN="TestPhase69SwBlockFrontendPublicationTargetSchema|TestPhase70FrontendPublicationExecutorPackagingIsStatusOnly|TestKubernetesStatusClientPatchesOnlyStatusSubresources|TestFrontendPublicationExecutorWritesDisabledStatus|TestFrontendPublicationExecutorDryRunDoesNotWriteStatus|TestFrontendPublicationExecutorMarksInvalidTargets"
CMD_PATTERN="TestOpsFrontendPublicationExecutorWritesDisabledStatus|TestOpsFrontendPublicationExecutorDryRunDoesNotWriteStatus"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_frontend_executor_tests=pass"
else
  write_summary "core_ops_frontend_executor_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./cmd/sw-block -run "${CMD_PATTERN}" -count=1 -v) >"${CMD_LOG}" 2>&1; then
  write_summary "cmd_sw_block_frontend_executor_tests=pass"
else
  write_summary "cmd_sw_block_frontend_executor_tests=fail"
  cat "${CMD_LOG}" >&2 || true
  exit 1
fi

require_log "frontend_publication_target_schema_locked" "^--- PASS: TestPhase69SwBlockFrontendPublicationTargetSchema" "${OPS_LOG}"
require_log "frontend_publication_executor_rbac_status_only" "^--- PASS: TestPhase70FrontendPublicationExecutorPackagingIsStatusOnly" "${OPS_LOG}"
require_log "kubernetes_writer_frontend_publication_status_subresource" "^--- PASS: TestKubernetesStatusClientPatchesOnlyStatusSubresources" "${OPS_LOG}"
require_log "frontend_publication_executor_writes_disabled_status" "^--- PASS: TestFrontendPublicationExecutorWritesDisabledStatus" "${OPS_LOG}"
require_log "frontend_publication_executor_dry_run_no_status_write" "^--- PASS: TestFrontendPublicationExecutorDryRunDoesNotWriteStatus" "${OPS_LOG}"
require_log "frontend_publication_executor_invalid_target_blocked" "^--- PASS: TestFrontendPublicationExecutorMarksInvalidTargets" "${OPS_LOG}"
require_log "cmd_frontend_publication_executor_writes_status" "^--- PASS: TestOpsFrontendPublicationExecutorWritesDisabledStatus" "${CMD_LOG}"
require_log "cmd_frontend_publication_executor_dry_run_no_status_write" "^--- PASS: TestOpsFrontendPublicationExecutorDryRunDoesNotWriteStatus" "${CMD_LOG}"

write_summary "frontend_publication_executor_status_writes=true"
write_summary "frontend_publication_executor_status=blocked"
write_summary "frontend_publication_executor_reason=frontend_publication_policy_disabled"
write_summary "frontend_publication_executor_status_mutation_allowed=true"
write_summary "frontend_publication_mutation_allowed=false"
write_summary "frontend_publication_executor_rbac_status_only=true"
write_summary "phase70_frontend_publication_executor_boundary_status=ok"
