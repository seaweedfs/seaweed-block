#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase69-frontend-publication-target-contract-gate}"
SUMMARY="${ARTIFACT_DIR}/phase69-frontend-publication-target-contract-summary.txt"
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

write_summary "phase69_frontend_publication_target_contract_status=running"
write_summary "phase69_scope=frontend_publication_target_contract"
write_summary "frontend_publication_attempts=0"
write_summary "failback_attempts=0"
write_summary "storage_mutation_allowed=false"

OPS_PATTERN="TestPhase69SwBlockFrontendPublicationTargetSchema|TestPhase69FrontendPublicationTargetOwnerPackagingIsNarrow|TestKubernetesStatusClientCreatesSwBlockFrontendPublicationWithoutStatus|TestFrontendPublicationTargetOwnerDryRunPlansTargetWithoutCreate|TestFrontendPublicationTargetOwnerCreatesMissingTarget|TestFrontendPublicationTargetOwnerRejectsEnabledPublication"
CMD_PATTERN="TestOpsFrontendPublicationTargetOwnerCreatesTarget|TestOpsFrontendPublicationTargetOwnerDryRunDoesNotCreateTarget"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_frontend_target_tests=pass"
else
  write_summary "core_ops_frontend_target_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./cmd/sw-block -run "${CMD_PATTERN}" -count=1 -v) >"${CMD_LOG}" 2>&1; then
  write_summary "cmd_sw_block_frontend_target_tests=pass"
else
  write_summary "cmd_sw_block_frontend_target_tests=fail"
  cat "${CMD_LOG}" >&2 || true
  exit 1
fi

require_log "frontend_publication_target_schema_locked" "^--- PASS: TestPhase69SwBlockFrontendPublicationTargetSchema" "${OPS_LOG}"
require_log "frontend_publication_target_owner_rbac_narrow" "^--- PASS: TestPhase69FrontendPublicationTargetOwnerPackagingIsNarrow" "${OPS_LOG}"
require_log "kubernetes_writer_frontend_target_camel_case" "^--- PASS: TestKubernetesStatusClientCreatesSwBlockFrontendPublicationWithoutStatus" "${OPS_LOG}"
require_log "frontend_publication_target_owner_dry_run_no_create" "^--- PASS: TestFrontendPublicationTargetOwnerDryRunPlansTargetWithoutCreate" "${OPS_LOG}"
require_log "frontend_publication_target_owner_creates_target" "^--- PASS: TestFrontendPublicationTargetOwnerCreatesMissingTarget" "${OPS_LOG}"
require_log "frontend_publication_target_owner_rejects_enabled_publication" "^--- PASS: TestFrontendPublicationTargetOwnerRejectsEnabledPublication" "${OPS_LOG}"
require_log "cmd_frontend_publication_target_owner_creates_target" "^--- PASS: TestOpsFrontendPublicationTargetOwnerCreatesTarget" "${CMD_LOG}"
require_log "cmd_frontend_publication_target_owner_dry_run_no_create" "^--- PASS: TestOpsFrontendPublicationTargetOwnerDryRunDoesNotCreateTarget" "${CMD_LOG}"

write_summary "frontend_publication_target_schema_locked=true"
write_summary "frontend_publication_target_owner_creates_target=true"
write_summary "frontend_publication_target_owner_dry_run_no_create=true"
write_summary "frontend_publication_target_owner_rejects_enabled_publication=true"
write_summary "frontend_publication_target_owner_rbac_narrow=true"
write_summary "frontend_publication_target_owner_status_writes_allowed=false"
write_summary "frontend_publication_decision=disabled"
write_summary "frontend_publication_reason=frontend_publication_policy_disabled"
write_summary "frontend_publication_mutation_allowed=false"
write_summary "phase69_frontend_publication_target_contract_status=ok"
