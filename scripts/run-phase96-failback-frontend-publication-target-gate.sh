#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase96-failback-frontend-publication-target-gate}"
SUMMARY="${ARTIFACT_DIR}/phase96-failback-frontend-publication-target-summary.txt"
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

write_summary "phase96_failback_frontend_publication_target_status=running"
write_summary "phase96_scope=failed_back_terminal_evidence_to_frontend_publication_target"
write_summary "frontend_publication_attempts=0"
write_summary "failback_attempts=0"
write_summary "storage_mutation_allowed=false"

OPS_PATTERN="TestPhase69SwBlockFrontendPublicationTargetSchema|TestPhase69FrontendPublicationTargetOwnerPackagingIsNarrow|TestKubernetesStatusClientCreatesSwBlockFrontendPublicationWithoutStatus|TestFrontendPublicationTargetOwnerCreatesTargetFromTerminalFailback|TestFrontendPublicationTargetOwnerRejectsNonTerminalFailback|TestFrontendPublicationExecutorAcceptsFailbackTerminalTargetAsDisabled"
CMD_PATTERN="TestOpsFrontendPublicationTargetOwnerCreatesTargetFromFailback"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_phase96_tests=pass"
else
  write_summary "core_ops_phase96_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./cmd/sw-block -run "${CMD_PATTERN}" -count=1 -v) >"${CMD_LOG}" 2>&1; then
  write_summary "cmd_sw_block_phase96_tests=pass"
else
  write_summary "cmd_sw_block_phase96_tests=fail"
  cat "${CMD_LOG}" >&2 || true
  exit 1
fi

require_log "frontend_publication_target_schema_accepts_failback_source" "^--- PASS: TestPhase69SwBlockFrontendPublicationTargetSchema" "${OPS_LOG}"
require_log "frontend_publication_target_owner_reads_failbacks_only" "^--- PASS: TestPhase69FrontendPublicationTargetOwnerPackagingIsNarrow" "${OPS_LOG}"
require_log "frontend_publication_writer_camel_case" "^--- PASS: TestKubernetesStatusClientCreatesSwBlockFrontendPublicationWithoutStatus" "${OPS_LOG}"
require_log "terminal_failed_back_creates_frontend_publication_target" "^--- PASS: TestFrontendPublicationTargetOwnerCreatesTargetFromTerminalFailback" "${OPS_LOG}"
require_log "non_terminal_failback_rejected" "^--- PASS: TestFrontendPublicationTargetOwnerRejectsNonTerminalFailback" "${OPS_LOG}"
require_log "executor_accepts_failback_target_as_disabled" "^--- PASS: TestFrontendPublicationExecutorAcceptsFailbackTerminalTargetAsDisabled" "${OPS_LOG}"
require_log "cmd_terminal_failback_creates_target" "^--- PASS: TestOpsFrontendPublicationTargetOwnerCreatesTargetFromFailback" "${CMD_LOG}"

write_summary "terminal_failback_state_required=failed_back"
write_summary "terminal_failback_reason_required=failback_completed"
write_summary "publish_target_swapped_after_failback_required=true"
write_summary "frontend_publication_target_created_from_failback=true"
write_summary "frontend_publication_target_source_failback_name=true"
write_summary "frontend_publication_decision=disabled"
write_summary "frontend_publication_reason=frontend_publication_policy_disabled"
write_summary "frontend_publication_mutation_allowed=false"
write_summary "frontend_publication_status_writes_allowed=false"
write_summary "frontend_publication_executor_default_off=true"
write_summary "failback_status_mutation_allowed=false"
write_summary "phase96_failback_frontend_publication_target_status=ok"
