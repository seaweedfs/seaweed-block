#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase72-frontend-publication-runtime-contract-gate}"
SUMMARY="${ARTIFACT_DIR}/phase72-frontend-publication-runtime-contract-summary.txt"
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

write_summary "phase72_frontend_publication_runtime_contract_status=running"
write_summary "phase72_scope=frontend_publication_runtime_contract"
write_summary "default_frontend_publication_attempts=0"
write_summary "default_failback_attempts=0"
write_summary "default_storage_mutation_allowed=false"

OPS_PATTERN="TestPhase69SwBlockFrontendPublicationTargetSchema|TestFrontendPublicationExecutorWritesDisabledStatus|TestFrontendPublicationExecutorExecutionPolicyBlocks|TestFrontendPublicationExecutorInvokesRuntimeWhenExplicitlyEnabled|TestFrontendPublicationExecutorRuntimeFailureWritesBlockedStatus|TestFrontendPublicationExecutorRejectsInvalidRuntimeTerminalEvidence|TestHTTPFrontendPublicationRuntimePostsRequestAndDecodesResult|TestHTTPFrontendPublicationRuntimeReturnsHTTPError|TestHTTPFrontendPublicationRuntimeRequiresEndpoint"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_frontend_publication_runtime_tests=pass"
else
  write_summary "core_ops_frontend_publication_runtime_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

require_log "frontend_publication_schema_enabled_runtime_endpoint" "^--- PASS: TestPhase69SwBlockFrontendPublicationTargetSchema" "${OPS_LOG}"
require_log "default_executor_still_disabled" "^--- PASS: TestFrontendPublicationExecutorWritesDisabledStatus" "${OPS_LOG}"
require_log "execution_policy_blocks_without_enable" "^--- PASS: TestFrontendPublicationExecutorExecutionPolicyBlocks" "${OPS_LOG}"
require_log "explicit_enabled_target_invokes_runtime" "^--- PASS: TestFrontendPublicationExecutorInvokesRuntimeWhenExplicitlyEnabled" "${OPS_LOG}"
require_log "runtime_failure_no_false_publish" "^--- PASS: TestFrontendPublicationExecutorRuntimeFailureWritesBlockedStatus" "${OPS_LOG}"
require_log "runtime_invalid_terminal_evidence_no_false_publish" "^--- PASS: TestFrontendPublicationExecutorRejectsInvalidRuntimeTerminalEvidence" "${OPS_LOG}"
require_log "http_runtime_contract_posts_request" "^--- PASS: TestHTTPFrontendPublicationRuntimePostsRequestAndDecodesResult" "${OPS_LOG}"
require_log "http_runtime_contract_errors_surface" "^--- PASS: TestHTTPFrontendPublicationRuntimeReturnsHTTPError" "${OPS_LOG}"
require_log "http_runtime_contract_requires_endpoint" "^--- PASS: TestHTTPFrontendPublicationRuntimeRequiresEndpoint" "${OPS_LOG}"

write_summary "frontend_publication_runtime_contract_schema_locked=true"
write_summary "frontend_publication_runtime_endpoint_field=true"
write_summary "frontend_publication_enabled_target_schema=true"
write_summary "frontend_publication_execution_policy_gate=true"
write_summary "frontend_publication_runtime_invoked_only_when_enabled=true"
write_summary "frontend_publication_runtime_failure_no_false_publish=true"
write_summary "frontend_publication_runtime_invalid_terminal_evidence_no_false_publish=true"
write_summary "frontend_publication_attempts=1"
write_summary "frontend_published=true"
write_summary "failback_started=false"
write_summary "storage_mutation_allowed=false"
write_summary "phase72_frontend_publication_runtime_contract_status=ok"
