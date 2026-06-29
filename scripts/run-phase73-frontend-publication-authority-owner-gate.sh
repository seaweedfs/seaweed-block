#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase73-frontend-publication-authority-owner-gate}"
SUMMARY="${ARTIFACT_DIR}/phase73-frontend-publication-authority-owner-summary.txt"
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

write_summary "phase73_frontend_publication_authority_owner_status=running"
write_summary "phase73_scope=frontend_publication_authority_owner_guard"
write_summary "storage_mutation_allowed=false"
write_summary "failback_started=false"

OPS_PATTERN="TestFrontendPublicationExecutorBlocksReturnedReplicaRuntimeWithoutAuthorityOwner|TestFrontendPublicationExecutorInvokesRuntimeWhenExplicitlyEnabled|TestFrontendPublicationExecutorRuntimeFailureWritesBlockedStatus|TestFrontendPublicationExecutorRejectsInvalidRuntimeTerminalEvidence|TestHTTPFrontendPublicationRuntimePostsRequestAndDecodesResult|TestHTTPFrontendPublicationRuntimeReturnsHTTPError|TestHTTPFrontendPublicationRuntimeRequiresEndpoint"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_frontend_publication_authority_owner_tests=pass"
else
  write_summary "core_ops_frontend_publication_authority_owner_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

require_log "returned_replica_frontend_publication_blocked" "^--- PASS: TestFrontendPublicationExecutorBlocksReturnedReplicaRuntimeWithoutAuthorityOwner" "${OPS_LOG}"
require_log "generic_runtime_contract_still_wired" "^--- PASS: TestFrontendPublicationExecutorInvokesRuntimeWhenExplicitlyEnabled" "${OPS_LOG}"
require_log "runtime_failure_no_false_publish" "^--- PASS: TestFrontendPublicationExecutorRuntimeFailureWritesBlockedStatus" "${OPS_LOG}"
require_log "runtime_invalid_terminal_evidence_no_false_publish" "^--- PASS: TestFrontendPublicationExecutorRejectsInvalidRuntimeTerminalEvidence" "${OPS_LOG}"
require_log "http_runtime_contract_posts_request" "^--- PASS: TestHTTPFrontendPublicationRuntimePostsRequestAndDecodesResult" "${OPS_LOG}"
require_log "http_runtime_contract_errors_surface" "^--- PASS: TestHTTPFrontendPublicationRuntimeReturnsHTTPError" "${OPS_LOG}"
require_log "http_runtime_contract_requires_endpoint" "^--- PASS: TestHTTPFrontendPublicationRuntimeRequiresEndpoint" "${OPS_LOG}"

write_summary "frontend_publication_requires_authority_owner=true"
write_summary "returned_replica_runtime_invocations=0"
write_summary "returned_replica_frontend_published=false"
write_summary "generic_runtime_seam_preserved=true"
write_summary "frontend_publication_attempts=0"
write_summary "failback_attempts=0"
write_summary "phase73_frontend_publication_authority_owner_status=ok"
