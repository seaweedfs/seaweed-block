#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase68-frontend-publication-preflight-gate}"
SUMMARY="${ARTIFACT_DIR}/phase68-frontend-publication-preflight-summary.txt"
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

write_summary "phase68_frontend_publication_preflight_status=running"
write_summary "phase68_scope=ack_eligibility_to_frontend_publication_preflight"
write_summary "frontend_publication_allowed=false"
write_summary "frontend_publication_attempts=0"
write_summary "failback_attempts=0"
write_summary "storage_mutation_allowed=false"

OPS_PATTERN="TestPhase54D2SwBlockReplicaEligibilityTargetSchema|TestKubernetesStatusClientPatchesOnlyStatusSubresources|TestAuthorityExecutorReconcilerPublishesAckEligibilityAfterRebuildCaughtUp|TestAuthorityExecutorReconcilerWritesAckEligibilityStatusWhenTerminalEvidenceReady"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_frontend_preflight_tests=pass"
else
  write_summary "core_ops_frontend_preflight_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

require_log "eligibility_status_schema_has_frontend_preflight" "^--- PASS: TestPhase54D2SwBlockReplicaEligibilityTargetSchema" "${OPS_LOG}"
require_log "kubernetes_writer_serializes_frontend_preflight" "^--- PASS: TestKubernetesStatusClientPatchesOnlyStatusSubresources" "${OPS_LOG}"
require_log "rebuild_ack_status_carries_frontend_preflight" "^--- PASS: TestAuthorityExecutorReconcilerPublishesAckEligibilityAfterRebuildCaughtUp" "${OPS_LOG}"
require_log "legacy_ack_status_carries_frontend_preflight" "^--- PASS: TestAuthorityExecutorReconcilerWritesAckEligibilityStatusWhenTerminalEvidenceReady" "${OPS_LOG}"

write_summary "frontend_publication_decision_schema_locked=true"
write_summary "frontend_publication_decision=disabled"
write_summary "frontend_publication_reason=frontend_publication_policy_disabled"
write_summary "frontend_publication_mutation_allowed=false"
write_summary "ack_eligibility_status_mutation_allowed=true"
write_summary "phase68_frontend_publication_preflight_status=ok"
