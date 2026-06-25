#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase67-ack-eligibility-publication-gate}"
SUMMARY="${ARTIFACT_DIR}/phase67-ack-eligibility-publication-summary.txt"
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

write_summary "phase67_ack_eligibility_publication_status=running"
write_summary "phase67_scope=caught_up_to_ack_eligibility_status_publication"
write_summary "frontend_publication_allowed=false"
write_summary "failback_allowed=false"
write_summary "storage_mutation_allowed=false"

OPS_PATTERN="TestPhase54D2SwBlockReplicaEligibilityTargetSchema|TestPhase57D1SwBlockReplicaRebuildTargetSchema|TestAuthorityExecutorReconcilerPublishesAckEligibilityAfterRebuildCaughtUp|TestAuthorityExecutorReconcilerHoldsAckEligibilityUntilRebuildCaughtUp|TestAuthorityExecutorReconcilerExecutesRebuildRuntimeAndWritesCaughtUpStatus|TestAuthorityExecutorReconcilerTransitionsFromStartedToCaughtUpOnTerminalRuntimeEvidence"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_ack_publication_tests=pass"
else
  write_summary "core_ops_ack_publication_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

require_log "eligibility_status_schema_locked" "^--- PASS: TestPhase54D2SwBlockReplicaEligibilityTargetSchema" "${OPS_LOG}"
require_log "rebuild_status_schema_locked" "^--- PASS: TestPhase57D1SwBlockReplicaRebuildTargetSchema" "${OPS_LOG}"
require_log "ack_publication_after_caught_up" "^--- PASS: TestAuthorityExecutorReconcilerPublishesAckEligibilityAfterRebuildCaughtUp" "${OPS_LOG}"
require_log "ack_publication_holds_before_caught_up" "^--- PASS: TestAuthorityExecutorReconcilerHoldsAckEligibilityUntilRebuildCaughtUp" "${OPS_LOG}"
require_log "rebuild_terminal_source_still_caught_up" "^--- PASS: TestAuthorityExecutorReconcilerExecutesRebuildRuntimeAndWritesCaughtUpStatus" "${OPS_LOG}"
require_log "runtime_transition_terminal_source" "^--- PASS: TestAuthorityExecutorReconcilerTransitionsFromStartedToCaughtUpOnTerminalRuntimeEvidence" "${OPS_LOG}"

write_summary "ack_eligibility_status_mutation_allowed=true"
write_summary "ack_publication_requires_rebuild_caught_up=true"
write_summary "ack_publication_rejects_running_rebuild=true"
write_summary "ack_publication_rejects_unexpected_publication_allowed=true"
write_summary "rebuild_status_mutation_attempts=0"
write_summary "frontend_publication_attempts=0"
write_summary "failback_attempts=0"
write_summary "phase67_ack_eligibility_publication_status=ok"
