#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase66-caught-up-publication-preflight-gate}"
SUMMARY="${ARTIFACT_DIR}/phase66-caught-up-publication-preflight-summary.txt"
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

write_summary "phase66_caught_up_publication_preflight_status=running"
write_summary "phase66_scope=caught_up_publication_decision_surface"
write_summary "frontend_publication_allowed=false"
write_summary "failback_allowed=false"
write_summary "ack_eligibility_mutation_allowed=false"

OPS_PATTERN="TestPhase57D1SwBlockReplicaRebuildTargetSchema|TestKubernetesStatusClientPatchesOnlyStatusSubresources|TestAuthorityExecutorReconcilerTransitionsFromStartedToCaughtUpOnTerminalRuntimeEvidence|TestAuthorityExecutorReconcilerExecutesRebuildRuntimeAndWritesCaughtUpStatus|TestAuthorityExecutorReconcilerKeepsRunningWhenRuntimeOnlyStarts"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_publication_preflight_tests=pass"
else
  write_summary "core_ops_publication_preflight_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

require_log "rebuild_status_schema_has_publication_fields" "^--- PASS: TestPhase57D1SwBlockReplicaRebuildTargetSchema" "${OPS_LOG}"
require_log "kubernetes_writer_serializes_publication_fields" "^--- PASS: TestKubernetesStatusClientPatchesOnlyStatusSubresources" "${OPS_LOG}"
require_log "running_requires_caught_up_before_publication" "^--- PASS: TestAuthorityExecutorReconcilerKeepsRunningWhenRuntimeOnlyStarts" "${OPS_LOG}"
require_log "caught_up_publication_policy_disabled" "^--- PASS: TestAuthorityExecutorReconcilerExecutesRebuildRuntimeAndWritesCaughtUpStatus" "${OPS_LOG}"
require_log "terminal_transition_publication_policy_disabled" "^--- PASS: TestAuthorityExecutorReconcilerTransitionsFromStartedToCaughtUpOnTerminalRuntimeEvidence" "${OPS_LOG}"

write_summary "publication_decision_schema_locked=true"
write_summary "publication_decision_camel_case=true"
write_summary "publication_blocked_until_caught_up=true"
write_summary "publication_disabled_after_caught_up=true"
write_summary "publication_mutation_allowed=false"
write_summary "phase66_caught_up_publication_preflight_status=ok"
