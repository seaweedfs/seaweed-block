#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase63-rebuild-runtime-target-contract-gate}"
SUMMARY="${ARTIFACT_DIR}/phase63-rebuild-runtime-target-contract-summary.txt"
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

write_summary "phase63_rebuild_runtime_target_contract_status=running"
write_summary "phase63_scope=rebuild_runtime_target_addressing_contract"
write_summary "blockvolume_runtime_endpoint_wired=false"
write_summary "start_rebuild_called=false"
write_summary "frontend_publication_allowed=false"
write_summary "failback_allowed=false"
write_summary "session_id_inferred=false"

OPS_PATTERN="TestPhase46D2SwBlockVolumeReturnedReplicaSchema|TestPhase57D1SwBlockReplicaRebuildTargetSchema|TestRebuildTargetOwnerCreatesMissingTarget|TestRebuildTargetOwnerDoesNotCreateTargetWithoutRuntimeFacts|TestAuthorityExecutorReconcilerBlocksWhenRuntimeTargetFactsMissing|TestAuthorityExecutorReconcilerExecutesRebuildRuntimeAndWritesCaughtUpStatus|TestKubernetesStatusClientCreatesSwBlockReplicaRebuildWithoutStatus"
CMD_PATTERN="TestOpsRebuildTargetOwnerCreatesTarget|TestOpsRebuildTargetOwnerDoesNotCreateTargetWhenRuntimeFactsMissing|TestOpsAuthorityExecutorRebuildRuntimeURLWritesCaughtUpStatus"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_runtime_target_contract_tests=pass"
else
  write_summary "core_ops_runtime_target_contract_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./cmd/sw-block -run "${CMD_PATTERN}" -count=1 -v) >"${CMD_LOG}" 2>&1; then
  write_summary "cmd_sw_block_runtime_target_contract_tests=pass"
else
  write_summary "cmd_sw_block_runtime_target_contract_tests=fail"
  cat "${CMD_LOG}" >&2 || true
  exit 1
fi

require_log "swblockvolume_returned_replica_runtime_schema" "^--- PASS: TestPhase46D2SwBlockVolumeReturnedReplicaSchema" "${OPS_LOG}"
require_log "swblockreplicarebuild_runtime_spec_schema" "^--- PASS: TestPhase57D1SwBlockReplicaRebuildTargetSchema" "${OPS_LOG}"
require_log "target_owner_runtime_facts_ready_create" "^--- PASS: TestRebuildTargetOwnerCreatesMissingTarget" "${OPS_LOG}"
require_log "target_owner_runtime_facts_missing_no_create" "^--- PASS: TestRebuildTargetOwnerDoesNotCreateTargetWithoutRuntimeFacts" "${OPS_LOG}"
require_log "authority_executor_runtime_target_missing_blocked" "^--- PASS: TestAuthorityExecutorReconcilerBlocksWhenRuntimeTargetFactsMissing" "${OPS_LOG}"
require_log "authority_executor_runtime_target_posts_lineage" "^--- PASS: TestAuthorityExecutorReconcilerExecutesRebuildRuntimeAndWritesCaughtUpStatus" "${OPS_LOG}"
require_log "kubernetes_writer_runtime_target_camel_case" "^--- PASS: TestKubernetesStatusClientCreatesSwBlockReplicaRebuildWithoutStatus" "${OPS_LOG}"
require_log "cli_target_owner_runtime_ready" "^--- PASS: TestOpsRebuildTargetOwnerCreatesTarget" "${CMD_LOG}"
require_log "cli_target_owner_runtime_missing" "^--- PASS: TestOpsRebuildTargetOwnerDoesNotCreateTargetWhenRuntimeFactsMissing" "${CMD_LOG}"
require_log "cli_runtime_request_lineage" "^--- PASS: TestOpsAuthorityExecutorRebuildRuntimeURLWritesCaughtUpStatus" "${CMD_LOG}"

write_summary "runtime_target_fields_schema_locked=true"
write_summary "runtime_target_camel_case=true"
write_summary "target_owner_requires_runtime_facts=true"
write_summary "target_owner_creates_only_when_runtime_facts_complete=true"
write_summary "target_owner_missing_runtime_no_target=true"
write_summary "authority_executor_missing_runtime_target_blocks=true"
write_summary "authority_executor_runtime_request_carries_target_lineage=true"
write_summary "runtime_target_can_drive_http_runtime=true"
write_summary "phase63_rebuild_runtime_target_contract_status=ok"
