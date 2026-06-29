#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase64-blockvolume-runtime-endpoint-gate}"
SUMMARY="${ARTIFACT_DIR}/phase64-blockvolume-runtime-endpoint-summary.txt"
OPS_LOG="${ARTIFACT_DIR}/core-ops-go-test.log"
VOLUME_LOG="${ARTIFACT_DIR}/core-host-volume-go-test.log"
REPLICATION_LOG="${ARTIFACT_DIR}/core-replication-go-test.log"
BLOCKVOLUME_LOG="${ARTIFACT_DIR}/cmd-blockvolume-go-test.log"

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

write_summary "phase64_blockvolume_runtime_endpoint_status=running"
write_summary "phase64_scope=blockvolume_runtime_rebuild_start_endpoint"
write_summary "runtime_endpoint_default_enabled=false"
write_summary "runtime_endpoint_terminal_frontier_claimed=false"
write_summary "frontend_publication_allowed=false"
write_summary "failback_allowed=false"

OPS_PATTERN="TestAuthorityExecutorReconcilerKeepsRunningWhenRuntimeOnlyStarts|TestAuthorityExecutorReconcilerExecutesRebuildRuntimeAndWritesCaughtUpStatus"
VOLUME_PATTERN="TestStatusServer_RuntimeRebuild_Disabled_Returns404|TestStatusServer_RuntimeRebuild_StartsRecoveryWithExactLineage|TestStatusServer_RuntimeRebuild_RejectsNonPrimary"
REPLICATION_PATTERN="TestReplicationVolume_StartRuntimeRecovery_ValidatesPeerLineage"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_runtime_started_tests=pass"
else
  write_summary "core_ops_runtime_started_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./core/host/volume -run "${VOLUME_PATTERN}" -count=1 -v) >"${VOLUME_LOG}" 2>&1; then
  write_summary "core_host_volume_runtime_endpoint_tests=pass"
else
  write_summary "core_host_volume_runtime_endpoint_tests=fail"
  cat "${VOLUME_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./core/replication -run "${REPLICATION_PATTERN}" -count=1 -v) >"${REPLICATION_LOG}" 2>&1; then
  write_summary "core_replication_runtime_recovery_tests=pass"
else
  write_summary "core_replication_runtime_recovery_tests=fail"
  cat "${REPLICATION_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./cmd/blockvolume -run TestParseFlags -count=1 -v) >"${BLOCKVOLUME_LOG}" 2>&1; then
  write_summary "cmd_blockvolume_flag_tests=pass"
else
  write_summary "cmd_blockvolume_flag_tests=fail"
  cat "${BLOCKVOLUME_LOG}" >&2 || true
  exit 1
fi

require_log "authority_executor_started_keeps_running" "^--- PASS: TestAuthorityExecutorReconcilerKeepsRunningWhenRuntimeOnlyStarts" "${OPS_LOG}"
require_log "authority_executor_terminal_still_caught_up" "^--- PASS: TestAuthorityExecutorReconcilerExecutesRebuildRuntimeAndWritesCaughtUpStatus" "${OPS_LOG}"
require_log "runtime_endpoint_disabled_404" "^--- PASS: TestStatusServer_RuntimeRebuild_Disabled_Returns404" "${VOLUME_LOG}"
require_log "runtime_endpoint_starts_exact_lineage" "^--- PASS: TestStatusServer_RuntimeRebuild_StartsRecoveryWithExactLineage" "${VOLUME_LOG}"
require_log "runtime_endpoint_rejects_non_primary" "^--- PASS: TestStatusServer_RuntimeRebuild_RejectsNonPrimary" "${VOLUME_LOG}"
require_log "replication_runtime_validates_lineage" "^--- PASS: TestReplicationVolume_StartRuntimeRecovery_ValidatesPeerLineage" "${REPLICATION_LOG}"

write_summary "runtime_state_started_supported=true"
write_summary "authority_executor_started_result_not_blocked=true"
write_summary "blockvolume_runtime_endpoint_opt_in=true"
write_summary "blockvolume_runtime_endpoint_posts_started=true"
write_summary "blockvolume_runtime_endpoint_requires_primary=true"
write_summary "blockvolume_runtime_endpoint_requires_lineage=true"
write_summary "replication_runtime_rejects_lineage_drift=true"
write_summary "phase64_blockvolume_runtime_endpoint_status=ok"
