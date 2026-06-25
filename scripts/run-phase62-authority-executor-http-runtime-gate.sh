#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase62-authority-executor-http-runtime-gate}"
SUMMARY="${ARTIFACT_DIR}/phase62-authority-executor-http-runtime-summary.txt"
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

write_summary "phase62_authority_executor_http_runtime_status=running"
write_summary "phase62_scope=authority_executor_http_runtime_transport"
write_summary "blockvolume_runtime_endpoint_wired=false"
write_summary "frontend_publication_allowed=false"
write_summary "failback_allowed=false"
write_summary "ack_eligibility_mutation_allowed=false"

OPS_PATTERN="TestHTTPAuthorityRebuildRuntime(PostsRequestAndDecodesResult|ReturnsHTTPError|RequiresEndpoint)|TestAuthorityExecutorReconcilerExecutesRebuildRuntimeAndWritesCaughtUpStatus|TestAuthorityExecutorReconcilerWritesBlockedStatusWhenRebuildRuntimeFails"
CMD_PATTERN="TestOpsAuthorityExecutorRebuildRuntimeURLWritesCaughtUpStatus|TestOpsAuthorityExecutorRejectsRebuildRuntimeURLForAckEligibility|TestOpsAuthorityExecutorWritesRebuildPlannedStatus"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_http_runtime_tests=pass"
else
  write_summary "core_ops_http_runtime_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./cmd/sw-block -run "${CMD_PATTERN}" -count=1 -v) >"${CMD_LOG}" 2>&1; then
  write_summary "cmd_sw_block_http_runtime_tests=pass"
else
  write_summary "cmd_sw_block_http_runtime_tests=fail"
  cat "${CMD_LOG}" >&2 || true
  exit 1
fi

require_log "http_runtime_client_success_test" "^--- PASS: TestHTTPAuthorityRebuildRuntimePostsRequestAndDecodesResult" "${OPS_LOG}"
require_log "http_runtime_client_error_test" "^--- PASS: TestHTTPAuthorityRebuildRuntimeReturnsHTTPError" "${OPS_LOG}"
require_log "http_runtime_client_endpoint_guard_test" "^--- PASS: TestHTTPAuthorityRebuildRuntimeRequiresEndpoint" "${OPS_LOG}"
require_log "runtime_callsite_caught_up_test" "^--- PASS: TestAuthorityExecutorReconcilerExecutesRebuildRuntimeAndWritesCaughtUpStatus" "${OPS_LOG}"
require_log "runtime_failure_blocked_test" "^--- PASS: TestAuthorityExecutorReconcilerWritesBlockedStatusWhenRebuildRuntimeFails" "${OPS_LOG}"
require_log "cli_runtime_url_caught_up_test" "^--- PASS: TestOpsAuthorityExecutorRebuildRuntimeURLWritesCaughtUpStatus" "${CMD_LOG}"
require_log "cli_runtime_url_ack_guard_test" "^--- PASS: TestOpsAuthorityExecutorRejectsRebuildRuntimeURLForAckEligibility" "${CMD_LOG}"
require_log "planned_without_runtime_test" "^--- PASS: TestOpsAuthorityExecutorWritesRebuildPlannedStatus" "${CMD_LOG}"

write_summary "http_runtime_posts_request=true"
write_summary "http_runtime_decodes_terminal_frontier=true"
write_summary "http_runtime_non_2xx_blocks=true"
write_summary "cli_rebuild_runtime_url_enabled=true"
write_summary "cli_rebuild_runtime_url_requires_rebuild_traffic=true"
write_summary "rebuild_status_running_written=true"
write_summary "rebuild_status_caught_up_written=true"
write_summary "rebuild_status_blocked_on_runtime_failure=true"
write_summary "durable_frontier_caught_up_after_runtime=true"
write_summary "planned_status_preserved_without_runtime=true"
write_summary "phase62_authority_executor_http_runtime_status=ok"
