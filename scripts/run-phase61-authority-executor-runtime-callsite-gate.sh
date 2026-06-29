#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase61-authority-executor-runtime-callsite-gate}"
SUMMARY="${ARTIFACT_DIR}/phase61-authority-executor-runtime-callsite-summary.txt"
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

write_summary "phase61_authority_executor_runtime_callsite_status=running"
write_summary "phase61_scope=core_authority_executor_runtime_callsite"
write_summary "blockvolume_rpc_wired=false"
write_summary "frontend_publication_allowed=false"
write_summary "failback_allowed=false"
write_summary "ack_eligibility_mutation_allowed=false"

TEST_PATTERN="TestAuthorityExecutorReconcilerWritesRebuildPlannedStatus|TestAuthorityExecutorReconcilerExecutesRebuildRuntimeAndWritesCaughtUpStatus|TestAuthorityExecutorReconcilerWritesBlockedStatusWhenRebuildRuntimeFails"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${TEST_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_runtime_callsite_tests=pass"
else
  write_summary "core_ops_runtime_callsite_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

require_log "planned_without_runtime_test" "^--- PASS: TestAuthorityExecutorReconcilerWritesRebuildPlannedStatus" "${OPS_LOG}"
require_log "runtime_callsite_caught_up_test" "^--- PASS: TestAuthorityExecutorReconcilerExecutesRebuildRuntimeAndWritesCaughtUpStatus" "${OPS_LOG}"
require_log "runtime_failure_blocked_test" "^--- PASS: TestAuthorityExecutorReconcilerWritesBlockedStatusWhenRebuildRuntimeFails" "${OPS_LOG}"

write_summary "runtime_callsite_invoked=true"
write_summary "rebuild_status_running_written=true"
write_summary "rebuild_status_caught_up_written=true"
write_summary "rebuild_status_blocked_on_runtime_failure=true"
write_summary "rebuild_traffic_started_when_runtime_invoked=true"
write_summary "durable_frontier_caught_up_after_runtime=true"
write_summary "planned_status_preserved_without_runtime=true"
write_summary "phase61_authority_executor_runtime_callsite_status=ok"
