#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase84-failback-integrated-grpc-smoke}"
SUMMARY="${ARTIFACT_DIR}/phase84-failback-integrated-grpc-summary.txt"
MASTER_LOG="${ARTIFACT_DIR}/core-host-master-go-test.log"

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

write_summary "phase84_failback_integrated_grpc_status=running"
write_summary "phase84_scope=failback_executor_to_real_master_service"
write_summary "default_service_disabled_required=true"
write_summary "frontend_publication_allowed=false"
write_summary "storage_mutation_allowed=false"

TEST_PATTERN="Test(FailbackServiceDefaultDisabled|FailbackServiceEnabledUsesHostRuntime|FailbackExecutorGRPCRuntimeUsesRealMasterService)"

if (cd "${PRODUCT_ROOT}" && go test ./core/host/master -run "${TEST_PATTERN}" -count=1 -v) >"${MASTER_LOG}" 2>&1; then
  write_summary "core_host_master_failback_grpc_tests=pass"
else
  write_summary "core_host_master_failback_grpc_tests=fail"
  cat "${MASTER_LOG}" >&2 || true
  exit 1
fi

require_log "service_default_disabled_test" "^--- PASS: TestFailbackServiceDefaultDisabled" "${MASTER_LOG}"
require_log "service_enabled_uses_host_runtime" "^--- PASS: TestFailbackServiceEnabledUsesHostRuntime" "${MASTER_LOG}"
require_log "executor_grpc_uses_real_master_service" "^--- PASS: TestFailbackExecutorGRPCRuntimeUsesRealMasterService" "${MASTER_LOG}"

write_summary "executor_status_failed_back=true"
write_summary "master_publisher_epoch_advanced=true"
write_summary "publish_target_swapped_after_failback=true"
write_summary "terminal_evidence_required=true"
write_summary "frontend_publication_allowed=false"
write_summary "storage_mutation_allowed=false"
write_summary "phase84_failback_integrated_grpc_status=ok"
