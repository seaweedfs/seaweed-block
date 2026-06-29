#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase86-failback-grpc-runtime-endpoint-decoupling-gate}"
SUMMARY="${ARTIFACT_DIR}/phase86-failback-grpc-runtime-endpoint-decoupling-summary.txt"
OPS_LOG="${ARTIFACT_DIR}/core-ops-go-test.log"
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

write_summary "phase86_failback_grpc_runtime_endpoint_decoupling_status=running"
write_summary "phase86_scope=failback_grpc_runtime_without_target_http_endpoint"
write_summary "frontend_publication_allowed=false"
write_summary "storage_mutation_allowed=false"

OPS_PATTERN="TestFailbackExecutor(GRPCRuntimeDoesNotRequireTargetRuntimeEndpoint|ExecutionInvalidTargetDoesNotCallRuntime|InvokesRuntimeWhenExplicitlyEnabled)"
MASTER_PATTERN="TestFailbackExecutorGRPCRuntimeUsesRealMasterService"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_failback_grpc_endpoint_decoupling_tests=pass"
else
  write_summary "core_ops_failback_grpc_endpoint_decoupling_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./core/host/master -run "${MASTER_PATTERN}" -count=1 -v) >"${MASTER_LOG}" 2>&1; then
  write_summary "core_host_master_failback_grpc_no_endpoint_test=pass"
else
  write_summary "core_host_master_failback_grpc_no_endpoint_test=fail"
  cat "${MASTER_LOG}" >&2 || true
  exit 1
fi

require_log "grpc_runtime_does_not_require_target_runtime_endpoint" "^--- PASS: TestFailbackExecutorGRPCRuntimeDoesNotRequireTargetRuntimeEndpoint" "${OPS_LOG}"
require_log "invalid_target_still_blocks_without_runtime_call" "^--- PASS: TestFailbackExecutorExecutionInvalidTargetDoesNotCallRuntime" "${OPS_LOG}"
require_log "http_runtime_endpoint_fallback_still_supported" "^--- PASS: TestFailbackExecutorInvokesRuntimeWhenExplicitlyEnabled" "${OPS_LOG}"
require_log "real_master_grpc_service_without_target_endpoint" "^--- PASS: TestFailbackExecutorGRPCRuntimeUsesRealMasterService" "${MASTER_LOG}"

write_summary "explicit_grpc_runtime_is_sufficient=true"
write_summary "legacy_http_runtime_endpoint_still_supported=true"
write_summary "invalid_target_writes_blocked_status=true"
write_summary "master_publisher_epoch_advanced=true"
write_summary "frontend_publication_allowed=false"
write_summary "storage_mutation_allowed=false"
write_summary "phase86_failback_grpc_runtime_endpoint_decoupling_status=ok"
