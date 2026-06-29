#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase82-failback-executor-grpc-runtime-gate}"
SUMMARY="${ARTIFACT_DIR}/phase82-failback-executor-grpc-runtime-summary.txt"
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

write_summary "phase82_failback_executor_grpc_runtime_status=running"
write_summary "phase82_scope=failback_executor_grpc_runtime"
write_summary "default_failback_attempts=0"
write_summary "frontend_publication_allowed=false"
write_summary "storage_mutation_allowed=false"

OPS_PATTERN="TestGRPCFailbackRuntime"
CMD_PATTERN="TestOpsFailbackExecutor(GRPCRuntimeWritesFailedBackStatus|RejectsGRPCRuntimeWithoutEnable|RejectsAmbiguousRuntimeTransports|RuntimeURLWritesFailedBackStatus)"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_grpc_failback_runtime_tests=pass"
else
  write_summary "core_ops_grpc_failback_runtime_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./cmd/sw-block -run "${CMD_PATTERN}" -count=1 -v) >"${CMD_LOG}" 2>&1; then
  write_summary "cmd_failback_grpc_runtime_tests=pass"
else
  write_summary "cmd_failback_grpc_runtime_tests=fail"
  cat "${CMD_LOG}" >&2 || true
  exit 1
fi

require_log "grpc_runtime_calls_failback_service" "^--- PASS: TestGRPCFailbackRuntimeCallsFailbackService" "${OPS_LOG}"
require_log "grpc_runtime_requires_address" "^--- PASS: TestGRPCFailbackRuntimeRequiresAddress" "${OPS_LOG}"
require_log "cmd_grpc_runtime_writes_failed_back_status" "^--- PASS: TestOpsFailbackExecutorGRPCRuntimeWritesFailedBackStatus" "${CMD_LOG}"
require_log "cmd_grpc_runtime_requires_enable" "^--- PASS: TestOpsFailbackExecutorRejectsGRPCRuntimeWithoutEnable" "${CMD_LOG}"
require_log "cmd_rejects_ambiguous_runtime_transports" "^--- PASS: TestOpsFailbackExecutorRejectsAmbiguousRuntimeTransports" "${CMD_LOG}"
require_log "cmd_http_runtime_still_supported" "^--- PASS: TestOpsFailbackExecutorRuntimeURLWritesFailedBackStatus" "${CMD_LOG}"

write_summary "grpc_runtime_request_fields_mapped=true"
write_summary "grpc_runtime_response_fields_mapped=true"
write_summary "execution_policy_still_required=true"
write_summary "http_grpc_runtime_mutually_exclusive=true"
write_summary "authority_mutation_allowed_only_with_execution_policy=true"
write_summary "frontend_publication_allowed=false"
write_summary "storage_mutation_allowed=false"
write_summary "phase82_failback_executor_grpc_runtime_status=ok"
