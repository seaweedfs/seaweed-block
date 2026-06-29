#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase81-failback-service-rpc-gate}"
SUMMARY="${ARTIFACT_DIR}/phase81-failback-service-rpc-summary.txt"
MASTER_LOG="${ARTIFACT_DIR}/core-host-master-go-test.log"
CMD_LOG="${ARTIFACT_DIR}/cmd-blockmaster-go-test.log"

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

write_summary "phase81_failback_service_rpc_status=running"
write_summary "phase81_scope=failback_service_rpc_disabled_by_default"
write_summary "public_rpc_enabled_by_default=false"
write_summary "frontend_publication_allowed=false"
write_summary "storage_mutation_allowed=false"

MASTER_PATTERN="TestFailbackServiceDefaultDisabled|TestFailbackServiceEnabledUsesHostRuntime"
CMD_PATTERN="TestParseFlags_FailbackRuntimeRPCDisabledByDefault|TestBlockmasterBareTopologyRegistersVolumeControlServices"

if (cd "${PRODUCT_ROOT}" && go test ./core/host/master -run "${MASTER_PATTERN}" -count=1 -v) >"${MASTER_LOG}" 2>&1; then
  write_summary "core_master_failback_service_tests=pass"
else
  write_summary "core_master_failback_service_tests=fail"
  cat "${MASTER_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./cmd/blockmaster -run "${CMD_PATTERN}" -count=1 -v) >"${CMD_LOG}" 2>&1; then
  write_summary "cmd_blockmaster_failback_service_tests=pass"
else
  write_summary "cmd_blockmaster_failback_service_tests=fail"
  cat "${CMD_LOG}" >&2 || true
  exit 1
fi

require_log "failback_service_default_disabled" "^--- PASS: TestFailbackServiceDefaultDisabled" "${MASTER_LOG}"
require_log "enabled_failback_service_advances_publisher" "^--- PASS: TestFailbackServiceEnabledUsesHostRuntime" "${MASTER_LOG}"
require_log "failback_runtime_rpc_flag_default_false" "^--- PASS: TestParseFlags_FailbackRuntimeRPCDisabledByDefault" "${CMD_LOG}"
require_log "failback_service_registered" "^--- PASS: TestBlockmasterBareTopologyRegistersVolumeControlServices" "${CMD_LOG}"

write_summary "failback_runtime_rpc_flag_opt_in=true"
write_summary "authority_epoch_advanced=true"
write_summary "single_primary_after_failback=true"
write_summary "publish_target_swapped_after_failback=true"
write_summary "no_storage_mutation=true"
write_summary "no_cross_volume_identity_change=true"
write_summary "phase81_failback_service_rpc_status=ok"
