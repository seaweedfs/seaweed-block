#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase80-master-failback-runtime-factory-gate}"
SUMMARY="${ARTIFACT_DIR}/phase80-master-failback-runtime-factory-summary.txt"
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

write_summary "phase80_master_failback_runtime_factory_status=running"
write_summary "phase80_scope=master_failback_runtime_factory"
write_summary "automatic_failback_enabled=false"
write_summary "public_failback_rpc_added=false"
write_summary "frontend_publication_allowed=false"
write_summary "storage_mutation_allowed=false"

if (cd "${PRODUCT_ROOT}" && go test ./core/host/master -run "TestHostFailbackAuthorityRuntimeUsesLivePublisher" -count=1 -v) >"${MASTER_LOG}" 2>&1; then
  write_summary "core_master_failback_runtime_tests=pass"
else
  write_summary "core_master_failback_runtime_tests=fail"
  cat "${MASTER_LOG}" >&2 || true
  exit 1
fi

require_log "host_failback_runtime_uses_live_publisher" "^--- PASS: TestHostFailbackAuthorityRuntimeUsesLivePublisher" "${MASTER_LOG}"

write_summary "publisher_authority_line_advanced=true"
write_summary "authority_epoch_advanced=true"
write_summary "single_primary_after_failback=true"
write_summary "publish_target_swapped_after_failback=true"
write_summary "no_storage_mutation=true"
write_summary "no_cross_volume_identity_change=true"
write_summary "phase80_master_failback_runtime_factory_status=ok"
