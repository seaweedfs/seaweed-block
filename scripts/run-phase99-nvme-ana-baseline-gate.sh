#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase99-nvme-ana-baseline-gate}"
SUMMARY="${ARTIFACT_DIR}/phase99-nvme-ana-baseline-summary.txt"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

run_capture() {
  local name="$1"
  shift
  "$@" >"${ARTIFACT_DIR}/${name}.stdout.txt" 2>"${ARTIFACT_DIR}/${name}.stderr.txt"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    write_summary "phase99_nvme_ana_baseline_status=blocked_missing_${1}"
    echo "missing required command: $1" >&2
    exit 2
  fi
}

select_go() {
  if [[ -n "${GO_BIN:-}" ]]; then
    GO_CMD=("${GO_BIN}")
    return
  fi
  if command -v go.exe >/dev/null 2>&1; then
    GO_CMD=(go.exe)
    return
  fi
  GO_CMD=(go)
}

select_swblock() {
  if [[ -n "${SWBLOCK_BIN:-}" ]]; then
    SWBLOCK_CMD=("${SWBLOCK_BIN}")
    return
  fi
  if command -v swblock.exe >/dev/null 2>&1; then
    SWBLOCK_CMD=(swblock.exe)
    return
  fi
  if command -v swblock >/dev/null 2>&1; then
    SWBLOCK_CMD=(swblock)
    return
  fi
  if [[ -x "/tmp/swblock" ]]; then
    SWBLOCK_CMD=("/tmp/swblock")
    return
  fi
  if [[ -x "/mnt/c/work/swblock.exe" ]]; then
    SWBLOCK_CMD=("/mnt/c/work/swblock.exe")
    return
  fi
  SWBLOCK_CMD=()
}

require_log() {
  local key="$1"
  local pattern="$2"
  local path="$3"
  if grep -Eq "$pattern" "$path"; then
    write_summary "${key}=true"
    return 0
  fi
  write_summary "${key}=false"
  echo "missing pattern ${pattern} in ${path}" >&2
  exit 1
}

write_summary "phase99_nvme_ana_baseline_status=running"
write_summary "phase99_scope=current_branch_nvme_ana_and_csi_protocol_baseline"

select_go
if ! "${GO_CMD[@]}" version >/dev/null 2>&1; then
  write_summary "phase99_nvme_ana_baseline_status=blocked_missing_go"
  echo "missing required command: ${GO_CMD[*]}" >&2
  exit 2
fi
write_summary "go_binary=${GO_CMD[*]}"
write_summary "go_version=$("${GO_CMD[@]}" version)"

(
  cd "${PRODUCT_ROOT}"
  "${GO_CMD[@]}" test -count=1 -v ./core/frontend/nvme ./cmd/blockvolume ./core/csi ./core/launcher
) >"${ARTIFACT_DIR}/go-test.log" 2>&1
write_summary "go_test_nvme_blockvolume_csi_launcher=pass"

require_log "ana_log_page_reports_provider_state" "^--- PASS: TestNVMeANALogPage_ReportsProviderState" "${ARTIFACT_DIR}/go-test.log"
require_log "ana_identify_and_log_consistent" "^--- PASS: TestNVMeANAIdentifyAndLogGroupIDsAreConsistent" "${ARTIFACT_DIR}/go-test.log"
require_log "ana_identify_controller_advertised_with_provider" "^--- PASS: TestNVMeIdentifyController_ANAFieldsAdvertisedWithProvider" "${ARTIFACT_DIR}/go-test.log"
require_log "ana_identify_namespace_advertised_with_provider" "^--- PASS: TestNVMeIdentifyNamespace_ANAFieldsAdvertisedWithProvider" "${ARTIFACT_DIR}/go-test.log"
require_log "projection_ana_state_mapping" "^--- PASS: TestProjectionANAProvider_StateMapping" "${ARTIFACT_DIR}/go-test.log"
require_log "projection_ana_group_dense" "^--- PASS: TestProjectionANAProvider_GroupIDIsDenseAndWithinAdvertisedRange" "${ARTIFACT_DIR}/go-test.log"
require_log "projection_ana_change_count_lineage" "^--- PASS: TestProjectionANAProvider_ChangeCountTracksLineage" "${ARTIFACT_DIR}/go-test.log"
require_log "csi_nvme_node_stage" "^--- PASS: TestNodeStage_NVMeProtocolUsesNVMeTarget" "${ARTIFACT_DIR}/go-test.log"
require_log "csi_nvme_unstage" "^--- PASS: TestNodeUnstage_NVMeDisconnectsAndCleansState" "${ARTIFACT_DIR}/go-test.log"
require_log "launcher_nvme_manifest" "^--- PASS: TestG15d_K8sRenderer_RendersNVMeBlockVolumeArgs" "${ARTIFACT_DIR}/go-test.log"

select_swblock
if [[ ${#SWBLOCK_CMD[@]} -eq 0 ]] || ! "${SWBLOCK_CMD[@]}" help >/dev/null 2>&1; then
  write_summary "phase99_nvme_ana_baseline_status=blocked_missing_swblock_runner"
  echo "missing required TestOps runner: set SWBLOCK_BIN or put swblock on PATH" >&2
  exit 2
fi
write_summary "swblock_binary=${SWBLOCK_CMD[*]}"

(
  cd "${PRODUCT_ROOT}"
  "${SWBLOCK_CMD[@]}" validate testops/scenarios/nvme-p4-multipath-failover-chain.yaml
  "${SWBLOCK_CMD[@]}" validate testops/scenarios/nvme-p5-csi-protocol-chain.yaml
  "${SWBLOCK_CMD[@]}" validate testops/scenarios/nvme-p5-protocol-component-gate.yaml
) >"${ARTIFACT_DIR}/scenario-validate.log" 2>&1
write_summary "nvme_scenarios_validate=pass"

require_log "nvme_p4_scenario_valid" "VALID: nvme-p4-multipath-failover-chain" "${ARTIFACT_DIR}/scenario-validate.log"
require_log "nvme_p5_csi_scenario_valid" "VALID: nvme-p5-csi-protocol-chain" "${ARTIFACT_DIR}/scenario-validate.log"
require_log "nvme_p5_component_scenario_valid" "VALID: nvme-p5-protocol-component-gate" "${ARTIFACT_DIR}/scenario-validate.log"

write_summary "live_nvme_multipath_required_for_release=true"
write_summary "live_nvme_csi_required_for_release=true"
write_summary "phase99_nvme_ana_baseline_status=ok"
