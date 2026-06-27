#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase100-nvme-csi-multipath-component-gate}"
SUMMARY="${ARTIFACT_DIR}/phase100-nvme-csi-multipath-component-summary.txt"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
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

write_summary "phase100_nvme_csi_multipath_component_status=running"
write_summary "phase100_scope=kubernetes_csi_nvme_multipath_publish_context_and_node_stage"

select_go
if ! "${GO_CMD[@]}" version >/dev/null 2>&1; then
  write_summary "phase100_nvme_csi_multipath_component_status=blocked_missing_go"
  echo "missing required command: ${GO_CMD[*]}" >&2
  exit 2
fi
write_summary "go_binary=${GO_CMD[*]}"
write_summary "go_version=$("${GO_CMD[@]}" version)"

(
  cd "${PRODUCT_ROOT}"
  "${GO_CMD[@]}" test -count=1 -v ./core/csi
) >"${ARTIFACT_DIR}/go-test-core-csi.log" 2>&1
write_summary "go_test_core_csi=pass"

require_log "control_status_nvme_multipath_grouping" "^--- PASS: TestControlStatusLookup_MapsMultipleNVMeFrontendsToMultipathTarget" "${ARTIFACT_DIR}/go-test-core-csi.log"
require_log "control_status_nvme_no_cross_nqn_merge" "^--- PASS: TestControlStatusLookup_DoesNotMergeNVMeFrontendsWithDifferentNQN" "${ARTIFACT_DIR}/go-test-core-csi.log"
require_log "node_stage_nvme_multipath_connects_all_targets" "^--- PASS: TestNodeStage_NVMeMultipathConnectsAllTargets" "${ARTIFACT_DIR}/go-test-core-csi.log"
require_log "node_stage_nvme_multipath_cleanup" "^--- PASS: TestNodeStage_NVMeMultipathCleansUpConnectsWhenMountFails" "${ARTIFACT_DIR}/go-test-core-csi.log"

write_summary "live_k8s_nvme_multipath_required_for_release=true"
write_summary "phase100_nvme_csi_multipath_component_status=ok"
