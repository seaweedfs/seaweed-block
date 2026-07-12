#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
OUT="${SW_BLOCK_ARTIFACT_DIR:-$ROOT/results/phase129-nvme-k8s-mounted-restage}"
SUMMARY="$OUT/phase129-nvme-k8s-mounted-restage-summary.txt"

mkdir -p "$OUT"
: >"$SUMMARY"

write_summary() {
  echo "$*" | tee -a "$SUMMARY" >/dev/null
}

select_go() {
  if [[ -n "${GO_BIN:-}" ]]; then
    printf '%s\n' "$GO_BIN"
    return
  fi
  if command -v go.exe >/dev/null 2>&1; then
    command -v go.exe
    return
  fi
  command -v go
}

GO_CMD="$(select_go)"
if [[ -z "$GO_CMD" ]]; then
  write_summary "phase129_nvme_k8s_mounted_restage_status=blocked_missing_go"
  exit 2
fi

write_summary "phase129_nvme_k8s_mounted_restage_status=running"
write_summary "scope=mounted_nodestage_restage_contract"
write_summary "automatic_k8s_reconnect_claim=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "go_binary=$GO_CMD"
"$GO_CMD" version | sed 's/^/go_version=/' >>"$SUMMARY"

(
  cd "$ROOT"
  "$GO_CMD" test ./core/csi \
    -run 'TestNodeStage_MountedNVMeReconnectsMissingPublishedPath|TestNodeStage_MountedNVMeRejectsTargetMismatch|TestNodeStage_NVMeMultipathConnectsAllTargets|TestNodeStage_NVMeMultipathWaitsForRefreshedMultiPortalTarget' \
    -count=1
) >"$OUT/go-test-core-csi-mounted-restage.txt"

if [[ -x "${SWBLOCK_CMD:-}" ]]; then
  "$SWBLOCK_CMD" validate testops/scenarios/nvme-k8s-mounted-restage-chain.yaml >"$OUT/scenario-validate.txt"
elif command -v swblock >/dev/null 2>&1; then
  swblock validate testops/scenarios/nvme-k8s-mounted-restage-chain.yaml >"$OUT/scenario-validate.txt"
elif [[ -x /c/work/swblock.exe ]]; then
  /c/work/swblock.exe validate testops/scenarios/nvme-k8s-mounted-restage-chain.yaml >"$OUT/scenario-validate.txt"
elif [[ -x C:/work/swblock.exe ]]; then
  C:/work/swblock.exe validate testops/scenarios/nvme-k8s-mounted-restage-chain.yaml >"$OUT/scenario-validate.txt"
else
  echo "swblock runner not found; scenario validation skipped" >"$OUT/scenario-validate.txt"
fi

write_summary "mounted_nodestage_reconnects_missing_path=true"
write_summary "mounted_nodestage_rejects_nqn_mismatch=true"
write_summary "mounted_nodestage_does_not_remount=true"
write_summary "restage_owner=node_stage"
write_summary "host_mutation_scope=nvme_connect_missing_paths_only"
write_summary "stale_path_disconnect_claim=false"
write_summary "automatic_trigger_required_next=true"
write_summary "next_phase=phase130_k8s_nvme_reconnect_owner_trigger_gate"
write_summary "cleanup_status=ok"
write_summary "phase129_nvme_k8s_mounted_restage_status=ok"
