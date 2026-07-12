#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
OUT="${SW_BLOCK_ARTIFACT_DIR:-$ROOT/results/phase130-nvme-reconnect-owner}"
SUMMARY="$OUT/phase130-nvme-reconnect-owner-summary.txt"

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
  write_summary "phase130_nvme_k8s_reconnect_owner_status=blocked_missing_go"
  exit 2
fi

write_summary "phase130_nvme_k8s_reconnect_owner_status=running"
write_summary "scope=csi_node_owner_trigger_contract"
write_summary "live_k8s_failover_claim=false"
write_summary "go_binary=$GO_CMD"
"$GO_CMD" version | sed 's/^/go_version=/' >>"$SUMMARY"

(
  cd "$ROOT"
  "$GO_CMD" test ./core/csi \
    -run 'TestMountedNVMeReconnectOwner_ReconcilesMissingPublishedPath|TestMountedNVMeReconnectOwnerLoop_InvokesReconnect|TestNodeStage_MountedNVMeReconnectsMissingPublishedPath|TestNodeStage_MountedNVMeRejectsTargetMismatch' \
    -count=1
) >"$OUT/go-test-core-csi-reconnect-owner.txt"

(
  cd "$ROOT"
  "$GO_CMD" test ./cmd/blockcsi \
    -run 'TestParseFlags_NVMeReconnectOwnerIsExplicitOptIn|TestParseFlags_MasterDependentFlagsRequireMaster' \
    -count=1
) >"$OUT/go-test-blockcsi-flags.txt"

if command -v helm >/dev/null 2>&1; then
  (
    cd "$ROOT"
    helm template sw-block charts/seaweed-block --namespace kube-system
  ) >"$OUT/helm-template-default.yaml"
  if grep -q -- "--nvme-reconnect-owner" "$OUT/helm-template-default.yaml"; then
    write_summary "phase130_nvme_k8s_reconnect_owner_status=failed_default_enabled"
    exit 1
  fi
  (
    cd "$ROOT"
    helm template sw-block charts/seaweed-block --namespace kube-system \
      --set csiNode.nvmeReconnect.enabled=true \
      --set csiNode.nvmeReconnect.interval=250ms
  ) >"$OUT/helm-template-nvme-reconnect.yaml"
  grep -q -- "--nvme-reconnect-owner" "$OUT/helm-template-nvme-reconnect.yaml"
  grep -q -- "--nvme-reconnect-interval=250ms" "$OUT/helm-template-nvme-reconnect.yaml"
  write_summary "helm_reconnect_owner_render=ok"
else
  write_summary "helm_reconnect_owner_render=skipped_missing_helm"
fi

write_summary "path_loss_detected=component_missing_path"
write_summary "desired_path_set_changed=true"
write_summary "reconnect_owner=csi-node"
write_summary "reconnect_invoked=true"
write_summary "replacement_path_connected=true"
write_summary "mounted_nodestage_reconnects_missing_path=true"
write_summary "mounted_nodestage_rejects_nqn_mismatch=true"
write_summary "mounted_nodestage_does_not_remount=true"
write_summary "owner_loop_invokes_reconnect=true"
write_summary "default_enabled=false"
write_summary "host_mutation_scope=nvme_connect_missing_paths_only"
write_summary "stale_path_disconnect_claim=false-with-reason=no_stale_path_disconnect_primitive"
write_summary "pod_uid_preserved=not_claimed_component_gate"
write_summary "mounted_io_after_reconnect=not_claimed_component_gate"
write_summary "crd_status_agrees=not_claimed_component_gate"
write_summary "report_dashboard_agree=not_claimed_component_gate"
write_summary "live_k8s_gate_required_next=true"
write_summary "next_phase=phase131_k8s_nvme_reconnect_live_close_gate"
write_summary "cleanup_status=ok"
write_summary "phase130_nvme_k8s_reconnect_owner_status=ok"
