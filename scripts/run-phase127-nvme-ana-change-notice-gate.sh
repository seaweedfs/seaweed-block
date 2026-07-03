#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
OUT="${SW_BLOCK_ARTIFACT_DIR:-$ROOT/results/phase127-nvme-ana-change-notice}"
mkdir -p "$OUT"

summary="$OUT/phase127-summary.txt"
: > "$summary"

{
  echo "phase127_nvme_ana_change_notice_status=running"
  echo "nvme_transport=tcp"
  echo "host_live_aer_claim=false"
  echo "k8s_dynamic_reconnect_claim=false"
  echo "nvme_rdma_claim_allowed=false"
  echo "performance_slo_claim_allowed=false"
} >> "$summary"

cd "$ROOT"

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

select_go
{
  echo "go_binary=${GO_CMD[*]}"
  echo "go_version=$("${GO_CMD[@]}" version)"
} >> "$summary"

"${GO_CMD[@]}" test ./core/frontend/nvme \
  -run 'TestNVMeIdentifyController_OAESANAChangeAdvertisedWithProvider|TestNVMeAER_CompletesOnANAChangeNotice|TestT2V2Port_NVMe_IdentifyCtrl_OAESAllBitsZero|TestT2Batch11b_AER_LimitExceeded' \
  -count=1 > "$OUT/go-test-nvme-aer.txt"

"${GO_CMD[@]}" test ./cmd/blockvolume \
  -run 'TestProjectionANAProvider_ChangeCountTracksLineage' \
  -count=1 > "$OUT/go-test-blockvolume-ana.txt"

{
  echo "ana_provider_oaes_ana_change_notice=true"
  echo "no_provider_oaes_zero=true"
  echo "aer_completes_on_ana_change=true"
  echo "aer_completion_event_type=notice"
  echo "aer_completion_event_info=ana_change"
  echo "aer_completion_log_page=ana"
  echo "aer_limit_still_enforced=true"
  echo "projection_change_count_source=lineage"
  echo "cleanup_status=ok"
  echo "phase127_nvme_ana_change_notice_status=ok"
} >> "$summary"

cat "$summary"
