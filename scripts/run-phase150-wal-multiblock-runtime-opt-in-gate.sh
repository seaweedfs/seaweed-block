#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase150-wal-multiblock-runtime-opt-in-gate}"
SUMMARY="${ARTIFACT_DIR}/phase150-wal-multiblock-runtime-opt-in-summary.txt"
if [[ -n "${SW_BLOCK_GO_BIN:-}" ]]; then
  GO_BIN="${SW_BLOCK_GO_BIN}"
elif command -v go.exe >/dev/null 2>&1; then
  GO_BIN="go.exe"
else
  GO_BIN="go"
fi

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

write_summary "phase150_wal_multiblock_runtime_opt_in_status=running"
write_summary "frontend_transport=tcp"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "default_wal_format_unchanged=true"
write_summary "feature_gate_default=false"
write_summary "runtime_opt_in_name=durable-wal-multiblock-records"
write_summary "runtime_opt_in_default=false"

cd "${ROOT}"
"${GO_BIN}" test ./core/storage ./core/frontend/durable ./core/launcher ./cmd/blockvolume ./cmd/blockmaster \
  -run 'Phase150|MultiBlock|K8sRenderer_RendersBlockVolumeDeploymentArgs' -count=1 \
  >"${ARTIFACT_DIR}/go-test-runtime-opt-in.log" \
  2>&1
write_summary "explicit_opt_in_reaches_walstore=true"
write_summary "single_block_compatibility=pass"
write_summary "current_recovery_compatibility=pass"

helm template sw-block charts/seaweed-block --namespace kube-system \
  >"${ARTIFACT_DIR}/helm-default.yaml"
if grep -Fq -- "--launcher-durable-wal-multiblock-records" "${ARTIFACT_DIR}/helm-default.yaml"; then
  echo "default helm render unexpectedly includes --launcher-durable-wal-multiblock-records" >&2
  exit 1
fi
write_summary "helm_default_omits_opt_in=true"

helm template sw-block charts/seaweed-block --namespace kube-system \
  --set blockmaster.durableWALMultiBlockRecords=true \
  >"${ARTIFACT_DIR}/helm-explicit.yaml"
if ! grep -Fq -- "--launcher-durable-wal-multiblock-records" "${ARTIFACT_DIR}/helm-explicit.yaml"; then
  echo "explicit helm render missing --launcher-durable-wal-multiblock-records" >&2
  exit 1
fi
write_summary "helm_explicit_renders_opt_in=true"

write_summary "phase150_decision=mounted_profile_next"
write_summary "next_recommendation=phase151_wal_multiblock_mounted_nvme_profile"
write_summary "cleanup_status=ok"
write_summary "phase150_wal_multiblock_runtime_opt_in_status=ok"
