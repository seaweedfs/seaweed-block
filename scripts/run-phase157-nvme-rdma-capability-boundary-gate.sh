#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase157-nvme-rdma-capability-boundary-gate}"
SUMMARY="${ARTIFACT_DIR}/phase157-nvme-rdma-capability-boundary-summary.txt"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

require_grep() {
  local file="$1"
  local needle="$2"
  local label="$3"
  if ! grep -Fq -- "${needle}" "${file}"; then
    echo "missing ${label}: ${needle} in ${file}" >&2
    exit 1
  fi
  write_summary "${label}=true"
}

write_summary "phase157_nvme_rdma_capability_boundary_status=running"
write_summary "current_nvme_tcp_supported_lab_status=source_gated"
write_summary "current_roce_claim_allowed=false"
write_summary "current_nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"

cd "${ROOT}"

require_grep "scripts/run-phase103-nvme-multihost-roce-preflight-gate.sh" "roce_claim_allowed=false" "phase103_keeps_roce_non_claim"
require_grep "scripts/run-phase103-nvme-multihost-roce-preflight-gate.sh" "roce_live_gate_required=true" "phase103_requires_live_roce_gate"
require_grep "scripts/run-phase103-nvme-multihost-roce-preflight-gate.sh" "roce_preflight_candidate" "phase103_records_candidate_not_claim"
require_grep "scripts/run-phase104-roce-live-io-feasibility-gate.sh" "target_nvme_rdma_supported=false" "phase104_target_rdma_unsupported"
require_grep "scripts/run-phase104-roce-live-io-feasibility-gate.sh" "phase104_roce_live_io_gate_required_before_claim=true" "phase104_requires_live_io_before_claim"
require_grep "scripts/run-phase118-nvme-rdma-transport-seam-gate.sh" "blockvolume_rdma_public_refusal=true" "phase118_public_refusal_documented"
require_grep "scripts/run-phase118-nvme-rdma-transport-seam-gate.sh" "rdma_listener_implemented=false" "phase118_no_rdma_listener"

require_grep "docs/releases/nvme-rdma-capability-boundary.md" "Status: **non-claim**" "rdma_boundary_doc_exists"
require_grep "docs/releases/nvme-rdma-capability-boundary.md" "Host Capability Inputs" "rdma_host_capability_inputs_documented"
require_grep "docs/releases/nvme-rdma-capability-boundary.md" "Volume-Server Capability Inputs" "rdma_volume_server_capability_inputs_documented"
require_grep "docs/releases/nvme-rdma-capability-boundary.md" "The missing product work is a real NVMe-oF/RDMA target path" "rdma_transport_product_gap_documented"
require_grep "docs/releases/nvme-rdma-capability-boundary.md" "Standalone live I/O gate" "required_live_io_gate_documented"
require_grep "docs/releases/nvme-rdma-capability-boundary.md" "Kubernetes publish/attach gate" "required_k8s_publish_gate_documented"
require_grep "docs/releases/nvme-rdma-capability-boundary.md" "External RDMA work under" "external_rdma_evidence_separated"
require_grep "docs/releases/nvme-rdma-capability-boundary.md" "no acceleration or SLO claim" "rdma_perf_non_claim_documented"
require_grep "docs/releases/nvme-rdma-capability-boundary.md" "read-only capability probe" "next_capability_probe_documented"

require_grep "docs/releases/README.md" "NVMe/RDMA Capability Boundary" "release_index_links_rdma_boundary_doc"
require_grep "docs/releases/nvme-tcp-supported-lab.md" "RoCE or NVMe/RDMA data path" "nvme_tcp_doc_keeps_rdma_non_claim"
require_grep "docs/roadmap.md" "Phase 157 keeps RoCE/NVMe-RDMA as a product non-claim" "roadmap_phase157_boundary_documented"
require_grep "internal/docs/product-roadmap.md" "Phase 157 keeps RoCE/NVMe-RDMA as a product non-claim" "product_roadmap_phase157_boundary_documented"

write_summary "rdma_host_capability_inputs_documented=true"
write_summary "rdma_volume_server_capability_inputs_documented=true"
write_summary "rdma_transport_product_gap_documented=true"
write_summary "required_live_io_gate_documented=true"
write_summary "required_k8s_publish_gate_documented=true"
write_summary "phase157_decision=keep_nvme_rdma_non_claim_until_product_transport_gates"
write_summary "next_recommendation=phase158_nvme_rdma_volume_capability_probe"
write_summary "cleanup_status=ok"
write_summary "phase157_nvme_rdma_capability_boundary_status=ok"
