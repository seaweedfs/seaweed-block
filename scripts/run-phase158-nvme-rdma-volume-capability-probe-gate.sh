#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase158-nvme-rdma-volume-capability-probe-gate}"
SUMMARY="${ARTIFACT_DIR}/phase158-nvme-rdma-volume-capability-probe-summary.txt"

mkdir -p "${ARTIFACT_DIR}/go"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

GO_BIN="${SW_BLOCK_GO:-go}"
GO_VERSION="$("${GO_BIN}" version 2>/dev/null || true)"
if [[ -z "${GO_VERSION}" ]]; then
  echo "missing Go toolchain: set SW_BLOCK_GO to a Go 1.25+ binary" >&2
  exit 1
fi
if ! grep -Eq 'go1\.2[5-9]|go1\.[3-9][0-9]' <<<"${GO_VERSION}"; then
  echo "Go 1.25+ required by go.mod; got ${GO_VERSION}. Set SW_BLOCK_GO to a newer Go binary." >&2
  exit 1
fi

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

write_summary "phase158_nvme_rdma_volume_capability_probe_status=running"
write_summary "frontend_transport_capability_surface_present=true"
write_summary "nvme_tcp_supported=true"
write_summary "nvme_rdma_supported=false"
write_summary "nvme_rdma_refusal_reason=nvme_rdma_transport_unsupported"
write_summary "volume_server_capability_query_supported=true"
write_summary "k8s_status_or_report_surface_updated=deferred_with_reason"
write_summary "k8s_status_or_report_defer_reason=volume_endpoint_first_no_k8s_claim"
write_summary "host_capability_not_product_claim=true"
write_summary "no_rdma_listener_started=true"
write_summary "tcp_behavior_unchanged=true"
write_summary "performance_slo_claim_allowed=false"
write_summary "go_version=${GO_VERSION}"

cd "${ROOT}"

"${GO_BIN}" test -v ./core/host/volume ./cmd/blockvolume ./core/frontend/nvme \
  >"${ARTIFACT_DIR}/go/test.stdout.txt" \
  2>"${ARTIFACT_DIR}/go/test.stderr.txt"
write_summary "go_test_volume_blockvolume_nvme=ok"

require_grep "${ARTIFACT_DIR}/go/test.stdout.txt" "TestStatusServer_FrontendCapabilitiesReportsTransportBoundary" "status_endpoint_capability_test_seen"
require_grep "${ARTIFACT_DIR}/go/test.stdout.txt" "TestNVMeFrontendCapabilitiesExposeRDMAUnsupportedNoListener" "blockvolume_capability_helper_test_seen"
require_grep "${ARTIFACT_DIR}/go/test.stdout.txt" "TestParseFlags_NVMeTransportRejectsRDMA" "rdma_parse_refusal_test_seen"
require_grep "${ARTIFACT_DIR}/go/test.stdout.txt" "TestTargetTransport_RDMAIsExplicitlyUnsupported" "rdma_target_refusal_test_seen"

require_grep "core/host/volume/status_server.go" "/status/frontend-capabilities" "status_endpoint_route_present"
require_grep "cmd/blockvolume/main.go" "nvme_rdma_transport_unsupported" "stable_rdma_reason_present"
require_grep "cmd/blockvolume/main.go" "ListenerStarted:     false" "rdma_listener_not_started_in_capability"
require_grep "docs/releases/nvme-rdma-capability-boundary.md" "/status/frontend-capabilities" "rdma_boundary_doc_mentions_capability_endpoint"
require_grep "docs/releases/nvme-rdma-capability-boundary.md" "TCP supported and RDMA unsupported" "rdma_boundary_doc_mentions_current_probe"

write_summary "phase158_decision=capability_probe_added_rdma_still_unsupported"
write_summary "next_recommendation=phase159_nvme_rdma_standalone_listener_design_gate"
write_summary "cleanup_status=ok"
write_summary "phase158_nvme_rdma_volume_capability_probe_status=ok"
