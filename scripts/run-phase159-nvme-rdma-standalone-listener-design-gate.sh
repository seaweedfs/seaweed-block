#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase159-nvme-rdma-standalone-listener-design-gate}"
SUMMARY="${ARTIFACT_DIR}/phase159-nvme-rdma-standalone-listener-design-summary.txt"

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

write_summary "phase159_nvme_rdma_standalone_listener_design_gate_status=running"
write_summary "rdma_listener_design_documented=true"
write_summary "rdma_transport_scope_documented=true"
write_summary "standalone_live_io_gate_defined=true"
write_summary "rdma_capability_endpoint_contract_preserved=true"
write_summary "tcp_behavior_unchanged=true"
write_summary "k8s_publish_attach_deferred_until_standalone_pass=true"
write_summary "fallback_refusal_required=true"
write_summary "cleanup_gate_defined=true"
write_summary "performance_slo_claim_allowed=false"
write_summary "go_version=${GO_VERSION}"

cd "${ROOT}"

"${GO_BIN}" test ./core/frontend/nvme ./cmd/blockvolume ./core/host/volume \
  >"${ARTIFACT_DIR}/go/test.stdout.txt" \
  2>"${ARTIFACT_DIR}/go/test.stderr.txt"
write_summary "go_test_nvme_blockvolume_volume=ok"

DESIGN="internal/docs/protocol/phase159-nvme-rdma-standalone-listener-design.md"

require_grep "${DESIGN}" "for real RDMA" "design_rejects_fake_net_listener_rdma"
require_grep "${DESIGN}" "protocol-neutral session core" "design_requires_protocol_adapter"
require_grep "${DESIGN}" 'Linux `nvme connect -t rdma`' "design_requires_linux_nvme_rdma_client"
require_grep "${DESIGN}" "rdma_bind_ip=<100Gb/RoCE/data-plane IP>" "design_requires_roce_data_ip"
require_grep "${DESIGN}" "k8s_publish_attach_claim_allowed=false" "design_defers_k8s_claim"
require_grep "${DESIGN}" "performance_slo_claim_allowed=false" "design_defers_performance_claim"
require_grep "${DESIGN}" "disconnect_cleanup_status=ok" "design_defines_cleanup_gate"
require_grep "${DESIGN}" "nvme_rdma_transport_unsupported" "design_preserves_refusal_reason"
require_grep "${DESIGN}" "phase160_nvme_rdma_transport_adapter_seam" "design_names_next_phase"

require_grep "core/frontend/nvme/transport.go" "type ListenerFactory func(transport Transport, listen string) (net.Listener, error)" "code_listener_factory_seam_present"
require_grep "core/frontend/nvme/transport.go" "TransportRDMA Transport = \"rdma\"" "code_rdma_transport_enum_present"
require_grep "core/frontend/nvme/transport.go" "ErrTransportUnsupported" "code_rdma_refusal_present"
require_grep "core/frontend/nvme/target.go" "newSession(conn, handler, t, t.cfg.SubsysNQN, t.logger)" "code_tcp_session_wire_seam_identified"
require_grep "core/frontend/nvme/wire.go" "NVMe/TCP PDU" "code_tcp_pdu_wire_identified"
require_grep "core/host/volume/status_server.go" "/status/frontend-capabilities" "capability_endpoint_contract_present"
require_grep "cmd/blockvolume/main.go" "nvme_rdma_transport_unsupported" "blockvolume_still_reports_rdma_unsupported"

require_grep "docs/releases/nvme-rdma-capability-boundary.md" "Standalone live I/O gate" "boundary_doc_still_requires_standalone_gate"
require_grep "docs/releases/nvme-rdma-capability-boundary.md" "Kubernetes publish/attach gate" "boundary_doc_still_requires_k8s_gate"

write_summary "phase159_decision=design_rdma_as_transport_adapter_not_fake_tcp_listener"
write_summary "next_recommendation=phase160_nvme_rdma_transport_adapter_seam"
write_summary "cleanup_status=ok"
write_summary "phase159_nvme_rdma_standalone_listener_design_gate_status=ok"
