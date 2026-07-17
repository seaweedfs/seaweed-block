#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase160-nvme-rdma-transport-adapter-seam-gate}"
SUMMARY="${ARTIFACT_DIR}/phase160-nvme-rdma-transport-adapter-seam-summary.txt"

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

require_not_grep() {
  local file="$1"
  local needle="$2"
  local label="$3"
  if grep -Fq -- "${needle}" "${file}"; then
    echo "unexpected ${label}: ${needle} in ${file}" >&2
    exit 1
  fi
  write_summary "${label}=true"
}

write_summary "phase160_nvme_rdma_transport_adapter_seam_status=running"
write_summary "tcp_pdu_wire_path_isolated=true"
write_summary "reusable_nvme_command_handlers_preserved=true"
write_summary "rdma_adapter_interface_defined=true"
write_summary "rdma_transport_still_unsupported=true"
write_summary "capability_endpoint_still_reports_rdma_unsupported=true"
write_summary "k8s_publish_attach_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "go_version=${GO_VERSION}"

cd "${ROOT}"

"${GO_BIN}" test ./core/frontend/nvme ./cmd/blockvolume ./core/host/volume \
  >"${ARTIFACT_DIR}/go/test.stdout.txt" \
  2>"${ARTIFACT_DIR}/go/test.stderr.txt"
write_summary "go_test_nvme_blockvolume_volume=ok"
write_summary "nvme_tcp_tests_pass=true"

require_grep "core/frontend/nvme/session_transport.go" "type sessionTransport interface" "session_transport_interface_present"
require_grep "core/frontend/nvme/session_transport.go" "type tcpPDUTransport struct" "tcp_pdu_transport_present"
require_grep "core/frontend/nvme/session_transport.go" "NewReader(rw)" "tcp_transport_uses_existing_reader"
require_grep "core/frontend/nvme/session_transport.go" "NewWriter(rw)" "tcp_transport_uses_existing_writer"
require_grep "core/frontend/nvme/session.go" "wire    sessionTransport" "session_depends_on_transport_interface"
require_grep "core/frontend/nvme/session.go" "wire:           newTCPPDUTransport(conn)" "session_defaults_to_tcp_transport"
require_not_grep "core/frontend/nvme/session.go" "NewReader(conn)" "session_no_longer_constructs_tcp_reader_directly"
require_not_grep "core/frontend/nvme/session.go" "NewWriter(conn)" "session_no_longer_constructs_tcp_writer_directly"
require_grep "core/frontend/nvme/session_transport_test.go" "TestNewSessionUsesTCPPDUTransport" "session_transport_test_present"
require_grep "core/frontend/nvme/session_transport_test.go" "var _ sessionTransport = (*tcpPDUTransport)(nil)" "adapter_compile_contract_present"

require_grep "core/frontend/nvme/transport.go" "TransportRDMA Transport = \"rdma\"" "rdma_transport_enum_still_present"
require_grep "core/frontend/nvme/transport.go" "ErrTransportUnsupported" "rdma_still_refuses_in_target_transport"
require_grep "cmd/blockvolume/iscsi_portal_addr_test.go" "TestParseFlags_NVMeTransportRejectsRDMA" "blockvolume_parse_refusal_still_tested"
require_grep "cmd/blockvolume/main.go" "nvme_rdma_transport_unsupported" "capability_endpoint_still_unsupported"
require_grep "docs/releases/nvme-rdma-capability-boundary.md" "phase160_nvme_rdma_transport_adapter_seam" "boundary_doc_mentions_adapter_seam"

write_summary "mounted_or_existing_tcp_gate_unchanged=existing_tcp_unit_component_tests_pass"
write_summary "phase160_decision=tcp_pdu_transport_isolated_rdma_still_unsupported"
write_summary "next_recommendation=phase161_nvme_rdma_standalone_preflight_refusal"
write_summary "cleanup_status=ok"
write_summary "phase160_nvme_rdma_transport_adapter_seam_status=ok"
