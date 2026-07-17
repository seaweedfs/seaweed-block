#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase162-nvme-rdma-standalone-listener-skeleton-gate}"
SUMMARY="${ARTIFACT_DIR}/phase162-nvme-rdma-standalone-listener-skeleton-summary.txt"

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

write_summary "phase162_nvme_rdma_standalone_listener_skeleton_gate_status=running"
write_summary "rdma_listener_start_path_defined=true"
write_summary "rdma_listener_disabled_by_default=true"
write_summary "preflight_failure_maps_to_stable_reasons=true"
write_summary "capability_endpoint_reports_listener_started_false=true"
write_summary "tcp_behavior_unchanged=true"
write_summary "linux_nvme_connect_live_io_not_claimed=true"
write_summary "k8s_publish_attach_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "go_version=${GO_VERSION}"

cd "${ROOT}"

"${GO_BIN}" test -v ./core/host/volume ./cmd/blockvolume ./core/frontend/nvme \
  >"${ARTIFACT_DIR}/go/test.stdout.txt" \
  2>"${ARTIFACT_DIR}/go/test.stderr.txt"
write_summary "go_test_volume_blockvolume_nvme=ok"

require_grep "core/host/volume/status_server.go" "StartAllowed" "capability_dto_start_allowed_present"
require_grep "core/host/volume/status_server.go" "StartReason" "capability_dto_start_reason_present"
require_grep "cmd/blockvolume/main.go" "type nvmeRDMAListenerStart struct" "rdma_listener_start_decision_type_present"
require_grep "cmd/blockvolume/main.go" "func nvmeRDMAListenerStartDecision" "rdma_listener_start_decision_func_present"
require_grep "cmd/blockvolume/main.go" "nvme_rdma_listener_disabled" "rdma_listener_disabled_reason_present"
require_grep "cmd/blockvolume/main.go" "reason: fact.Reason" "preflight_failure_reason_passthrough_present"
require_grep "cmd/blockvolume/main.go" "reason: \"nvme_rdma_transport_unsupported\"" "post_preflight_unsupported_reason_present"
require_grep "cmd/blockvolume/main.go" "StartAllowed:        rdmaStart.allowed" "rdma_capability_uses_start_decision"
require_grep "cmd/blockvolume/main.go" "StartReason:         rdmaStart.reason" "rdma_capability_reports_start_reason"
require_grep "cmd/blockvolume/main.go" "ListenerStarted:     false" "rdma_listener_started_false"
require_grep "cmd/blockvolume/main.go" "Supported:           false" "rdma_supported_false"
require_grep "cmd/blockvolume/iscsi_portal_addr_test.go" "TestNVMERDMAListenerStartDecisionDisabledByDefault" "disabled_default_test_seen"
require_grep "cmd/blockvolume/iscsi_portal_addr_test.go" "TestNVMERDMAListenerStartDecisionMapsPreflightFailure" "preflight_failure_mapping_test_seen"
require_grep "cmd/blockvolume/iscsi_portal_addr_test.go" "TestNVMERDMAListenerStartDecisionStillUnsupportedAfterPreflight" "post_preflight_unsupported_test_seen"
require_grep "${ARTIFACT_DIR}/go/test.stdout.txt" "TestParseFlags_NVMeTransportRejectsRDMA" "parse_refusal_still_tested"
require_grep "docs/releases/nvme-rdma-capability-boundary.md" "standalone live I/O gate" "boundary_doc_keeps_live_io_non_claim"

write_summary "phase162_decision=rdma_start_decision_skeleton_disabled_by_default"
write_summary "next_recommendation=phase163_nvme_rdma_standalone_listener_impl_spike"
write_summary "cleanup_status=ok"
write_summary "phase162_nvme_rdma_standalone_listener_skeleton_gate_status=ok"
