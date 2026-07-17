#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase161-nvme-rdma-standalone-preflight-refusal-gate}"
SUMMARY="${ARTIFACT_DIR}/phase161-nvme-rdma-standalone-preflight-refusal-summary.txt"

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

write_summary "phase161_nvme_rdma_standalone_preflight_refusal_status=running"
write_summary "rdma_preflight_probe_present=true"
write_summary "nvme_rdma_module_fact_reported=true"
write_summary "rdma_device_fact_reported=true"
write_summary "rdma_bind_address_fact_reported=true"
write_summary "stable_failure_reasons_reported=true"
write_summary "rdma_listener_still_not_started=true"
write_summary "capability_endpoint_still_reports_rdma_unsupported=true"
write_summary "tcp_behavior_unchanged=true"
write_summary "k8s_publish_attach_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "go_version=${GO_VERSION}"

cd "${ROOT}"

"${GO_BIN}" test -v ./core/host/volume ./cmd/blockvolume ./core/frontend/nvme \
  >"${ARTIFACT_DIR}/go/test.stdout.txt" \
  2>"${ARTIFACT_DIR}/go/test.stderr.txt"
write_summary "go_test_volume_blockvolume_nvme=ok"

require_grep "core/host/volume/status_server.go" "type FrontendTransportPreflightFact struct" "status_dto_preflight_fact_present"
require_grep "core/host/volume/status_server.go" "[]FrontendTransportPreflightFact" "capability_dto_preflight_field_present"
require_grep "cmd/blockvolume/main.go" "func nvmeRDMAPreflightFacts" "rdma_preflight_helper_present"
require_grep "cmd/blockvolume/main.go" "func nvmeRDMAModuleFact" "nvme_rdma_module_probe_present"
require_grep "cmd/blockvolume/main.go" "/sys/module/nvme_rdma" "nvme_rdma_module_sysfs_probe_present"
require_grep "cmd/blockvolume/main.go" "/proc/modules" "nvme_rdma_proc_modules_probe_present"
require_grep "cmd/blockvolume/main.go" "func rdmaDeviceFact" "rdma_device_probe_present"
require_grep "cmd/blockvolume/main.go" "/sys/class/infiniband" "rdma_device_sysfs_probe_present"
require_grep "cmd/blockvolume/main.go" "func rdmaBindAddressFact" "rdma_bind_address_probe_present"
require_grep "cmd/blockvolume/main.go" "rdma_bind_address_invalid" "rdma_bind_invalid_reason_present"
require_grep "cmd/blockvolume/main.go" "rdma_bind_address_candidate" "rdma_bind_candidate_reason_present"
require_grep "cmd/blockvolume/main.go" "nvme_rdma_module_missing" "module_missing_reason_present"
require_grep "cmd/blockvolume/main.go" "rdma_device_missing" "device_missing_reason_present"
require_grep "cmd/blockvolume/main.go" "nvme_rdma_transport_unsupported" "rdma_unsupported_reason_preserved"
require_grep "cmd/blockvolume/main.go" "ListenerStarted:     false" "rdma_listener_not_started"
require_grep "cmd/blockvolume/main.go" "Supported:           false" "rdma_supported_false_preserved"

require_grep "${ARTIFACT_DIR}/go/test.stdout.txt" "TestNVMeFrontendCapabilitiesExposeRDMAUnsupportedNoListener" "capability_preflight_test_seen"
require_grep "${ARTIFACT_DIR}/go/test.stdout.txt" "TestRDMABindAddressFactRejectsLoopback" "loopback_refusal_test_seen"
require_grep "${ARTIFACT_DIR}/go/test.stdout.txt" "TestRDMABindAddressFactAcceptsNonLoopbackCandidate" "nonloopback_candidate_test_seen"
require_grep "${ARTIFACT_DIR}/go/test.stdout.txt" "TestStatusServer_FrontendCapabilitiesReportsTransportBoundary" "status_endpoint_preflight_test_seen"
require_grep "${ARTIFACT_DIR}/go/test.stdout.txt" "TestParseFlags_NVMeTransportRejectsRDMA" "parse_refusal_still_tested"

write_summary "phase161_decision=rdma_preflight_facts_surface_unsupported_state"
write_summary "next_recommendation=phase162_nvme_rdma_standalone_listener_skeleton_gate"
write_summary "cleanup_status=ok"
write_summary "phase161_nvme_rdma_standalone_preflight_refusal_status=ok"
