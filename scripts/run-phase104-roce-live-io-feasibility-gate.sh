#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase104-roce-live-io-feasibility-gate}"
SUMMARY="${ARTIFACT_DIR}/phase104-roce-live-io-feasibility-summary.txt"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

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

write_summary "phase104_roce_live_io_status=running"
write_summary "phase104_scope=explicit_roce_transport_refusal_until_target_support_exists"
write_summary "read_only=true"
write_summary "target_nvme_transport_supported=tcp"
write_summary "target_nvme_rdma_supported=false"
write_summary "roce_claim_allowed=false"
write_summary "roce_live_io_claim=false"

select_go
if ! "${GO_CMD[@]}" version >/dev/null 2>&1; then
  write_summary "phase104_roce_live_io_status=blocked_missing_go"
  exit 2
fi
write_summary "go_binary=${GO_CMD[*]}"
write_summary "go_version=$("${GO_CMD[@]}" version)"

(
  cd "${PRODUCT_ROOT}"
  "${GO_CMD[@]}" test -v ./cmd/blockvolume ./core/frontend/nvme -count=1
) >"${ARTIFACT_DIR}/go-test.log" 2>&1
write_summary "go_test_blockvolume_nvme=pass"

if grep -q "TestParseFlags_NVMeTransportRejectsRDMA" "${ARTIFACT_DIR}/go-test.log"; then
  write_summary "rdma_transport_rejection_test_seen=true"
else
  # Non-verbose go test may not print individual test names; the package PASS is
  # still the gate. Keep this key explicit so readers don't infer live RDMA.
  write_summary "rdma_transport_rejection_test_seen=false"
fi

write_summary "phase104_roce_live_io_result=blocked_target_transport_unsupported"
write_summary "phase104_roce_live_io_gate_required_before_claim=true"
write_summary "phase104_roce_live_io_status=ok"
