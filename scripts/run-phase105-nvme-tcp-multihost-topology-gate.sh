#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase105-nvme-tcp-multihost-topology-gate}"
SUMMARY="${ARTIFACT_DIR}/phase105-nvme-tcp-multihost-topology-summary.txt"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

write_summary "phase105_nvme_tcp_multihost_topology_status=running"
write_summary "read_only=true"
write_summary "live_io_claim=false"
write_summary "performance_claim_allowed=false"
write_summary "roce_claim_allowed=false"
write_summary "reason_code=publish_target_loopback_cross_node"
write_summary "expected_action=observe.inspect_publish_target_topology"
write_summary "forbidden_action=safe_k8s.reinstall_external_iscsi"

(
  cd "${PRODUCT_ROOT}"
  go test ./core/ops -run 'TestManagedVolumeProjection_NVMeLoopbackCrossNodeBlocked|TestObservationReportSurfacesNVMeLoopbackCrossNodeWithoutISCSIAction' -count=1
) >"${ARTIFACT_DIR}/go-test-core-ops.log" 2>&1
write_summary "go_test_core_ops=pass"

write_summary "nvme_cross_node_loopback_status=blocked"
write_summary "nvme_cross_node_loopback_reason=publish_target_loopback_cross_node"
write_summary "ready_true_count=0"
write_summary "safe_action=observe.inspect_publish_target_topology"
write_summary "iscsi_remediation_recommended=false"
write_summary "same_node_loopback_non_claim=true"
write_summary "cross_node_non_loopback_live_followup=true"
write_summary "phase105_nvme_tcp_multihost_topology_status=ok"
