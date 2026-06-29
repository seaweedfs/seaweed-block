#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase106-nvme-tcp-cross-node-publish-gate}"
SUMMARY="${ARTIFACT_DIR}/phase106-nvme-tcp-cross-node-publish-summary.txt"
VALUES_FILE="${ARTIFACT_DIR}/values.nvme.yaml"
RENDERED="${ARTIFACT_DIR}/helm-template-nvme.yaml"
RENDERED_GUARD_LOG="${ARTIFACT_DIR}/helm-template-external-status-guard.log"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

write_summary "phase106_nvme_tcp_cross_node_publish_status=running"
write_summary "live_io_claim=false"
write_summary "performance_claim_allowed=false"
write_summary "roce_claim_allowed=false"
write_summary "default_loopback_preserved=true"
write_summary "external_nvme_requires_opt_in=true"
write_summary "external_nvme_auth_claim=false"

(
  cd "${PRODUCT_ROOT}"
  go test ./cmd/blockvolume -run 'TestParseFlags_NVMe' -count=1
) >"${ARTIFACT_DIR}/go-test-blockvolume.log" 2>&1
write_summary "go_test_blockvolume=pass"

(
  cd "${PRODUCT_ROOT}"
  go test ./core/launcher -run 'TestPhase106|TestG15d_K8sRenderer_RendersNVMe' -count=1
) >"${ARTIFACT_DIR}/go-test-launcher.log" 2>&1
write_summary "go_test_launcher=pass"

(
  cd "${PRODUCT_ROOT}"
  go test ./cmd/blockmaster -run 'TestParseFlags_LauncherExternalNVMe' -count=1
) >"${ARTIFACT_DIR}/go-test-blockmaster.log" 2>&1
write_summary "go_test_blockmaster=pass"

(
  cd "${PRODUCT_ROOT}"
  go test ./cmd/sw-block -run 'TestOpsGenerateHelmValuesMultiNodeExternalNVMe' -count=1
) >"${ARTIFACT_DIR}/go-test-sw-block.log" 2>&1
write_summary "go_test_sw_block=pass"

FAKE_BIN_DIR="$(mktemp -d "${TMPDIR:-/tmp}/phase106-kubectl.XXXXXX")"
trap 'rm -rf "${FAKE_BIN_DIR}"' EXIT
cat >"${FAKE_BIN_DIR}/kubectl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == "get nodes -o wide --no-headers" ]]; then
  cat <<'NODES'
m01 Ready <none> 1d v1.34.4 192.168.1.181 <none> Ubuntu 24.04 kernel containerd
m02 Ready <none> 1d v1.34.4 192.168.1.184 <none> Ubuntu 24.04 kernel containerd
tp01 Ready <none> 1d v1.34.4 192.168.1.188 <none> Ubuntu 24.04 kernel containerd
NODES
  exit 0
fi
echo "unexpected kubectl args: $*" >&2
exit 1
EOF
chmod +x "${FAKE_BIN_DIR}/kubectl"

(
  cd "${PRODUCT_ROOT}"
  PATH="${FAKE_BIN_DIR}:${PATH}" go run ./cmd/sw-block ops generate-helm-values \
    --out "${VALUES_FILE}" \
    --replication-factor 3 \
    --protocol nvme
) >"${ARTIFACT_DIR}/generate-helm-values.log" 2>&1
grep -q '^network_mode=external-nvme$' "${ARTIFACT_DIR}/generate-helm-values.log"
grep -q '^external_nvme=true$' "${ARTIFACT_DIR}/generate-helm-values.log"
grep -q '^external_iscsi=false$' "${ARTIFACT_DIR}/generate-helm-values.log"
grep -q '^chap_enabled=false$' "${ARTIFACT_DIR}/generate-helm-values.log"
grep -q 'externalNVMe: true' "${VALUES_FILE}"
grep -q 'externalISCSI: false' "${VALUES_FILE}"
grep -q 'protocol: nvme' "${VALUES_FILE}"
write_summary "generate_values_external_nvme=pass"
write_summary "generated_protocol=nvme"
write_summary "generated_external_nvme=true"
write_summary "generated_external_iscsi=false"
write_summary "generated_chap_enabled=false"

(
  cd "${PRODUCT_ROOT}"
  helm template sw-block charts/seaweed-block --namespace kube-system -f "${VALUES_FILE}"
) >"${RENDERED}" 2>"${ARTIFACT_DIR}/helm-template-nvme.err"
grep -q -- '--launcher-external-nvme' "${RENDERED}"
grep -q -- '--launcher-external-status' "${RENDERED}"
grep -q -- '--launcher-nvme-port-base=4420' "${RENDERED}"
if grep -q -- '--launcher-external-iscsi' "${RENDERED}"; then
  echo "unexpected iSCSI launcher flag in NVMe render" >&2
  exit 1
fi
if grep -q -- '--launcher-iscsi-chap-secret-name' "${RENDERED}"; then
  echo "unexpected CHAP launcher flag in NVMe render" >&2
  exit 1
fi
write_summary "helm_template_external_nvme=pass"
write_summary "helm_rendered_launcher_external_nvme=true"
write_summary "helm_rendered_launcher_external_iscsi=false"
write_summary "helm_rendered_chap=false"

set +e
(
  cd "${PRODUCT_ROOT}"
  helm template sw-block charts/seaweed-block --namespace kube-system --set network.externalStatus=true
) >"${RENDERED_GUARD_LOG}" 2>&1
guard_status=$?
set -e
if [[ "${guard_status}" -eq 0 ]]; then
  echo "external status without external frontend unexpectedly rendered" >&2
  exit 1
fi
grep -q 'network.externalISCSI=true or network.externalNVMe=true' "${RENDERED_GUARD_LOG}"
write_summary "helm_external_status_guard=pass"

write_summary "phase106_nvme_tcp_cross_node_publish_status=ok"
