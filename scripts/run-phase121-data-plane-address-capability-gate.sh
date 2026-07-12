#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase121-data-plane-address-capability-gate}"
SUMMARY="${ARTIFACT_DIR}/phase121-data-plane-address-capability-summary.txt"
GO_BIN="${SW_BLOCK_GO_BIN:-go}"

mkdir -p "${ARTIFACT_DIR}"/{bin,values,render}
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

require_cmd "${GO_BIN}"
require_cmd helm
require_cmd python3

cd "${ROOT}"

write_summary "phase121_data_plane_address_capability_status=running"
write_summary "frontend_transport=tcp"
write_summary "nvme_rdma_supported=false"
write_summary "roce_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"

"${GO_BIN}" test ./cmd/sw-block ./core/host/master ./core/ops >"${ARTIFACT_DIR}/go-test.log" 2>&1
write_summary "go_test_phase121=pass"

"${GO_BIN}" build -o "${ARTIFACT_DIR}/bin/sw-block" ./cmd/sw-block

cat >"${ARTIFACT_DIR}/bin/kubectl" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == "get nodes -o wide --no-headers" ]]; then
  cat <<'EOF'
m01    Ready    worker    10d   v1.34.4+k3s1   192.168.1.181   <none>   Ubuntu   6.8.0   containerd://2.0.0
m02    Ready    worker    10d   v1.34.4+k3s1   192.168.1.184   <none>   Ubuntu   6.8.0   containerd://2.0.0
tp01   Ready    worker    10d   v1.34.4+k3s1   192.168.1.188   <none>   Ubuntu   6.8.0   containerd://2.0.0
EOF
  exit 0
fi
echo "unexpected kubectl args: $*" >&2
exit 1
SH
chmod +x "${ARTIFACT_DIR}/bin/kubectl"

PATH="${ARTIFACT_DIR}/bin:${PATH}" "${ARTIFACT_DIR}/bin/sw-block" ops generate-helm-values \
  --out "${ARTIFACT_DIR}/values/values.phase121.yaml" \
  --replication-factor 3 \
  --protocol nvme \
  --frontend-ip-map "m01=10.0.0.181,m02=10.0.0.184,tp01=10.0.0.188" \
  --frontend-network-class 100gbe_tcp \
  >"${ARTIFACT_DIR}/values/generate.stdout.txt" \
  2>"${ARTIFACT_DIR}/values/generate.stderr.txt"

grep -q '^network_mode=external-nvme$' "${ARTIFACT_DIR}/values/generate.stdout.txt"
grep -q '^frontend_ip_map=m01=10.0.0.181,m02=10.0.0.184,tp01=10.0.0.188$' "${ARTIFACT_DIR}/values/generate.stdout.txt"
grep -q '^frontend_network_class=100gbe_tcp$' "${ARTIFACT_DIR}/values/generate.stdout.txt"

grep -q 'internalIP: 192.168.1.181' "${ARTIFACT_DIR}/values/values.phase121.yaml"
grep -q 'managementIP: 192.168.1.181' "${ARTIFACT_DIR}/values/values.phase121.yaml"
grep -q 'frontendIP: 10.0.0.181' "${ARTIFACT_DIR}/values/values.phase121.yaml"
grep -q 'frontendNetworkClass: 100gbe_tcp' "${ARTIFACT_DIR}/values/values.phase121.yaml"
write_summary "generated_values_frontend_ip_map=true"

helm lint charts/seaweed-block >"${ARTIFACT_DIR}/render/helm-lint.log" 2>&1
write_summary "helm_lint=pass"

helm template sw-block charts/seaweed-block -f "${ARTIFACT_DIR}/values/values.phase121.yaml" \
  >"${ARTIFACT_DIR}/render/rendered.yaml"

python3 - "${ARTIFACT_DIR}/render/rendered.yaml" <<'PY' >"${ARTIFACT_DIR}/render/cluster-spec-checks.txt"
from pathlib import Path
import sys
rendered = Path(sys.argv[1]).read_text()
checks = {
    "cluster_spec_data_addr_uses_data_plane": 'data_addr: "10.0.0.181:19101"' in rendered,
    "cluster_spec_ctrl_addr_uses_management": 'ctrl_addr: "192.168.1.181:19102"' in rendered,
    "cluster_spec_management_label_present": 'sw-block.seaweedfs.com/management-ip: "192.168.1.181"' in rendered,
    "cluster_spec_frontend_label_present": 'sw-block.seaweedfs.com/frontend-ip: "10.0.0.181"' in rendered,
    "cluster_spec_network_class_present": 'sw-block.seaweedfs.com/frontend-network-class: "100gbe_tcp"' in rendered,
}
for key, ok in checks.items():
    print(f"{key}={str(ok).lower()}")
if not all(checks.values()):
    raise SystemExit(1)
PY

cat "${ARTIFACT_DIR}/render/cluster-spec-checks.txt" >>"${SUMMARY}"
write_summary "management_ip_m01=192.168.1.181"
write_summary "publish_target_ip_m01=10.0.0.181"
write_summary "publish_target_network_class=100gbe_tcp"
write_summary "publish_target_source=configured_data_plane"
write_summary "internal_ip_not_reused_as_performance_target=true"
write_summary "cleanup_status=ok"
write_summary "phase121_data_plane_address_capability_status=ok"
