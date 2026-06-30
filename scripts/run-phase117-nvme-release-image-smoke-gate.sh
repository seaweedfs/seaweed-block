#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase117-nvme-release-image-smoke-gate}"
SUMMARY="${ARTIFACT_DIR}/phase117-nvme-release-image-smoke-summary.txt"
HELM_RELEASE="${SW_BLOCK_HELM_RELEASE:-sw-block}"
HELM_NAMESPACE="${SW_BLOCK_HELM_NAMESPACE:-kube-system}"
APP_NAMESPACE="${SW_BLOCK_APP_NAMESPACE:-default}"
IMAGE="${SW_BLOCK_RELEASE_IMAGE:-${SW_BLOCK_IMAGE:-}}"
CSI_IMAGE="${SW_BLOCK_CSI_RELEASE_IMAGE:-${SW_BLOCK_CSI_IMAGE:-}}"
EXPECTED_COMMIT="${SW_BLOCK_RELEASE_COMMIT:-}"
STATUS_PORT="${SW_BLOCK_MASTER_PORT_FORWARD_PORT:-29337}"

mkdir -p "${ARTIFACT_DIR}"/{bin,images,values,install,pvc,status,cleanup}
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

safe_image_name() {
  printf '%s' "$1" | tr '/:@' '___'
}

manifest_exists() {
  local image="$1"
  local safe
  safe="$(safe_image_name "${image}")"
  docker manifest inspect "${image}" >"${ARTIFACT_DIR}/images/manifest-${safe}.json" 2>"${ARTIFACT_DIR}/images/manifest-${safe}.stderr.txt"
}

tag_commit() {
  local image="$1"
  local tag
  tag="${image##*:}"
  if [[ "${tag}" == sha-* ]]; then
    printf '%s' "${tag#sha-}"
  fi
}

extract_revision() {
  local file="$1"
  sed -n 's/.* revision=\([^ ]*\).*/\1/p' "${file}" | head -1
}

version_matches_expected() {
  local revision="$1"
  [[ -z "${revision}" || "${revision}" == "unknown" ]] && return 0
  [[ -n "${EXPECTED_COMMIT}" ]] || return 0
  [[ "${revision}" == "${EXPECTED_COMMIT}"* ]]
}

cleanup() {
  set +e
  kubectl -n "${APP_NAMESPACE}" delete pod sw-block-multi-reader-1 sw-block-multi-writer-1 --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  kubectl -n "${APP_NAMESPACE}" delete pvc sw-block-multi-pvc-1 --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  kubectl delete storageclass sw-block-multi --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  sudo -n nvme disconnect-all >/dev/null 2>&1 || true
  helm status "${HELM_RELEASE}" --namespace "${HELM_NAMESPACE}" >/dev/null 2>&1 && \
    helm uninstall "${HELM_RELEASE}" --namespace "${HELM_NAMESPACE}" --wait --timeout 240s \
      >"${ARTIFACT_DIR}/cleanup/helm-uninstall.txt" 2>&1
  kubectl -n "${APP_NAMESPACE}" delete deploy -l app=sw-blockvolume --ignore-not-found=true --wait=false >/dev/null 2>&1
  kubectl -n "${HELM_NAMESPACE}" get swblockvolumes.block.seaweedfs.com -o name 2>/dev/null | \
    xargs -r -n1 kubectl -n "${HELM_NAMESPACE}" patch --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null 2>&1
  kubectl -n "${HELM_NAMESPACE}" delete swblockvolumes.block.seaweedfs.com --all --ignore-not-found=true --wait=true --timeout=60s >/dev/null 2>&1
  kubectl -n "${HELM_NAMESPACE}" delete swblockclusters.block.seaweedfs.com --all --ignore-not-found=true --wait=true --timeout=60s >/dev/null 2>&1
  kubectl delete validatingadmissionpolicy,validatingadmissionpolicybinding -l "app.kubernetes.io/instance=${HELM_RELEASE}" --ignore-not-found=true >/dev/null 2>&1
  kubectl delete crd \
    swblockclusters.block.seaweedfs.com \
    swblockvolumes.block.seaweedfs.com \
    swblockreplicaeligibilities.block.seaweedfs.com \
    swblockreplicarebuilds.block.seaweedfs.com \
    swblockreplicafailbacks.block.seaweedfs.com \
    swblockfrontendpublications.block.seaweedfs.com \
    --ignore-not-found=true --wait=true --timeout=60s >/dev/null 2>&1
  kubectl get pv --no-headers 2>/dev/null | awk '/sw-block-multi/ {print $1}' | \
    xargs -r -n1 kubectl patch pv --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null 2>&1
  kubectl get pv --no-headers 2>/dev/null | awk '/sw-block-multi/ {print $1}' | xargs -r kubectl delete pv --wait=false >/dev/null 2>&1
  sudo -n nvme disconnect-all >/dev/null 2>&1 || true
  set -e
}

wait_for_port() {
  local port="$1"
  for _ in $(seq 1 60); do
    if (echo >"/dev/tcp/127.0.0.1/${port}") >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

with_master_port_forward() {
  local log="$1"
  shift
  kubectl -n "${HELM_NAMESPACE}" port-forward deploy/sw-blockmaster "${STATUS_PORT}:9333" >"${log}" 2>&1 &
  local pf_pid=$!
  if ! wait_for_port "${STATUS_PORT}"; then
    kill "${pf_pid}" >/dev/null 2>&1 || true
    wait "${pf_pid}" >/dev/null 2>&1 || true
    return 1
  fi
  "$@"
  local rc=$?
  kill "${pf_pid}" >/dev/null 2>&1 || true
  wait "${pf_pid}" >/dev/null 2>&1 || true
  return "${rc}"
}

wait_for_volume_ready() {
  local out_json="${ARTIFACT_DIR}/status/swblockvolumes.ready.json"
  for _ in $(seq 1 120); do
    kubectl -n "${HELM_NAMESPACE}" get swblockvolumes -o json >"${out_json}" 2>/dev/null || true
    python3 - "${out_json}" <<'PY' && return 0 || true
import json, sys
try:
    doc=json.load(open(sys.argv[1]))
except Exception:
    raise SystemExit(1)
items=doc.get("items") or []
if len(items) != 1:
    raise SystemExit(1)
st=items[0].get("status") or {}
nv=st.get("nvme") or {}
if st.get("pvcName") != "sw-block-multi-pvc-1":
    raise SystemExit(1)
if st.get("status") != "ready" or st.get("reasonCode") != "first_volume_verified":
    raise SystemExit(1)
if int(nv.get("pathCount", -1)) != 2:
    raise SystemExit(1)
if not any(c.get("type") == "Ready" and c.get("status") == "True" for c in st.get("conditions") or []):
    raise SystemExit(1)
PY
    sleep 2
  done
  kubectl -n "${HELM_NAMESPACE}" get swblockvolumes -o yaml >&2 || true
  return 1
}

write_summary "phase117_nvme_release_image_smoke_status=running"
write_summary "release_image=${IMAGE:-missing}"
write_summary "release_csi_image=${CSI_IMAGE:-missing}"

if [[ -z "${IMAGE}" || -z "${CSI_IMAGE}" ]]; then
  write_summary "phase117_nvme_release_image_smoke_status=blocked_missing_release_images"
  write_summary "reason=missing_SW_BLOCK_RELEASE_IMAGE_or_SW_BLOCK_CSI_RELEASE_IMAGE"
  write_summary "example_SW_BLOCK_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block:sha-<commit>"
  write_summary "example_SW_BLOCK_CSI_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block-csi:sha-<same-commit>"
  exit 2
fi

IMAGE_TAG_COMMIT="$(tag_commit "${IMAGE}")"
CSI_TAG_COMMIT="$(tag_commit "${CSI_IMAGE}")"
if [[ -z "${EXPECTED_COMMIT}" && -n "${IMAGE_TAG_COMMIT}" && "${IMAGE_TAG_COMMIT}" == "${CSI_TAG_COMMIT}" ]]; then
  EXPECTED_COMMIT="${IMAGE_TAG_COMMIT}"
fi
write_summary "expected_release_commit=${EXPECTED_COMMIT:-unknown}"

cd "${ROOT}"
require_cmd docker
require_cmd kubectl
require_cmd helm
require_cmd python3
require_cmd sudo
require_cmd nvme

if ! manifest_exists "${IMAGE}"; then
  write_summary "release_image_manifest=missing"
  write_summary "phase117_nvme_release_image_smoke_status=blocked_missing_release_images"
  write_summary "missing_image=${IMAGE}"
  exit 2
fi
write_summary "release_image_manifest=present"
if ! manifest_exists "${CSI_IMAGE}"; then
  write_summary "release_csi_image_manifest=missing"
  write_summary "phase117_nvme_release_image_smoke_status=blocked_missing_release_images"
  write_summary "missing_image=${CSI_IMAGE}"
  exit 2
fi
write_summary "release_csi_image_manifest=present"

docker pull "${IMAGE}" >"${ARTIFACT_DIR}/images/docker-pull-sw-block.txt" 2>&1
docker pull "${CSI_IMAGE}" >"${ARTIFACT_DIR}/images/docker-pull-sw-block-csi.txt" 2>&1
docker run --rm "${IMAGE}" /usr/local/bin/sw-block --version >"${ARTIFACT_DIR}/images/sw-block.version.txt" 2>&1 || true
docker run --rm "${IMAGE}" /usr/local/bin/blockmaster --version >"${ARTIFACT_DIR}/images/blockmaster.version.txt" 2>&1 || true
docker run --rm "${IMAGE}" /usr/local/bin/blockvolume --version >"${ARTIFACT_DIR}/images/blockvolume.version.txt" 2>&1 || true
docker run --rm --entrypoint /usr/local/bin/blockcsi "${CSI_IMAGE}" --version >"${ARTIFACT_DIR}/images/blockcsi.version.txt" 2>&1 || true

SW_BLOCK_REVISION="$(extract_revision "${ARTIFACT_DIR}/images/sw-block.version.txt")"
BLOCKMASTER_REVISION="$(extract_revision "${ARTIFACT_DIR}/images/blockmaster.version.txt")"
BLOCKVOLUME_REVISION="$(extract_revision "${ARTIFACT_DIR}/images/blockvolume.version.txt")"
BLOCKCSI_REVISION="$(extract_revision "${ARTIFACT_DIR}/images/blockcsi.version.txt")"
write_summary "sw_block_revision=${SW_BLOCK_REVISION:-unknown}"
write_summary "blockmaster_revision=${BLOCKMASTER_REVISION:-unknown}"
write_summary "blockvolume_revision=${BLOCKVOLUME_REVISION:-unknown}"
write_summary "blockcsi_revision=${BLOCKCSI_REVISION:-unknown}"

image_pair_commit_match=false
if [[ -n "${EXPECTED_COMMIT}" && ( -z "${IMAGE_TAG_COMMIT}" || "${IMAGE_TAG_COMMIT}" == "${EXPECTED_COMMIT}" ) && ( -z "${CSI_TAG_COMMIT}" || "${CSI_TAG_COMMIT}" == "${EXPECTED_COMMIT}" ) ]] &&
  version_matches_expected "${SW_BLOCK_REVISION}" &&
  version_matches_expected "${BLOCKMASTER_REVISION}" &&
  version_matches_expected "${BLOCKVOLUME_REVISION}" &&
  version_matches_expected "${BLOCKCSI_REVISION}"; then
  image_pair_commit_match=true
fi
write_summary "image_pair_commit_match=${image_pair_commit_match}"
if [[ "${image_pair_commit_match}" != "true" ]]; then
  write_summary "phase117_nvme_release_image_smoke_status=failed_image_pair_mismatch"
  exit 1
fi

trap cleanup EXIT
cleanup

go build -o "${ARTIFACT_DIR}/bin/sw-block" ./cmd/sw-block
"${ARTIFACT_DIR}/bin/sw-block" ops generate-helm-values \
  --kubeconfig /etc/rancher/k3s/k3s.yaml \
  --out "${ARTIFACT_DIR}/values/values.nvme.yaml" \
  --image "${IMAGE}" \
  --csi-image "${CSI_IMAGE}" \
  --protocol nvme \
  --stage2-multipath \
  --replication-factor 2 \
  --node-limit 2 >"${ARTIFACT_DIR}/values/generate.stdout.txt"

cat >>"${ARTIFACT_DIR}/values/values.nvme.yaml" <<'YAML'
operatorStatus:
  create: true
  dryRun: false
  interval: 5s
lifecycleOwner:
  create: true
  dryRun: false
  interval: 5s
YAML

python3 - "${ARTIFACT_DIR}" <<'PY'
from pathlib import Path
import sys
base=Path(sys.argv[1])
nodes=[]
cur=None
for raw in (base/"values/values.nvme.yaml").read_text().splitlines():
    line=raw.strip()
    if line.startswith("- name:"):
        if cur:
            nodes.append(cur)
        cur={}
    if cur is None:
        continue
    if line.startswith("kubernetesNode:"):
        cur["kubernetesNode"]=line.split(":",1)[1].strip().strip('"')
if cur:
    nodes.append(cur)
if len(nodes) < 2:
    raise SystemExit("need at least two generated nodes")
(base/"values/app-node.txt").write_text(nodes[1]["kubernetesNode"]+"\n")
PY

grep -q '^network_mode=external-nvme$' "${ARTIFACT_DIR}/values/generate.stdout.txt"
grep -q 'externalNVMe: true' "${ARTIFACT_DIR}/values/values.nvme.yaml"
grep -q 'stage2Multipath:' "${ARTIFACT_DIR}/values/values.nvme.yaml"
grep -q 'enabled: true' "${ARTIFACT_DIR}/values/values.nvme.yaml"
write_summary "helm_values_protocol=nvme"
write_summary "stage2_multipath_enabled=true"

helm lint charts/seaweed-block -f "${ARTIFACT_DIR}/values/values.nvme.yaml" >"${ARTIFACT_DIR}/install/helm-lint.txt"
helm template "${HELM_RELEASE}" charts/seaweed-block --namespace "${HELM_NAMESPACE}" \
  -f "${ARTIFACT_DIR}/values/values.nvme.yaml" >"${ARTIFACT_DIR}/install/helm-template.yaml"
grep -q -- '--launcher-external-nvme' "${ARTIFACT_DIR}/install/helm-template.yaml"
grep -q -- '--stage2-multipath' "${ARTIFACT_DIR}/install/helm-template.yaml"
helm install "${HELM_RELEASE}" charts/seaweed-block --namespace "${HELM_NAMESPACE}" --create-namespace \
  -f "${ARTIFACT_DIR}/values/values.nvme.yaml" --wait --timeout 10m >"${ARTIFACT_DIR}/install/helm-install.txt"

APP_NODE="$(cat "${ARTIFACT_DIR}/values/app-node.txt")"
SW_BLOCK_MULTI_VOLUME_NODE_SELECTOR="${APP_NODE}" \
SW_BLOCK_MULTI_VOLUME_PROTOCOL=nvme \
SW_BLOCK_MULTI_VOLUME_RF=2 \
SW_BLOCK_MULTI_VOLUME_COUNT=1 \
SW_BLOCK_MULTI_VOLUME_STAGE2_MULTIPATH=1 \
SW_BLOCK_MULTI_VOLUME_CLEANUP=0 \
SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/pvc" \
SW_BLOCK_CLI="${ARTIFACT_DIR}/bin/sw-block" \
  bash scripts/run-multi-volume-example.sh "${ROOT}"

grep -q '^multi_volume_status=ok$' "${ARTIFACT_DIR}/pvc/multi-volume-summary.txt"
grep -q '^writer_verified_count=1$' "${ARTIFACT_DIR}/pvc/multi-volume-summary.txt"
grep -q '^reader_verified_count=1$' "${ARTIFACT_DIR}/pvc/multi-volume-summary.txt"
write_summary "writer_verified=true"
write_summary "reader_verified=true"

wait_for_volume_ready
python3 - "${ARTIFACT_DIR}/status/swblockvolumes.ready.json" "${SUMMARY}" <<'PY'
import json, sys
doc=json.load(open(sys.argv[1]))
st=doc["items"][0]["status"]
nv=st.get("nvme") or {}
with open(sys.argv[2], "a") as out:
    out.write(f"volume_status={st.get('status')}\n")
    out.write(f"volume_reason={st.get('reasonCode')}\n")
    out.write(f"nvme_path_count={nv.get('pathCount')}\n")
PY

with_master_port_forward "${ARTIFACT_DIR}/status/blockmaster-port-forward.log" \
  "${ARTIFACT_DIR}/bin/sw-block" ops report \
    --master-api "127.0.0.1:${STATUS_PORT}" \
    --namespace "${APP_NAMESPACE}" \
    --out "${ARTIFACT_DIR}/status/report" \
    --timeout 30s >"${ARTIFACT_DIR}/status/report.stdout.txt" 2>"${ARTIFACT_DIR}/status/report.stderr.txt"
grep -q 'reason=first_volume_verified' "${ARTIFACT_DIR}/status/report/summary.txt"

write_summary "phase117_nvme_release_image_smoke_status=ok"

cleanup
verify_rc=0
SW_BLOCK_CLEANUP_WAIT_SECONDS=180 \
SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/cleanup/verify" bash scripts/verify-helm-cleanup.sh "${ROOT}" \
  >"${ARTIFACT_DIR}/cleanup/verify.stdout.txt" 2>"${ARTIFACT_DIR}/cleanup/verify.stderr.txt" || verify_rc=$?
cat "${ARTIFACT_DIR}/cleanup/verify/cleanup-summary.txt" >>"${SUMMARY}"
grep -q '^cleanup_status=ok$' "${ARTIFACT_DIR}/cleanup/verify/cleanup-summary.txt"
grep -q '^failure_count=0$' "${ARTIFACT_DIR}/cleanup/verify/cleanup-summary.txt"
if [[ "${verify_rc}" -ne 0 ]]; then
  exit "${verify_rc}"
fi

echo "[phase117] PASS"
