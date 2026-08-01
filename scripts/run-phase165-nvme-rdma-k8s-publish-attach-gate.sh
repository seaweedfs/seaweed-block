#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
RUN_ID="${SW_BLOCK_PHASE165_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase165-nvme-rdma-k8s-publish-attach-gate}"
SUMMARY="${ARTIFACT_DIR}/phase165-nvme-rdma-k8s-publish-attach-summary.txt"
VALUES_FILE="${ARTIFACT_DIR}/values/values.nvme-rdma.yaml"
HELM_RELEASE="${SW_BLOCK_HELM_RELEASE:-sw-block}"
HELM_NAMESPACE="${SW_BLOCK_HELM_NAMESPACE:-kube-system}"
APP_NAMESPACE="${SW_BLOCK_PHASE165_APP_NAMESPACE:-default}"
APP_PVC="sw-block-example-pvc"
TARGET_NODE="${SW_BLOCK_PHASE165_TARGET_NODE:-m02}"
TARGET_RDMA_IP="${SW_BLOCK_PHASE165_TARGET_RDMA_IP:-10.0.0.3}"
APP_NODE="${SW_BLOCK_PHASE165_APP_NODE:-m01}"
APP_RDMA_IP="${SW_BLOCK_PHASE165_APP_RDMA_IP:-10.0.0.1}"
APP_SSH_ADDR="${SW_BLOCK_PHASE165_APP_SSH_ADDR:-192.168.1.181}"
APP_SSH_USER="${SW_BLOCK_PHASE165_APP_SSH_USER:-testdev}"
APP_SSH_KEY="${SW_BLOCK_PHASE165_APP_SSH_KEY:-/opt/work/testdev_key}"
KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"
export KUBECONFIG

VOLUME_ID=""
NQN=""
NBD_BASELINE=""

mkdir -p "${ARTIFACT_DIR}"/{build,values,install,basic-app,live,cleanup}
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

active_nbd_count() {
  local count=0 pid_file
  for pid_file in /sys/block/nbd*/pid; do
    [[ -e "${pid_file}" ]] || continue
    [[ -s "${pid_file}" ]] && count=$((count + 1))
  done
  echo "${count}"
}

app_ssh() {
  ssh -i "${APP_SSH_KEY}" -o BatchMode=yes -o ConnectTimeout=8 \
    "${APP_SSH_USER}@${APP_SSH_ADDR}" "$@"
}

disconnect_gate_nqn() {
  [[ -n "${NQN}" ]] || return 0
  app_ssh "sudo nvme disconnect -n '${NQN}' >/dev/null 2>&1 || true" >/dev/null 2>&1 || true
}

cleanup() {
  set +e
  kubectl -n "${APP_NAMESPACE}" delete pod phase165-rdma-hold sw-block-example-reader sw-block-example-writer \
    --ignore-not-found=true --wait=true --timeout=90s >/dev/null 2>&1 || true
  kubectl -n "${APP_NAMESPACE}" delete pvc "${APP_PVC}" --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1 || true
  kubectl delete storageclass sw-block-example --ignore-not-found=true --wait=true --timeout=60s >/dev/null 2>&1 || true
  helm status "${HELM_RELEASE}" -n "${HELM_NAMESPACE}" >/dev/null 2>&1 && \
    helm uninstall "${HELM_RELEASE}" -n "${HELM_NAMESPACE}" --wait --timeout 180s >/dev/null 2>&1 || true
  disconnect_gate_nqn
  kubectl -n "${HELM_NAMESPACE}" delete swblockvolume "${APP_PVC}" --ignore-not-found=true --wait=true --timeout=60s >/dev/null 2>&1 || true
  kubectl -n "${HELM_NAMESPACE}" delete swblockcluster sw-block --ignore-not-found=true --wait=true --timeout=60s >/dev/null 2>&1 || true
  if [[ -n "${VOLUME_ID}" ]] && kubectl get pv "${VOLUME_ID}" >/dev/null 2>&1; then
    claim_ref="$(kubectl get pv "${VOLUME_ID}" -o jsonpath='{.spec.claimRef.namespace}/{.spec.claimRef.name}' 2>/dev/null)"
    if [[ "${claim_ref}" == "${APP_NAMESPACE}/${APP_PVC}" ]]; then
      kubectl patch pv "${VOLUME_ID}" --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null 2>&1 || true
      kubectl delete pv "${VOLUME_ID}" --ignore-not-found=true --wait=true --timeout=60s >/dev/null 2>&1 || true
    fi
  fi
  if [[ "$(kubectl get swblockvolumes -A --no-headers 2>/dev/null | wc -l)" == 0 &&
        "$(kubectl get swblockclusters -A --no-headers 2>/dev/null | wc -l)" == 0 ]]; then
    kubectl delete crd \
      swblockclusters.block.seaweedfs.com \
      swblockvolumes.block.seaweedfs.com \
      swblockreplicaeligibilities.block.seaweedfs.com \
      swblockreplicarebuilds.block.seaweedfs.com \
      swblockreplicafailbacks.block.seaweedfs.com \
      swblockfrontendpublications.block.seaweedfs.com \
      --ignore-not-found=true >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

write_summary "phase165_nvme_rdma_k8s_publish_attach_status=running"
write_summary "target_node=${TARGET_NODE}"
write_summary "target_rdma_ip=${TARGET_RDMA_IP}"
write_summary "app_node=${APP_NODE}"
write_summary "app_rdma_ip=${APP_RDMA_IP}"
write_summary "performance_slo_claim_allowed=false"
write_summary "failover_claim_allowed=false"

# Refuse to build evidence on top of an existing product install or controller.
if helm list -A -q | grep -q '^sw-block$'; then
  echo "existing sw-block Helm release; refusing unscoped cleanup" >&2
  exit 1
fi
if kubectl get pods,pvc,pv -A -o name 2>/dev/null | grep -Eq '(sw-block|seaweed-block)'; then
  echo "existing Seaweed Block Kubernetes resources; refusing unscoped cleanup" >&2
  exit 1
fi
if kubectl get volumeattachments.storage.k8s.io \
    -o custom-columns=ATTACHER:.spec.attacher --no-headers 2>/dev/null \
    | grep -q '^block\.csi\.seaweedfs\.com$'; then
  echo "existing Seaweed Block VolumeAttachment; refusing contaminated lab" >&2
  exit 1
fi
if app_ssh "sudo nvme list-subsys -o json 2>/dev/null" | grep -q 'nqn.2026-05.io.seaweedfs'; then
  echo "existing Seaweed Block NVMe controller on app host; refusing unscoped cleanup" >&2
  exit 1
fi
if sudo find /sys/kernel/config/nvmet/subsystems -mindepth 1 -maxdepth 1 -type d \
    -name '*io.seaweedfs*' -print -quit 2>/dev/null | grep -q .; then
  echo "existing Seaweed Block NVMe target; refusing contaminated lab" >&2
  exit 1
fi

kubectl get node "${TARGET_NODE}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' | grep -q '^True$'
kubectl get node "${APP_NODE}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' | grep -q '^True$'
ip -4 -o addr show | grep -q " ${TARGET_RDMA_IP}/"
rdma link show | grep -q 'state ACTIVE'
app_ssh "ip -4 -o addr show | grep -q ' ${APP_RDMA_IP}/'; rdma link show | grep -q 'state ACTIVE'; sudo modprobe nvme-rdma"
sudo modprobe nvmet-rdma
sudo modprobe nbd
NBD_BASELINE="$(active_nbd_count)"
if [[ "${NBD_BASELINE}" != 0 ]]; then
  echo "existing active NBD device; refusing contaminated lab" >&2
  exit 1
fi
write_summary "rdma_host_preflight=ok"
write_summary "nbd_baseline=${NBD_BASELINE}"

cd "${ROOT}"
go test ./core/lifecycle ./core/csi ./core/host/master ./core/ops ./core/launcher ./cmd/blockvolume ./cmd/sw-block \
  >"${ARTIFACT_DIR}/local-go-test.log" 2>&1
helm lint charts/seaweed-block >"${ARTIFACT_DIR}/helm-lint-default.log" 2>&1
if helm template guard charts/seaweed-block --set storageClass.protocol=nvme \
    --set storageClass.nvmeTransport=rdma >"${ARTIFACT_DIR}/rdma-guard.log" 2>&1; then
  echo "RDMA chart rendered without external NVMe" >&2
  exit 1
fi
grep -q 'requires network.externalNVMe=true' "${ARTIFACT_DIR}/rdma-guard.log"
write_summary "local_contract_gate=ok"
write_summary "rdma_external_address_guard=ok"

SW_BLOCK_IMPORT_K3S=1 \
SW_BLOCK_IMPORT_K3S_NODES="${APP_SSH_ADDR}" \
SW_BLOCK_IMPORT_K3S_SSH_USER="${APP_SSH_USER}" \
SW_BLOCK_IMPORT_K3S_SSH_KEY="${APP_SSH_KEY}" \
SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/build" \
SW_BLOCK_DOCKER_NO_CACHE=1 \
  bash scripts/build-alpha-images.sh "${ROOT}" >"${ARTIFACT_DIR}/build/build.log" 2>&1
grep -q '^SW_BLOCK_IMAGE_ID=' "${ARTIFACT_DIR}/build/alpha-images.env"
grep -q '^SW_BLOCK_CSI_IMAGE_ID=' "${ARTIFACT_DIR}/build/alpha-images.env"
write_summary "fresh_matching_images=ok"

go build -o "${ARTIFACT_DIR}/sw-block" ./cmd/sw-block
"${ARTIFACT_DIR}/sw-block" ops generate-helm-values \
  --kubeconfig "${KUBECONFIG}" \
  --out "${VALUES_FILE}" \
  --image sw-block:local \
  --csi-image sw-block-csi:local \
  --target-node "${TARGET_NODE}" \
  --protocol nvme \
  --nvme-transport rdma \
  --frontend-ip-map "${TARGET_NODE}=${TARGET_RDMA_IP}" \
  --frontend-network-class 100gbe_roce \
  >"${ARTIFACT_DIR}/values/generate.stdout.txt"
grep -q '^nvme_transport=rdma$' "${ARTIFACT_DIR}/values/generate.stdout.txt"
grep -q 'nvmeTransport: rdma' "${VALUES_FILE}"
grep -q "frontendIP: ${TARGET_RDMA_IP}" "${VALUES_FILE}"
write_summary "generated_nvme_transport=rdma"
write_summary "generated_frontend_network_class=100gbe_roce"

helm lint charts/seaweed-block -f "${VALUES_FILE}" >"${ARTIFACT_DIR}/install/helm-lint-rdma.log" 2>&1
helm template "${HELM_RELEASE}" charts/seaweed-block -n "${HELM_NAMESPACE}" -f "${VALUES_FILE}" \
  --set operatorStatus.create=true --set operatorStatus.dryRun=false --set operatorStatus.interval=2s \
  >"${ARTIFACT_DIR}/install/helm-template.yaml"
grep -q 'sw-block.seaweedfs.com/nvme-transport: "rdma"' "${ARTIFACT_DIR}/install/helm-template.yaml"
grep -q 'modprobe nvme_rdma' "${ARTIFACT_DIR}/install/helm-template.yaml"
helm install "${HELM_RELEASE}" charts/seaweed-block -n "${HELM_NAMESPACE}" --create-namespace \
  -f "${VALUES_FILE}" \
  --set operatorStatus.create=true --set operatorStatus.dryRun=false --set operatorStatus.interval=2s \
  --wait --timeout 10m >"${ARTIFACT_DIR}/install/helm-install.log"
cat <<'YAML' | kubectl apply -f -
apiVersion: block.seaweedfs.com/v1alpha1
kind: SwBlockCluster
metadata:
  name: sw-block
  namespace: kube-system
spec: {}
YAML
write_summary "helm_install=ok"

SW_BLOCK_BASIC_APP_NODE_SELECTOR="${APP_NODE}" \
SW_BLOCK_BASIC_APP_PROTOCOL=nvme \
SW_BLOCK_BASIC_APP_NVME_TRANSPORT=rdma \
SW_BLOCK_BASIC_APP_CLEANUP=0 \
SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/basic-app" \
SW_BLOCK_INSTALL_MODE=helm \
SW_BLOCK_HELM_RELEASE="${HELM_RELEASE}" \
SW_BLOCK_HELM_NAMESPACE="${HELM_NAMESPACE}" \
SW_BLOCK_HELM_VALUES_FILE="${VALUES_FILE}" \
SW_BLOCK_CLI="${ARTIFACT_DIR}/sw-block" \
  bash scripts/run-basic-app-example.sh "${ROOT}" \
  >"${ARTIFACT_DIR}/basic-app/stdout.txt" 2>"${ARTIFACT_DIR}/basic-app/stderr.txt"
grep -q '^writer_verified=true$' "${ARTIFACT_DIR}/basic-app/first-volume-summary.txt"
grep -q '^reader_verified=true$' "${ARTIFACT_DIR}/basic-app/first-volume-summary.txt"
grep -q '^app_nvme_transport=rdma$' "${ARTIFACT_DIR}/basic-app/first-volume-summary.txt"
VOLUME_ID="$(sed -n 's/^volume_id=//p' "${ARTIFACT_DIR}/basic-app/first-volume-summary.txt" | head -1)"
[[ -n "${VOLUME_ID}" && "${VOLUME_ID}" != "unknown" ]]

cat <<YAML | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: phase165-rdma-hold
  namespace: default
spec:
  nodeSelector:
    kubernetes.io/hostname: "${APP_NODE}"
  restartPolicy: Never
  containers:
    - name: hold
      image: busybox:1.36
      command: ["/bin/sh", "-c", "sha256sum /data/demo.bin; sleep 600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: sw-block-example-pvc
YAML
kubectl -n default wait --for=condition=Ready pod/phase165-rdma-hold --timeout=240s

for _ in $(seq 1 90); do
  NQN="$(kubectl -n "${HELM_NAMESPACE}" get swblockvolume sw-block-example-pvc -o jsonpath='{.status.nvme.nqn}' 2>/dev/null || true)"
  TRANSPORT="$(kubectl -n "${HELM_NAMESPACE}" get swblockvolume sw-block-example-pvc -o jsonpath='{.status.nvme.transport}' 2>/dev/null || true)"
  [[ -n "${NQN}" && "${TRANSPORT}" == "rdma" ]] && break
  sleep 2
done
[[ -n "${NQN}" ]]
[[ "${TRANSPORT}" == "rdma" ]]
kubectl -n "${HELM_NAMESPACE}" get swblockvolume sw-block-example-pvc -o yaml >"${ARTIFACT_DIR}/live/swblockvolume.yaml"
grep -q 'transport: rdma' "${ARTIFACT_DIR}/live/swblockvolume.yaml"
grep -q 'nvmeAddr: 10.0.0.3:' "${ARTIFACT_DIR}/live/swblockvolume.yaml"

DEPLOYMENT="$(kubectl -n "${APP_NAMESPACE}" get deploy -l sw-block.seaweedfs.com/volume="${VOLUME_ID}" -o jsonpath='{.items[0].metadata.name}')"
[[ -n "${DEPLOYMENT}" ]]
kubectl -n "${APP_NAMESPACE}" get deploy "${DEPLOYMENT}" -o yaml >"${ARTIFACT_DIR}/live/blockvolume-deployment.yaml"
grep -q -- '--nvme-transport=rdma' "${ARTIFACT_DIR}/live/blockvolume-deployment.yaml"
grep -q 'privileged: true' "${ARTIFACT_DIR}/live/blockvolume-deployment.yaml"
grep -q 'mountPath: /sys/kernel/config' "${ARTIFACT_DIR}/live/blockvolume-deployment.yaml"

app_ssh "sudo bash -s" >"${ARTIFACT_DIR}/live/app-nvme-controller.txt" <<HOST
set -euo pipefail
NQN='${NQN}'
found=0
for p in /sys/class/nvme/nvme*/subsysnqn; do
  [[ -e "\$p" ]] || continue
  [[ "\$(cat "\$p")" == "\$NQN" ]] || continue
  c="\$(basename "\$(dirname "\$p")")"
  transport="\$(cat "/sys/class/nvme/\$c/transport")"
  address="\$(cat "/sys/class/nvme/\$c/address")"
  echo "controller=\$c"
  echo "transport=\$transport"
  echo "address=\$address"
  [[ "\$transport" == "rdma" ]]
  [[ "\$address" == *"traddr=${TARGET_RDMA_IP}"* ]]
  found=1
done
[[ "\$found" == 1 ]]
HOST
grep -q '^transport=rdma$' "${ARTIFACT_DIR}/live/app-nvme-controller.txt"
grep -q "traddr=${TARGET_RDMA_IP}" "${ARTIFACT_DIR}/live/app-nvme-controller.txt"
write_summary "csi_publish_context_transport=rdma"
write_summary "active_host_controller_transport=rdma"
write_summary "active_host_controller_traddr=${TARGET_RDMA_IP}"
write_summary "swblockvolume_status_transport=rdma"
write_summary "writer_verified=true"
write_summary "reader_verified=true"
write_summary "tcp_fallback_observed=false"

kubectl -n "${APP_NAMESPACE}" delete pod phase165-rdma-hold sw-block-example-reader sw-block-example-writer \
  --ignore-not-found=true --wait=true --timeout=120s
detached_samples=0
for _ in $(seq 1 120); do
  attachment_count="$(kubectl get volumeattachments.storage.k8s.io \
    -o custom-columns=PV:.spec.source.persistentVolumeName --no-headers 2>/dev/null \
    | awk -v volume="${VOLUME_ID}" '$1 == volume {count++} END {print count + 0}')"
  if [[ "${attachment_count}" == 0 ]]; then
    detached_samples=$((detached_samples + 1))
    [[ "${detached_samples}" == 3 ]] && break
  else
    detached_samples=0
  fi
  sleep 1
done
if [[ "${detached_samples}" != 3 ]]; then
  kubectl get volumeattachments.storage.k8s.io -o yaml \
    >"${ARTIFACT_DIR}/cleanup/volumeattachments-detach-timeout.yaml" 2>&1 || true
  echo "VolumeAttachment for ${VOLUME_ID} did not detach before PVC deletion" >&2
  exit 1
fi
write_summary "volume_detached_before_pvc_delete=true"
kubectl -n "${APP_NAMESPACE}" delete pvc "${APP_PVC}" --wait=true --timeout=180s
kubectl delete storageclass sw-block-example --ignore-not-found=true --wait=true --timeout=60s
for _ in $(seq 1 60); do
  kubectl get pv "${VOLUME_ID}" >/dev/null 2>&1 || break
  sleep 2
done
if kubectl get pv "${VOLUME_ID}" >/dev/null 2>&1; then
  kubectl get pv "${VOLUME_ID}" -o yaml >"${ARTIFACT_DIR}/cleanup/pv-stuck.yaml" 2>&1 || true
  kubectl get volumeattachments.storage.k8s.io -o yaml \
    >"${ARTIFACT_DIR}/cleanup/volumeattachments-stuck.yaml" 2>&1 || true
  kubectl -n "${APP_NAMESPACE}" get events --sort-by=.lastTimestamp \
    >"${ARTIFACT_DIR}/cleanup/events-stuck.txt" 2>&1 || true
  controller_pod="$(kubectl -n "${HELM_NAMESPACE}" get pod -l app=sw-block-csi-controller \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -n "${controller_pod}" ]]; then
    for container in block-csi csi-attacher csi-provisioner; do
      kubectl -n "${HELM_NAMESPACE}" logs "${controller_pod}" -c "${container}" --since=15m \
        >"${ARTIFACT_DIR}/cleanup/${container}-stuck.log" 2>&1 || true
    done
  fi
  echo "PV ${VOLUME_ID} still exists before Helm uninstall" >&2
  exit 1
fi
helm uninstall "${HELM_RELEASE}" -n "${HELM_NAMESPACE}" --wait --timeout 180s \
  >"${ARTIFACT_DIR}/cleanup/helm-uninstall.log"
kubectl -n "${HELM_NAMESPACE}" delete swblockvolume "${APP_PVC}" \
  --ignore-not-found=true --wait=true --timeout=60s
kubectl -n "${HELM_NAMESPACE}" delete swblockcluster sw-block --ignore-not-found=true --wait=true --timeout=60s

for _ in $(seq 1 90); do
  target_present=0
  [[ -e "/sys/kernel/config/nvmet/subsystems/${NQN}" ]] && target_present=1
  controller_present="$(app_ssh "sudo nvme list-subsys -o json 2>/dev/null | grep -c '${NQN}' || true")"
  [[ "${target_present}" == 0 && "${controller_present}" == 0 && "$(active_nbd_count)" == "${NBD_BASELINE}" ]] && break
  sleep 2
done
[[ ! -e "/sys/kernel/config/nvmet/subsystems/${NQN}" ]]
[[ "$(app_ssh "sudo nvme list-subsys -o json 2>/dev/null | grep -c '${NQN}' || true")" == 0 ]]
[[ "$(active_nbd_count)" == "${NBD_BASELINE}" ]]

SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/cleanup/verifier" \
SW_BLOCK_HELM_RELEASE="${HELM_RELEASE}" \
SW_BLOCK_HELM_NAMESPACE="${HELM_NAMESPACE}" \
  bash scripts/verify-helm-cleanup.sh >"${ARTIFACT_DIR}/cleanup/verifier.stdout.txt"
grep -q '^cleanup_status=ok$' "${ARTIFACT_DIR}/cleanup/verifier/cleanup-summary.txt"

# Helm intentionally retains CRDs. The gate removes them only after proving no
# instances remain, so the shared lab returns to its pre-run baseline.
[[ "$(kubectl get swblockvolumes -A --no-headers 2>/dev/null | wc -l)" == 0 ]]
[[ "$(kubectl get swblockclusters -A --no-headers 2>/dev/null | wc -l)" == 0 ]]
kubectl delete crd \
  swblockclusters.block.seaweedfs.com \
  swblockvolumes.block.seaweedfs.com \
  swblockreplicaeligibilities.block.seaweedfs.com \
  swblockreplicarebuilds.block.seaweedfs.com \
  swblockreplicafailbacks.block.seaweedfs.com \
  swblockfrontendpublications.block.seaweedfs.com \
  --ignore-not-found=true >/dev/null

write_summary "target_configfs_residue_count=0"
write_summary "target_nbd_residue_count=0"
write_summary "app_nvme_controller_residue_count=0"
write_summary "kubernetes_product_residue_count=0"
write_summary "test_owned_status_cr_cleanup=true"
write_summary "cleanup_status=ok"
write_summary "phase165_nvme_rdma_k8s_publish_attach_status=ok"

cat "${SUMMARY}"
