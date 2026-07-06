#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase120-nvme-tcp-performance-baseline-gate}"
SUMMARY="${ARTIFACT_DIR}/phase120-nvme-tcp-performance-baseline-summary.txt"
HELM_RELEASE="${SW_BLOCK_HELM_RELEASE:-sw-block}"
HELM_NAMESPACE="${SW_BLOCK_HELM_NAMESPACE:-kube-system}"
APP_NAMESPACE="${SW_BLOCK_APP_NAMESPACE:-default}"
IMAGE="${SW_BLOCK_IMAGE:-sw-block:local}"
CSI_IMAGE="${SW_BLOCK_CSI_IMAGE:-sw-block-csi:local}"
STATUS_PORT="${SW_BLOCK_MASTER_PORT_FORWARD_PORT:-29335}"
PVC_NAME="${SW_BLOCK_PHASE120_PVC_NAME:-sw-block-phase120-pvc}"
SC_NAME="${SW_BLOCK_PHASE120_STORAGECLASS:-sw-block-phase120}"
PERF_POD="${SW_BLOCK_PHASE120_POD:-sw-block-phase120-perf}"
PVC_SIZE="${SW_BLOCK_PHASE120_PVC_SIZE:-512Mi}"
SEQ_MIB="${SW_BLOCK_PHASE120_SEQ_MIB:-64}"
SMALL_OPS="${SW_BLOCK_PHASE120_SMALL_OPS:-256}"
SMALL_BLOCK_BYTES="${SW_BLOCK_PHASE120_SMALL_BLOCK_BYTES:-4096}"
FRONTEND_IP_MAP="${SW_BLOCK_FRONTEND_IP_MAP:-}"
FRONTEND_NETWORK_CLASS="${SW_BLOCK_FRONTEND_NETWORK_CLASS:-}"
EXPECTED_FRONTEND_ROUTE_DEV="${SW_BLOCK_EXPECTED_FRONTEND_ROUTE_DEV:-}"
PROFILE_WRITE="${SW_BLOCK_PHASE120_PROFILE_WRITE:-false}"
PROFILE_INTERVAL_SECONDS="${SW_BLOCK_PHASE120_PROFILE_INTERVAL_SECONDS:-1}"

mkdir -p "${ARTIFACT_DIR}"/{bin,build,values,install,pvc,perf,status,cleanup,profile}
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

rate_mibps() {
  python3 - "$1" "$2" <<'PY'
import sys
bytes_count = int(sys.argv[1])
duration_ms = max(int(sys.argv[2]), 1)
print(f"{(bytes_count / 1048576.0) / (duration_ms / 1000.0):.2f}")
PY
}

rate_iops() {
  python3 - "$1" "$2" <<'PY'
import sys
ops = int(sys.argv[1])
duration_ms = max(int(sys.argv[2]), 1)
print(f"{ops / (duration_ms / 1000.0):.2f}")
PY
}

cleanup() {
  set +e
  kubectl -n "${APP_NAMESPACE}" delete pod "${PERF_POD}" --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  kubectl -n "${APP_NAMESPACE}" delete pvc "${PVC_NAME}" --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  kubectl delete storageclass "${SC_NAME}" --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  kubectl get pv --no-headers 2>/dev/null | awk -v pvc="${PVC_NAME}" '$0 ~ pvc {print $1}' | \
    xargs -r -n1 kubectl patch pv --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null 2>&1
  kubectl get pv --no-headers 2>/dev/null | awk -v pvc="${PVC_NAME}" '$0 ~ pvc {print $1}' | \
    xargs -r kubectl delete pv --wait=false >/dev/null 2>&1
  sudo -n nvme disconnect-all >/dev/null 2>&1 || true
  helm status "${HELM_RELEASE}" --namespace "${HELM_NAMESPACE}" >/dev/null 2>&1 && \
    helm uninstall "${HELM_RELEASE}" --namespace "${HELM_NAMESPACE}" --wait --timeout 240s \
      >"${ARTIFACT_DIR}/cleanup/helm-uninstall.txt" 2>&1
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
  sudo -n nvme disconnect-all >/dev/null 2>&1 || true
  set -e
}
trap cleanup EXIT

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

collect_cluster_evidence() {
  "${ARTIFACT_DIR}/bin/sw-block" ops cluster \
    --master-api "127.0.0.1:${STATUS_PORT}" \
    --timeout 30s -o json \
    >"${ARTIFACT_DIR}/status/cluster-evidence.json" \
    2>"${ARTIFACT_DIR}/status/cluster-evidence.stderr.txt"
  "${ARTIFACT_DIR}/bin/sw-block" ops inventory \
    --namespace "${APP_NAMESPACE}" \
    --master "127.0.0.1:${STATUS_PORT}" \
    --timeout 30s \
    --out "${ARTIFACT_DIR}/status/inventory" \
    >"${ARTIFACT_DIR}/status/inventory.stdout.txt" \
    2>"${ARTIFACT_DIR}/status/inventory.stderr.txt"
}

collect_durable_write_profile() {
  python3 - \
    "${ARTIFACT_DIR}/status/cluster-evidence.json" \
    "${ARTIFACT_DIR}/status/inventory/volume-inventory.json" \
    "${ARTIFACT_DIR}/status/status-durable-after-seq-write.json" \
    "${ARTIFACT_DIR}/status/write-profile-summary.txt" <<'PY'
import json
import sys
import urllib.request

cluster_path, inventory_path, durable_out, summary_out = sys.argv[1:]

def field(obj, *names, default=None):
    for name in names:
        if isinstance(obj, dict) and name in obj:
            return obj[name]
    return default

doc = json.load(open(cluster_path))
managed = field(doc, "managed_volumes", "managedVolumes", default=[]) or []
if len(managed) != 1:
    raise SystemExit(f"managed_volume_count={len(managed)}, want 1")
vol = managed[0]
volume_id = field(vol, "volume_id", "volumeID")
if not volume_id:
    raise SystemExit("volume_id unavailable")
replicas = field(vol, "replicas", default=[]) or []
if not replicas:
    inv = json.load(open(inventory_path))
    for inv_vol in field(inv, "volumes", default=[]) or []:
        if field(inv_vol, "volume_id", "volumeID") == volume_id:
            replicas = field(inv_vol, "replicas", default=[]) or []
            break
chosen = None
for replica in replicas:
    if field(replica, "frontend_primary_ready", "frontendPrimaryReady", default=False):
        chosen = replica
        break
if chosen is None:
    for replica in replicas:
        if field(replica, "healthy", default=False):
            chosen = replica
            break
if chosen is None and replicas:
    chosen = replicas[0]
status_addr = field(chosen or {}, "status_addr", "statusAddr", "status_address", "statusAddress")
if not status_addr:
    raise SystemExit("status_addr unavailable")
url = f"http://{status_addr}/status/durable?volume={volume_id}"
with urllib.request.urlopen(url, timeout=10) as resp:
    raw = resp.read()
open(durable_out, "wb").write(raw)
body = json.loads(raw)
vols = field(body, "Volumes", "volumes", default=[]) or []
if len(vols) != 1:
    raise SystemExit(f"durable_volume_count={len(vols)}, want 1")
profile = field(vols[0], "WriteProfile", "writeProfile", default={}) or {}
target_ops = int(field(profile, "TargetWriteOps", "targetWriteOps", default=0) or 0)
target_bytes = int(field(profile, "TargetWriteBytes", "targetWriteBytes", default=0) or 0)
target_ns = int(field(profile, "TargetWriteDurationNanos", "targetWriteDurationNanos", default=0) or 0)
backend_write_request_ops = int(field(profile, "BackendWriteRequestOps", "backendWriteRequestOps", default=0) or 0)
backend_write_request_bytes = int(field(profile, "BackendWriteRequestBytes", "backendWriteRequestBytes", default=0) or 0)
backend_write_request_max_bytes = int(field(profile, "BackendWriteRequestMaxBytes", "backendWriteRequestMaxBytes", default=0) or 0)
backend_write_request_avg_bytes = int(backend_write_request_bytes / backend_write_request_ops) if backend_write_request_ops else 0
backend_ops = int(field(profile, "BackendWriteOps", "backendWriteOps", default=0) or 0)
backend_bytes = int(field(profile, "BackendWriteBytes", "backendWriteBytes", default=0) or 0)
backend_ns = int(field(profile, "BackendWriteDurationNanos", "backendWriteDurationNanos", default=0) or 0)
backend_storage_write_calls = int(field(profile, "BackendStorageWriteCalls", "backendStorageWriteCalls", default=0) or 0)
backend_storage_write_blocks = int(field(profile, "BackendStorageWriteBlocks", "backendStorageWriteBlocks", default=0) or 0)
backend_storage_batch_calls = int(field(profile, "BackendStorageBatchCalls", "backendStorageBatchCalls", default=0) or 0)
backend_storage_batch_blocks = int(field(profile, "BackendStorageBatchBlocks", "backendStorageBatchBlocks", default=0) or 0)
backend_full_block_batch_calls = int(field(profile, "BackendFullBlockBatchCalls", "backendFullBlockBatchCalls", default=0) or 0)
backend_full_block_batch_blocks = int(field(profile, "BackendFullBlockBatchBlocks", "backendFullBlockBatchBlocks", default=0) or 0)
backend_full_block_batch_max = int(field(profile, "BackendFullBlockBatchMax", "backendFullBlockBatchMax", default=0) or 0)
backend_full_block_batch_avg = int(backend_full_block_batch_blocks / backend_full_block_batch_calls) if backend_full_block_batch_calls else 0
wal_copy_ops = int(field(profile, "WALCopyOps", "walCopyOps", default=0) or 0)
wal_copy_bytes = int(field(profile, "WALCopyBytes", "walCopyBytes", default=0) or 0)
wal_copy_ns = int(field(profile, "WALCopyDurationNanos", "walCopyDurationNanos", default=0) or 0)
wal_encode_ops = int(field(profile, "WALEncodeOps", "walEncodeOps", default=0) or 0)
wal_encode_bytes = int(field(profile, "WALEncodeBytes", "walEncodeBytes", default=0) or 0)
wal_encode_ns = int(field(profile, "WALEncodeDurationNanos", "walEncodeDurationNanos", default=0) or 0)
wal_checksum_ops = int(field(profile, "WALChecksumOps", "walChecksumOps", default=0) or 0)
wal_checksum_bytes = int(field(profile, "WALChecksumBytes", "walChecksumBytes", default=0) or 0)
wal_checksum_ns = int(field(profile, "WALChecksumDurationNanos", "walChecksumDurationNanos", default=0) or 0)
wal_append_ops = int(field(profile, "WALAppendOps", "walAppendOps", default=0) or 0)
wal_append_bytes = int(field(profile, "WALAppendBytes", "walAppendBytes", default=0) or 0)
wal_append_ns = int(field(profile, "WALAppendDurationNanos", "walAppendDurationNanos", default=0) or 0)
wal_append_writeat_calls = int(field(profile, "WALAppendWriteAtCalls", "walAppendWriteAtCalls", default=0) or 0)
wal_append_writeat_bytes = int(field(profile, "WALAppendWriteAtBytes", "walAppendWriteAtBytes", default=0) or 0)
wal_append_writeat_max_bytes = int(field(profile, "WALAppendWriteAtMaxBytes", "walAppendWriteAtMaxBytes", default=0) or 0)
wal_append_writeat_avg_bytes = int(wal_append_writeat_bytes / wal_append_writeat_calls) if wal_append_writeat_calls else 0
wal_append_wrap_count = int(field(profile, "WALAppendWrapCount", "walAppendWrapCount", default=0) or 0)
wal_append_padding_bytes = int(field(profile, "WALAppendPaddingBytes", "walAppendPaddingBytes", default=0) or 0)
dirty_map_update_ops = int(field(profile, "DirtyMapUpdateOps", "dirtyMapUpdateOps", default=0) or 0)
dirty_map_update_ns = int(field(profile, "DirtyMapUpdateDurationNanos", "dirtyMapUpdateDurationNanos", default=0) or 0)
sync_ops = int(field(profile, "BackendSyncOps", "backendSyncOps", default=0) or 0)
sync_ns = int(field(profile, "BackendSyncDurationNanos", "backendSyncDurationNanos", default=0) or 0)
def ms(ns):
    return (ns + 999_999) // 1_000_000 if ns > 0 else 0
with open(summary_out, "w") as f:
    f.write("phase120_write_profile_status=ok\n")
    f.write(f"write_profile_volume_id={volume_id}\n")
    f.write(f"write_profile_status_addr={status_addr}\n")
    f.write(f"target_write_observed={str(target_ops > 0 and target_bytes > 0).lower()}\n")
    f.write(f"target_write_ops={target_ops}\n")
    f.write(f"target_write_bytes={target_bytes}\n")
    f.write(f"target_write_duration_ms={ms(target_ns)}\n")
    f.write(f"backend_write_request_ops={backend_write_request_ops}\n")
    f.write(f"backend_write_request_bytes={backend_write_request_bytes}\n")
    f.write(f"backend_write_request_max_bytes={backend_write_request_max_bytes}\n")
    f.write(f"backend_write_request_avg_bytes={backend_write_request_avg_bytes}\n")
    f.write(f"backend_write_ops={backend_ops}\n")
    f.write(f"backend_write_bytes={backend_bytes}\n")
    f.write(f"backend_write_duration_ms={ms(backend_ns)}\n")
    f.write(f"backend_storage_write_calls={backend_storage_write_calls}\n")
    f.write(f"backend_storage_write_blocks={backend_storage_write_blocks}\n")
    f.write(f"backend_storage_batch_calls={backend_storage_batch_calls}\n")
    f.write(f"backend_storage_batch_blocks={backend_storage_batch_blocks}\n")
    f.write(f"backend_full_block_batch_calls={backend_full_block_batch_calls}\n")
    f.write(f"backend_full_block_batch_blocks={backend_full_block_batch_blocks}\n")
    f.write(f"backend_full_block_batch_max={backend_full_block_batch_max}\n")
    f.write(f"backend_full_block_batch_avg={backend_full_block_batch_avg}\n")
    f.write(f"wal_copy_ops={wal_copy_ops}\n")
    f.write(f"wal_copy_bytes={wal_copy_bytes}\n")
    f.write(f"wal_copy_duration_ms={ms(wal_copy_ns)}\n")
    f.write(f"wal_encode_ops={wal_encode_ops}\n")
    f.write(f"wal_encode_bytes={wal_encode_bytes}\n")
    f.write(f"wal_encode_duration_ms={ms(wal_encode_ns)}\n")
    f.write(f"wal_checksum_ops={wal_checksum_ops}\n")
    f.write(f"wal_checksum_bytes={wal_checksum_bytes}\n")
    f.write(f"wal_checksum_duration_ms={ms(wal_checksum_ns)}\n")
    f.write(f"wal_append_ops={wal_append_ops}\n")
    f.write(f"wal_append_bytes={wal_append_bytes}\n")
    f.write(f"wal_append_duration_ms={ms(wal_append_ns)}\n")
    f.write(f"wal_append_writeat_calls={wal_append_writeat_calls}\n")
    f.write(f"wal_append_writeat_bytes={wal_append_writeat_bytes}\n")
    f.write(f"wal_append_writeat_max_bytes={wal_append_writeat_max_bytes}\n")
    f.write(f"wal_append_writeat_avg_bytes={wal_append_writeat_avg_bytes}\n")
    f.write(f"wal_append_wrap_count={wal_append_wrap_count}\n")
    f.write(f"wal_append_padding_bytes={wal_append_padding_bytes}\n")
    f.write(f"dirty_map_update_ops={dirty_map_update_ops}\n")
    f.write(f"dirty_map_update_duration_ms={ms(dirty_map_update_ns)}\n")
    f.write(f"backend_sync_ops={sync_ops}\n")
    f.write(f"backend_sync_duration_ms={ms(sync_ns)}\n")
PY
  cat "${ARTIFACT_DIR}/status/write-profile-summary.txt" >>"${SUMMARY}"
}

measure_exec_ms() {
  local log="$1"
  shift
  local start_ns end_ns
  start_ns="$(date +%s%N)"
  "$@" >"${log}" 2>&1
  end_ns="$(date +%s%N)"
  python3 - "$start_ns" "$end_ns" <<'PY'
import sys
start = int(sys.argv[1])
end = int(sys.argv[2])
print(max((end - start) // 1_000_000, 1))
PY
}

profile_write_sample_loop() {
  local target_pid="$1"
  local sample_file="$2"
  local sample=0
  : >"${sample_file}"
  while kill -0 "${target_pid}" >/dev/null 2>&1; do
    {
      echo "=== sample=${sample} ts=$(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
      kubectl get pods -A -o wide 2>&1 || true
      kubectl top pods -A --containers 2>&1 || true
      kubectl top nodes 2>&1 || true
      ps -eo pid,ppid,pcpu,pmem,comm,args | grep -E 'blockmaster|blockvolume|blockcsi|sw-block' | grep -v grep || true
      kubectl -n "${APP_NAMESPACE}" logs -l app=sw-blockvolume --tail=30 --prefix 2>&1 || true
    } >>"${sample_file}"
    sample=$((sample + 1))
    sleep "${PROFILE_INTERVAL_SECONDS}"
  done
}

measure_profiled_exec_ms() {
  local log="$1"
  local sample_file="$2"
  shift 2
  local start_ns end_ns cmd_pid sampler_pid rc
  start_ns="$(date +%s%N)"
  "$@" >"${log}" 2>&1 &
  cmd_pid=$!
  sampler_pid=""
  if [[ "${PROFILE_WRITE}" == "true" ]]; then
    profile_write_sample_loop "${cmd_pid}" "${sample_file}" &
    sampler_pid=$!
  fi
  set +e
  wait "${cmd_pid}"
  rc=$?
  set -e
  if [[ -n "${sampler_pid}" ]]; then
    kill "${sampler_pid}" >/dev/null 2>&1 || true
    wait "${sampler_pid}" >/dev/null 2>&1 || true
  fi
  end_ns="$(date +%s%N)"
  if [[ "${rc}" -ne 0 ]]; then
    return "${rc}"
  fi
  python3 - "$start_ns" "$end_ns" <<'PY'
import sys
start = int(sys.argv[1])
end = int(sys.argv[2])
print(max((end - start) // 1_000_000, 1))
PY
}

require_cmd go
require_cmd helm
require_cmd kubectl
require_cmd python3

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

write_summary "phase120_nvme_tcp_performance_baseline_status=running"
write_summary "protocol=nvme"
write_summary "frontend_transport=tcp"
write_summary "frontend_ip_map=${FRONTEND_IP_MAP:-<empty>}"
write_summary "frontend_network_class=${FRONTEND_NETWORK_CLASS:-<empty>}"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "perf_gate_type=baseline_no_slo"

cd "${ROOT}"
go test ./cmd/blockvolume ./cmd/sw-block >"${ARTIFACT_DIR}/go-test.log" 2>&1
write_summary "go_test_blockvolume_sw_block=pass"

import_env=(
  "SW_BLOCK_IMPORT_K3S=1"
  "SW_BLOCK_IMPORT_K3S_NODES=${SW_BLOCK_IMPORT_K3S_NODES:-192.168.1.181}"
  "SW_BLOCK_IMPORT_K3S_SSH_USER=${SW_BLOCK_IMPORT_K3S_SSH_USER:-testdev}"
  "SW_BLOCK_ARTIFACT_DIR=${ARTIFACT_DIR}/build"
)
if [[ -n "${SW_BLOCK_IMPORT_K3S_SSH_KEY:-}" ]]; then
  import_env+=("SW_BLOCK_IMPORT_K3S_SSH_KEY=${SW_BLOCK_IMPORT_K3S_SSH_KEY}")
fi
env "${import_env[@]}" bash scripts/build-alpha-images.sh "${ROOT}"
grep -q 'SW_BLOCK_IMAGE_ID=' "${ARTIFACT_DIR}/build/alpha-images.env"
grep -q 'SW_BLOCK_CSI_IMAGE_ID=' "${ARTIFACT_DIR}/build/alpha-images.env"
write_summary "image_ready=true"

go build -o "${ARTIFACT_DIR}/bin/sw-block" ./cmd/sw-block
generate_args=(
  --kubeconfig "${KUBECONFIG}"
  --out "${ARTIFACT_DIR}/values/values.nvme.yaml"
  --image "${IMAGE}"
  --csi-image "${CSI_IMAGE}"
  --protocol nvme
  --node-limit 2
)
if [[ -n "${FRONTEND_IP_MAP}" ]]; then
  generate_args+=(--frontend-ip-map "${FRONTEND_IP_MAP}")
  generate_args+=(--frontend-network-class "${FRONTEND_NETWORK_CLASS}")
fi
"${ARTIFACT_DIR}/bin/sw-block" ops generate-helm-values \
  "${generate_args[@]}" \
  >"${ARTIFACT_DIR}/values/generate.stdout.txt" \
  2>"${ARTIFACT_DIR}/values/generate.stderr.txt"
grep -q '^network_mode=external-nvme$' "${ARTIFACT_DIR}/values/generate.stdout.txt"
write_summary "generated_external_nvme=true"

python3 - "${ARTIFACT_DIR}/values/values.nvme.yaml" "${ARTIFACT_DIR}/values" <<'PY'
from pathlib import Path
import sys
values = Path(sys.argv[1])
out = Path(sys.argv[2])
nodes = []
cur = None
for raw in values.read_text().splitlines():
    line = raw.strip()
    if line.startswith("- name:"):
        if cur:
            nodes.append(cur)
        cur = {}
    if cur is None:
        continue
    if line.startswith("kubernetesNode:"):
        cur["kubernetesNode"] = line.split(":", 1)[1].strip().strip('"')
if cur:
    nodes.append(cur)
if len(nodes) < 2:
    raise SystemExit("need at least two generated nodes")
(out / "blockvolume-node.txt").write_text(nodes[0]["kubernetesNode"] + "\n")
(out / "app-node.txt").write_text(nodes[1]["kubernetesNode"] + "\n")
PY

BLOCK_NODE="$(cat "${ARTIFACT_DIR}/values/blockvolume-node.txt")"
APP_NODE="$(cat "${ARTIFACT_DIR}/values/app-node.txt")"
write_summary "blockvolume_node=${BLOCK_NODE}"
write_summary "app_node=${APP_NODE}"

helm lint charts/seaweed-block -f "${ARTIFACT_DIR}/values/values.nvme.yaml" >"${ARTIFACT_DIR}/install/helm-lint.txt" 2>&1
helm template "${HELM_RELEASE}" charts/seaweed-block --namespace "${HELM_NAMESPACE}" \
  -f "${ARTIFACT_DIR}/values/values.nvme.yaml" \
  >"${ARTIFACT_DIR}/install/helm-template.yaml" \
  2>"${ARTIFACT_DIR}/install/helm-template.stderr.txt"
grep -q -- '--launcher-external-nvme' "${ARTIFACT_DIR}/install/helm-template.yaml"
if grep -q -- '--launcher-external-iscsi' "${ARTIFACT_DIR}/install/helm-template.yaml"; then
  echo "unexpected iSCSI launcher flag in NVMe render" >&2
  exit 1
fi
write_summary "helm_template_external_nvme=true"

helm install "${HELM_RELEASE}" charts/seaweed-block \
  --namespace "${HELM_NAMESPACE}" \
  --create-namespace \
  -f "${ARTIFACT_DIR}/values/values.nvme.yaml" \
  --wait --timeout 10m \
  >"${ARTIFACT_DIR}/install/helm-install.txt" \
  2>"${ARTIFACT_DIR}/install/helm-install.stderr.txt"
write_summary "helm_install=pass"

cat >"${ARTIFACT_DIR}/pvc/perf-pvc.yaml" <<YAML
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: ${SC_NAME}
provisioner: block.csi.seaweedfs.com
volumeBindingMode: Immediate
allowVolumeExpansion: false
parameters:
  replicationFactor: "1"
  sw-block.seaweedfs.com/protocol: "nvme"
  protocol: "nvme"
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${PVC_NAME}
  namespace: ${APP_NAMESPACE}
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: ${SC_NAME}
  resources:
    requests:
      storage: ${PVC_SIZE}
YAML
kubectl apply -f "${ARTIFACT_DIR}/pvc/perf-pvc.yaml" >"${ARTIFACT_DIR}/pvc/apply-pvc.log" 2>&1
kubectl -n "${APP_NAMESPACE}" wait --for=jsonpath='{.status.phase}'=Bound "pvc/${PVC_NAME}" --timeout=180s \
  >"${ARTIFACT_DIR}/pvc/wait-pvc-bound.log" 2>&1
kubectl -n "${APP_NAMESPACE}" get pvc "${PVC_NAME}" -o wide >"${ARTIFACT_DIR}/pvc/pvc.after-bound.txt" 2>&1
write_summary "pvc_bound=true"

cat >"${ARTIFACT_DIR}/perf/perf-pod.yaml" <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${PERF_POD}
  namespace: ${APP_NAMESPACE}
  labels:
    sw-block-test: phase120-perf
spec:
  restartPolicy: Never
  nodeSelector:
    kubernetes.io/hostname: "${APP_NODE}"
  containers:
    - name: app
      image: busybox:1.36
      command: ["/bin/sh", "-c", "sleep 3600"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: ${PVC_NAME}
YAML
kubectl apply -f "${ARTIFACT_DIR}/perf/perf-pod.yaml" >"${ARTIFACT_DIR}/perf/apply-perf-pod.log" 2>&1
kubectl -n "${APP_NAMESPACE}" wait --for=condition=Ready "pod/${PERF_POD}" --timeout=240s \
  >"${ARTIFACT_DIR}/perf/wait-perf-pod-ready.log" 2>&1
write_summary "perf_pod_ready=true"

with_master_port_forward "${ARTIFACT_DIR}/status/blockmaster-port-forward.log" collect_cluster_evidence
python3 - "${ARTIFACT_DIR}/status/cluster-evidence.json" "${ARTIFACT_DIR}/status/phase120-status-summary.txt" "${FRONTEND_NETWORK_CLASS}" <<'PY'
import json
import sys
doc = json.load(open(sys.argv[1]))
expected_network_class = sys.argv[3]
managed = doc.get("managed_volumes") or []
raw = doc.get("volumes") or []
nodes = doc.get("nodes") or []
if len(managed) != 1:
    raise SystemExit(f"managed_volume_count={len(managed)}, want 1")
vol = managed[0]
target = vol.get("publish_target") or (raw[0].get("publish_target") if raw else "")
status = vol.get("status")
reason = vol.get("reason_code") or vol.get("reasonCode") or ""
if status != "ready":
    raise SystemExit(f"managed volume status={status}, want ready")
if reason != "first_volume_verified":
    raise SystemExit(f"managed volume reason={reason}, want first_volume_verified")
if ":4420" not in target:
    raise SystemExit(f"unexpected publish_target={target}")
if target.startswith("127.") or target.startswith("localhost"):
    raise SystemExit(f"loopback target={target}")
target_host = target.rsplit(":", 1)[0]
node = None
for item in nodes:
    if item.get("frontend_ip") == target_host:
        node = item
        break
if expected_network_class:
    if node is None:
        raise SystemExit(f"no node has frontend_ip={target_host}")
    network_class = node.get("frontend_network_class") or ""
    if network_class != expected_network_class:
        raise SystemExit(f"frontend_network_class={network_class}, want {expected_network_class}")
with open(sys.argv[2], "w") as f:
    f.write(f"managed_volume_status={status}\n")
    f.write(f"managed_volume_reason={reason}\n")
    f.write("publish_target_loopback=false\n")
    f.write(f"publish_target={target}\n")
    if node is not None:
        f.write(f"management_ip={node.get('internal_ip', '')}\n")
        f.write(f"frontend_ip={node.get('frontend_ip', '')}\n")
        f.write(f"publish_target_network_class={node.get('frontend_network_class', '')}\n")
        f.write("publish_target_source=configured_data_plane\n")
PY
cat "${ARTIFACT_DIR}/status/phase120-status-summary.txt" >>"${SUMMARY}"

if [[ -n "${FRONTEND_IP_MAP}" ]]; then
  PUBLISH_TARGET="$(awk -F= '$1=="publish_target" {print $2; exit}' "${ARTIFACT_DIR}/status/phase120-status-summary.txt")"
  PUBLISH_TARGET_HOST="${PUBLISH_TARGET%:*}"
  if [[ "${PUBLISH_TARGET_HOST}" == 192.168.* ]]; then
    echo "publish target uses management LAN: ${PUBLISH_TARGET}" >&2
    exit 1
  fi
  ip route get "${PUBLISH_TARGET_HOST}" >"${ARTIFACT_DIR}/status/publish-target-route.txt" 2>&1
  if [[ -n "${EXPECTED_FRONTEND_ROUTE_DEV}" ]] && ! grep -q " dev ${EXPECTED_FRONTEND_ROUTE_DEV} " "${ARTIFACT_DIR}/status/publish-target-route.txt"; then
    echo "publish target route does not use ${EXPECTED_FRONTEND_ROUTE_DEV}: $(cat "${ARTIFACT_DIR}/status/publish-target-route.txt")" >&2
    exit 1
  fi
  ROUTE_DEV="$(awk '{for (i=1; i<NF; i++) if ($i=="dev") {print $(i+1); exit}}' "${ARTIFACT_DIR}/status/publish-target-route.txt")"
  write_summary "publish_target_route_dev=${ROUTE_DEV}"
  write_summary "internal_ip_not_reused_as_performance_target=true"
fi

MARKER_MS="$(measure_exec_ms "${ARTIFACT_DIR}/perf/marker.log" \
  kubectl -n "${APP_NAMESPACE}" exec "${PERF_POD}" -- sh -c \
  'set -eu; dd if=/dev/urandom of=/data/phase120-marker.bin bs=4096 count=1; sha256sum /data/phase120-marker.bin > /data/phase120-marker.sha256; sync; sha256sum -c /data/phase120-marker.sha256')"
write_summary "marker_verify_ms=${MARKER_MS}"
write_summary "marker_verified=true"

SEQ_BYTES=$((SEQ_MIB * 1024 * 1024))
SEQ_WRITE_MS="$(measure_profiled_exec_ms "${ARTIFACT_DIR}/perf/seq-write.log" "${ARTIFACT_DIR}/profile/seq-write-samples.txt" \
  kubectl -n "${APP_NAMESPACE}" exec "${PERF_POD}" -- sh -c \
  "set -eu; dd if=/dev/zero of=/data/phase120-seq.bin bs=1M count=${SEQ_MIB} conv=fsync; sync")"
SEQ_READ_MS="$(measure_exec_ms "${ARTIFACT_DIR}/perf/seq-read.log" \
  kubectl -n "${APP_NAMESPACE}" exec "${PERF_POD}" -- sh -c \
  "set -eu; dd if=/data/phase120-seq.bin of=/dev/null bs=1M; test \$(wc -c < /data/phase120-seq.bin) -eq ${SEQ_BYTES}")"
SEQ_WRITE_MIBPS="$(rate_mibps "${SEQ_BYTES}" "${SEQ_WRITE_MS}")"
SEQ_READ_MIBPS="$(rate_mibps "${SEQ_BYTES}" "${SEQ_READ_MS}")"
write_summary "seq_size_mib=${SEQ_MIB}"
write_summary "seq_write_duration_ms=${SEQ_WRITE_MS}"
write_summary "seq_write_mibps=${SEQ_WRITE_MIBPS}"
write_summary "seq_read_duration_ms=${SEQ_READ_MS}"
write_summary "seq_read_mibps=${SEQ_READ_MIBPS}"
if [[ "${PROFILE_WRITE}" == "true" ]]; then
  collect_durable_write_profile
fi

SMALL_BYTES=$((SMALL_OPS * SMALL_BLOCK_BYTES))
SMALL_WRITE_MS="$(measure_exec_ms "${ARTIFACT_DIR}/perf/small-write.log" \
  kubectl -n "${APP_NAMESPACE}" exec "${PERF_POD}" -- sh -c \
  "set -eu; rm -f /data/phase120-small.bin; i=0; while [ \$i -lt ${SMALL_OPS} ]; do seek=\$(( (i * 17) % 1024 )); dd if=/dev/zero of=/data/phase120-small.bin bs=${SMALL_BLOCK_BYTES} count=1 seek=\$seek conv=notrunc 2>/dev/null; i=\$((i+1)); done; sync")"
SMALL_IOPS="$(rate_iops "${SMALL_OPS}" "${SMALL_WRITE_MS}")"
SMALL_MIBPS="$(rate_mibps "${SMALL_BYTES}" "${SMALL_WRITE_MS}")"
write_summary "small_write_ops=${SMALL_OPS}"
write_summary "small_write_block_bytes=${SMALL_BLOCK_BYTES}"
write_summary "small_write_duration_ms=${SMALL_WRITE_MS}"
write_summary "small_write_iops=${SMALL_IOPS}"
write_summary "small_write_mibps=${SMALL_MIBPS}"

kubectl -n "${APP_NAMESPACE}" exec "${PERF_POD}" -- sh -c \
  'set -eu; sha256sum -c /data/phase120-marker.sha256; test -s /data/phase120-seq.bin; test -s /data/phase120-small.bin' \
  >"${ARTIFACT_DIR}/perf/final-verify.log" 2>&1
write_summary "final_data_verified=true"

kubectl -n "${APP_NAMESPACE}" delete pod "${PERF_POD}" --ignore-not-found=true --wait=true --timeout=120s \
  >"${ARTIFACT_DIR}/cleanup/delete-perf-pod.txt" 2>&1
kubectl -n "${APP_NAMESPACE}" delete pvc "${PVC_NAME}" --ignore-not-found=true --wait=true --timeout=120s \
  >"${ARTIFACT_DIR}/cleanup/delete-pvc.txt" 2>&1
kubectl delete storageclass "${SC_NAME}" --ignore-not-found=true --wait=true --timeout=120s \
  >"${ARTIFACT_DIR}/cleanup/delete-storageclass.txt" 2>&1
sudo -n nvme disconnect-all >/dev/null 2>&1 || true
helm uninstall "${HELM_RELEASE}" --namespace "${HELM_NAMESPACE}" --wait --timeout 240s \
  >"${ARTIFACT_DIR}/cleanup/helm-uninstall.txt" 2>&1 || true
SW_BLOCK_CLEANUP_WAIT_SECONDS="${SW_BLOCK_CLEANUP_WAIT_SECONDS:-180}" \
SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/cleanup/verify" \
  bash scripts/verify-helm-cleanup.sh "${ROOT}" \
  >"${ARTIFACT_DIR}/cleanup/verify-helm-cleanup.stdout.txt" \
  2>"${ARTIFACT_DIR}/cleanup/verify-helm-cleanup.stderr.txt"
grep -q '^cleanup_status=ok$' "${ARTIFACT_DIR}/cleanup/verify/cleanup-summary.txt"
write_summary "cleanup_status=ok"

write_summary "phase120_nvme_tcp_performance_baseline_status=ok"
