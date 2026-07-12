#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase111-nvme-k8s-path-loss-crd-gate}"
SUMMARY_NAME="${SW_BLOCK_NVME_PATH_LOSS_SUMMARY_NAME:-phase111-nvme-k8s-path-loss-crd-summary.txt}"
SUMMARY="${ARTIFACT_DIR}/${SUMMARY_NAME}"
PHASE_STATUS_KEY="${SW_BLOCK_NVME_PATH_LOSS_PHASE_STATUS_KEY:-phase111_nvme_k8s_path_loss_crd_status}"
HELM_RELEASE="${SW_BLOCK_HELM_RELEASE:-sw-block}"
HELM_NAMESPACE="${SW_BLOCK_HELM_NAMESPACE:-kube-system}"
APP_NAMESPACE="${SW_BLOCK_APP_NAMESPACE:-default}"
IMAGE="${SW_BLOCK_IMAGE:-sw-block:local}"
CSI_IMAGE="${SW_BLOCK_CSI_IMAGE:-sw-block-csi:local}"
STATUS_PORT="${SW_BLOCK_MASTER_PORT_FORWARD_PORT:-29333}"
MOUNTED_IO="${SW_BLOCK_NVME_MOUNTED_IO:-0}"
MOUNTED_POD="${SW_BLOCK_NVME_MOUNTED_POD:-sw-block-phase112-mounted}"
RESTORE_PATH="${SW_BLOCK_NVME_RESTORE_PATH:-0}"
RECONNECT_OWNER="${SW_BLOCK_NVME_RECONNECT_OWNER:-0}"
RECONNECT_INTERVAL="${SW_BLOCK_NVME_RECONNECT_INTERVAL:-5s}"
HOST_PATH_DISCONNECT="${SW_BLOCK_NVME_HOST_PATH_DISCONNECT:-0}"
FORCE_STAGE2_MULTIPATH="${SW_BLOCK_NVME_FORCE_STAGE2_MULTIPATH:-0}"
DESIRED_PATH_CHANGE="${SW_BLOCK_NVME_DESIRED_PATH_CHANGE:-0}"
REQUIRE_STALE_PATH_PRUNE="${SW_BLOCK_NVME_REQUIRE_STALE_PATH_PRUNE:-0}"

mkdir -p "${ARTIFACT_DIR}"/{bin,build,values,install,multi-volume,inject,surfaces,cleanup}
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

capture() {
  local out="$1"
  shift
  "$@" >"$out" 2>&1 || true
}

delete_pod_before_uninstall() {
  local pod="$1"
  local log="${ARTIFACT_DIR}/cleanup/delete-${pod}.txt"
  kubectl -n "${APP_NAMESPACE}" delete pod "${pod}" --ignore-not-found=true --wait=true --timeout=120s >"${log}" 2>&1 && return 0
  {
    echo "--- pod did not delete gracefully; forcing test cleanup ---"
    kubectl -n "${APP_NAMESPACE}" get pod "${pod}" -o wide || true
    kubectl -n "${APP_NAMESPACE}" describe pod "${pod}" || true
    kubectl -n "${APP_NAMESPACE}" delete pod "${pod}" --ignore-not-found=true --force --grace-period=0 --wait=false || true
  } >>"${log}" 2>&1
  for _ in $(seq 1 60); do
    kubectl -n "${APP_NAMESPACE}" get pod "${pod}" >/dev/null 2>&1 || return 0
    sleep 1
  done
}

wait_for_no_sw_block_k8s() {
  local log="$1"
  local deadline=$((SECONDS + 180))
  : >"${log}"
  while (( SECONDS < deadline )); do
    local residue
    residue="$(
      {
        kubectl get deploy,daemonset,statefulset,pod,svc,pvc,pv,configmap,secret,serviceaccount -A -o name
        kubectl get storageclass,csidriver,clusterrole,clusterrolebinding -o name
      } 2>/dev/null | grep -E '(sw-block|seaweed-block|block\.csi\.seaweedfs\.com)' || true
    )"
    if [[ -z "${residue}" ]]; then
      echo "sw_block_k8s_residue=0" >>"${log}"
      return 0
    fi
    printf '%s\n' "--- residue still present at $(date -u +%Y-%m-%dT%H:%M:%SZ) ---" "${residue}" >>"${log}"
    sleep 2
  done
  echo "sw_block_k8s_residue=timeout" >>"${log}"
  return 1
}

cleanup() {
  set +e
  # Delete mounted consumers while CSI is still installed so normal detach/delete can run.
  delete_pod_before_uninstall "${MOUNTED_POD}"
  kubectl -n "${APP_NAMESPACE}" delete pod -l sw-block-test=multi-volume --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  kubectl -n "${APP_NAMESPACE}" delete pod sw-block-multi-reader-1 sw-block-multi-writer-1 --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  kubectl -n "${APP_NAMESPACE}" delete pvc sw-block-multi-pvc-1 --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  kubectl delete storageclass sw-block-multi --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  sudo -n nvme disconnect-all >/dev/null 2>&1 || true
  helm status "${HELM_RELEASE}" --namespace "${HELM_NAMESPACE}" >/dev/null 2>&1 && \
    helm uninstall "${HELM_RELEASE}" --namespace "${HELM_NAMESPACE}" --wait --timeout 240s \
      >"${ARTIFACT_DIR}/cleanup/helm-uninstall.txt" 2>&1
  wait_for_no_sw_block_k8s "${ARTIFACT_DIR}/cleanup/wait-after-helm-uninstall.txt" || true
  kubectl -n "${APP_NAMESPACE}" delete deploy -l app=sw-blockvolume --ignore-not-found=true --wait=false >/dev/null 2>&1
  kubectl -n "${HELM_NAMESPACE}" delete deploy -l app=sw-blockvolume --ignore-not-found=true --wait=false >/dev/null 2>&1
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
  kubectl -n "${APP_NAMESPACE}" get pvc sw-block-multi-pvc-1 >/dev/null 2>&1 && \
    kubectl -n "${APP_NAMESPACE}" patch pvc sw-block-multi-pvc-1 --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null 2>&1
  kubectl get pv --no-headers 2>/dev/null | awk '/sw-block-multi/ {print $1}' | xargs -r kubectl delete pv --wait=false >/dev/null 2>&1
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

python_read_nodes='
from pathlib import Path
base=Path(__import__("sys").argv[1])
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
        cur["kubernetesNode"]=line.split(":",1)[1].strip().strip("\"")
if cur:
    nodes.append(cur)
if len(nodes) < 2:
    raise SystemExit("need at least two generated nodes")
(base/"values/blockvolume-node.txt").write_text(nodes[0]["kubernetesNode"]+"\n")
(base/"values/app-node.txt").write_text(nodes[1]["kubernetesNode"]+"\n")
'

wait_for_crd_status() {
  local phase="$1"
  local want_status="$2"
  local want_reason="$3"
  local want_path_count="$4"
  local out_json="${ARTIFACT_DIR}/surfaces/swblockvolumes.${phase}.json"
  for _ in $(seq 1 90); do
    kubectl -n "${HELM_NAMESPACE}" get swblockvolumes -o json >"${out_json}" 2>/dev/null || true
    python3 - "${out_json}" "${want_status}" "${want_reason}" "${want_path_count}" <<'PY' && return 0 || true
import json, sys
path, want_status, want_reason, want_path_count = sys.argv[1:]
try:
    doc=json.load(open(path))
except Exception:
    raise SystemExit(1)
items=doc.get("items") or []
if len(items) != 1:
    raise SystemExit(1)
st=items[0].get("status") or {}
nv=st.get("nvme") or {}
if st.get("status") != want_status or st.get("reasonCode") != want_reason:
    raise SystemExit(1)
if want_path_count != "-" and int(nv.get("pathCount", -1)) != int(want_path_count):
    raise SystemExit(1)
conds={c.get("type"): c.get("status") for c in st.get("conditions") or []}
if want_status == "blocked" and conds.get("Ready") == "True":
    raise SystemExit(1)
PY
    sleep 2
  done
  echo "SwBlockVolume did not reach ${want_status}/${want_reason}" >&2
  kubectl -n "${HELM_NAMESPACE}" get swblockvolumes -o yaml >&2 || true
  return 1
}

write_host_nvme_path_info() {
  local label="$1"
  local nqn="$2"
  local json_path="${ARTIFACT_DIR}/inject/nvme-list-subsys.${label}.json"
  local env_path="${ARTIFACT_DIR}/inject/nvme-path-info.${label}.env"
  sudo -n nvme list-subsys -o json >"${json_path}"
  python3 - "${json_path}" "${nqn}" >"${env_path}" <<'PY'
import json
import sys

path, want_nqn = sys.argv[1:]
doc = json.load(open(path))

def walk(node):
    if isinstance(node, dict):
        if node.get("NQN") == want_nqn and isinstance(node.get("Paths"), list):
            yield node
        for value in node.values():
            yield from walk(value)
    elif isinstance(node, list):
        for item in node:
            yield from walk(item)

def addr(raw):
    fields = {}
    for part in str(raw or "").split(","):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        fields[k.strip()] = v.strip()
    if fields.get("traddr") and fields.get("trsvcid"):
        return f"{fields['traddr']}:{fields['trsvcid']}"
    return ""

paths = []
for sub in walk(doc):
    paths.extend(sub.get("Paths") or [])

print(f"path_count={len(paths)}")
print("addrs=" + ",".join(a for a in [addr(p.get("Address")) for p in paths] if a))
if paths:
    first = paths[0]
    name = first.get("Name") or first.get("Controller") or first.get("Device") or ""
    if name and not name.startswith("/dev/"):
        name = "/dev/" + name
    print(f"controller={name}")
    print(f"addr={addr(first.get('Address'))}")
PY
}

read_env_value() {
  local path="$1"
  local key="$2"
  awk -F= -v key="${key}" '$1 == key {value = substr($0, length(key) + 2)} END {print value}' "${path}"
}

wait_for_host_nvme_path_count() {
  local label="$1"
  local nqn="$2"
  local want="$3"
  for _ in $(seq 1 60); do
    write_host_nvme_path_info "${label}" "${nqn}" || true
    local got
    got="$(read_env_value "${ARTIFACT_DIR}/inject/nvme-path-info.${label}.env" path_count)"
    if [[ "${got}" == "${want}" ]]; then
      return 0
    fi
    sleep 1
  done
  echo "NVMe host path count did not reach ${want} for ${nqn}" >&2
  return 1
}

wait_for_host_nvme_addr() {
	local label="$1"
	local nqn="$2"
	local want_addr="$3"
  for _ in $(seq 1 90); do
    write_host_nvme_path_info "${label}" "${nqn}" || true
    local addrs
    addrs="$(read_env_value "${ARTIFACT_DIR}/inject/nvme-path-info.${label}.env" addrs)"
    if python3 - "${addrs}" "${want_addr}" <<'PY'
import sys
addrs = [a for a in sys.argv[1].split(",") if a]
raise SystemExit(0 if sys.argv[2] in addrs else 1)
PY
    then
      return 0
    fi
    sleep 1
  done
  echo "NVMe host path ${want_addr} did not appear for ${nqn}" >&2
	return 1
}

wait_for_host_nvme_addr_absent_count() {
  local label="$1"
  local nqn="$2"
  local stale_addr="$3"
  local want_count="$4"
  for _ in $(seq 1 90); do
    write_host_nvme_path_info "${label}" "${nqn}" || true
    local addrs
    local count
    addrs="$(read_env_value "${ARTIFACT_DIR}/inject/nvme-path-info.${label}.env" addrs)"
    count="$(read_env_value "${ARTIFACT_DIR}/inject/nvme-path-info.${label}.env" path_count)"
    if python3 - "${addrs}" "${stale_addr}" "${count}" "${want_count}" <<'PY'
import sys
addrs = [a for a in sys.argv[1].split(",") if a]
stale_addr = sys.argv[2]
count, want_count = sys.argv[3], sys.argv[4]
raise SystemExit(0 if stale_addr not in addrs and count == want_count else 1)
PY
    then
      return 0
    fi
    sleep 1
  done
  echo "NVMe stale host path ${stale_addr} was not pruned to count ${want_count} for ${nqn}" >&2
  return 1
}

wait_for_reconnect_owner_log() {
	local out="$1"
	for _ in $(seq 1 90); do
    kubectl -n "${HELM_NAMESPACE}" logs -l app=sw-block-csi-node -c block-csi --since=10m --prefix=true >"${out}" 2>&1 || true
    if grep -q 'MountedNVMeReconnectOwner: iteration .*reconnected=1' "${out}" || \
       grep -q 'reconciled mounted NVMe paths' "${out}"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_for_reconnect_owner_log_for_addr() {
  local out="$1"
  local addr="$2"
  for _ in $(seq 1 90); do
    kubectl -n "${HELM_NAMESPACE}" logs -l app=sw-block-csi-node -c block-csi --since=10m --prefix=true >"${out}" 2>&1 || true
    if grep -q 'reconciled mounted NVMe paths' "${out}" && grep -Fq "${addr}" "${out}"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

volume_field_from_crd() {
  local path="$1"
  local expr="$2"
  python3 - "${path}" "${expr}" <<'PY'
import json
import sys
doc = json.load(open(sys.argv[1]))
expr = sys.argv[2].split(".")
value = doc["items"][0]
for part in expr:
    value = value.get(part, {}) if isinstance(value, dict) else {}
print(value if isinstance(value, (str, int, float, bool)) else "")
PY
}

wait_for_crd_nvme_addr_set_changed() {
  local phase="$1"
  local old_addr="$2"
  local new_addr="$3"
  local out_json="${ARTIFACT_DIR}/surfaces/swblockvolumes.${phase}.json"
  for _ in $(seq 1 120); do
    kubectl -n "${HELM_NAMESPACE}" get swblockvolumes -o json >"${out_json}" 2>/dev/null || true
    python3 - "${out_json}" "${old_addr}" "${new_addr}" <<'PY' && return 0 || true
import json
import sys

path, old_addr, new_addr = sys.argv[1:]
try:
    doc = json.load(open(path))
except Exception:
    raise SystemExit(1)
items = doc.get("items") or []
if len(items) != 1:
    raise SystemExit(1)
st = items[0].get("status") or {}
nv = st.get("nvme") or {}
addrs = nv.get("nvmeAddrs") or []
if st.get("status") != "ready" or st.get("reasonCode") != "first_volume_verified":
    raise SystemExit(1)
if nv.get("pathCount") != 2:
    raise SystemExit(1)
if new_addr not in addrs or old_addr in addrs:
    raise SystemExit(1)
PY
    sleep 2
  done
  echo "SwBlockVolume desired NVMe path set did not replace ${old_addr} with ${new_addr}" >&2
  kubectl -n "${HELM_NAMESPACE}" get swblockvolumes -o yaml >&2 || true
  return 1
}

write_summary "${PHASE_STATUS_KEY}=running"
cleanup

echo "[phase111] build sw-block CLI"
(
  cd "${ROOT}"
  go build -o "${ARTIFACT_DIR}/bin/sw-block" ./cmd/sw-block
  "${ARTIFACT_DIR}/bin/sw-block" ops generate-helm-values \
    --kubeconfig /etc/rancher/k3s/k3s.yaml \
    --out "${ARTIFACT_DIR}/values/values.nvme.yaml" \
    --image "${IMAGE}" \
    --csi-image "${CSI_IMAGE}" \
    --protocol nvme \
    --replication-factor 2 \
    --node-limit 2 >"${ARTIFACT_DIR}/values/generate.stdout.txt"
)
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
if [[ "${RECONNECT_OWNER}" == "1" || "${RECONNECT_OWNER}" == "true" ]]; then
  cat >>"${ARTIFACT_DIR}/values/values.nvme.yaml" <<YAML
csiNode:
  nvmeReconnect:
    enabled: true
    interval: ${RECONNECT_INTERVAL}
YAML
  write_summary "reconnect_owner_enabled=true"
  write_summary "reconnect_owner_interval=${RECONNECT_INTERVAL}"
fi
if [[ "${FORCE_STAGE2_MULTIPATH}" == "1" || "${FORCE_STAGE2_MULTIPATH}" == "true" ]]; then
  cat >>"${ARTIFACT_DIR}/values/values.nvme.yaml" <<'YAML'
stage2Multipath:
  enabled: true
YAML
  write_summary "stage2_multipath_enabled=true"
fi
python3 -c "${python_read_nodes}" "${ARTIFACT_DIR}"

grep -q '^network_mode=external-nvme$' "${ARTIFACT_DIR}/values/generate.stdout.txt"
grep -q 'externalNVMe: true' "${ARTIFACT_DIR}/values/values.nvme.yaml"

echo "[phase111] install Helm stack"
(
  cd "${ROOT}"
  helm lint charts/seaweed-block -f "${ARTIFACT_DIR}/values/values.nvme.yaml" >"${ARTIFACT_DIR}/install/helm-lint.txt"
  helm template "${HELM_RELEASE}" charts/seaweed-block --namespace "${HELM_NAMESPACE}" \
    -f "${ARTIFACT_DIR}/values/values.nvme.yaml" >"${ARTIFACT_DIR}/install/helm-template.yaml"
  grep -q -- '--launcher-external-nvme' "${ARTIFACT_DIR}/install/helm-template.yaml"
  grep -q 'sw-block-operator-status' "${ARTIFACT_DIR}/install/helm-template.yaml"
  helm install "${HELM_RELEASE}" charts/seaweed-block --namespace "${HELM_NAMESPACE}" --create-namespace \
    -f "${ARTIFACT_DIR}/values/values.nvme.yaml" --wait --timeout 10m >"${ARTIFACT_DIR}/install/helm-install.txt"
)

APP_NODE="$(cat "${ARTIFACT_DIR}/values/app-node.txt")"
echo "[phase111] create one RF2 NVMe PVC and verify data path"
(
  cd "${ROOT}"
  SW_BLOCK_MULTI_VOLUME_NODE_SELECTOR="${APP_NODE}" \
  SW_BLOCK_MULTI_VOLUME_PROTOCOL=nvme \
  SW_BLOCK_MULTI_VOLUME_RF=2 \
  SW_BLOCK_MULTI_VOLUME_COUNT=1 \
  SW_BLOCK_MULTI_VOLUME_CLEANUP=0 \
  SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/multi-volume" \
  SW_BLOCK_CLI="${ARTIFACT_DIR}/bin/sw-block" \
    bash scripts/run-multi-volume-example.sh "${ROOT}"
)
grep -q '^multi_volume_status=ok$' "${ARTIFACT_DIR}/multi-volume/multi-volume-summary.txt"
grep -q '^writer_verified_count=1$' "${ARTIFACT_DIR}/multi-volume/multi-volume-summary.txt"
grep -q '^reader_verified_count=1$' "${ARTIFACT_DIR}/multi-volume/multi-volume-summary.txt"

wait_for_crd_status "healthy" "ready" "first_volume_verified" "2"

MOUNTED_POD_UID_BEFORE=""
if [[ "${MOUNTED_IO}" == "1" || "${MOUNTED_IO}" == "true" ]]; then
  echo "[phase111] create mounted pod for post-path-loss I/O"
  cat >"${ARTIFACT_DIR}/inject/mounted-pod.yaml" <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${MOUNTED_POD}
  namespace: ${APP_NAMESPACE}
  labels:
    sw-block-test: nvme-mounted-path-loss
spec:
  restartPolicy: Never
  nodeSelector:
    kubernetes.io/hostname: ${APP_NODE}
  containers:
    - name: app
      image: busybox:1.36
      command: ["/bin/sh", "-c", "trap : TERM INT; sleep 3600 & wait"]
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: sw-block-multi-pvc-1
YAML
  kubectl -n "${APP_NAMESPACE}" apply -f "${ARTIFACT_DIR}/inject/mounted-pod.yaml" >"${ARTIFACT_DIR}/inject/apply-mounted-pod.txt"
  kubectl -n "${APP_NAMESPACE}" wait --for=condition=Ready "pod/${MOUNTED_POD}" --timeout=180s >"${ARTIFACT_DIR}/inject/wait-mounted-pod.txt"
  MOUNTED_POD_UID_BEFORE="$(kubectl -n "${APP_NAMESPACE}" get pod "${MOUNTED_POD}" -o jsonpath='{.metadata.uid}')"
  kubectl -n "${APP_NAMESPACE}" exec "${MOUNTED_POD}" -- sh -c 'set -eu; echo before-path-loss > /data/phase112-mounted.txt; sync; grep before-path-loss /data/phase112-mounted.txt' >"${ARTIFACT_DIR}/inject/mounted-before.log"
  write_summary "mounted_pod_uid_before=${MOUNTED_POD_UID_BEFORE}"
fi

if [[ "${DESIRED_PATH_CHANGE}" == "1" || "${DESIRED_PATH_CHANGE}" == "true" ]]; then
  echo "[phase111] replace one desired NVMe frontend path and wait for mounted reconnect"
  kubectl -n "${APP_NAMESPACE}" get deploy -l app=sw-blockvolume -o json >"${ARTIFACT_DIR}/inject/blockvolume-deployments.before-desired-change.json"
  python3 - "${ARTIFACT_DIR}" <<'PY' >"${ARTIFACT_DIR}/inject/desired-path-change.env"
import json
import sys
from pathlib import Path

base = Path(sys.argv[1])
crd = json.load(open(base/"surfaces/swblockvolumes.healthy.json"))
deploys = json.load(open(base/"inject/blockvolume-deployments.before-desired-change.json"))
items = crd.get("items") or []
if len(items) != 1:
    raise SystemExit(f"need one SwBlockVolume, got {len(items)}")
st = items[0].get("status") or {}
nv = st.get("nvme") or {}
desired = nv.get("nvmeAddrs") or []
if len(desired) != 2:
    raise SystemExit(f"need two desired NVMe paths, got {desired}")
primary = st.get("primaryReplicaID") or ""
volume_id = st.get("volumeID") or ""
nqn = nv.get("nqn") or ""
if not volume_id or not nqn:
    raise SystemExit(f"missing volume_id/nqn in status: {st}")

current_addrs = set(desired)
candidates = []
for item in deploys.get("items") or []:
    meta = item.get("metadata") or {}
    labels = meta.get("labels") or {}
    replica = labels.get("sw-block.seaweedfs.com/replica") or ""
    namespace = meta.get("namespace") or "default"
    name = meta.get("name") or ""
    containers = ((item.get("spec") or {}).get("template") or {}).get("spec", {}).get("containers") or []
    for ci, container in enumerate(containers):
        if container.get("name") != "blockvolume":
            continue
        for ai, arg in enumerate(container.get("args") or []):
            if not str(arg).startswith("--nvme-listen="):
                continue
            old_addr = str(arg).split("=", 1)[1]
            if old_addr in current_addrs:
                candidates.append((replica == primary, replica, namespace, name, ci, ai, old_addr))

if not candidates:
    raise SystemExit(f"no deployment --nvme-listen matched desired addrs {desired}")
candidates.sort(key=lambda x: (x[0], x[1], x[3]))
_, replica, namespace, name, ci, ai, old_addr = candidates[0]
host, port_text = old_addr.rsplit(":", 1)
port = int(port_text)
used = {int(a.rsplit(":", 1)[1]) for a in current_addrs if a.startswith(host + ":")}
new_port = None
for offset in (100, 101, 102, 103, 104, 105, 200):
    candidate = port + offset
    if candidate not in used and candidate < 65535:
        new_port = candidate
        break
if new_port is None:
    raise SystemExit(f"no replacement port for {old_addr}")
new_addr = f"{host}:{new_port}"
patch = [{"op": "replace", "path": f"/spec/template/spec/containers/{ci}/args/{ai}", "value": "--nvme-listen=" + new_addr}]
(base/"inject/desired-path-change-patch.json").write_text(json.dumps(patch))
for key, value in {
    "volume_id": volume_id,
    "nvme_nqn": nqn,
    "target_namespace": namespace,
    "target_deployment": name,
    "target_replica": replica,
    "old_desired_path": old_addr,
    "new_desired_path": new_addr,
    "initial_desired_paths": ",".join(desired),
    "initial_path_count": str(len(desired)),
}.items():
    print(f"{key}={value}")
PY
  TARGET_NAMESPACE="$(read_env_value "${ARTIFACT_DIR}/inject/desired-path-change.env" target_namespace)"
  TARGET_DEPLOY="$(read_env_value "${ARTIFACT_DIR}/inject/desired-path-change.env" target_deployment)"
  VOLUME_ID="$(read_env_value "${ARTIFACT_DIR}/inject/desired-path-change.env" volume_id)"
  NQN="$(read_env_value "${ARTIFACT_DIR}/inject/desired-path-change.env" nvme_nqn)"
  OLD_DESIRED_PATH="$(read_env_value "${ARTIFACT_DIR}/inject/desired-path-change.env" old_desired_path)"
  NEW_DESIRED_PATH="$(read_env_value "${ARTIFACT_DIR}/inject/desired-path-change.env" new_desired_path)"
  INITIAL_PATH_COUNT="$(read_env_value "${ARTIFACT_DIR}/inject/desired-path-change.env" initial_path_count)"
  INITIAL_DESIRED_PATHS="$(read_env_value "${ARTIFACT_DIR}/inject/desired-path-change.env" initial_desired_paths)"
  TARGET_REPLICA="$(read_env_value "${ARTIFACT_DIR}/inject/desired-path-change.env" target_replica)"
  write_summary "volume_id=${VOLUME_ID}"
  write_summary "nvme_nqn=${NQN}"
  write_summary "initial_path_count=${INITIAL_PATH_COUNT}"
  write_summary "initial_desired_paths=${INITIAL_DESIRED_PATHS}"
  write_summary "target_deployment=${TARGET_NAMESPACE}/${TARGET_DEPLOY}"
  write_summary "target_replica=${TARGET_REPLICA}"
  write_summary "old_desired_path=${OLD_DESIRED_PATH}"
  write_summary "new_desired_path=${NEW_DESIRED_PATH}"

  kubectl -n "${TARGET_NAMESPACE}" patch "deploy/${TARGET_DEPLOY}" --type=json --patch-file "${ARTIFACT_DIR}/inject/desired-path-change-patch.json" \
    >"${ARTIFACT_DIR}/inject/patch-desired-path.txt" 2>&1
  kubectl -n "${TARGET_NAMESPACE}" rollout status "deploy/${TARGET_DEPLOY}" --timeout=240s \
    >"${ARTIFACT_DIR}/inject/rollout-desired-path-change.txt" 2>&1
  kubectl -n "${TARGET_NAMESPACE}" get deploy,pod -l app=sw-blockvolume -o wide >"${ARTIFACT_DIR}/inject/blockvolume.after-desired-change.txt"

  wait_for_crd_nvme_addr_set_changed "desired-change" "${OLD_DESIRED_PATH}" "${NEW_DESIRED_PATH}"
  write_summary "desired_path_set_changed=true"
  write_summary "path_loss_or_replacement_detected=true"
  write_summary "reconnect_owner=csi-node"

  if ! wait_for_reconnect_owner_log_for_addr "${ARTIFACT_DIR}/inject/csi-node-desired-path-change-owner.log" "${NEW_DESIRED_PATH}"; then
    echo "CSI-node reconnect owner did not report new desired path ${NEW_DESIRED_PATH}" >&2
    exit 1
  fi
  wait_for_host_nvme_addr "after-desired-path-change" "${NQN}" "${NEW_DESIRED_PATH}"
  write_summary "reconnect_invoked=true"
  write_summary "new_desired_path_connected=true"
  HOST_PATH_COUNT_AFTER_DESIRED_CHANGE="$(read_env_value "${ARTIFACT_DIR}/inject/nvme-path-info.after-desired-path-change.env" path_count)"
  HOST_PATHS_AFTER_DESIRED_CHANGE="$(read_env_value "${ARTIFACT_DIR}/inject/nvme-path-info.after-desired-path-change.env" addrs)"
  write_summary "host_path_count_after_desired_change=${HOST_PATH_COUNT_AFTER_DESIRED_CHANGE}"
  write_summary "host_paths_after_desired_change=${HOST_PATHS_AFTER_DESIRED_CHANGE}"
  if python3 - "${HOST_PATHS_AFTER_DESIRED_CHANGE}" "${OLD_DESIRED_PATH}" <<'PY'
import sys
addrs = [a for a in sys.argv[1].split(",") if a]
raise SystemExit(0 if sys.argv[2] in addrs else 1)
PY
  then
    write_summary "stale_old_host_path_after_desired_change=true"
  else
    write_summary "stale_old_host_path_after_desired_change=false"
  fi

  if [[ "${REQUIRE_STALE_PATH_PRUNE}" == "1" || "${REQUIRE_STALE_PATH_PRUNE}" == "true" ]]; then
    write_summary "stale_old_path_detected=true"
    wait_for_host_nvme_addr_absent_count "after-stale-path-prune" "${NQN}" "${OLD_DESIRED_PATH}" "2"
    HOST_PATH_COUNT_AFTER_PRUNE="$(read_env_value "${ARTIFACT_DIR}/inject/nvme-path-info.after-stale-path-prune.env" path_count)"
    HOST_PATHS_AFTER_PRUNE="$(read_env_value "${ARTIFACT_DIR}/inject/nvme-path-info.after-stale-path-prune.env" addrs)"
    write_summary "stale_old_path_pruned=true"
    write_summary "host_path_count_after_prune=${HOST_PATH_COUNT_AFTER_PRUNE}"
    write_summary "host_paths_after_prune=${HOST_PATHS_AFTER_PRUNE}"
  fi

  if [[ "${MOUNTED_IO}" == "1" || "${MOUNTED_IO}" == "true" ]]; then
    MOUNTED_POD_UID_CHANGED="$(kubectl -n "${APP_NAMESPACE}" get pod "${MOUNTED_POD}" -o jsonpath='{.metadata.uid}')"
    if [[ "${MOUNTED_POD_UID_CHANGED}" != "${MOUNTED_POD_UID_BEFORE}" ]]; then
      echo "mounted pod UID changed after desired path replacement: before=${MOUNTED_POD_UID_BEFORE} changed=${MOUNTED_POD_UID_CHANGED}" >&2
      exit 1
    fi
    kubectl -n "${APP_NAMESPACE}" exec "${MOUNTED_POD}" -- sh -c 'set -eu; echo after-desired-path-change >> /data/phase132-mounted.txt; sync; grep after-desired-path-change /data/phase132-mounted.txt' >"${ARTIFACT_DIR}/inject/mounted-after-desired-path-change.log"
    write_summary "pod_uid_preserved=true"
    write_summary "mounted_io_after_reconnect=ok"
  fi

  with_master_port_forward "${ARTIFACT_DIR}/surfaces/desired-change-blockmaster-port-forward.log" \
    "${ARTIFACT_DIR}/bin/sw-block" ops report \
      --master-api "127.0.0.1:${STATUS_PORT}" \
      --namespace "${APP_NAMESPACE}" \
      --out "${ARTIFACT_DIR}/surfaces/desired-change-report" \
      --timeout 30s >"${ARTIFACT_DIR}/surfaces/desired-change-report.stdout.txt" 2>"${ARTIFACT_DIR}/surfaces/desired-change-report.stderr.txt"
  DASHBOARD_PORT="$(python3 - <<'PY'
import socket
s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()
PY
)"
  "${ARTIFACT_DIR}/bin/sw-block" ops dashboard \
    --from-bundle "${ARTIFACT_DIR}/surfaces/desired-change-report" \
    --listen "127.0.0.1:${DASHBOARD_PORT}" >"${ARTIFACT_DIR}/surfaces/desired-change-dashboard.stdout.txt" 2>"${ARTIFACT_DIR}/surfaces/desired-change-dashboard.stderr.txt" &
  DASH_PID=$!
  for _ in $(seq 1 50); do
    code="$(curl -s -o "${ARTIFACT_DIR}/surfaces/desired-change-dashboard-operator-snapshot.json" -w '%{http_code}' "http://127.0.0.1:${DASHBOARD_PORT}/operator-snapshot.json" || true)"
    [[ "${code}" == "200" ]] && break
    sleep 0.2
  done
  kill "${DASH_PID}" >/dev/null 2>&1 || true
  wait "${DASH_PID}" >/dev/null 2>&1 || true

  python3 - "${ARTIFACT_DIR}" "${VOLUME_ID}" "${OLD_DESIRED_PATH}" "${NEW_DESIRED_PATH}" <<'PY'
import json
import sys
from pathlib import Path

base = Path(sys.argv[1])
volume_id, old_addr, new_addr = sys.argv[2:]
crd = json.load(open(base/"surfaces/swblockvolumes.desired-change.json"))
status = crd["items"][0]["status"]
nvme = status.get("nvme") or {}
summary = (base/"surfaces/desired-change-report/summary.txt").read_text()
snapshot = json.load(open(base/"surfaces/desired-change-report/operator-snapshot.json"))
dashboard = json.load(open(base/"surfaces/desired-change-dashboard-operator-snapshot.json"))

def assert_status(name, st):
    nv = st.get("nvme") or {}
    addrs = nv.get("nvme_addrs") or nv.get("nvmeAddrs") or []
    if st.get("status") != "ready" or (st.get("reason_code") or st.get("reasonCode")) != "first_volume_verified":
        raise SystemExit(f"{name} status mismatch: {st}")
    if nv.get("path_count") != 2 and nv.get("pathCount") != 2:
        raise SystemExit(f"{name} path count mismatch: {nv}")
    if new_addr not in addrs or old_addr in addrs:
        raise SystemExit(f"{name} desired addrs mismatch: {addrs}")

if status.get("status") != "ready" or status.get("reasonCode") != "first_volume_verified":
    raise SystemExit(f"CRD status mismatch: {status}")
if nvme.get("pathCount") != 2:
    raise SystemExit(f"CRD path count mismatch: {nvme}")
if new_addr not in (nvme.get("nvmeAddrs") or []) or old_addr in (nvme.get("nvmeAddrs") or []):
    raise SystemExit(f"CRD desired addrs mismatch: {nvme}")
if f"managed_volume={volume_id} status=ready reason=first_volume_verified" not in summary:
    raise SystemExit("report missing ready/first_volume_verified")
nvme_lines = [line for line in summary.splitlines() if line.startswith(f"managed_volume_nvme={volume_id} ")]
if len(nvme_lines) != 1:
    raise SystemExit("report missing managed_volume_nvme line")
if new_addr not in nvme_lines[0] or old_addr in nvme_lines[0] or "path_count=2" not in nvme_lines[0]:
    raise SystemExit("report did not reflect desired path replacement")
for name, doc in (("operator_snapshot", snapshot), ("dashboard", dashboard)):
    vols = doc.get("volumes") or []
    if len(vols) != 1:
        raise SystemExit(f"{name} volume count={len(vols)}")
    assert_status(name, vols[0].get("status") or {})
(base/"nvme-k8s-desired-path-change-asserts.txt").write_text("\n".join([
    "crd_status_agrees=true",
    "report_dashboard_agree=true",
    "surface_ready_reason=first_volume_verified",
]) + "\n")
PY
  cat "${ARTIFACT_DIR}/nvme-k8s-desired-path-change-asserts.txt" >>"${SUMMARY}"
  write_summary "${PHASE_STATUS_KEY}=ok"
  cleanup
  verify_rc=0
  (
    cd "${ROOT}"
    SW_BLOCK_CLEANUP_WAIT_SECONDS=180 \
    SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/cleanup/verify" bash scripts/verify-helm-cleanup.sh "${ROOT}" \
      >"${ARTIFACT_DIR}/cleanup/verify.stdout.txt" 2>"${ARTIFACT_DIR}/cleanup/verify.stderr.txt"
  ) || verify_rc=$?
  cat "${ARTIFACT_DIR}/cleanup/verify/cleanup-summary.txt" >>"${SUMMARY}"
  grep -q '^cleanup_status=ok$' "${ARTIFACT_DIR}/cleanup/verify/cleanup-summary.txt"
  if [[ "${verify_rc}" -ne 0 ]]; then
    exit "${verify_rc}"
  fi
  echo "[phase111] PASS"
  exit 0
fi

if [[ "${HOST_PATH_DISCONNECT}" == "1" || "${HOST_PATH_DISCONNECT}" == "true" ]]; then
  echo "[phase111] disconnect one host NVMe path and wait for CSI-node reconnect owner"
  VOLUME_ID="$(volume_field_from_crd "${ARTIFACT_DIR}/surfaces/swblockvolumes.healthy.json" "status.volumeID")"
  NQN="$(volume_field_from_crd "${ARTIFACT_DIR}/surfaces/swblockvolumes.healthy.json" "status.nvme.nqn")"
  write_summary "volume_id=${VOLUME_ID}"
  write_summary "nvme_nqn=${NQN}"
  write_summary "reconnect_owner=csi-node"
  write_summary "desired_path_set_changed=false-with-reason=host_path_disconnect_uses_stable_publish_evidence"
  write_host_nvme_path_info "before-host-disconnect" "${NQN}"
  INITIAL_PATH_COUNT="$(read_env_value "${ARTIFACT_DIR}/inject/nvme-path-info.before-host-disconnect.env" path_count)"
  TARGET_CONTROLLER="$(read_env_value "${ARTIFACT_DIR}/inject/nvme-path-info.before-host-disconnect.env" controller)"
  TARGET_ADDR="$(read_env_value "${ARTIFACT_DIR}/inject/nvme-path-info.before-host-disconnect.env" addr)"
  write_summary "initial_path_count=${INITIAL_PATH_COUNT}"
  write_summary "target_controller=${TARGET_CONTROLLER}"
  write_summary "target_addr=${TARGET_ADDR}"
  if [[ "${INITIAL_PATH_COUNT}" != "2" || -z "${TARGET_CONTROLLER}" ]]; then
    echo "need exactly two host NVMe paths and a target controller, got count=${INITIAL_PATH_COUNT} controller=${TARGET_CONTROLLER}" >&2
    exit 1
  fi
  sudo -n nvme disconnect -d "${TARGET_CONTROLLER}" >"${ARTIFACT_DIR}/inject/nvme-disconnect-one-path.txt" 2>&1
  wait_for_host_nvme_path_count "after-host-disconnect" "${NQN}" "1"
  write_summary "path_loss_detected=true"
  write_summary "after_disconnect_path_count=1"
  if ! wait_for_reconnect_owner_log "${ARTIFACT_DIR}/inject/csi-node-reconnect-owner.log"; then
    echo "CSI-node reconnect owner did not report reconnected=1" >&2
    exit 1
  fi
  wait_for_host_nvme_path_count "after-owner-reconnect" "${NQN}" "2"
  write_summary "reconnect_invoked=true"
  write_summary "replacement_path_connected=true"
  write_summary "reconnected_path_count=2"
  write_summary "host_mutation_scope=nvme_connect_missing_paths_only"
  write_summary "stale_path_disconnect_claim=false-with-reason=gate_disconnects_one_test_path_no_product_stale_disconnect"

  if [[ "${MOUNTED_IO}" == "1" || "${MOUNTED_IO}" == "true" ]]; then
    MOUNTED_POD_UID_RECONNECTED="$(kubectl -n "${APP_NAMESPACE}" get pod "${MOUNTED_POD}" -o jsonpath='{.metadata.uid}')"
    if [[ "${MOUNTED_POD_UID_RECONNECTED}" != "${MOUNTED_POD_UID_BEFORE}" ]]; then
      echo "mounted pod UID changed after reconnect: before=${MOUNTED_POD_UID_BEFORE} reconnected=${MOUNTED_POD_UID_RECONNECTED}" >&2
      exit 1
    fi
    kubectl -n "${APP_NAMESPACE}" exec "${MOUNTED_POD}" -- sh -c 'set -eu; echo after-owner-reconnect >> /data/phase131-mounted.txt; sync; grep after-owner-reconnect /data/phase131-mounted.txt' >"${ARTIFACT_DIR}/inject/mounted-after-owner-reconnect.log"
    write_summary "pod_uid_preserved=true"
    write_summary "mounted_io_after_reconnect=ok"
  fi

  wait_for_crd_status "reconnected" "ready" "first_volume_verified" "2"
  with_master_port_forward "${ARTIFACT_DIR}/surfaces/reconnect-blockmaster-port-forward.log" \
    "${ARTIFACT_DIR}/bin/sw-block" ops report \
      --master-api "127.0.0.1:${STATUS_PORT}" \
      --namespace "${APP_NAMESPACE}" \
      --out "${ARTIFACT_DIR}/surfaces/reconnect-report" \
      --timeout 30s >"${ARTIFACT_DIR}/surfaces/reconnect-report.stdout.txt" 2>"${ARTIFACT_DIR}/surfaces/reconnect-report.stderr.txt"
  DASHBOARD_PORT="$(python3 - <<'PY'
import socket
s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()
PY
)"
  "${ARTIFACT_DIR}/bin/sw-block" ops dashboard \
    --from-bundle "${ARTIFACT_DIR}/surfaces/reconnect-report" \
    --listen "127.0.0.1:${DASHBOARD_PORT}" >"${ARTIFACT_DIR}/surfaces/reconnect-dashboard.stdout.txt" 2>"${ARTIFACT_DIR}/surfaces/reconnect-dashboard.stderr.txt" &
  DASH_PID=$!
  for _ in $(seq 1 50); do
    code="$(curl -s -o "${ARTIFACT_DIR}/surfaces/reconnect-dashboard-operator-snapshot.json" -w '%{http_code}' "http://127.0.0.1:${DASHBOARD_PORT}/operator-snapshot.json" || true)"
    [[ "${code}" == "200" ]] && break
    sleep 0.2
  done
  kill "${DASH_PID}" >/dev/null 2>&1 || true
  wait "${DASH_PID}" >/dev/null 2>&1 || true

  python3 - "${ARTIFACT_DIR}" "${VOLUME_ID}" <<'PY'
import json
import sys
from pathlib import Path
base = Path(sys.argv[1])
volume_id = sys.argv[2]
crd = json.load(open(base/"surfaces/swblockvolumes.reconnected.json"))
status = crd["items"][0]["status"]
summary = (base/"surfaces/reconnect-report/summary.txt").read_text()
snapshot = json.load(open(base/"surfaces/reconnect-report/operator-snapshot.json"))
dashboard = json.load(open(base/"surfaces/reconnect-dashboard-operator-snapshot.json"))
if status.get("status") != "ready" or status.get("reasonCode") != "first_volume_verified":
    raise SystemExit(f"CRD status mismatch: {status}")
if (status.get("nvme") or {}).get("pathCount") != 2:
    raise SystemExit(f"CRD path count mismatch: {status.get('nvme')}")
if f"managed_volume={volume_id} status=ready reason=first_volume_verified" not in summary:
    raise SystemExit("report missing ready/first_volume_verified")
if f"managed_volume_nvme={volume_id}" not in summary or "path_count=2" not in summary:
    raise SystemExit("report missing NVMe path_count=2")
for name, doc in (("operator_snapshot", snapshot), ("dashboard", dashboard)):
    vols = doc.get("volumes") or []
    if len(vols) != 1:
        raise SystemExit(f"{name} volume count={len(vols)}")
    st = vols[0].get("status") or {}
    nvme = st.get("nvme") or {}
    if st.get("status") != "ready" or st.get("reason_code") != "first_volume_verified":
        raise SystemExit(f"{name} status mismatch: {st}")
    if nvme.get("path_count") != 2:
        raise SystemExit(f"{name} nvme mismatch: {nvme}")
(base/"nvme-k8s-reconnect-live-asserts.txt").write_text("\n".join([
    "crd_status_agrees=true",
    "report_dashboard_agree=true",
    "surface_ready_reason=first_volume_verified",
]) + "\n")
PY
  cat "${ARTIFACT_DIR}/nvme-k8s-reconnect-live-asserts.txt" >>"${SUMMARY}"
  write_summary "${PHASE_STATUS_KEY}=ok"
  cleanup
  verify_rc=0
  (
    cd "${ROOT}"
    SW_BLOCK_CLEANUP_WAIT_SECONDS=180 \
    SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/cleanup/verify" bash scripts/verify-helm-cleanup.sh "${ROOT}" \
      >"${ARTIFACT_DIR}/cleanup/verify.stdout.txt" 2>"${ARTIFACT_DIR}/cleanup/verify.stderr.txt"
  ) || verify_rc=$?
  cat "${ARTIFACT_DIR}/cleanup/verify/cleanup-summary.txt" >>"${SUMMARY}"
  grep -q '^cleanup_status=ok$' "${ARTIFACT_DIR}/cleanup/verify/cleanup-summary.txt"
  if [[ "${verify_rc}" -ne 0 ]]; then
    exit "${verify_rc}"
  fi
  echo "[phase111] PASS"
  exit 0
fi

echo "[phase111] scale one blockvolume deployment to zero to remove one NVMe path"
kubectl -n "${APP_NAMESPACE}" get deploy -l app=sw-blockvolume -o json >"${ARTIFACT_DIR}/inject/blockvolume-deployments.before.json"
TARGET_DEPLOY="$(python3 - "${ARTIFACT_DIR}/inject/blockvolume-deployments.before.json" <<'PY'
import json, sys
doc=json.load(open(sys.argv[1]))
items=doc.get("items") or []
if len(items) < 2:
    raise SystemExit(f"need at least two blockvolume deployments, got {len(items)}")
items=sorted(items, key=lambda i: i["metadata"]["name"])
print(items[0]["metadata"]["name"])
PY
)"
write_summary "target_deployment=${TARGET_DEPLOY}"
kubectl -n "${APP_NAMESPACE}" scale "deploy/${TARGET_DEPLOY}" --replicas=0 >"${ARTIFACT_DIR}/inject/scale-target.txt"
kubectl -n "${APP_NAMESPACE}" rollout status "deploy/${TARGET_DEPLOY}" --timeout=120s >"${ARTIFACT_DIR}/inject/rollout-target-zero.txt" 2>&1 || true
for _ in $(seq 1 60); do
  pods="$(kubectl -n "${APP_NAMESPACE}" get pod -l app=sw-blockvolume -o jsonpath='{range .items[*]}{.metadata.ownerReferences[0].name}{"\n"}{end}' 2>/dev/null | grep -c "^${TARGET_DEPLOY}$" || true)"
  [[ "${pods}" == "0" ]] && break
  sleep 1
done
kubectl -n "${APP_NAMESPACE}" get deploy,pod -l app=sw-blockvolume -o wide >"${ARTIFACT_DIR}/inject/blockvolume.after-scale.txt"

wait_for_crd_status "pathloss" "blocked" "nvme_multipath_path_missing" "1"
VOLUME_ID="$(python3 - "${ARTIFACT_DIR}/surfaces/swblockvolumes.pathloss.json" <<'PY'
import json, sys
doc=json.load(open(sys.argv[1]))
print(doc["items"][0]["status"]["volumeID"])
PY
)"
write_summary "volume_id=${VOLUME_ID}"

if [[ "${MOUNTED_IO}" == "1" || "${MOUNTED_IO}" == "true" ]]; then
  echo "[phase111] verify mounted pod survives and writes after one path loss"
  MOUNTED_POD_UID_AFTER="$(kubectl -n "${APP_NAMESPACE}" get pod "${MOUNTED_POD}" -o jsonpath='{.metadata.uid}')"
  if [[ "${MOUNTED_POD_UID_AFTER}" != "${MOUNTED_POD_UID_BEFORE}" ]]; then
    echo "mounted pod UID changed: before=${MOUNTED_POD_UID_BEFORE} after=${MOUNTED_POD_UID_AFTER}" >&2
    exit 1
  fi
  kubectl -n "${APP_NAMESPACE}" exec "${MOUNTED_POD}" -- sh -c 'set -eu; echo after-path-loss >> /data/phase112-mounted.txt; sync; grep before-path-loss /data/phase112-mounted.txt; grep after-path-loss /data/phase112-mounted.txt' >"${ARTIFACT_DIR}/inject/mounted-after.log"
  write_summary "mounted_pod_uid_after=${MOUNTED_POD_UID_AFTER}"
  write_summary "mounted_pod_uid_preserved=true"
  write_summary "mounted_io_after_path_loss=ok"
fi

echo "[phase111] collect live surfaces"
with_master_port_forward "${ARTIFACT_DIR}/surfaces/blockmaster-port-forward.log" \
  "${ARTIFACT_DIR}/bin/sw-block" ops report \
    --master-api "127.0.0.1:${STATUS_PORT}" \
    --namespace "${APP_NAMESPACE}" \
    --out "${ARTIFACT_DIR}/surfaces/report" \
    --timeout 30s >"${ARTIFACT_DIR}/surfaces/report.stdout.txt" 2>"${ARTIFACT_DIR}/surfaces/report.stderr.txt"

"${ARTIFACT_DIR}/bin/sw-block" ops explain volume \
  --from-bundle "${ARTIFACT_DIR}/surfaces/report" \
  "${VOLUME_ID}" >"${ARTIFACT_DIR}/surfaces/explain.txt" 2>"${ARTIFACT_DIR}/surfaces/explain.stderr.txt"

DASHBOARD_PORT="$(python3 - <<'PY'
import socket
s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()
PY
)"
"${ARTIFACT_DIR}/bin/sw-block" ops dashboard \
  --from-bundle "${ARTIFACT_DIR}/surfaces/report" \
  --listen "127.0.0.1:${DASHBOARD_PORT}" >"${ARTIFACT_DIR}/surfaces/dashboard.stdout.txt" 2>"${ARTIFACT_DIR}/surfaces/dashboard.stderr.txt" &
DASH_PID=$!
for _ in $(seq 1 50); do
  code="$(curl -s -o "${ARTIFACT_DIR}/surfaces/dashboard-operator-snapshot.json" -w '%{http_code}' "http://127.0.0.1:${DASHBOARD_PORT}/operator-snapshot.json" || true)"
  [[ "${code}" == "200" ]] && break
  sleep 0.2
done
kill "${DASH_PID}" >/dev/null 2>&1 || true
wait "${DASH_PID}" >/dev/null 2>&1 || true

SW_BLOCK_NVME_PATH_LOSS_PHASE_STATUS_KEY="${PHASE_STATUS_KEY}" python3 - "${ARTIFACT_DIR}" <<'PY'
import json, re, sys
from pathlib import Path
base=Path(sys.argv[1])
crd=json.load(open(base/"surfaces/swblockvolumes.pathloss.json"))
item=crd["items"][0]
st=item["status"]
nv=st["nvme"]
volume_id=st["volumeID"]
summary=(base/"surfaces/report/summary.txt").read_text()
snapshot=json.load(open(base/"surfaces/report/operator-snapshot.json"))
dashboard=json.load(open(base/"surfaces/dashboard-operator-snapshot.json"))
explain=(base/"surfaces/explain.txt").read_text()
docs=[("report", snapshot), ("dashboard", dashboard)]
if st.get("status") != "blocked" or st.get("reasonCode") != "nvme_multipath_path_missing":
    raise SystemExit(f"crd mismatch: {st}")
if nv.get("pathCount") != 1:
    raise SystemExit(f"crd nvme path count mismatch: {nv}")
if any(c.get("type") == "Ready" and c.get("status") == "True" for c in st.get("conditions") or []):
    raise SystemExit("CRD contains Ready=True")
if f"managed_volume={volume_id} status=blocked reason=nvme_multipath_path_missing" not in summary:
    raise SystemExit("report summary missing blocked/path-missing")
if "Ready=True" in summary:
    raise SystemExit("report summary contains Ready=True")
if f"managed_volume_nvme={volume_id}" not in summary or "path_count=1" not in summary:
    raise SystemExit("report summary missing nvme path_count=1")
for name, doc in docs:
    vols=doc.get("volumes") or []
    if len(vols) != 1:
        raise SystemExit(f"{name} volume count={len(vols)}")
    s=vols[0]["status"]
    n=s.get("nvme") or {}
    if s.get("status") != "blocked" or s.get("reason_code") != "nvme_multipath_path_missing":
        raise SystemExit(f"{name} status mismatch: {s}")
    if n.get("path_count") != 1:
        raise SystemExit(f"{name} nvme mismatch: {n}")
    if any(c.get("type") == "Ready" and c.get("status") == "True" for c in s.get("conditions") or []):
        raise SystemExit(f"{name} contains Ready=True")
if "status=blocked reason=nvme_multipath_path_missing" not in explain:
    raise SystemExit("explain missing blocked/path-missing")
if "mutation_allowed=false" not in summary and '"mutation_allowed":false' not in json.dumps(snapshot):
    raise SystemExit("mutation_allowed=false not surfaced")
(base/"nvme-k8s-path-loss-crd-asserts.txt").write_text("\n".join([
    __import__("os").environ.get("SW_BLOCK_NVME_PATH_LOSS_PHASE_STATUS_KEY", "phase111_nvme_k8s_path_loss_crd_status")+"=ok",
    "before_path_count=2",
    "after_path_count=1",
    "crd_reason=nvme_multipath_path_missing",
    "report_reason=nvme_multipath_path_missing",
    "operator_snapshot_reason=nvme_multipath_path_missing",
    "dashboard_reason=nvme_multipath_path_missing",
    "explain_reason=nvme_multipath_path_missing",
    "surface_ready_true_count=0",
    "mutation_allowed=false",
])+"\n")
PY
cat "${ARTIFACT_DIR}/nvme-k8s-path-loss-crd-asserts.txt" >>"${SUMMARY}"

if [[ "${RESTORE_PATH}" == "1" || "${RESTORE_PATH}" == "true" ]]; then
  echo "[phase111] restore removed NVMe path and verify mounted I/O"
  kubectl -n "${APP_NAMESPACE}" scale "deploy/${TARGET_DEPLOY}" --replicas=1 >"${ARTIFACT_DIR}/inject/restore-target.txt"
  kubectl -n "${APP_NAMESPACE}" rollout status "deploy/${TARGET_DEPLOY}" --timeout=240s >"${ARTIFACT_DIR}/inject/rollout-target-restore.txt" 2>&1

  wait_for_crd_status "restored" "ready" "first_volume_verified" "2"
  if [[ "${MOUNTED_IO}" == "1" || "${MOUNTED_IO}" == "true" ]]; then
    MOUNTED_POD_UID_RESTORED="$(kubectl -n "${APP_NAMESPACE}" get pod "${MOUNTED_POD}" -o jsonpath='{.metadata.uid}')"
    if [[ "${MOUNTED_POD_UID_RESTORED}" != "${MOUNTED_POD_UID_BEFORE}" ]]; then
      echo "mounted pod UID changed after restore: before=${MOUNTED_POD_UID_BEFORE} restored=${MOUNTED_POD_UID_RESTORED}" >&2
      exit 1
    fi
    kubectl -n "${APP_NAMESPACE}" exec "${MOUNTED_POD}" -- sh -c 'set -eu; echo after-restore >> /data/phase112-mounted.txt; sync; grep before-path-loss /data/phase112-mounted.txt; grep after-path-loss /data/phase112-mounted.txt; grep after-restore /data/phase112-mounted.txt' >"${ARTIFACT_DIR}/inject/mounted-after-restore.log"
    write_summary "mounted_pod_uid_after_restore=${MOUNTED_POD_UID_RESTORED}"
    write_summary "mounted_pod_uid_preserved_after_restore=true"
    write_summary "mounted_io_after_restore=ok"
  fi

  with_master_port_forward "${ARTIFACT_DIR}/surfaces/restore-blockmaster-port-forward.log" \
    "${ARTIFACT_DIR}/bin/sw-block" ops report \
      --master-api "127.0.0.1:${STATUS_PORT}" \
      --namespace "${APP_NAMESPACE}" \
      --out "${ARTIFACT_DIR}/surfaces/restore-report" \
      --timeout 30s >"${ARTIFACT_DIR}/surfaces/restore-report.stdout.txt" 2>"${ARTIFACT_DIR}/surfaces/restore-report.stderr.txt"

  "${ARTIFACT_DIR}/bin/sw-block" ops explain volume \
    --from-bundle "${ARTIFACT_DIR}/surfaces/restore-report" \
    "${VOLUME_ID}" >"${ARTIFACT_DIR}/surfaces/restore-explain.txt" 2>"${ARTIFACT_DIR}/surfaces/restore-explain.stderr.txt"

  python3 - "${ARTIFACT_DIR}" "${VOLUME_ID}" <<'PY'
import json, sys
from pathlib import Path
base=Path(sys.argv[1])
volume_id=sys.argv[2]
crd=json.load(open(base/"surfaces/swblockvolumes.restored.json"))
item=crd["items"][0]
st=item["status"]
nv=st.get("nvme") or {}
summary=(base/"surfaces/restore-report/summary.txt").read_text()
snapshot=json.load(open(base/"surfaces/restore-report/operator-snapshot.json"))
explain=(base/"surfaces/restore-explain.txt").read_text()
if st.get("status") != "ready" or st.get("reasonCode") != "first_volume_verified":
    raise SystemExit(f"restored CRD mismatch: {st}")
if nv.get("pathCount") != 2:
    raise SystemExit(f"restored CRD path count mismatch: {nv}")
if f"managed_volume={volume_id} status=ready reason=first_volume_verified" not in summary:
    raise SystemExit("restore report summary missing ready/first_volume_verified")
if f"managed_volume_nvme={volume_id}" not in summary or "path_count=2" not in summary:
    raise SystemExit("restore report missing nvme path_count=2")
vols=snapshot.get("volumes") or []
if len(vols) != 1:
    raise SystemExit(f"restore snapshot volume count={len(vols)}")
s=vols[0].get("status") or {}
n=s.get("nvme") or {}
if s.get("status") != "ready" or s.get("reason_code") != "first_volume_verified":
    raise SystemExit(f"restore snapshot mismatch: {s}")
if n.get("path_count") != 2:
    raise SystemExit(f"restore snapshot nvme mismatch: {n}")
if "status=ready reason=first_volume_verified" not in explain:
    raise SystemExit("restore explain missing ready/first_volume_verified")
(base/"nvme-k8s-path-restore-asserts.txt").write_text("\n".join([
    "restored_path_count=2",
    "restore_crd_reason=first_volume_verified",
    "restore_report_reason=first_volume_verified",
    "restore_operator_snapshot_reason=first_volume_verified",
    "restore_explain_reason=first_volume_verified",
])+"\n")
PY
  cat "${ARTIFACT_DIR}/nvme-k8s-path-restore-asserts.txt" >>"${SUMMARY}"
fi

cleanup
verify_rc=0
(
  cd "${ROOT}"
  SW_BLOCK_CLEANUP_WAIT_SECONDS=180 \
  SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/cleanup/verify" bash scripts/verify-helm-cleanup.sh "${ROOT}" \
    >"${ARTIFACT_DIR}/cleanup/verify.stdout.txt" 2>"${ARTIFACT_DIR}/cleanup/verify.stderr.txt"
) || verify_rc=$?
cat "${ARTIFACT_DIR}/cleanup/verify/cleanup-summary.txt" >>"${SUMMARY}"
grep -q '^cleanup_status=ok$' "${ARTIFACT_DIR}/cleanup/verify/cleanup-summary.txt"
if [[ "${verify_rc}" -ne 0 ]]; then
  exit "${verify_rc}"
fi

echo "[phase111] PASS"
