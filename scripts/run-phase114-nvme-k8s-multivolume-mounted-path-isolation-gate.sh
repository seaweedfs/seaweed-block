#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase114-nvme-k8s-multivolume-mounted-path-isolation-gate}"
SUMMARY="${ARTIFACT_DIR}/phase114-nvme-k8s-multivolume-mounted-path-isolation-summary.txt"
HELM_RELEASE="${SW_BLOCK_HELM_RELEASE:-sw-block}"
HELM_NAMESPACE="${SW_BLOCK_HELM_NAMESPACE:-kube-system}"
APP_NAMESPACE="${SW_BLOCK_APP_NAMESPACE:-default}"
IMAGE="${SW_BLOCK_IMAGE:-sw-block:local}"
CSI_IMAGE="${SW_BLOCK_CSI_IMAGE:-sw-block-csi:local}"
STATUS_PORT="${SW_BLOCK_MASTER_PORT_FORWARD_PORT:-29333}"
VOLUME_COUNT=2

mkdir -p "${ARTIFACT_DIR}"/{bin,build,values,install,multi-volume,mounted,inject,surfaces,cleanup}
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

collect_mounted_failure_diagnostics() {
  local pod="$1"
  local idx="$2"
  local marker="$3"
  local dir="${ARTIFACT_DIR}/mounted/diagnostics-${marker}-${idx}"
  mkdir -p "${dir}"
  {
    echo "pod=${pod}"
    echo "idx=${idx}"
    echo "marker=${marker}"
    date -u +%Y-%m-%dT%H:%M:%SZ
  } >"${dir}/context.txt"
  kubectl -n "${APP_NAMESPACE}" get pod "${pod}" -o wide >"${dir}/pod.get.txt" 2>&1 || true
  kubectl -n "${APP_NAMESPACE}" describe pod "${pod}" >"${dir}/pod.describe.txt" 2>&1 || true
  kubectl -n "${APP_NAMESPACE}" exec "${pod}" -- sh -c 'cat /proc/mounts; echo ---; ls -la /data || true; echo ---; cat /data/phase114-mounted-'"${idx}"'.txt || true' \
    >"${dir}/pod-mounted-state.txt" 2>&1 || true
  kubectl -n "${APP_NAMESPACE}" get deploy,pod -l sw-block.seaweedfs.com/volume -o wide >"${dir}/blockvolume.get.txt" 2>&1 || true
  for p in $(kubectl -n "${APP_NAMESPACE}" get pod -l sw-block.seaweedfs.com/volume -o name 2>/dev/null || true); do
    safe="${p#pod/}"
    kubectl -n "${APP_NAMESPACE}" logs "${p}" --tail=300 >"${dir}/blockvolume-${safe}.log" 2>&1 || true
  done
  sudo -n nvme list -v >"${dir}/host-nvme-list-v.txt" 2>&1 || true
  sudo -n nvme list-subsys -v >"${dir}/host-nvme-list-subsys-v.txt" 2>&1 || true
  sudo -n find /sys/class/nvme-subsystem -maxdepth 5 -type f \
    \( -name ana_state -o -name state -o -name address -o -name subsysnqn -o -name cntlid \) \
    -print -exec cat {} \; >"${dir}/host-nvme-sysfs.txt" 2>&1 || true
  sudo -n dmesg --ctime | tail -200 >"${dir}/host-dmesg-tail.txt" 2>&1 || true
}

run_mounted_exec_with_retry() {
  local pod="$1"
  local idx="$2"
  local marker="$3"
  local out="$4"
  local tmp_out tmp_err
  tmp_out="$(mktemp)"
  tmp_err="$(mktemp)"
  for attempt in $(seq 1 60); do
    : >"${tmp_out}"
    : >"${tmp_err}"
    if kubectl -n "${APP_NAMESPACE}" exec "${pod}" -- sh -c "set -eu; echo ${marker}-${idx} >> /data/phase114-mounted-${idx}.txt; sync; grep before-loss-${idx} /data/phase114-mounted-${idx}.txt; grep after-loss-${idx} /data/phase114-mounted-${idx}.txt; grep ${marker}-${idx} /data/phase114-mounted-${idx}.txt" >"${tmp_out}" 2>"${tmp_err}" &&
      grep -q "^before-loss-${idx}$" "${tmp_out}" &&
      grep -q "^after-loss-${idx}$" "${tmp_out}" &&
      grep -q "^${marker}-${idx}$" "${tmp_out}"; then
      cp "${tmp_out}" "${out}"
      rm -f "${tmp_out}" "${tmp_err}"
      return 0
    fi
    {
      echo "--- attempt ${attempt} failed at $(date -u +%Y-%m-%dT%H:%M:%SZ) ---"
      cat "${tmp_err}" || true
      cat "${tmp_out}" || true
    } >>"${out}.attempts"
    sleep 2
  done
  cp "${tmp_out}" "${out}" 2>/dev/null || true
  cp "${tmp_err}" "${out}.stderr" 2>/dev/null || true
  collect_mounted_failure_diagnostics "${pod}" "${idx}" "${marker}"
  rm -f "${tmp_out}" "${tmp_err}"
  return 1
}

delete_pod() {
  local pod="$1"
  local log="${ARTIFACT_DIR}/cleanup/delete-${pod}.txt"
  kubectl -n "${APP_NAMESPACE}" delete pod "${pod}" --ignore-not-found=true --wait=true --timeout=120s >"${log}" 2>&1 && return 0
  {
    echo "--- pod did not delete gracefully; forcing test cleanup ---"
    kubectl -n "${APP_NAMESPACE}" get pod "${pod}" -o wide || true
    kubectl -n "${APP_NAMESPACE}" describe pod "${pod}" || true
    kubectl -n "${APP_NAMESPACE}" delete pod "${pod}" --ignore-not-found=true --force --grace-period=0 --wait=false || true
  } >>"${log}" 2>&1
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
  for idx in $(seq 1 "${VOLUME_COUNT}"); do
    delete_pod "sw-block-phase114-mounted-${idx}"
  done
  kubectl -n "${APP_NAMESPACE}" delete pod -l sw-block-test=multi-volume --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  for idx in $(seq 1 "${VOLUME_COUNT}"); do
    kubectl -n "${APP_NAMESPACE}" delete pod "sw-block-multi-reader-${idx}" "sw-block-multi-writer-${idx}" --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
    kubectl -n "${APP_NAMESPACE}" delete pvc "sw-block-multi-pvc-${idx}" --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  done
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
  for idx in $(seq 1 "${VOLUME_COUNT}"); do
    kubectl -n "${APP_NAMESPACE}" get pvc "sw-block-multi-pvc-${idx}" >/dev/null 2>&1 && \
      kubectl -n "${APP_NAMESPACE}" patch pvc "sw-block-multi-pvc-${idx}" --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null 2>&1
  done
  kubectl get pv --no-headers 2>/dev/null | awk '/sw-block-multi/ {print $1}' | \
    xargs -r -n1 kubectl patch pv --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null 2>&1
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

wait_for_volume_states() {
  local phase="$1"
  local out_json="${ARTIFACT_DIR}/surfaces/swblockvolumes.${phase}.json"
  shift
  local expected="$*"
  for _ in $(seq 1 120); do
    kubectl -n "${HELM_NAMESPACE}" get swblockvolumes -o json >"${out_json}" 2>/dev/null || true
    python3 - "${out_json}" "${expected}" <<'PY' && return 0 || true
import json, sys
path, expected = sys.argv[1:3]
try:
    doc=json.load(open(path))
except Exception:
    raise SystemExit(1)
want={}
for item in expected.split(","):
    pvc,status,reason,path_count=item.split(":")
    want[pvc]=(status, reason, int(path_count))
items=doc.get("items") or []
if len(items) != len(want):
    raise SystemExit(1)
seen={}
for item in items:
    st=item.get("status") or {}
    pvc=st.get("pvcName")
    if pvc not in want:
        raise SystemExit(1)
    nv=st.get("nvme") or {}
    status, reason, path_count = want[pvc]
    if st.get("status") != status or st.get("reasonCode") != reason:
        raise SystemExit(1)
    if int(nv.get("pathCount", -1)) != path_count:
        raise SystemExit(1)
    if status == "blocked" and any(c.get("type") == "Ready" and c.get("status") == "True" for c in st.get("conditions") or []):
        raise SystemExit(1)
    seen[pvc]=st.get("volumeID")
if set(seen) != set(want):
    raise SystemExit(1)
PY
    sleep 2
  done
  echo "SwBlockVolumes did not reach expected ${phase} states: ${expected}" >&2
  kubectl -n "${HELM_NAMESPACE}" get swblockvolumes -o yaml >&2 || true
  return 1
}

capture_host_nvme_state() {
  local phase="$1"
  local dir="${ARTIFACT_DIR}/mounted/host-nvme-${phase}"
  mkdir -p "${dir}"
  sudo -n nvme list -v >"${dir}/list-v.txt" 2>&1 || true
  sudo -n nvme list-subsys -v >"${dir}/list-subsys-v.txt" 2>&1 || true
  sudo -n nvme list -o json >"${dir}/list.json" 2>&1 || true
  sudo -n nvme list-subsys -o json >"${dir}/list-subsys.json" 2>&1 || true
  sudo -n find /sys/class/nvme-subsystem -maxdepth 5 -type f \
    \( -name ana_state -o -name state -o -name address -o -name subsysnqn -o -name cntlid \) \
    -print -exec cat {} \; >"${dir}/nvme-sysfs.txt" 2>&1 || true
  kubectl -n "${APP_NAMESPACE}" get pvc -o yaml >"${dir}/pvc.yaml" 2>&1 || true
  kubectl get pv -o yaml >"${dir}/pv.yaml" 2>&1 || true
  kubectl get volumeattachments -o yaml >"${dir}/volumeattachments.yaml" 2>&1 || true
  kubectl -n "${APP_NAMESPACE}" get deploy,pod -l sw-block.seaweedfs.com/volume -o wide >"${dir}/blockvolume.get.txt" 2>&1 || true
  for p in $(kubectl -n "${APP_NAMESPACE}" get pod -l sw-block.seaweedfs.com/volume -o name 2>/dev/null || true); do
    safe="${p#pod/}"
    kubectl -n "${APP_NAMESPACE}" logs "${p}" --tail=500 >"${dir}/blockvolume-${safe}.log" 2>&1 || true
  done
  kubectl -n "${HELM_NAMESPACE}" get pod -o wide >"${dir}/kube-system-pods.txt" 2>&1 || true
  kubectl -n "${HELM_NAMESPACE}" logs ds/sw-block-csi-node --all-containers --tail=500 >"${dir}/csi-node.log" 2>&1 || true
  kubectl -n "${HELM_NAMESPACE}" logs deploy/sw-block-csi-controller --all-containers --tail=500 >"${dir}/csi-controller.log" 2>&1 || true
}

host_nvme_live_path_count() {
  local nqn="$1"
  sudo -n nvme list-subsys -v 2>/dev/null | awk -v needle="NQN=${nqn}" '
    $0 ~ needle { inside=1; next }
    inside && /^$/ { inside=0 }
    inside && / live$/ { count++ }
    END { print count + 0 }
  '
}

assert_host_nvme_paths() {
  local phase="$1"
  local idx="$2"
  local nqn="$3"
  local want_min="$4"
  local deadline="${5:-90}"
  local count
  local attempt=0
  local end=$((SECONDS + deadline))
  count=0
  while (( SECONDS <= end )); do
    attempt=$((attempt + 1))
    capture_host_nvme_state "${phase}"
    count="$(host_nvme_live_path_count "${nqn}")"
    if (( count >= want_min )); then
      break
    fi
    cp -f "${ARTIFACT_DIR}/mounted/host-nvme-${phase}/list-subsys-v.txt" \
      "${ARTIFACT_DIR}/mounted/host-nvme-${phase}/list-subsys-v.attempt-${attempt}.txt" 2>/dev/null || true
    sleep 2
  done
  write_summary "host_${phase}_${idx}_live_path_count=${count}"
  if (( count < want_min )); then
    echo "host NVMe ${phase} volume ${idx} live path count ${count}, want >= ${want_min} for ${nqn} after ${deadline}s" >&2
    return 1
  fi
}

write_summary "phase114_nvme_k8s_multivolume_mounted_path_isolation_status=running"
cleanup

echo "[phase114] build sw-block CLI"
(
  cd "${ROOT}"
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
python3 -c "${python_read_nodes}" "${ARTIFACT_DIR}"
grep -q '^network_mode=external-nvme$' "${ARTIFACT_DIR}/values/generate.stdout.txt"
grep -q 'externalNVMe: true' "${ARTIFACT_DIR}/values/values.nvme.yaml"
grep -q 'stage2Multipath:' "${ARTIFACT_DIR}/values/values.nvme.yaml"
grep -q 'enabled: true' "${ARTIFACT_DIR}/values/values.nvme.yaml"

echo "[phase114] install Helm stack"
(
  cd "${ROOT}"
  helm lint charts/seaweed-block -f "${ARTIFACT_DIR}/values/values.nvme.yaml" >"${ARTIFACT_DIR}/install/helm-lint.txt"
  helm template "${HELM_RELEASE}" charts/seaweed-block --namespace "${HELM_NAMESPACE}" \
    -f "${ARTIFACT_DIR}/values/values.nvme.yaml" >"${ARTIFACT_DIR}/install/helm-template.yaml"
  grep -q -- '--launcher-external-nvme' "${ARTIFACT_DIR}/install/helm-template.yaml"
  grep -q -- '--stage2-multipath' "${ARTIFACT_DIR}/install/helm-template.yaml"
  helm install "${HELM_RELEASE}" charts/seaweed-block --namespace "${HELM_NAMESPACE}" --create-namespace \
    -f "${ARTIFACT_DIR}/values/values.nvme.yaml" --wait --timeout 10m >"${ARTIFACT_DIR}/install/helm-install.txt"
)

APP_NODE="$(cat "${ARTIFACT_DIR}/values/app-node.txt")"
echo "[phase114] create two RF2 NVMe PVCs"
(
  cd "${ROOT}"
  SW_BLOCK_MULTI_VOLUME_NODE_SELECTOR="${APP_NODE}" \
  SW_BLOCK_MULTI_VOLUME_PROTOCOL=nvme \
  SW_BLOCK_MULTI_VOLUME_RF=2 \
  SW_BLOCK_MULTI_VOLUME_COUNT=2 \
  SW_BLOCK_MULTI_VOLUME_STAGE2_MULTIPATH=1 \
  SW_BLOCK_MULTI_VOLUME_CLEANUP=0 \
  SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/multi-volume" \
  SW_BLOCK_CLI="${ARTIFACT_DIR}/bin/sw-block" \
    bash scripts/run-multi-volume-example.sh "${ROOT}"
)
grep -q '^multi_volume_status=ok$' "${ARTIFACT_DIR}/multi-volume/multi-volume-summary.txt"
grep -q '^stage2_multipath=1$' "${ARTIFACT_DIR}/multi-volume/multi-volume-summary.txt"
grep -q 'stage2_multipath: "true"' "${ARTIFACT_DIR}/multi-volume/manifests/storageclass.yaml"
grep -q '^writer_verified_count=2$' "${ARTIFACT_DIR}/multi-volume/multi-volume-summary.txt"
grep -q '^reader_verified_count=2$' "${ARTIFACT_DIR}/multi-volume/multi-volume-summary.txt"

wait_for_volume_states "healthy" \
  "sw-block-multi-pvc-1:ready:first_volume_verified:2,sw-block-multi-pvc-2:ready:first_volume_verified:2"

python3 - "${ARTIFACT_DIR}/surfaces/swblockvolumes.healthy.json" "${ARTIFACT_DIR}/surfaces/volume-map.env" <<'PY'
import json, sys
doc=json.load(open(sys.argv[1]))
items=doc.get("items") or []
by_pvc={item["status"]["pvcName"]: item["status"] for item in items}
with open(sys.argv[2], "w") as out:
    for idx in (1, 2):
        pvc=f"sw-block-multi-pvc-{idx}"
        st=by_pvc[pvc]
        out.write(f"VOLUME_ID_{idx}={st['volumeID']}\n")
        out.write(f"NQN_{idx}={st.get('nvme',{}).get('nqn','')}\n")
        out.write(f"ADDR_{idx}={st.get('nvme',{}).get('nvmeAddr','')}\n")
        out.write(f"ADDRS_{idx}={','.join(st.get('nvme',{}).get('nvmeAddrs') or [])}\n")
PY
# shellcheck disable=SC1090
source "${ARTIFACT_DIR}/surfaces/volume-map.env"
write_summary "volume_1_id=${VOLUME_ID_1}"
write_summary "volume_2_id=${VOLUME_ID_2}"

for idx in $(seq 1 "${VOLUME_COUNT}"); do
  pod="sw-block-phase114-mounted-${idx}"
  cat >"${ARTIFACT_DIR}/mounted/pod-${idx}.yaml" <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  namespace: ${APP_NAMESPACE}
  labels:
    sw-block-test: nvme-multivolume-mounted-path-isolation
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
        claimName: sw-block-multi-pvc-${idx}
YAML
  kubectl -n "${APP_NAMESPACE}" apply -f "${ARTIFACT_DIR}/mounted/pod-${idx}.yaml" >"${ARTIFACT_DIR}/mounted/apply-pod-${idx}.txt"
  kubectl -n "${APP_NAMESPACE}" wait --for=condition=Ready "pod/${pod}" --timeout=180s >"${ARTIFACT_DIR}/mounted/wait-pod-${idx}.txt"
  uid="$(kubectl -n "${APP_NAMESPACE}" get pod "${pod}" -o jsonpath='{.metadata.uid}')"
  echo "${uid}" >"${ARTIFACT_DIR}/mounted/pod-${idx}.uid.before"
  kubectl -n "${APP_NAMESPACE}" exec "${pod}" -- sh -c "set -eu; echo before-loss-${idx} > /data/phase114-mounted-${idx}.txt; sync; grep before-loss-${idx} /data/phase114-mounted-${idx}.txt" >"${ARTIFACT_DIR}/mounted/before-${idx}.log"
  grep -q "^before-loss-${idx}$" "${ARTIFACT_DIR}/mounted/before-${idx}.log"
  write_summary "mounted_${idx}_pod_uid_before=${uid}"
done
assert_host_nvme_paths "pre_loss" 1 "${NQN_1}" 2
assert_host_nvme_paths "pre_loss" 2 "${NQN_2}" 2

echo "[phase114] remove one path from volume 1"
kubectl -n "${APP_NAMESPACE}" get deploy -l app=sw-blockvolume -o json >"${ARTIFACT_DIR}/inject/blockvolume-deployments.before.json"
TARGET_DEPLOY="$(python3 - "${ARTIFACT_DIR}/inject/blockvolume-deployments.before.json" "${VOLUME_ID_1}" <<'PY'
import json, sys
doc=json.load(open(sys.argv[1]))
volume_id=sys.argv[2]
matches=[i["metadata"]["name"] for i in doc.get("items", []) if volume_id in i["metadata"]["name"]]
if not matches:
    raise SystemExit(f"no deployment for {volume_id}")
print(sorted(matches)[0])
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

wait_for_volume_states "degraded" \
  "sw-block-multi-pvc-1:blocked:nvme_multipath_path_missing:1,sw-block-multi-pvc-2:ready:first_volume_verified:2"
assert_host_nvme_paths "after_loss" 1 "${NQN_1}" 1
assert_host_nvme_paths "after_loss" 2 "${NQN_2}" 2

for idx in $(seq 1 "${VOLUME_COUNT}"); do
  pod="sw-block-phase114-mounted-${idx}"
  uid_before="$(cat "${ARTIFACT_DIR}/mounted/pod-${idx}.uid.before")"
  uid_after="$(kubectl -n "${APP_NAMESPACE}" get pod "${pod}" -o jsonpath='{.metadata.uid}')"
  if [[ "${uid_after}" != "${uid_before}" ]]; then
    echo "mounted pod ${idx} UID changed after loss: before=${uid_before} after=${uid_after}" >&2
    exit 1
  fi
  kubectl -n "${APP_NAMESPACE}" exec "${pod}" -- sh -c "set -eu; echo after-loss-${idx} >> /data/phase114-mounted-${idx}.txt; sync; grep before-loss-${idx} /data/phase114-mounted-${idx}.txt; grep after-loss-${idx} /data/phase114-mounted-${idx}.txt" >"${ARTIFACT_DIR}/mounted/after-loss-${idx}.log"
  grep -q "^before-loss-${idx}$" "${ARTIFACT_DIR}/mounted/after-loss-${idx}.log"
  grep -q "^after-loss-${idx}$" "${ARTIFACT_DIR}/mounted/after-loss-${idx}.log"
  write_summary "mounted_${idx}_pod_uid_after_loss=${uid_after}"
  write_summary "mounted_${idx}_io_after_loss=ok"
done
write_summary "mounted_pods_preserved_after_loss=true"
write_summary "mounted_io_after_loss_count=2"

echo "[phase114] collect degraded surfaces"
with_master_port_forward "${ARTIFACT_DIR}/surfaces/degraded-blockmaster-port-forward.log" \
  "${ARTIFACT_DIR}/bin/sw-block" ops report \
    --master-api "127.0.0.1:${STATUS_PORT}" \
    --namespace "${APP_NAMESPACE}" \
    --out "${ARTIFACT_DIR}/surfaces/degraded-report" \
    --timeout 30s >"${ARTIFACT_DIR}/surfaces/degraded-report.stdout.txt" 2>"${ARTIFACT_DIR}/surfaces/degraded-report.stderr.txt"

python3 - "${ARTIFACT_DIR}" "${VOLUME_ID_1}" "${VOLUME_ID_2}" <<'PY'
import json, sys
from pathlib import Path
base=Path(sys.argv[1])
v1, v2=sys.argv[2:4]
summary=(base/"surfaces/degraded-report/summary.txt").read_text()
snapshot=json.load(open(base/"surfaces/degraded-report/operator-snapshot.json"))
if f"managed_volume={v1} status=blocked reason=nvme_multipath_path_missing" not in summary:
    raise SystemExit("volume 1 degraded summary missing")
if f"managed_volume={v2} status=ready reason=first_volume_verified" not in summary:
    raise SystemExit("volume 2 healthy summary missing")
if f"managed_volume_nvme={v1}" not in summary or "path_count=1" not in summary:
    raise SystemExit("volume 1 path_count=1 missing")
if f"managed_volume_nvme={v2}" not in summary or "path_count=2" not in summary:
    raise SystemExit("volume 2 path_count=2 missing")
vols={v["status"]["volume_id"]: v["status"] for v in snapshot.get("volumes") or []}
if set(vols) != {v1, v2}:
    raise SystemExit(f"snapshot IDs mismatch {set(vols)}")
if vols[v1].get("status") != "blocked" or vols[v1].get("reason_code") != "nvme_multipath_path_missing":
    raise SystemExit(f"volume 1 snapshot mismatch {vols[v1]}")
if vols[v2].get("status") != "ready" or vols[v2].get("reason_code") != "first_volume_verified":
    raise SystemExit(f"volume 2 snapshot mismatch {vols[v2]}")
if (vols[v1].get("nvme") or {}).get("path_count") != 1:
    raise SystemExit("volume 1 snapshot path count mismatch")
if (vols[v2].get("nvme") or {}).get("path_count") != 2:
    raise SystemExit("volume 2 snapshot path count mismatch")
if any(c.get("type") == "Ready" and c.get("status") == "True" for c in vols[v1].get("conditions") or []):
    raise SystemExit("volume 1 false Ready=True while degraded")
(base/"degraded-asserts.txt").write_text("\n".join([
    "degraded_volume_id="+v1,
    "untouched_volume_id="+v2,
    "degraded_volume_path_count=1",
    "untouched_volume_path_count=2",
    "degraded_volume_reason=nvme_multipath_path_missing",
    "untouched_volume_reason=first_volume_verified",
    "cross_volume_reason_mixup=false",
    "degraded_surface_ready_true_count=0",
])+"\n")
PY
cat "${ARTIFACT_DIR}/degraded-asserts.txt" >>"${SUMMARY}"

echo "[phase114] restore volume 1 path"
kubectl -n "${APP_NAMESPACE}" scale "deploy/${TARGET_DEPLOY}" --replicas=1 >"${ARTIFACT_DIR}/inject/restore-target.txt"
kubectl -n "${APP_NAMESPACE}" rollout status "deploy/${TARGET_DEPLOY}" --timeout=240s >"${ARTIFACT_DIR}/inject/rollout-target-restore.txt" 2>&1
wait_for_volume_states "restored" \
  "sw-block-multi-pvc-1:ready:first_volume_verified:2,sw-block-multi-pvc-2:ready:first_volume_verified:2"
assert_host_nvme_paths "after_restore" 1 "${NQN_1}" 2
assert_host_nvme_paths "after_restore" 2 "${NQN_2}" 2

for idx in $(seq 1 "${VOLUME_COUNT}"); do
  pod="sw-block-phase114-mounted-${idx}"
  uid_before="$(cat "${ARTIFACT_DIR}/mounted/pod-${idx}.uid.before")"
  uid_after="$(kubectl -n "${APP_NAMESPACE}" get pod "${pod}" -o jsonpath='{.metadata.uid}')"
  if [[ "${uid_after}" != "${uid_before}" ]]; then
    echo "mounted pod ${idx} UID changed after restore: before=${uid_before} after=${uid_after}" >&2
    exit 1
  fi
  run_mounted_exec_with_retry "${pod}" "${idx}" "after-restore" "${ARTIFACT_DIR}/mounted/after-restore-${idx}.log"
  write_summary "mounted_${idx}_pod_uid_after_restore=${uid_after}"
  write_summary "mounted_${idx}_io_after_restore=ok"
done
write_summary "mounted_pods_preserved_after_restore=true"
write_summary "mounted_io_after_restore_count=2"

with_master_port_forward "${ARTIFACT_DIR}/surfaces/restored-blockmaster-port-forward.log" \
  "${ARTIFACT_DIR}/bin/sw-block" ops report \
    --master-api "127.0.0.1:${STATUS_PORT}" \
    --namespace "${APP_NAMESPACE}" \
    --out "${ARTIFACT_DIR}/surfaces/restored-report" \
    --timeout 30s >"${ARTIFACT_DIR}/surfaces/restored-report.stdout.txt" 2>"${ARTIFACT_DIR}/surfaces/restored-report.stderr.txt"

python3 - "${ARTIFACT_DIR}" "${VOLUME_ID_1}" "${VOLUME_ID_2}" <<'PY'
import json, sys
from pathlib import Path
base=Path(sys.argv[1])
ids=sys.argv[2:4]
summary=(base/"surfaces/restored-report/summary.txt").read_text()
snapshot=json.load(open(base/"surfaces/restored-report/operator-snapshot.json"))
vols={v["status"]["volume_id"]: v["status"] for v in snapshot.get("volumes") or []}
for volume_id in ids:
    if f"managed_volume={volume_id} status=ready reason=first_volume_verified" not in summary:
        raise SystemExit(f"restored summary missing ready {volume_id}")
    if f"managed_volume_nvme={volume_id}" not in summary or "path_count=2" not in summary:
        raise SystemExit(f"restored summary missing path_count=2 {volume_id}")
    st=vols.get(volume_id)
    if not st or st.get("status") != "ready" or st.get("reason_code") != "first_volume_verified":
        raise SystemExit(f"restored snapshot mismatch {volume_id}: {st}")
    if (st.get("nvme") or {}).get("path_count") != 2:
        raise SystemExit(f"restored snapshot path_count mismatch {volume_id}")
(base/"restored-asserts.txt").write_text("\n".join([
    "restored_volume_count=2",
    "restored_all_path_count=2",
    "restored_all_reason=first_volume_verified",
])+"\n")
PY
cat "${ARTIFACT_DIR}/restored-asserts.txt" >>"${SUMMARY}"

write_summary "phase114_nvme_k8s_multivolume_mounted_path_isolation_status=ok"

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

echo "[phase114] PASS"
