#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase115-nvme-k8s-multivolume-mounted-path-churn-soak-gate}"
SUMMARY="${ARTIFACT_DIR}/phase115-nvme-k8s-multivolume-mounted-path-churn-soak-summary.txt"
HELM_RELEASE="${SW_BLOCK_HELM_RELEASE:-sw-block}"
HELM_NAMESPACE="${SW_BLOCK_HELM_NAMESPACE:-kube-system}"
APP_NAMESPACE="${SW_BLOCK_APP_NAMESPACE:-default}"
IMAGE="${SW_BLOCK_IMAGE:-sw-block:local}"
CSI_IMAGE="${SW_BLOCK_CSI_IMAGE:-sw-block-csi:local}"
STATUS_PORT="${SW_BLOCK_MASTER_PORT_FORWARD_PORT:-29334}"
VOLUME_COUNT=2
CYCLE_PLAN=(1 2 1)

mkdir -p "${ARTIFACT_DIR}"/{bin,build,values,install,multi-volume,mounted,cycles,surfaces,cleanup}
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
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
    delete_pod "sw-block-phase115-mounted-${idx}"
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
  kubectl -n "${APP_NAMESPACE}" get deploy,pod -l sw-block.seaweedfs.com/volume -o wide >"${dir}/blockvolume.get.txt" 2>&1 || true
  kubectl -n "${HELM_NAMESPACE}" get pod -o wide >"${dir}/kube-system-pods.txt" 2>&1 || true
  kubectl -n "${HELM_NAMESPACE}" logs ds/sw-block-csi-node --all-containers --tail=500 >"${dir}/csi-node.log" 2>&1 || true
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
  local count=0
  local attempt=0
  local end=$((SECONDS + deadline))
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
    if kubectl -n "${APP_NAMESPACE}" exec "${pod}" -- sh -c "set -eu; echo ${marker}-${idx} >> /data/phase115-mounted-${idx}.txt; sync; grep before-churn-${idx} /data/phase115-mounted-${idx}.txt; grep ${marker}-${idx} /data/phase115-mounted-${idx}.txt" >"${tmp_out}" 2>"${tmp_err}" &&
      grep -q "^before-churn-${idx}$" "${tmp_out}" &&
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
  rm -f "${tmp_out}" "${tmp_err}"
  return 1
}

volume_var() {
  local prefix="$1"
  local idx="$2"
  local name="${prefix}_${idx}"
  printf '%s' "${!name}"
}

expected_for_loss() {
  local affected="$1"
  local other
  if [[ "${affected}" == "1" ]]; then
    other=2
  else
    other=1
  fi
  printf 'sw-block-multi-pvc-%s:blocked:nvme_multipath_path_missing:1,sw-block-multi-pvc-%s:ready:first_volume_verified:2' "${affected}" "${other}"
}

assert_surface_isolation() {
  local phase="$1"
  local affected="$2"
  local other
  if [[ "${affected}" == "1" ]]; then
    other=2
  else
    other=1
  fi
  local affected_id other_id
  affected_id="$(volume_var VOLUME_ID "${affected}")"
  other_id="$(volume_var VOLUME_ID "${other}")"
  with_master_port_forward "${ARTIFACT_DIR}/surfaces/${phase}-blockmaster-port-forward.log" \
    "${ARTIFACT_DIR}/bin/sw-block" ops report \
      --master-api "127.0.0.1:${STATUS_PORT}" \
      --namespace "${APP_NAMESPACE}" \
      --out "${ARTIFACT_DIR}/surfaces/${phase}-report" \
      --timeout 30s >"${ARTIFACT_DIR}/surfaces/${phase}-report.stdout.txt" 2>"${ARTIFACT_DIR}/surfaces/${phase}-report.stderr.txt"

  python3 - "${ARTIFACT_DIR}/surfaces/${phase}-report" "${affected_id}" "${other_id}" <<'PY'
import json, sys
from pathlib import Path
base=Path(sys.argv[1])
affected, other=sys.argv[2:4]
summary=(base/"summary.txt").read_text()
snapshot=json.load(open(base/"operator-snapshot.json"))
vols={v["status"]["volume_id"]: v["status"] for v in snapshot.get("volumes") or []}
if set(vols) != {affected, other}:
    raise SystemExit(f"volume identity mismatch {set(vols)}")
a=vols[affected]
o=vols[other]
if a.get("status") != "blocked" or a.get("reason_code") != "nvme_multipath_path_missing":
    raise SystemExit(f"affected mismatch {a}")
if o.get("status") != "ready" or o.get("reason_code") != "first_volume_verified":
    raise SystemExit(f"other mismatch {o}")
if (a.get("nvme") or {}).get("path_count") != 1:
    raise SystemExit("affected path_count mismatch")
if (o.get("nvme") or {}).get("path_count") != 2:
    raise SystemExit("other path_count mismatch")
if any(c.get("type") == "Ready" and c.get("status") == "True" for c in a.get("conditions") or []):
    raise SystemExit("affected false Ready=True")
ap=(a.get("nvme") or {}).get("publish_target") or (a.get("nvme") or {}).get("nvme_addr")
op=(o.get("nvme") or {}).get("publish_target") or (o.get("nvme") or {}).get("nvme_addr")
an=(a.get("nvme") or {}).get("nqn")
on=(o.get("nvme") or {}).get("nqn")
if ap and op and ap == op:
    raise SystemExit("publish target collision")
if an and on and an == on:
    raise SystemExit("NQN collision")
if f"managed_volume={affected} status=blocked reason=nvme_multipath_path_missing" not in summary:
    raise SystemExit("affected summary missing")
if f"managed_volume={other} status=ready reason=first_volume_verified" not in summary:
    raise SystemExit("other summary missing")
PY
}

run_cycle() {
  local cycle="$1"
  local affected="$2"
  local other
  if [[ "${affected}" == "1" ]]; then
    other=2
  else
    other=1
  fi
  local affected_id affected_nqn other_nqn target_deploy
  affected_id="$(volume_var VOLUME_ID "${affected}")"
  affected_nqn="$(volume_var NQN "${affected}")"
  other_nqn="$(volume_var NQN "${other}")"
  local dir="${ARTIFACT_DIR}/cycles/cycle-${cycle}-volume-${affected}"
  mkdir -p "${dir}"

  kubectl -n "${APP_NAMESPACE}" get deploy -l app=sw-blockvolume -o json >"${dir}/blockvolume-deployments.before.json"
  target_deploy="$(python3 - "${dir}/blockvolume-deployments.before.json" "${affected_id}" <<'PY'
import json, sys
doc=json.load(open(sys.argv[1]))
volume_id=sys.argv[2]
matches=[i["metadata"]["name"] for i in doc.get("items", []) if volume_id in i["metadata"]["name"]]
if not matches:
    raise SystemExit(f"no deployment for {volume_id}")
print(sorted(matches)[0])
PY
)"
  write_summary "cycle_${cycle}_affected_volume=${affected}"
  write_summary "cycle_${cycle}_target_deployment=${target_deploy}"

  kubectl -n "${APP_NAMESPACE}" scale "deploy/${target_deploy}" --replicas=0 >"${dir}/scale-target-zero.txt"
  kubectl -n "${APP_NAMESPACE}" rollout status "deploy/${target_deploy}" --timeout=120s >"${dir}/rollout-target-zero.txt" 2>&1 || true
  for _ in $(seq 1 60); do
    pods="$(kubectl -n "${APP_NAMESPACE}" get pod -l app=sw-blockvolume -o jsonpath='{range .items[*]}{.metadata.ownerReferences[0].name}{"\n"}{end}' 2>/dev/null | grep -c "^${target_deploy}$" || true)"
    [[ "${pods}" == "0" ]] && break
    sleep 1
  done

  wait_for_volume_states "cycle-${cycle}-loss" "$(expected_for_loss "${affected}")"
  assert_host_nvme_paths "cycle-${cycle}-loss-v${affected}" "${affected}" "${affected_nqn}" 1
  assert_host_nvme_paths "cycle-${cycle}-loss-v${other}" "${other}" "${other_nqn}" 2
  assert_surface_isolation "cycle-${cycle}-loss" "${affected}"

  for idx in $(seq 1 "${VOLUME_COUNT}"); do
    local pod uid_before uid_after
    pod="sw-block-phase115-mounted-${idx}"
    uid_before="$(cat "${ARTIFACT_DIR}/mounted/pod-${idx}.uid.before")"
    uid_after="$(kubectl -n "${APP_NAMESPACE}" get pod "${pod}" -o jsonpath='{.metadata.uid}')"
    if [[ "${uid_after}" != "${uid_before}" ]]; then
      echo "mounted pod ${idx} UID changed after cycle ${cycle} loss: before=${uid_before} after=${uid_after}" >&2
      exit 1
    fi
    run_mounted_exec_with_retry "${pod}" "${idx}" "cycle-${cycle}-loss" "${dir}/mounted-loss-${idx}.log"
  done

  kubectl -n "${APP_NAMESPACE}" scale "deploy/${target_deploy}" --replicas=1 >"${dir}/scale-target-restore.txt"
  kubectl -n "${APP_NAMESPACE}" rollout status "deploy/${target_deploy}" --timeout=240s >"${dir}/rollout-target-restore.txt" 2>&1
  wait_for_volume_states "cycle-${cycle}-restore" \
    "sw-block-multi-pvc-1:ready:first_volume_verified:2,sw-block-multi-pvc-2:ready:first_volume_verified:2"
  assert_host_nvme_paths "cycle-${cycle}-restore-v1" 1 "${NQN_1}" 2
  assert_host_nvme_paths "cycle-${cycle}-restore-v2" 2 "${NQN_2}" 2

  for idx in $(seq 1 "${VOLUME_COUNT}"); do
    local pod uid_before uid_after
    pod="sw-block-phase115-mounted-${idx}"
    uid_before="$(cat "${ARTIFACT_DIR}/mounted/pod-${idx}.uid.before")"
    uid_after="$(kubectl -n "${APP_NAMESPACE}" get pod "${pod}" -o jsonpath='{.metadata.uid}')"
    if [[ "${uid_after}" != "${uid_before}" ]]; then
      echo "mounted pod ${idx} UID changed after cycle ${cycle} restore: before=${uid_before} after=${uid_after}" >&2
      exit 1
    fi
    run_mounted_exec_with_retry "${pod}" "${idx}" "cycle-${cycle}-restore" "${dir}/mounted-restore-${idx}.log"
  done
}

write_summary "phase115_nvme_k8s_multivolume_mounted_path_churn_soak_status=running"
cleanup

echo "[phase115] build sw-block CLI"
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

echo "[phase115] install Helm stack"
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
echo "[phase115] create two RF2 NVMe PVCs"
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
PY
# shellcheck disable=SC1090
source "${ARTIFACT_DIR}/surfaces/volume-map.env"
write_summary "volume_1_id=${VOLUME_ID_1}"
write_summary "volume_2_id=${VOLUME_ID_2}"

for idx in $(seq 1 "${VOLUME_COUNT}"); do
  pod="sw-block-phase115-mounted-${idx}"
  cat >"${ARTIFACT_DIR}/mounted/pod-${idx}.yaml" <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${pod}
  namespace: ${APP_NAMESPACE}
  labels:
    sw-block-test: nvme-multivolume-mounted-path-churn-soak
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
  kubectl -n "${APP_NAMESPACE}" exec "${pod}" -- sh -c "set -eu; echo before-churn-${idx} > /data/phase115-mounted-${idx}.txt; sync; grep before-churn-${idx} /data/phase115-mounted-${idx}.txt" >"${ARTIFACT_DIR}/mounted/before-${idx}.log"
  grep -q "^before-churn-${idx}$" "${ARTIFACT_DIR}/mounted/before-${idx}.log"
  write_summary "mounted_${idx}_pod_uid_before=${uid}"
done
assert_host_nvme_paths "pre_churn" 1 "${NQN_1}" 2
assert_host_nvme_paths "pre_churn" 2 "${NQN_2}" 2

cycle=0
for affected in "${CYCLE_PLAN[@]}"; do
  cycle=$((cycle + 1))
  echo "[phase115] cycle ${cycle}: churn volume ${affected}"
  run_cycle "${cycle}" "${affected}"
done

write_summary "cycle_count=${cycle}"
write_summary "mounted_pods_preserved=true"
write_summary "mounted_io_after_loss_count=$((cycle * VOLUME_COUNT))"
write_summary "mounted_io_after_restore_count=$((cycle * VOLUME_COUNT))"
write_summary "cross_volume_reason_mixup=false"
write_summary "cross_volume_publish_target_mixup=false"
write_summary "all_restored_path_count=2"

with_master_port_forward "${ARTIFACT_DIR}/surfaces/final-blockmaster-port-forward.log" \
  "${ARTIFACT_DIR}/bin/sw-block" ops report \
    --master-api "127.0.0.1:${STATUS_PORT}" \
    --namespace "${APP_NAMESPACE}" \
    --out "${ARTIFACT_DIR}/surfaces/final-report" \
    --timeout 30s >"${ARTIFACT_DIR}/surfaces/final-report.stdout.txt" 2>"${ARTIFACT_DIR}/surfaces/final-report.stderr.txt"

python3 - "${ARTIFACT_DIR}/surfaces/final-report" "${VOLUME_ID_1}" "${VOLUME_ID_2}" <<'PY'
import json, sys
from pathlib import Path
base=Path(sys.argv[1])
ids=set(sys.argv[2:4])
summary=(base/"summary.txt").read_text()
snapshot=json.load(open(base/"operator-snapshot.json"))
vols={v["status"]["volume_id"]: v["status"] for v in snapshot.get("volumes") or []}
if set(vols) != ids:
    raise SystemExit(f"final identity mismatch {set(vols)}")
publish_targets=set()
nqns=set()
for volume_id, st in vols.items():
    if st.get("status") != "ready" or st.get("reason_code") != "first_volume_verified":
        raise SystemExit(f"final status mismatch {volume_id}: {st}")
    nv=st.get("nvme") or {}
    if nv.get("path_count") != 2:
        raise SystemExit(f"final path count mismatch {volume_id}: {nv}")
    publish_target=nv.get("publish_target") or nv.get("nvme_addr")
    if publish_target:
        publish_targets.add(publish_target)
    if nv.get("nqn"):
        nqns.add(nv["nqn"])
    if f"managed_volume={volume_id} status=ready reason=first_volume_verified" not in summary:
        raise SystemExit(f"final summary missing {volume_id}")
if len(publish_targets) != len(ids) or len(nqns) != len(ids):
    raise SystemExit("final identity/publish-target collision")
PY

write_summary "phase115_nvme_k8s_multivolume_mounted_path_churn_soak_status=ok"

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

echo "[phase115] PASS"
