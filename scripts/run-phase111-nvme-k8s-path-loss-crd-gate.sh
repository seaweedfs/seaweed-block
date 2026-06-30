#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase111-nvme-k8s-path-loss-crd-gate}"
SUMMARY="${ARTIFACT_DIR}/phase111-nvme-k8s-path-loss-crd-summary.txt"
HELM_RELEASE="${SW_BLOCK_HELM_RELEASE:-sw-block}"
HELM_NAMESPACE="${SW_BLOCK_HELM_NAMESPACE:-kube-system}"
APP_NAMESPACE="${SW_BLOCK_APP_NAMESPACE:-default}"
IMAGE="${SW_BLOCK_IMAGE:-sw-block:local}"
CSI_IMAGE="${SW_BLOCK_CSI_IMAGE:-sw-block-csi:local}"
STATUS_PORT="${SW_BLOCK_MASTER_PORT_FORWARD_PORT:-29333}"

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

cleanup() {
  set +e
  helm status "${HELM_RELEASE}" --namespace "${HELM_NAMESPACE}" >/dev/null 2>&1 && \
    helm uninstall "${HELM_RELEASE}" --namespace "${HELM_NAMESPACE}" --wait --timeout 240s \
      >"${ARTIFACT_DIR}/cleanup/helm-uninstall.txt" 2>&1
  kubectl -n "${APP_NAMESPACE}" delete pod -l sw-block-test=multi-volume --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  kubectl -n "${APP_NAMESPACE}" delete pod sw-block-multi-reader-1 sw-block-multi-writer-1 --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  kubectl -n "${APP_NAMESPACE}" delete pvc sw-block-multi-pvc-1 --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  kubectl delete storageclass sw-block-multi --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
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
  kubectl get pv --no-headers 2>/dev/null | awk '/sw-block-multi/ {print $1}' | xargs -r kubectl delete pv --wait=false >/dev/null 2>&1
  sudo -n nvme disconnect-all >/dev/null 2>&1 || true
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

write_summary "phase111_nvme_k8s_path_loss_crd_status=running"
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

python3 - "${ARTIFACT_DIR}" <<'PY'
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
(base/"phase111-nvme-k8s-path-loss-crd-asserts.txt").write_text("\n".join([
    "phase111_nvme_k8s_path_loss_crd_status=ok",
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
cat "${ARTIFACT_DIR}/phase111-nvme-k8s-path-loss-crd-asserts.txt" >>"${SUMMARY}"

cleanup
(
  cd "${ROOT}"
  SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/cleanup/verify" bash scripts/verify-helm-cleanup.sh "${ROOT}"
)
grep -q '^cleanup_status=ok$' "${ARTIFACT_DIR}/cleanup/verify/cleanup-summary.txt"
write_summary "cleanup_status=ok"

echo "[phase111] PASS"
