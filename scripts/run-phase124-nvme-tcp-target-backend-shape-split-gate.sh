#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase124-nvme-tcp-target-backend-shape-split-gate}"
SUMMARY="${ARTIFACT_DIR}/phase124-nvme-tcp-target-backend-shape-split-summary.txt"
PHASE123_DIR="${ARTIFACT_DIR}/phase123-inner"
LOCAL_DIR="${ARTIFACT_DIR}/local-path"
DIAG_DIR="${ARTIFACT_DIR}/diagnostics"

APP_NAMESPACE="${SW_BLOCK_APP_NAMESPACE:-default}"
LOCAL_PVC="${SW_BLOCK_PHASE124_LOCAL_PVC_NAME:-sw-block-phase124-local-pvc}"
LOCAL_POD="${SW_BLOCK_PHASE124_LOCAL_POD:-sw-block-phase124-local-perf}"
LOCAL_PVC_SIZE="${SW_BLOCK_PHASE124_LOCAL_PVC_SIZE:-512Mi}"
SEQ_MIB="${SW_BLOCK_PHASE124_SEQ_MIB:-64}"
SMALL_OPS="${SW_BLOCK_PHASE124_SMALL_OPS:-256}"
SMALL_BLOCK_BYTES="${SW_BLOCK_PHASE124_SMALL_BLOCK_BYTES:-4096}"

FRONTEND_IP_MAP="${SW_BLOCK_PHASE124_FRONTEND_IP_MAP:-m01=10.0.0.1,m02=10.0.0.3}"
FRONTEND_NETWORK_CLASS="${SW_BLOCK_PHASE124_FRONTEND_NETWORK_CLASS:-100gbe_tcp}"
EXPECTED_ROUTE_DEV="${SW_BLOCK_PHASE124_EXPECTED_ROUTE_DEV:-enp1s0np0}"
IMPORT_K3S_NODES="${SW_BLOCK_IMPORT_K3S_NODES:-192.168.1.181,192.168.1.184}"

mkdir -p "${ARTIFACT_DIR}" "${LOCAL_DIR}" "${DIAG_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

summary_value() {
  local file="$1"
  local key="$2"
  awk -F= -v key="$key" '$1 == key {value = substr($0, length(key) + 2)} END {if (value != "") print value}' "$file"
}

require_summary_value() {
  local file="$1"
  local key="$2"
  local value
  value="$(summary_value "$file" "$key")"
  if [[ -z "${value}" ]]; then
    echo "missing summary key ${key} in ${file}" >&2
    exit 1
  fi
  printf '%s' "${value}"
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

ratio() {
  python3 - "$1" "$2" <<'PY'
import sys
try:
    a = float(sys.argv[1])
    b = float(sys.argv[2])
except ValueError:
    print("unknown")
    sys.exit(0)
if b <= 0:
    print("unknown")
else:
    print(f"{a / b:.3f}")
PY
}

classify_split() {
  python3 - "$1" "$2" "$3" "$4" "$5" "$6" <<'PY'
import sys

def f(v):
    try:
        return float(v)
    except ValueError:
        return 0.0

network, local_w, local_r, block_w, block_r, fsync_penalty = map(f, sys.argv[1:])
if min(local_w, local_r, block_w, block_r) <= 0:
    print("unknown")
elif local_w >= block_w * 2 or local_r >= block_r * 2:
    print("block_target_or_backend")
elif fsync_penalty >= 2:
    print("test_shape")
elif network > 0 and local_w < network * 0.30 and local_r < network * 0.30:
    print("k8s_mount")
else:
    print("unknown")
PY
}

recommendation_for() {
  case "$1" in
    block_target_or_backend)
      printf '%s' "phase125_blockvolume_target_cpu_profile"
      ;;
    test_shape)
      printf '%s' "phase125_test_shape_correction"
      ;;
    k8s_mount)
      printf '%s' "phase125_kubernetes_mount_host_profile"
      ;;
    *)
      printf '%s' "phase125_backend_durable_write_profile"
      ;;
  esac
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

resolve_local_storageclass() {
  local sc_json="${LOCAL_DIR}/storageclasses.json"
  kubectl get storageclass -o json >"${sc_json}" 2>"${LOCAL_DIR}/storageclasses.stderr.txt"
  python3 - "${sc_json}" <<'PY'
import json
import sys

doc = json.load(open(sys.argv[1], encoding="utf-8"))
items = doc.get("items") or []
for item in items:
    anns = item.get("metadata", {}).get("annotations", {}) or {}
    if anns.get("storageclass.kubernetes.io/is-default-class") == "true":
        print(item["metadata"]["name"])
        raise SystemExit(0)
for preferred in ("local-path", "standard", "hostpath"):
    for item in items:
        if item.get("metadata", {}).get("name") == preferred:
            print(preferred)
            raise SystemExit(0)
if items:
    print(items[0]["metadata"]["name"])
    raise SystemExit(0)
raise SystemExit("no StorageClass available for local-path comparator")
PY
}

first_ready_node() {
  kubectl get nodes -o json >"${LOCAL_DIR}/nodes.json" 2>"${LOCAL_DIR}/nodes.stderr.txt"
  python3 - "${LOCAL_DIR}/nodes.json" <<'PY'
import json
import sys

doc = json.load(open(sys.argv[1], encoding="utf-8"))
for item in doc.get("items") or []:
    name = item.get("metadata", {}).get("name")
    spec = item.get("spec", {})
    if spec.get("unschedulable"):
        continue
    conditions = item.get("status", {}).get("conditions", []) or []
    ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions)
    if ready and name:
        print(name)
        raise SystemExit(0)
raise SystemExit("no schedulable Ready node found")
PY
}

cleanup_local_path() {
  set +e
  kubectl -n "${APP_NAMESPACE}" delete pod "${LOCAL_POD}" --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  kubectl -n "${APP_NAMESPACE}" delete pvc "${LOCAL_PVC}" --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  kubectl get pv -o json >"${LOCAL_DIR}/pv-cleanup.json" 2>/dev/null
  python3 - "${LOCAL_DIR}/pv-cleanup.json" "${APP_NAMESPACE}" "${LOCAL_PVC}" 2>/dev/null <<'PY' | \
    xargs -r -n1 kubectl patch pv --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null 2>&1
import json
import sys

path = sys.argv[1]
namespace = sys.argv[2]
pvc = sys.argv[3]
try:
    doc = json.load(open(path, encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    raise SystemExit(0)
for item in doc.get("items") or []:
    claim = item.get("spec", {}).get("claimRef") or {}
    if claim.get("namespace") == namespace and claim.get("name") == pvc:
        print(item.get("metadata", {}).get("name", ""))
PY
  kubectl get pv -o json >"${LOCAL_DIR}/pv-cleanup.json" 2>/dev/null
  python3 - "${LOCAL_DIR}/pv-cleanup.json" "${APP_NAMESPACE}" "${LOCAL_PVC}" 2>/dev/null <<'PY' | \
    xargs -r kubectl delete pv --wait=false >/dev/null 2>&1
import json
import sys

path = sys.argv[1]
namespace = sys.argv[2]
pvc = sys.argv[3]
try:
    doc = json.load(open(path, encoding="utf-8"))
except (OSError, json.JSONDecodeError):
    raise SystemExit(0)
for item in doc.get("items") or []:
    claim = item.get("spec", {}).get("claimRef") or {}
    if claim.get("namespace") == namespace and claim.get("name") == pvc:
        print(item.get("metadata", {}).get("name", ""))
PY
  set -e
}

cleanup() {
  cleanup_local_path || true
}
trap cleanup EXIT

measure_local_path() {
  local storage_class="$1"
  local app_node="$2"

  cat >"${LOCAL_DIR}/local-path-pvc.yaml" <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${LOCAL_PVC}
  namespace: ${APP_NAMESPACE}
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: ${storage_class}
  resources:
    requests:
      storage: ${LOCAL_PVC_SIZE}
YAML

  cat >"${LOCAL_DIR}/local-path-pod.yaml" <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${LOCAL_POD}
  namespace: ${APP_NAMESPACE}
  labels:
    sw-block-test: phase124-local-path
spec:
  restartPolicy: Never
  nodeSelector:
    kubernetes.io/hostname: "${app_node}"
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
        claimName: ${LOCAL_PVC}
YAML

  kubectl apply -f "${LOCAL_DIR}/local-path-pvc.yaml" >"${LOCAL_DIR}/apply-pvc.log" 2>&1
  kubectl apply -f "${LOCAL_DIR}/local-path-pod.yaml" >"${LOCAL_DIR}/apply-pod.log" 2>&1
  kubectl -n "${APP_NAMESPACE}" wait --for=condition=Ready "pod/${LOCAL_POD}" --timeout=240s \
    >"${LOCAL_DIR}/wait-pod-ready.log" 2>&1
  kubectl -n "${APP_NAMESPACE}" get pvc "${LOCAL_PVC}" -o wide >"${LOCAL_DIR}/pvc.after-ready.txt" 2>&1
  kubectl -n "${APP_NAMESPACE}" get pod "${LOCAL_POD}" -o wide >"${LOCAL_DIR}/pod.after-ready.txt" 2>&1

  local marker_ms seq_bytes seq_write_ms seq_read_ms seq_nofsync_ms small_bytes small_write_ms
  local seq_write_mibps seq_read_mibps seq_nofsync_mibps small_iops small_mibps

  marker_ms="$(measure_exec_ms "${LOCAL_DIR}/marker.log" \
    kubectl -n "${APP_NAMESPACE}" exec "${LOCAL_POD}" -- sh -c \
    'set -eu; dd if=/dev/urandom of=/data/phase124-marker.bin bs=4096 count=1; sha256sum /data/phase124-marker.bin > /data/phase124-marker.sha256; sync; sha256sum -c /data/phase124-marker.sha256')"
  write_summary "local_path_marker_verify_ms=${marker_ms}"
  write_summary "local_path_marker_verified=true"

  seq_bytes=$((SEQ_MIB * 1024 * 1024))
  seq_write_ms="$(measure_exec_ms "${LOCAL_DIR}/seq-write.log" \
    kubectl -n "${APP_NAMESPACE}" exec "${LOCAL_POD}" -- sh -c \
    "set -eu; dd if=/dev/zero of=/data/phase124-seq.bin bs=1M count=${SEQ_MIB} conv=fsync; sync")"
  seq_read_ms="$(measure_exec_ms "${LOCAL_DIR}/seq-read.log" \
    kubectl -n "${APP_NAMESPACE}" exec "${LOCAL_POD}" -- sh -c \
    "set -eu; dd if=/data/phase124-seq.bin of=/dev/null bs=1M; test \$(wc -c < /data/phase124-seq.bin) -eq ${seq_bytes}")"
  seq_nofsync_ms="$(measure_exec_ms "${LOCAL_DIR}/seq-write-nofsync.log" \
    kubectl -n "${APP_NAMESPACE}" exec "${LOCAL_POD}" -- sh -c \
    "set -eu; rm -f /data/phase124-nofsync.bin; dd if=/dev/zero of=/data/phase124-nofsync.bin bs=1M count=${SEQ_MIB}")"

  seq_write_mibps="$(rate_mibps "${seq_bytes}" "${seq_write_ms}")"
  seq_read_mibps="$(rate_mibps "${seq_bytes}" "${seq_read_ms}")"
  seq_nofsync_mibps="$(rate_mibps "${seq_bytes}" "${seq_nofsync_ms}")"
  write_summary "local_path_seq_size_mib=${SEQ_MIB}"
  write_summary "local_path_seq_write_duration_ms=${seq_write_ms}"
  write_summary "local_path_seq_write_mibps=${seq_write_mibps}"
  write_summary "local_path_seq_read_duration_ms=${seq_read_ms}"
  write_summary "local_path_seq_read_mibps=${seq_read_mibps}"
  write_summary "local_path_seq_write_nofsync_duration_ms=${seq_nofsync_ms}"
  write_summary "local_path_seq_write_nofsync_mibps=${seq_nofsync_mibps}"

  small_bytes=$((SMALL_OPS * SMALL_BLOCK_BYTES))
  small_write_ms="$(measure_exec_ms "${LOCAL_DIR}/small-write.log" \
    kubectl -n "${APP_NAMESPACE}" exec "${LOCAL_POD}" -- sh -c \
    "set -eu; rm -f /data/phase124-small.bin; i=0; while [ \$i -lt ${SMALL_OPS} ]; do seek=\$(( (i * 17) % 1024 )); dd if=/dev/zero of=/data/phase124-small.bin bs=${SMALL_BLOCK_BYTES} count=1 seek=\$seek conv=notrunc 2>/dev/null; i=\$((i+1)); done; sync")"
  small_iops="$(rate_iops "${SMALL_OPS}" "${small_write_ms}")"
  small_mibps="$(rate_mibps "${small_bytes}" "${small_write_ms}")"
  write_summary "local_path_small_write_ops=${SMALL_OPS}"
  write_summary "local_path_small_write_block_bytes=${SMALL_BLOCK_BYTES}"
  write_summary "local_path_small_write_duration_ms=${small_write_ms}"
  write_summary "local_path_small_write_iops=${small_iops}"
  write_summary "local_path_small_write_mibps=${small_mibps}"

  kubectl -n "${APP_NAMESPACE}" exec "${LOCAL_POD}" -- sh -c \
    'set -eu; sha256sum -c /data/phase124-marker.sha256; test -s /data/phase124-seq.bin; test -s /data/phase124-small.bin' \
    >"${LOCAL_DIR}/final-verify.log" 2>&1
  write_summary "local_path_final_data_verified=true"
}

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

write_summary "phase124_nvme_tcp_target_backend_shape_split_status=running"
write_summary "frontend_transport=tcp"
write_summary "frontend_ip_map=${FRONTEND_IP_MAP}"
write_summary "frontend_network_class=${FRONTEND_NETWORK_CLASS}"
write_summary "nvme_rdma_supported=false"
write_summary "roce_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "test_shape=dd_exec_baseline"

SW_BLOCK_ARTIFACT_DIR="${PHASE123_DIR}" \
SW_BLOCK_PHASE123_FRONTEND_IP_MAP="${FRONTEND_IP_MAP}" \
SW_BLOCK_PHASE123_FRONTEND_NETWORK_CLASS="${FRONTEND_NETWORK_CLASS}" \
SW_BLOCK_PHASE123_EXPECTED_ROUTE_DEV="${EXPECTED_ROUTE_DEV}" \
SW_BLOCK_IMPORT_K3S_NODES="${IMPORT_K3S_NODES}" \
SW_BLOCK_PHASE123_PVC_NAME="${SW_BLOCK_PHASE124_BLOCK_PVC_NAME:-sw-block-phase124-block-pvc}" \
SW_BLOCK_PHASE123_STORAGECLASS="${SW_BLOCK_PHASE124_BLOCK_STORAGECLASS:-sw-block-phase124-block}" \
SW_BLOCK_PHASE123_POD="${SW_BLOCK_PHASE124_BLOCK_POD:-sw-block-phase124-block-perf}" \
  bash "${ROOT}/scripts/run-phase123-nvme-tcp-bottleneck-triage-gate.sh" "${ROOT}" \
  >"${DIAG_DIR}/phase123.stdout.txt" \
  2>"${DIAG_DIR}/phase123.stderr.txt"

PHASE123_SUMMARY="${PHASE123_DIR}/phase123-nvme-tcp-bottleneck-triage-summary.txt"
PHASE120_SUMMARY="${PHASE123_DIR}/phase122-inner/phase120-inner/phase120-nvme-tcp-performance-baseline-summary.txt"

if [[ "$(require_summary_value "${PHASE123_SUMMARY}" phase123_nvme_tcp_bottleneck_triage_status)" != "ok" ]]; then
  echo "phase123 inner gate did not finish ok" >&2
  exit 1
fi

NETWORK_MIBPS="$(require_summary_value "${PHASE123_SUMMARY}" network_baseline_mibps)"
BLOCK_WRITE="$(require_summary_value "${PHASE123_SUMMARY}" k8s_mounted_seq_write_mibps)"
BLOCK_READ="$(require_summary_value "${PHASE123_SUMMARY}" k8s_mounted_seq_read_mibps)"
BLOCK_IOPS="$(require_summary_value "${PHASE123_SUMMARY}" k8s_mounted_small_write_iops)"
APP_NODE="$(summary_value "${PHASE120_SUMMARY}" app_node)"
if [[ -z "${APP_NODE}" ]]; then
  APP_NODE="$(first_ready_node)"
fi
LOCAL_SC="$(resolve_local_storageclass)"

write_summary "network_baseline_mibps=${NETWORK_MIBPS}"
write_summary "publish_target=$(require_summary_value "${PHASE123_SUMMARY}" publish_target)"
write_summary "route_dev=$(require_summary_value "${PHASE123_SUMMARY}" route_dev)"
write_summary "publish_target_network_class=$(require_summary_value "${PHASE123_SUMMARY}" publish_target_network_class)"
write_summary "management_ip=$(require_summary_value "${PHASE123_SUMMARY}" management_ip)"
write_summary "frontend_ip=$(require_summary_value "${PHASE123_SUMMARY}" frontend_ip)"
write_summary "block_app_node=${APP_NODE}"
write_summary "local_path_storageclass=${LOCAL_SC}"
write_summary "block_nvme_seq_write_mibps=${BLOCK_WRITE}"
write_summary "block_nvme_seq_read_mibps=${BLOCK_READ}"
write_summary "block_nvme_small_write_iops=${BLOCK_IOPS}"

cleanup_local_path
measure_local_path "${LOCAL_SC}" "${APP_NODE}"

LOCAL_WRITE="$(require_summary_value "${SUMMARY}" local_path_seq_write_mibps)"
LOCAL_READ="$(require_summary_value "${SUMMARY}" local_path_seq_read_mibps)"
LOCAL_NOFSYNC="$(require_summary_value "${SUMMARY}" local_path_seq_write_nofsync_mibps)"
FSYNC_PENALTY="$(ratio "${LOCAL_NOFSYNC}" "${LOCAL_WRITE}")"
READ_RATIO="$(ratio "${BLOCK_READ}" "${LOCAL_READ}")"
WRITE_RATIO="$(ratio "${BLOCK_WRITE}" "${LOCAL_WRITE}")"
LOCAL_NET_READ_RATIO="$(ratio "${LOCAL_READ}" "${NETWORK_MIBPS}")"
LOCAL_NET_WRITE_RATIO="$(ratio "${LOCAL_WRITE}" "${NETWORK_MIBPS}")"
TOP_BOTTLENECK="$(classify_split "${NETWORK_MIBPS}" "${LOCAL_WRITE}" "${LOCAL_READ}" "${BLOCK_WRITE}" "${BLOCK_READ}" "${FSYNC_PENALTY}")"
NEXT_RECOMMENDATION="$(recommendation_for "${TOP_BOTTLENECK}")"

write_summary "block_vs_local_read_ratio=${READ_RATIO}"
write_summary "block_vs_local_write_ratio=${WRITE_RATIO}"
write_summary "local_vs_network_read_ratio=${LOCAL_NET_READ_RATIO}"
write_summary "local_vs_network_write_ratio=${LOCAL_NET_WRITE_RATIO}"
write_summary "shape_fsync_penalty=${FSYNC_PENALTY}"
write_summary "top_bottleneck=${TOP_BOTTLENECK}"
write_summary "next_recommendation=${NEXT_RECOMMENDATION}"

cleanup_local_path
SW_BLOCK_CLEANUP_WAIT_SECONDS="${SW_BLOCK_CLEANUP_WAIT_SECONDS:-180}" \
SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/cleanup/verify" \
  bash "${ROOT}/scripts/verify-helm-cleanup.sh" "${ROOT}" \
  >"${ARTIFACT_DIR}/cleanup-verify.stdout.txt" \
  2>"${ARTIFACT_DIR}/cleanup-verify.stderr.txt"
grep -q '^cleanup_status=ok$' "${ARTIFACT_DIR}/cleanup/verify/cleanup-summary.txt"
write_summary "cleanup_status=ok"
write_summary "phase124_nvme_tcp_target_backend_shape_split_status=ok"
