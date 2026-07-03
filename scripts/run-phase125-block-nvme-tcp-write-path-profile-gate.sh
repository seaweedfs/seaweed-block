#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase125-block-nvme-tcp-write-path-profile-gate}"
SUMMARY="${ARTIFACT_DIR}/phase125-block-nvme-tcp-write-path-profile-summary.txt"
BLOCK_DIR="${ARTIFACT_DIR}/block-profile"
LOCAL_DIR="${ARTIFACT_DIR}/local-path"
DIAG_DIR="${ARTIFACT_DIR}/diagnostics"

APP_NAMESPACE="${SW_BLOCK_APP_NAMESPACE:-default}"
SEQ_MIB="${SW_BLOCK_PHASE125_SEQ_MIB:-512}"
LOCAL_PVC="${SW_BLOCK_PHASE125_LOCAL_PVC_NAME:-sw-block-phase125-local-pvc}"
LOCAL_POD="${SW_BLOCK_PHASE125_LOCAL_POD:-sw-block-phase125-local-perf}"
LOCAL_PVC_SIZE="${SW_BLOCK_PHASE125_LOCAL_PVC_SIZE:-1Gi}"

FRONTEND_IP_MAP="${SW_BLOCK_PHASE125_FRONTEND_IP_MAP:-m01=10.0.0.1,m02=10.0.0.3}"
FRONTEND_NETWORK_CLASS="${SW_BLOCK_PHASE125_FRONTEND_NETWORK_CLASS:-100gbe_tcp}"
EXPECTED_ROUTE_DEV="${SW_BLOCK_PHASE125_EXPECTED_ROUTE_DEV:-enp1s0np0}"
IMPORT_K3S_NODES="${SW_BLOCK_IMPORT_K3S_NODES:-192.168.1.181,192.168.1.184}"
NETWORK_SERVER_NODE="${SW_BLOCK_PHASE125_NETWORK_SERVER_NODE:-m01}"
NETWORK_CLIENT_NODE="${SW_BLOCK_PHASE125_NETWORK_CLIENT_NODE:-m02}"
NETWORK_SERVER_HOST="${SW_BLOCK_PHASE125_NETWORK_SERVER_HOST:-192.168.1.181}"
NETWORK_SSH_USER="${SW_BLOCK_PHASE125_NETWORK_SSH_USER:-testdev}"
NETWORK_SSH_KEY="${SW_BLOCK_PHASE125_NETWORK_SSH_KEY:-${SW_BLOCK_IMPORT_K3S_SSH_KEY:-/opt/work/testdev_key}}"
IPERF_PORT="${SW_BLOCK_PHASE125_IPERF_PORT:-51225}"
IPERF_SECONDS="${SW_BLOCK_PHASE125_IPERF_SECONDS:-5}"

mkdir -p "${ARTIFACT_DIR}" "${BLOCK_DIR}" "${LOCAL_DIR}" "${DIAG_DIR}"
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

map_value() {
  local key="$1"
  local pair name value
  IFS=',' read -r -a pairs <<<"${FRONTEND_IP_MAP}"
  for pair in "${pairs[@]}"; do
    name="${pair%%=*}"
    value="${pair#*=}"
    if [[ "${name}" == "${key}" && "${value}" != "${pair}" ]]; then
      printf '%s\n' "${value}"
      return 0
    fi
  done
}

rate_mibps() {
  python3 - "$1" "$2" <<'PY'
import sys
bytes_count = int(sys.argv[1])
duration_ms = max(int(sys.argv[2]), 1)
print(f"{(bytes_count / 1048576.0) / (duration_ms / 1000.0):.2f}")
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

mibps_from_iperf_json() {
  python3 - "$1" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    doc = json.load(f)
end = doc.get("end", {})
bits = 0.0
for key in ("sum_received", "sum", "sum_sent"):
    item = end.get(key)
    if isinstance(item, dict):
        bits = float(item.get("bits_per_second") or 0)
        if bits > 0:
            break
print(f"{bits / 8 / 1048576:.2f}")
PY
}

run_network_baseline() {
  local server_ip="$1"
  local client_ip="$2"

  write_summary "network_baseline_server_node=${NETWORK_SERVER_NODE}"
  write_summary "network_baseline_client_node=${NETWORK_CLIENT_NODE}"
  write_summary "network_baseline_server_ip=${server_ip}"
  write_summary "network_baseline_client_ip=${client_ip}"
  write_summary "network_baseline_tool=iperf3"

  if ! command -v iperf3 >/dev/null 2>&1; then
    write_summary "network_baseline_status=unavailable"
    write_summary "network_baseline_reason=local_iperf3_missing"
    write_summary "network_baseline_mibps=unavailable"
    return 0
  fi
  if [[ ! -r "${NETWORK_SSH_KEY}" ]]; then
    write_summary "network_baseline_status=unavailable"
    write_summary "network_baseline_reason=ssh_key_missing"
    write_summary "network_baseline_mibps=unavailable"
    return 0
  fi
  if ! ssh -i "${NETWORK_SSH_KEY}" -o BatchMode=yes -o ConnectTimeout=5 "${NETWORK_SSH_USER}@${NETWORK_SERVER_HOST}" \
    "command -v iperf3 >/dev/null 2>&1" \
    >"${DIAG_DIR}/iperf3-remote-check.stdout.txt" \
    2>"${DIAG_DIR}/iperf3-remote-check.stderr.txt"; then
    write_summary "network_baseline_status=unavailable"
    write_summary "network_baseline_reason=remote_iperf3_or_ssh_unavailable"
    write_summary "network_baseline_mibps=unavailable"
    return 0
  fi

  ssh -i "${NETWORK_SSH_KEY}" -o BatchMode=yes -o ConnectTimeout=5 "${NETWORK_SSH_USER}@${NETWORK_SERVER_HOST}" \
    "pkill -x iperf3 >/dev/null 2>&1 || true; nohup iperf3 -s -1 -B ${server_ip} -p ${IPERF_PORT} >/tmp/phase125-iperf3-${IPERF_PORT}.log 2>&1 &" \
    >"${DIAG_DIR}/iperf3-server-start.stdout.txt" \
    2>"${DIAG_DIR}/iperf3-server-start.stderr.txt" || {
      write_summary "network_baseline_status=unavailable"
      write_summary "network_baseline_reason=remote_iperf3_start_failed"
      write_summary "network_baseline_mibps=unavailable"
      return 0
    }
  sleep 2

  if iperf3 -J -c "${server_ip}" -B "${client_ip}" -p "${IPERF_PORT}" -t "${IPERF_SECONDS}" \
    >"${DIAG_DIR}/iperf3-client.json" \
    2>"${DIAG_DIR}/iperf3-client.stderr.txt"; then
    write_summary "network_baseline_status=ok"
    write_summary "network_baseline_mibps=$(mibps_from_iperf_json "${DIAG_DIR}/iperf3-client.json")"
  else
    write_summary "network_baseline_status=unavailable"
    write_summary "network_baseline_reason=iperf3_client_failed"
    write_summary "network_baseline_mibps=unavailable"
  fi

  ssh -i "${NETWORK_SSH_KEY}" -o BatchMode=yes -o ConnectTimeout=5 "${NETWORK_SSH_USER}@${NETWORK_SERVER_HOST}" \
    "cat /tmp/phase125-iperf3-${IPERF_PORT}.log 2>/dev/null || true; pkill -x iperf3 >/dev/null 2>&1 || true" \
    >"${DIAG_DIR}/iperf3-server.log" \
    2>"${DIAG_DIR}/iperf3-server-fetch.stderr.txt" || true
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

cleanup_local_path() {
  set +e
  kubectl -n "${APP_NAMESPACE}" delete pod "${LOCAL_POD}" --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  kubectl -n "${APP_NAMESPACE}" delete pvc "${LOCAL_PVC}" --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  kubectl get pv --no-headers 2>/dev/null | awk -v pvc="${LOCAL_PVC}" '$0 ~ pvc {print $1}' | \
    xargs -r -n1 kubectl patch pv --type=merge -p '{"metadata":{"finalizers":[]}}' >/dev/null 2>&1
  kubectl get pv --no-headers 2>/dev/null | awk -v pvc="${LOCAL_PVC}" '$0 ~ pvc {print $1}' | \
    xargs -r kubectl delete pv --wait=false >/dev/null 2>&1
  set -e
}

cleanup() {
  cleanup_local_path || true
}
trap cleanup EXIT

measure_local_write() {
  local storage_class="$1"
  local app_node="$2"
  local seq_bytes seq_write_ms seq_read_ms seq_write_mibps seq_read_mibps

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
    sw-block-test: phase125-local-path
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
  kubectl -n "${APP_NAMESPACE}" get pod "${LOCAL_POD}" -o wide >"${LOCAL_DIR}/pod.after-ready.txt" 2>&1

  seq_bytes=$((SEQ_MIB * 1024 * 1024))
  seq_write_ms="$(measure_exec_ms "${LOCAL_DIR}/seq-write.log" \
    kubectl -n "${APP_NAMESPACE}" exec "${LOCAL_POD}" -- sh -c \
    "set -eu; dd if=/dev/zero of=/data/phase125-seq.bin bs=1M count=${SEQ_MIB} conv=fsync; sync")"
  seq_read_ms="$(measure_exec_ms "${LOCAL_DIR}/seq-read.log" \
    kubectl -n "${APP_NAMESPACE}" exec "${LOCAL_POD}" -- sh -c \
    "set -eu; dd if=/data/phase125-seq.bin of=/dev/null bs=1M; test \$(wc -c < /data/phase125-seq.bin) -eq ${seq_bytes}")"
  seq_write_mibps="$(rate_mibps "${seq_bytes}" "${seq_write_ms}")"
  seq_read_mibps="$(rate_mibps "${seq_bytes}" "${seq_read_ms}")"

  write_summary "local_path_seq_size_mib=${SEQ_MIB}"
  write_summary "local_path_seq_write_duration_ms=${seq_write_ms}"
  write_summary "local_path_seq_write_mibps=${seq_write_mibps}"
  write_summary "local_path_seq_read_duration_ms=${seq_read_ms}"
  write_summary "local_path_seq_read_mibps=${seq_read_mibps}"
}

parse_blockvolume_cpu_profile() {
  python3 - "$1" <<'PY'
import re
import sys

path = sys.argv[1]
values = []

def cpu_to_millicores(token):
    token = token.strip()
    if token.endswith("m"):
        return float(token[:-1])
    if token.endswith("n"):
        return float(token[:-1]) / 1_000_000
    if token.endswith("u"):
        return float(token[:-1]) / 1000
    return float(token) * 1000

try:
    lines = open(path, encoding="utf-8", errors="replace").read().splitlines()
except OSError:
    lines = []

for line in lines:
    parts = line.split()
    if len(parts) < 5:
        continue
    if not parts[1].startswith("sw-blockvolume-"):
        continue
    cpu = parts[3]
    if not re.match(r"^[0-9.]+(m|u|n)?$", cpu):
        continue
    try:
        values.append(cpu_to_millicores(cpu))
    except ValueError:
        pass

if not values:
    print("blockvolume_cpu_sample_count=0")
    print("blockvolume_cpu_peak_percent=unknown")
    print("blockvolume_cpu_avg_percent=unknown")
else:
    peak = max(values)
    avg = sum(values) / len(values)
    print(f"blockvolume_cpu_sample_count={len(values)}")
    print(f"blockvolume_cpu_peak_percent={peak / 10:.2f}")
    print(f"blockvolume_cpu_avg_percent={avg / 10:.2f}")
PY
}

classify_write_path() {
  python3 - "$1" "$2" "$3" <<'PY'
import sys

def f(v):
    try:
        return float(v)
    except ValueError:
        return None

block_vs_local = f(sys.argv[1])
cpu_peak = f(sys.argv[2])
cpu_samples = f(sys.argv[3])
if block_vs_local is None or block_vs_local <= 0:
    print("unknown")
elif block_vs_local >= 0.80:
    print("benchmark_shape")
elif cpu_samples is None or cpu_samples <= 0 or cpu_peak is None:
    print("unknown")
elif cpu_peak >= 80:
    print("target_cpu")
elif cpu_peak <= 50:
    print("backend_sync")
else:
    print("unknown")
PY
}

recommendation_for() {
  case "$1" in
    target_cpu)
      printf '%s' "phase126_target_copy_cpu_optimization"
      ;;
    backend_sync)
      printf '%s' "phase126_durable_backend_write_optimization"
      ;;
    benchmark_shape)
      printf '%s' "phase126_benchmark_shape_correction"
      ;;
    *)
      printf '%s' "phase126_write_path_instrumentation"
      ;;
  esac
}

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

SERVER_FRONTEND_IP="$(map_value "${NETWORK_SERVER_NODE}")"
CLIENT_FRONTEND_IP="$(map_value "${NETWORK_CLIENT_NODE}")"
if [[ -z "${SERVER_FRONTEND_IP}" || -z "${CLIENT_FRONTEND_IP}" ]]; then
  echo "frontend map ${FRONTEND_IP_MAP} must include ${NETWORK_SERVER_NODE} and ${NETWORK_CLIENT_NODE}" >&2
  exit 1
fi

write_summary "phase125_block_nvme_tcp_write_path_profile_status=running"
write_summary "frontend_transport=tcp"
write_summary "frontend_ip_map=${FRONTEND_IP_MAP}"
write_summary "frontend_network_class=${FRONTEND_NETWORK_CLASS}"
write_summary "nvme_rdma_supported=false"
write_summary "roce_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "test_shape=dd_exec_profiled_seq_write"

run_network_baseline "${SERVER_FRONTEND_IP}" "${CLIENT_FRONTEND_IP}"

SW_BLOCK_ARTIFACT_DIR="${BLOCK_DIR}" \
SW_BLOCK_FRONTEND_IP_MAP="${FRONTEND_IP_MAP}" \
SW_BLOCK_FRONTEND_NETWORK_CLASS="${FRONTEND_NETWORK_CLASS}" \
SW_BLOCK_EXPECTED_FRONTEND_ROUTE_DEV="${EXPECTED_ROUTE_DEV}" \
SW_BLOCK_IMPORT_K3S_NODES="${IMPORT_K3S_NODES}" \
SW_BLOCK_PHASE120_PVC_NAME="${SW_BLOCK_PHASE125_BLOCK_PVC_NAME:-sw-block-phase125-block-pvc}" \
SW_BLOCK_PHASE120_STORAGECLASS="${SW_BLOCK_PHASE125_BLOCK_STORAGECLASS:-sw-block-phase125-block}" \
SW_BLOCK_PHASE120_POD="${SW_BLOCK_PHASE125_BLOCK_POD:-sw-block-phase125-block-perf}" \
SW_BLOCK_PHASE120_PVC_SIZE="${SW_BLOCK_PHASE125_BLOCK_PVC_SIZE:-1Gi}" \
SW_BLOCK_PHASE120_SEQ_MIB="${SEQ_MIB}" \
SW_BLOCK_PHASE120_PROFILE_WRITE=true \
SW_BLOCK_PHASE120_PROFILE_INTERVAL_SECONDS="${SW_BLOCK_PHASE125_PROFILE_INTERVAL_SECONDS:-1}" \
  bash "${ROOT}/scripts/run-phase120-nvme-tcp-performance-baseline-gate.sh" "${ROOT}" \
  >"${DIAG_DIR}/phase120-profile.stdout.txt" \
  2>"${DIAG_DIR}/phase120-profile.stderr.txt"

BLOCK_SUMMARY="${BLOCK_DIR}/phase120-nvme-tcp-performance-baseline-summary.txt"
if [[ "$(require_summary_value "${BLOCK_SUMMARY}" phase120_nvme_tcp_performance_baseline_status)" != "ok" ]]; then
  echo "profiled block gate did not finish ok" >&2
  exit 1
fi

APP_NODE="$(require_summary_value "${BLOCK_SUMMARY}" app_node)"
LOCAL_SC="$(resolve_local_storageclass)"
cleanup_local_path
measure_local_write "${LOCAL_SC}" "${APP_NODE}"

BLOCK_WRITE="$(require_summary_value "${BLOCK_SUMMARY}" seq_write_mibps)"
BLOCK_READ="$(require_summary_value "${BLOCK_SUMMARY}" seq_read_mibps)"
BLOCK_WRITE_MS="$(require_summary_value "${BLOCK_SUMMARY}" seq_write_duration_ms)"
LOCAL_WRITE="$(require_summary_value "${SUMMARY}" local_path_seq_write_mibps)"
LOCAL_READ="$(require_summary_value "${SUMMARY}" local_path_seq_read_mibps)"
LOCAL_WRITE_MS="$(require_summary_value "${SUMMARY}" local_path_seq_write_duration_ms)"
WRITE_RATIO="$(ratio "${BLOCK_WRITE}" "${LOCAL_WRITE}")"
READ_RATIO="$(ratio "${BLOCK_READ}" "${LOCAL_READ}")"

write_summary "publish_target=$(require_summary_value "${BLOCK_SUMMARY}" publish_target)"
write_summary "route_dev=$(require_summary_value "${BLOCK_SUMMARY}" publish_target_route_dev)"
write_summary "publish_target_network_class=$(require_summary_value "${BLOCK_SUMMARY}" publish_target_network_class)"
write_summary "management_ip=$(require_summary_value "${BLOCK_SUMMARY}" management_ip)"
write_summary "frontend_ip=$(require_summary_value "${BLOCK_SUMMARY}" frontend_ip)"
write_summary "block_app_node=${APP_NODE}"
write_summary "local_path_storageclass=${LOCAL_SC}"
write_summary "block_seq_size_mib=$(require_summary_value "${BLOCK_SUMMARY}" seq_size_mib)"
write_summary "block_write_duration_ms=${BLOCK_WRITE_MS}"
write_summary "block_nvme_seq_write_mibps=${BLOCK_WRITE}"
write_summary "block_nvme_seq_read_mibps=${BLOCK_READ}"
write_summary "local_write_duration_ms=${LOCAL_WRITE_MS}"
write_summary "block_vs_local_write_ratio=${WRITE_RATIO}"
write_summary "block_vs_local_read_ratio=${READ_RATIO}"
parse_blockvolume_cpu_profile "${BLOCK_DIR}/profile/seq-write-samples.txt" | while IFS= read -r line; do
  write_summary "${line}"
done

CPU_PEAK="$(summary_value "${SUMMARY}" blockvolume_cpu_peak_percent)"
CPU_SAMPLES="$(summary_value "${SUMMARY}" blockvolume_cpu_sample_count)"
WRITE_OBSERVATION="$(classify_write_path "${WRITE_RATIO}" "${CPU_PEAK:-unknown}" "${CPU_SAMPLES:-0}")"
NEXT_RECOMMENDATION="$(recommendation_for "${WRITE_OBSERVATION}")"
write_summary "write_path_observation=${WRITE_OBSERVATION}"
write_summary "top_bottleneck=${WRITE_OBSERVATION}"
write_summary "next_recommendation=${NEXT_RECOMMENDATION}"

cleanup_local_path
SW_BLOCK_CLEANUP_WAIT_SECONDS="${SW_BLOCK_CLEANUP_WAIT_SECONDS:-180}" \
SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/cleanup/verify" \
  bash "${ROOT}/scripts/verify-helm-cleanup.sh" "${ROOT}" \
  >"${ARTIFACT_DIR}/cleanup-verify.stdout.txt" \
  2>"${ARTIFACT_DIR}/cleanup-verify.stderr.txt"
grep -q '^cleanup_status=ok$' "${ARTIFACT_DIR}/cleanup/verify/cleanup-summary.txt"
write_summary "cleanup_status=ok"
write_summary "phase125_block_nvme_tcp_write_path_profile_status=ok"
