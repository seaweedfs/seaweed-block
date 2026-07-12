#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase123-nvme-tcp-bottleneck-triage-gate}"
SUMMARY="${ARTIFACT_DIR}/phase123-nvme-tcp-bottleneck-triage-summary.txt"
PHASE122_DIR="${ARTIFACT_DIR}/phase122-inner"
DIAG_DIR="${ARTIFACT_DIR}/diagnostics"

FRONTEND_IP_MAP="${SW_BLOCK_PHASE123_FRONTEND_IP_MAP:-m01=10.0.0.1,m02=10.0.0.3}"
FRONTEND_NETWORK_CLASS="${SW_BLOCK_PHASE123_FRONTEND_NETWORK_CLASS:-100gbe_tcp}"
EXPECTED_ROUTE_DEV="${SW_BLOCK_PHASE123_EXPECTED_ROUTE_DEV:-enp1s0np0}"
IMPORT_K3S_NODES="${SW_BLOCK_IMPORT_K3S_NODES:-192.168.1.181,192.168.1.184}"
NETWORK_SERVER_NODE="${SW_BLOCK_PHASE123_NETWORK_SERVER_NODE:-m01}"
NETWORK_CLIENT_NODE="${SW_BLOCK_PHASE123_NETWORK_CLIENT_NODE:-m02}"
NETWORK_SERVER_HOST="${SW_BLOCK_PHASE123_NETWORK_SERVER_HOST:-192.168.1.181}"
NETWORK_SSH_USER="${SW_BLOCK_PHASE123_NETWORK_SSH_USER:-testdev}"
NETWORK_SSH_KEY="${SW_BLOCK_PHASE123_NETWORK_SSH_KEY:-${SW_BLOCK_IMPORT_K3S_SSH_KEY:-/opt/work/testdev_key}}"
IPERF_PORT="${SW_BLOCK_PHASE123_IPERF_PORT:-51223}"
IPERF_SECONDS="${SW_BLOCK_PHASE123_IPERF_SECONDS:-5}"

mkdir -p "${ARTIFACT_DIR}" "${DIAG_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
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

classify_bottleneck() {
  python3 - "$1" "$2" <<'PY'
import sys

network = sys.argv[1]
k8s = sys.argv[2]
try:
    n = float(network)
    k = float(k8s)
except ValueError:
    print("unknown")
    sys.exit(0)

if n <= 0 or k <= 0:
    print("unknown")
elif k >= n * 0.70:
    print("network")
else:
    print("unknown")
PY
}

recommendation_for() {
  case "$1" in
    network)
      printf '%s' "phase124_network_fabric_validation"
      ;;
    *)
      printf '%s' "phase124_target_backend_shape_split"
      ;;
  esac
}

collect_runtime_diagnostics() {
  local gate_pid="$1"
  local sample=0
  while kill -0 "${gate_pid}" >/dev/null 2>&1; do
    {
      echo "=== sample=${sample} ts=$(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
      kubectl get nodes -o wide 2>&1 || true
      kubectl get pods -A -o wide 2>&1 || true
      kubectl -n kube-system top pods --containers 2>&1 || true
      kubectl top nodes 2>&1 || true
      ps -eo pid,ppid,pcpu,pmem,comm,args | grep -E 'blockmaster|blockvolume|blockcsi|sw-block' | grep -v grep || true
      kubectl -n kube-system logs -l app=sw-blockvolume --tail=40 --prefix 2>&1 || true
    } >>"${DIAG_DIR}/runtime-samples.txt"
    sample=$((sample + 1))
    sleep 5
  done
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
    "pkill -x iperf3 >/dev/null 2>&1 || true; nohup iperf3 -s -1 -B ${server_ip} -p ${IPERF_PORT} >/tmp/phase123-iperf3-${IPERF_PORT}.log 2>&1 &" \
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
    local mibps
    mibps="$(mibps_from_iperf_json "${DIAG_DIR}/iperf3-client.json")"
    write_summary "network_baseline_status=ok"
    write_summary "network_baseline_mibps=${mibps}"
  else
    write_summary "network_baseline_status=unavailable"
    write_summary "network_baseline_reason=iperf3_client_failed"
    write_summary "network_baseline_mibps=unavailable"
  fi

  ssh -i "${NETWORK_SSH_KEY}" -o BatchMode=yes -o ConnectTimeout=5 "${NETWORK_SSH_USER}@${NETWORK_SERVER_HOST}" \
    "cat /tmp/phase123-iperf3-${IPERF_PORT}.log 2>/dev/null || true" \
    >"${DIAG_DIR}/iperf3-server.log" \
    2>"${DIAG_DIR}/iperf3-server-fetch.stderr.txt" || true
  ssh -i "${NETWORK_SSH_KEY}" -o BatchMode=yes -o ConnectTimeout=5 "${NETWORK_SSH_USER}@${NETWORK_SERVER_HOST}" \
    "pkill -x iperf3 >/dev/null 2>&1 || true" \
    >"${DIAG_DIR}/iperf3-server-stop.stdout.txt" \
    2>"${DIAG_DIR}/iperf3-server-stop.stderr.txt" || true
}

SERVER_FRONTEND_IP="$(map_value "${NETWORK_SERVER_NODE}")"
CLIENT_FRONTEND_IP="$(map_value "${NETWORK_CLIENT_NODE}")"
if [[ -z "${SERVER_FRONTEND_IP}" || -z "${CLIENT_FRONTEND_IP}" ]]; then
  echo "frontend map ${FRONTEND_IP_MAP} must include ${NETWORK_SERVER_NODE} and ${NETWORK_CLIENT_NODE}" >&2
  exit 1
fi

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

write_summary "phase123_nvme_tcp_bottleneck_triage_status=running"
write_summary "frontend_transport=tcp"
write_summary "frontend_ip_map=${FRONTEND_IP_MAP}"
write_summary "frontend_network_class=${FRONTEND_NETWORK_CLASS}"
write_summary "nvme_rdma_supported=false"
write_summary "roce_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "test_shape=dd_exec_baseline"

ip route get "${SERVER_FRONTEND_IP}" >"${DIAG_DIR}/network-route.txt" 2>&1 || true
run_network_baseline "${SERVER_FRONTEND_IP}" "${CLIENT_FRONTEND_IP}"

set +e
SW_BLOCK_ARTIFACT_DIR="${PHASE122_DIR}" \
SW_BLOCK_PHASE122_FRONTEND_IP_MAP="${FRONTEND_IP_MAP}" \
SW_BLOCK_PHASE122_FRONTEND_NETWORK_CLASS="${FRONTEND_NETWORK_CLASS}" \
SW_BLOCK_PHASE122_EXPECTED_ROUTE_DEV="${EXPECTED_ROUTE_DEV}" \
SW_BLOCK_IMPORT_K3S_NODES="${IMPORT_K3S_NODES}" \
SW_BLOCK_PHASE122_PVC_NAME="${SW_BLOCK_PHASE123_PVC_NAME:-sw-block-phase123-pvc}" \
SW_BLOCK_PHASE122_STORAGECLASS="${SW_BLOCK_PHASE123_STORAGECLASS:-sw-block-phase123}" \
SW_BLOCK_PHASE122_POD="${SW_BLOCK_PHASE123_POD:-sw-block-phase123-perf}" \
  bash "${ROOT}/scripts/run-phase122-nvme-tcp-100gbe-baseline-gate.sh" "${ROOT}" \
  >"${DIAG_DIR}/phase122.stdout.txt" \
  2>"${DIAG_DIR}/phase122.stderr.txt" &
gate_pid=$!
collect_runtime_diagnostics "${gate_pid}" &
sampler_pid=$!
wait "${gate_pid}"
gate_rc=$?
kill "${sampler_pid}" >/dev/null 2>&1 || true
wait "${sampler_pid}" >/dev/null 2>&1 || true
set -e

if [[ "${gate_rc}" -ne 0 ]]; then
  echo "phase122 inner gate failed rc=${gate_rc}" >&2
  exit "${gate_rc}"
fi

PHASE122_SUMMARY="${PHASE122_DIR}/phase122-nvme-tcp-100gbe-baseline-summary.txt"
INNER_STATUS="$(require_summary_value "${PHASE122_SUMMARY}" phase122_nvme_tcp_100gbe_baseline_status)"
if [[ "${INNER_STATUS}" != "ok" ]]; then
  echo "phase122 inner status=${INNER_STATUS}, want ok" >&2
  exit 1
fi

PUBLISH_TARGET="$(require_summary_value "${PHASE122_SUMMARY}" publish_target)"
ROUTE_DEV="$(require_summary_value "${PHASE122_SUMMARY}" publish_target_route_dev)"
K8S_WRITE="$(require_summary_value "${PHASE122_SUMMARY}" seq_write_mibps)"
K8S_READ="$(require_summary_value "${PHASE122_SUMMARY}" seq_read_mibps)"
K8S_IOPS="$(require_summary_value "${PHASE122_SUMMARY}" small_write_iops)"
CLEANUP_STATUS="$(require_summary_value "${PHASE122_SUMMARY}" cleanup_status)"
NETWORK_MIBPS="$(summary_value "${SUMMARY}" network_baseline_mibps)"
TOP_BOTTLENECK="$(classify_bottleneck "${NETWORK_MIBPS:-unavailable}" "${K8S_READ}")"
NEXT_RECOMMENDATION="$(recommendation_for "${TOP_BOTTLENECK}")"

write_summary "publish_target=${PUBLISH_TARGET}"
write_summary "route_dev=${ROUTE_DEV}"
write_summary "publish_target_network_class=$(require_summary_value "${PHASE122_SUMMARY}" publish_target_network_class)"
write_summary "publish_target_source=$(require_summary_value "${PHASE122_SUMMARY}" publish_target_source)"
write_summary "management_ip=$(require_summary_value "${PHASE122_SUMMARY}" management_ip)"
write_summary "frontend_ip=$(require_summary_value "${PHASE122_SUMMARY}" frontend_ip)"
write_summary "host_local_nvme_tcp_status=unavailable"
write_summary "host_local_nvme_tcp_reason=not_safe_in_status_only_gate"
write_summary "host_local_nvme_tcp_mibps=unavailable"
write_summary "k8s_mounted_nvme_tcp_mibps=${K8S_READ}"
write_summary "k8s_mounted_seq_write_mibps=${K8S_WRITE}"
write_summary "k8s_mounted_seq_read_mibps=${K8S_READ}"
write_summary "k8s_mounted_small_write_iops=${K8S_IOPS}"
write_summary "runtime_diagnostics_collected=true"
write_summary "runtime_diagnostics_path=${DIAG_DIR}/runtime-samples.txt"
write_summary "top_bottleneck=${TOP_BOTTLENECK}"
write_summary "next_recommendation=${NEXT_RECOMMENDATION}"
write_summary "cleanup_status=${CLEANUP_STATUS}"
write_summary "phase123_nvme_tcp_bottleneck_triage_status=ok"
