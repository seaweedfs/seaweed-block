#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
RUN_ID="${SW_BLOCK_PHASE164_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase164-nvme-rdma-standalone-hardening-gate}"
SUMMARY="${ARTIFACT_DIR}/phase164-nvme-rdma-standalone-hardening-summary.txt"
WORK="/tmp/sw-block-phase164-${RUN_ID}"
BIN="${WORK}/bin"
TARGET_IP="${SW_BLOCK_PHASE164_TARGET_IP:-10.0.0.3}"
INVALID_TARGET_IP="${SW_BLOCK_PHASE164_INVALID_TARGET_IP:-10.0.0.254}"
RDMA_DEVICE="${SW_BLOCK_PHASE164_RDMA_DEVICE:-rocep1s0}"
RDMA_NETDEV="${SW_BLOCK_PHASE164_RDMA_NETDEV:-enp1s0np0}"
HOST_SSH_ADDR="${SW_BLOCK_PHASE164_HOST_SSH_ADDR:-192.168.1.181}"
HOST_SSH_USER="${SW_BLOCK_PHASE164_HOST_SSH_USER:-testdev}"
HOST_SSH_KEY="${SW_BLOCK_PHASE164_HOST_SSH_KEY:-/opt/work/testdev_key}"
CONFIGFS_ROOT="/sys/kernel/config/nvmet"

NQN1="nqn.2026-07.io.seaweedfs:phase164-v1-${RUN_ID}"
NQN2="nqn.2026-07.io.seaweedfs:phase164-v2-${RUN_ID}"
NQN_BAD="nqn.2026-07.io.seaweedfs:phase164-bad-${RUN_ID}"
NQN_REFUSE="nqn.2026-07.io.seaweedfs:phase164-refuse-${RUN_ID}"
NQN_NONROOT="nqn.2026-07.io.seaweedfs:phase164-nonroot-${RUN_ID}"

MASTER_PID=""
V1_PID=""
V2_PID=""
BAD_PID=""
REFUSE_PID=""
NONROOT_PID=""
CONFLICT_PID=""
NBD1=""
NBD1_RESTART=""
NBD2=""

mkdir -p "${ARTIFACT_DIR}" "${BIN}" "${WORK}/master-store" \
  "${WORK}/v1-store" "${WORK}/v2-store" "${WORK}/bad-store"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

stop_pid() {
  local pid="${1:-}"
  [[ -n "${pid}" ]] || return 0
  sudo kill -TERM "${pid}" >/dev/null 2>&1 || kill -TERM "${pid}" >/dev/null 2>&1 || true
  for _ in $(seq 1 100); do
    if ! sudo kill -0 "${pid}" >/dev/null 2>&1 && ! kill -0 "${pid}" >/dev/null 2>&1; then
      wait "${pid}" >/dev/null 2>&1 || true
      return 0
    fi
    sleep 0.1
  done
  sudo kill -KILL "${pid}" >/dev/null 2>&1 || kill -KILL "${pid}" >/dev/null 2>&1 || true
  wait "${pid}" >/dev/null 2>&1 || true
}

disconnect_host_nqns() {
  ssh -i "${HOST_SSH_KEY}" -o BatchMode=yes -o ConnectTimeout=5 \
    "${HOST_SSH_USER}@${HOST_SSH_ADDR}" \
    "sudo nvme disconnect -n '${NQN1}' >/dev/null 2>&1 || true; sudo nvme disconnect -n '${NQN2}' >/dev/null 2>&1 || true; sudo rm -f /tmp/phase164-*-${RUN_ID}.bin" \
    >/dev/null 2>&1 || true
}

cleanup() {
  set +e
  disconnect_host_nqns
  stop_pid "${V2_PID}"
  stop_pid "${V1_PID}"
  stop_pid "${BAD_PID}"
  stop_pid "${REFUSE_PID}"
  stop_pid "${NONROOT_PID}"
  stop_pid "${CONFLICT_PID}"
  stop_pid "${MASTER_PID}"
  case "${WORK}" in
    /tmp/sw-block-phase164-*) sudo rm -rf -- "${WORK}" ;;
    *) echo "refusing unsafe Phase 164 work cleanup: ${WORK}" >&2 ;;
  esac
}
trap cleanup EXIT

active_nbd_count() {
  local count=0
  local pid_file
  for pid_file in /sys/block/nbd*/pid; do
    [[ -e "${pid_file}" ]] || continue
    if [[ -s "${pid_file}" ]]; then
      count=$((count + 1))
    fi
  done
  echo "${count}"
}

allocate_rdma_port() {
  local excluded=" $* "
  local p
  for p in $(shuf -i 11640-11999); do
    [[ "${excluded}" == *" ${p} "* ]] && continue
    [[ -e "${CONFIGFS_ROOT}/ports/${p}" ]] && continue
    echo "${p}"
    return 0
  done
  return 1
}

wait_log() {
  local pid="$1" log="$2" pattern="$3"
  for _ in $(seq 1 300); do
    if ! sudo kill -0 "${pid}" >/dev/null 2>&1 && ! kill -0 "${pid}" >/dev/null 2>&1; then
      cat "${log}" >&2
      return 1
    fi
    grep -a -q "${pattern}" "${log}" 2>/dev/null && return 0
    sleep 0.1
  done
  cat "${log}" >&2
  return 1
}

wait_status_ready() {
  local addr="$1" volume="$2" output="$3"
  for _ in $(seq 1 300); do
    if curl -fsS "http://${addr}/status?volume=${volume}" >"${output}" 2>/dev/null &&
        grep -q '"FrontendPrimaryReady":true' "${output}"; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

start_volume() {
  local volume="$1" replica="$2" server="$3" data_port="$4" ctrl_port="$5"
  local status_port="$6" rdma_port="$7" nqn="$8" storage_root="$9" log="${10}"
  sudo "${BIN}/blockvolume" \
    --master "127.0.0.1:${MASTER_PORT}" \
    --server-id "${server}" \
    --volume-id "${volume}" \
    --replica-id "${replica}" \
    --data-addr "127.0.0.1:${data_port}" \
    --ctrl-addr "127.0.0.1:${ctrl_port}" \
    --status-addr "127.0.0.1:${status_port}" \
    --heartbeat-interval 200ms \
    --t1-readiness \
    --durable-root "${storage_root}" \
    --durable-impl smartwal \
    --durable-blocks 8192 \
    --durable-blocksize 4096 \
    --nvme-listen "${TARGET_IP}:${rdma_port}" \
    --nvme-transport rdma \
    --allow-external-nvme-bind \
    --nvme-subsysnqn "${nqn}" \
    --nvme-ns 1 \
    >"${log}" 2>&1 &
  STARTED_PID=$!
}

wait_failed_process() {
  local pid="$1" log="$2"
  for _ in $(seq 1 300); do
    if ! sudo kill -0 "${pid}" >/dev/null 2>&1 && ! kill -0 "${pid}" >/dev/null 2>&1; then
      set +e
      wait "${pid}"
      local rc=$?
      set -e
      [[ "${rc}" -ne 0 ]]
      return
    fi
    sleep 0.1
  done
  cat "${log}" >&2
  return 1
}

wait_target_gone() {
  local nqn="$1" port="$2" nbd_path="$3"
  for _ in $(seq 1 200); do
    local nbd_active=false
    if [[ -n "${nbd_path}" && -e "/sys/block/$(basename "${nbd_path}")/pid" && -s "/sys/block/$(basename "${nbd_path}")/pid" ]]; then
      nbd_active=true
    fi
    if [[ ! -e "${CONFIGFS_ROOT}/subsystems/${nqn}" && ! -e "${CONFIGFS_ROOT}/ports/${port}" && "${nbd_active}" == false ]]; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

write_summary "phase164_nvme_rdma_standalone_hardening_status=running"
write_summary "rdma_bind_ip=${TARGET_IP}"
write_summary "rdma_device=${RDMA_DEVICE}"
write_summary "rdma_netdev=${RDMA_NETDEV}"
write_summary "rdma_not_published_to_csi=true"
write_summary "performance_slo_claim_allowed=false"

[[ -d "/sys/class/infiniband/${RDMA_DEVICE}" ]]
[[ -d "/sys/class/infiniband/${RDMA_DEVICE}/device/net/${RDMA_NETDEV}" ]]
ip -4 -o addr show dev "${RDMA_NETDEV}" | grep -q " ${TARGET_IP}/"

readarray -t TCP_PORTS < <(python3 - <<'PY'
import socket
sockets = []
for _ in range(16):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sockets.append(sock)
for sock in sockets:
    print(sock.getsockname()[1])
for sock in sockets:
    sock.close()
PY
)
MASTER_PORT="${TCP_PORTS[0]}"
V1_DATA_PORT="${TCP_PORTS[1]}"
V1_CTRL_PORT="${TCP_PORTS[2]}"
V1_STATUS_PORT="${TCP_PORTS[3]}"
V2_DATA_PORT="${TCP_PORTS[4]}"
V2_CTRL_PORT="${TCP_PORTS[5]}"
V2_STATUS_PORT="${TCP_PORTS[6]}"
BAD_DATA_PORT="${TCP_PORTS[7]}"
BAD_CTRL_PORT="${TCP_PORTS[8]}"
BAD_STATUS_PORT="${TCP_PORTS[9]}"
REFUSE_DATA_PORT="${TCP_PORTS[10]}"
REFUSE_CTRL_PORT="${TCP_PORTS[11]}"
REFUSE_STATUS_PORT="${TCP_PORTS[12]}"
NONROOT_DATA_PORT="${TCP_PORTS[13]}"
NONROOT_CTRL_PORT="${TCP_PORTS[14]}"
NONROOT_STATUS_PORT="${TCP_PORTS[15]}"

RDMA_PORT1="$(allocate_rdma_port)"
RDMA_PORT2="$(allocate_rdma_port "${RDMA_PORT1}")"
RDMA_PORT_BAD="$(allocate_rdma_port "${RDMA_PORT1}" "${RDMA_PORT2}")"
RDMA_PORT_REFUSE="$(allocate_rdma_port "${RDMA_PORT1}" "${RDMA_PORT2}" "${RDMA_PORT_BAD}")"
RDMA_PORT_NONROOT="$(allocate_rdma_port "${RDMA_PORT1}" "${RDMA_PORT2}" "${RDMA_PORT_BAD}" "${RDMA_PORT_REFUSE}")"

cd "${ROOT}"
go test ./core/frontend/nbd ./core/frontend/nvmerdma ./core/frontend/nvme ./cmd/blockvolume \
  >"${ARTIFACT_DIR}/go-test.stdout.txt" 2>"${ARTIFACT_DIR}/go-test.stderr.txt"
write_summary "go_test_nbd_nvmerdma_nvme_blockvolume=ok"
write_summary "tcp_behavior_unchanged=true"

go build -o "${BIN}/blockmaster" ./cmd/blockmaster
go build -o "${BIN}/blockvolume" ./cmd/blockvolume
cat >"${WORK}/topology.yaml" <<'YAML'
volumes:
  - volume_id: v1
    slots:
      - replica_id: r1
        server_id: s1
  - volume_id: v2
    slots:
      - replica_id: r2
        server_id: s2
YAML

"${BIN}/blockmaster" \
  --authority-store "${WORK}/master-store" \
  --listen "127.0.0.1:${MASTER_PORT}" \
  --topology "${WORK}/topology.yaml" \
  --expected-slots-per-volume 1 \
  --t0-print-ready \
  >"${ARTIFACT_DIR}/blockmaster.log" 2>&1 &
MASTER_PID=$!
wait_log "${MASTER_PID}" "${ARTIFACT_DIR}/blockmaster.log" '"phase":"listening"'

NBD_BASELINE="$(active_nbd_count)"

# Stable preflight refusal: loopback is syntactically valid but invalid for RDMA.
sudo "${BIN}/blockvolume" \
  --master "127.0.0.1:${MASTER_PORT}" --server-id s1 --volume-id v1 --replica-id r1 \
  --data-addr "127.0.0.1:${REFUSE_DATA_PORT}" --ctrl-addr "127.0.0.1:${REFUSE_CTRL_PORT}" \
  --status-addr "127.0.0.1:${REFUSE_STATUS_PORT}" --heartbeat-interval 200ms --t1-readiness \
  --nvme-listen "127.0.0.1:${RDMA_PORT_REFUSE}" --nvme-transport rdma \
  --nvme-subsysnqn "${NQN_REFUSE}" --nvme-ns 1 \
  >"${ARTIFACT_DIR}/refusal.log" 2>&1 &
REFUSE_PID=$!
wait_failed_process "${REFUSE_PID}" "${ARTIFACT_DIR}/refusal.log"
REFUSE_PID=""
grep -a -q 'reason=rdma_bind_address_invalid' "${ARTIFACT_DIR}/refusal.log"
[[ "$(active_nbd_count)" == "${NBD_BASELINE}" ]]

# Root boundary: a normal user cannot own kernel NBD/configfs target state.
"${BIN}/blockvolume" \
  --master "127.0.0.1:${MASTER_PORT}" --server-id s1 --volume-id v1 --replica-id r1 \
  --data-addr "127.0.0.1:${NONROOT_DATA_PORT}" --ctrl-addr "127.0.0.1:${NONROOT_CTRL_PORT}" \
  --status-addr "127.0.0.1:${NONROOT_STATUS_PORT}" --heartbeat-interval 200ms --t1-readiness \
  --nvme-listen "${TARGET_IP}:${RDMA_PORT_NONROOT}" --nvme-transport rdma --allow-external-nvme-bind \
  --nvme-subsysnqn "${NQN_NONROOT}" --nvme-ns 1 \
  >"${ARTIFACT_DIR}/nonroot-refusal.log" 2>&1 &
NONROOT_PID=$!
wait_failed_process "${NONROOT_PID}" "${ARTIFACT_DIR}/nonroot-refusal.log"
NONROOT_PID=""
grep -a -q 'reason=rdma_target_permission_denied' "${ARTIFACT_DIR}/nonroot-refusal.log"
[[ "$(active_nbd_count)" == "${NBD_BASELINE}" ]]

# Force failure after NBD allocation/configfs setup begins with an unassigned IP.
sudo "${BIN}/blockvolume" \
  --master "127.0.0.1:${MASTER_PORT}" --server-id s1 --volume-id v1 --replica-id r1 \
  --data-addr "127.0.0.1:${BAD_DATA_PORT}" --ctrl-addr "127.0.0.1:${BAD_CTRL_PORT}" \
  --status-addr "127.0.0.1:${BAD_STATUS_PORT}" --heartbeat-interval 200ms --t1-readiness \
  --durable-root "${WORK}/bad-store" --durable-impl smartwal --durable-blocks 8192 --durable-blocksize 4096 \
  --nvme-listen "${INVALID_TARGET_IP}:${RDMA_PORT_BAD}" --nvme-transport rdma --allow-external-nvme-bind \
  --nvme-subsysnqn "${NQN_BAD}" --nvme-ns 1 \
  >"${ARTIFACT_DIR}/startup-rollback.log" 2>&1 &
BAD_PID=$!
wait_failed_process "${BAD_PID}" "${ARTIFACT_DIR}/startup-rollback.log"
BAD_PID=""
grep -a -q 'reason=rdma_bind_address_unassigned' "${ARTIFACT_DIR}/startup-rollback.log"
[[ ! -e "${CONFIGFS_ROOT}/subsystems/${NQN_BAD}" ]]
[[ ! -e "${CONFIGFS_ROOT}/ports/${RDMA_PORT_BAD}" ]]
[[ "$(active_nbd_count)" == "${NBD_BASELINE}" ]]
write_summary "startup_rollback_verified=true"
write_summary "negative_preflight_refusal_verified=true"

start_volume v1 r1 s1 "${V1_DATA_PORT}" "${V1_CTRL_PORT}" "${V1_STATUS_PORT}" \
  "${RDMA_PORT1}" "${NQN1}" "${WORK}/v1-store" "${ARTIFACT_DIR}/v1-first.log"
V1_PID="${STARTED_PID}"
wait_log "${V1_PID}" "${ARTIFACT_DIR}/v1-first.log" '"phase":"nvme-listening"'
wait_status_ready "127.0.0.1:${V1_STATUS_PORT}" v1 "${ARTIFACT_DIR}/v1-first-status.json"
curl -fsS "http://127.0.0.1:${V1_STATUS_PORT}/status/frontend-capabilities?volume=v1" \
  >"${ARTIFACT_DIR}/v1-first-capabilities.json"
python3 - "${ARTIFACT_DIR}/v1-first-capabilities.json" <<'PY'
import json, sys
doc = json.load(open(sys.argv[1]))
rdma = next(x for x in doc["frontendTransports"] if x["transport"] == "rdma")
assert rdma["supported"] is True
assert rdma["listenerImplemented"] is True
assert rdma["listenerStarted"] is True
assert rdma["startAllowed"] is True
PY
NBD1="$(cat "${CONFIGFS_ROOT}/subsystems/${NQN1}/namespaces/1/device_path")"
[[ -b "${NBD1}" ]]

# A second target cannot claim the live target's RDMA port. Its partial NBD and
# subsystem state must roll back without disrupting the first target.
start_volume v2 r2 s2 "${V2_DATA_PORT}" "${V2_CTRL_PORT}" "${V2_STATUS_PORT}" \
  "${RDMA_PORT1}" "${NQN2}" "${WORK}/v2-store" "${ARTIFACT_DIR}/port-conflict.log"
CONFLICT_PID="${STARTED_PID}"
wait_failed_process "${CONFLICT_PID}" "${ARTIFACT_DIR}/port-conflict.log"
CONFLICT_PID=""
grep -a -q 'reason=rdma_port_conflict' "${ARTIFACT_DIR}/port-conflict.log"
[[ ! -e "${CONFIGFS_ROOT}/subsystems/${NQN2}" ]]
[[ -e "${CONFIGFS_ROOT}/subsystems/${NQN1}" ]]
[[ "$(active_nbd_count)" -eq $((NBD_BASELINE + 1)) ]]
write_summary "port_conflict_refusal_verified=true"

ssh -i "${HOST_SSH_KEY}" -o BatchMode=yes "${HOST_SSH_USER}@${HOST_SSH_ADDR}" \
  "sudo bash -s" >"${ARTIFACT_DIR}/v1-io.txt" <<HOST
set -euo pipefail
NQN='${NQN1}'
TARGET_IP='${TARGET_IP}'
TARGET_PORT='${RDMA_PORT1}'
find_dev() {
  local nqn="\$1"
  local p base candidate
  for _ in \$(seq 1 100); do
    for p in /sys/class/nvme-subsystem/*/subsysnqn; do
      [[ -e "\$p" ]] || continue
      [[ "\$(cat "\$p")" == "\$nqn" ]] || continue
      base="\$(dirname "\$p")"
      for candidate in "\$base"/nvme*n*; do
        [[ -e "\$candidate" ]] || continue
        echo "/dev/\$(basename "\$candidate")"
        return 0
      done
    done
    sleep 0.1
  done
  return 1
}
nvme disconnect -n "\$NQN" >/dev/null 2>&1 || true
modprobe nvme-rdma
nvme connect -t rdma -a "\$TARGET_IP" -s "\$TARGET_PORT" -n "\$NQN"
DEV="\$(find_dev "\$NQN")"
SMALL=/tmp/phase164-small-${RUN_ID}.bin
LARGE=/tmp/phase164-large-${RUN_ID}.bin
RESTART=/tmp/phase164-restart-${RUN_ID}.bin
python3 -c 'import sys; sys.stdout.buffer.write((b"phase164-small-fua"*256)[:4096])' >"\$SMALL"
python3 -c 'import sys; sys.stdout.buffer.write((b"phase164-large-sequential"*65536)[:1048576])' >"\$LARGE"
python3 -c 'import sys; sys.stdout.buffer.write((b"phase164-restart"*256)[:4096])' >"\$RESTART"
nvme write "\$DEV" -n 1 -s 0 -c 0 -z 4096 -d "\$SMALL" -f --force
dd if="\$LARGE" of="\$DEV" bs=4096 seek=8 count=256 oflag=direct conv=fsync status=none
nvme write "\$DEV" -n 1 -s 512 -c 0 -z 4096 -d "\$RESTART" -f --force
nvme flush "\$DEV" -n 1
dd if="\$DEV" bs=4096 count=1 status=none | cmp - "\$SMALL"
dd if="\$DEV" bs=4096 skip=8 count=256 status=none | cmp - "\$LARGE"
dd if="\$DEV" bs=4096 skip=512 count=1 status=none | cmp - "\$RESTART"
nvme disconnect -n "\$NQN"
rm -f "\$SMALL" "\$LARGE" "\$RESTART"
printf 'small_io_verified=true\nlarge_io_verified=true\nfua_issued=true\nflush_issued=true\nrestart_pattern_written=true\nhost_device=%s\n' "\$DEV"
HOST

grep -q '^small_io_verified=true$' "${ARTIFACT_DIR}/v1-io.txt"
grep -q '^large_io_verified=true$' "${ARTIFACT_DIR}/v1-io.txt"
grep -q '^fua_issued=true$' "${ARTIFACT_DIR}/v1-io.txt"
grep -q '^flush_issued=true$' "${ARTIFACT_DIR}/v1-io.txt"
curl -fsS "http://127.0.0.1:${V1_STATUS_PORT}/status/durable?volume=v1" \
  >"${ARTIFACT_DIR}/v1-durable-before-restart.json"
python3 - "${ARTIFACT_DIR}/v1-durable-before-restart.json" <<'PY'
import json, sys
doc = json.load(open(sys.argv[1]))
st = doc["Volumes"][0]
profile = st["WriteProfile"]
assert profile["BackendWriteRequestBytes"] >= 1024 * 1024
assert profile["BackendSyncOps"] >= 3
assert st["DurableLSN"] > 0
PY
write_summary "small_and_large_io_verified=true"
write_summary "flush_and_fua_verified=true"

stop_pid "${V1_PID}"
V1_PID=""
wait_target_gone "${NQN1}" "${RDMA_PORT1}" "${NBD1}"
if curl -fsS "http://127.0.0.1:${V1_STATUS_PORT}/status/frontend-capabilities?volume=v1" >/dev/null 2>&1; then
  echo "capability endpoint remained reachable after target shutdown" >&2
  exit 1
fi

start_volume v1 r1 s1 "${V1_DATA_PORT}" "${V1_CTRL_PORT}" "${V1_STATUS_PORT}" \
  "${RDMA_PORT1}" "${NQN1}" "${WORK}/v1-store" "${ARTIFACT_DIR}/v1-restart.log"
V1_PID="${STARTED_PID}"
wait_log "${V1_PID}" "${ARTIFACT_DIR}/v1-restart.log" '"phase":"nvme-listening"'
wait_status_ready "127.0.0.1:${V1_STATUS_PORT}" v1 "${ARTIFACT_DIR}/v1-restart-status.json"
curl -fsS "http://127.0.0.1:${V1_STATUS_PORT}/status/frontend-capabilities?volume=v1" \
  >"${ARTIFACT_DIR}/v1-restart-capabilities.json"
python3 - "${ARTIFACT_DIR}/v1-restart-capabilities.json" <<'PY'
import json, sys
doc = json.load(open(sys.argv[1]))
rdma = next(x for x in doc["frontendTransports"] if x["transport"] == "rdma")
assert rdma["supported"] is True
assert rdma["listenerImplemented"] is True
assert rdma["listenerStarted"] is True
assert rdma["startAllowed"] is True
PY
NBD1_RESTART="$(cat "${CONFIGFS_ROOT}/subsystems/${NQN1}/namespaces/1/device_path")"

ssh -i "${HOST_SSH_KEY}" -o BatchMode=yes "${HOST_SSH_USER}@${HOST_SSH_ADDR}" \
  "sudo bash -s" >"${ARTIFACT_DIR}/v1-reconnect.txt" <<HOST
set -euo pipefail
NQN='${NQN1}'
nvme connect -t rdma -a '${TARGET_IP}' -s '${RDMA_PORT1}' -n "\$NQN"
DEV=''
for _ in \$(seq 1 100); do
  for p in /sys/class/nvme-subsystem/*/subsysnqn; do
    [[ -e "\$p" ]] || continue
    [[ "\$(cat "\$p")" == "\$NQN" ]] || continue
    base="\$(dirname "\$p")"
    for candidate in "\$base"/nvme*n*; do
      [[ -e "\$candidate" ]] || continue
      DEV="/dev/\$(basename "\$candidate")"
      break 3
    done
  done
  sleep 0.1
done
[[ -b "\$DEV" ]]
python3 -c 'import sys; sys.stdout.buffer.write((b"phase164-restart"*256)[:4096])' | \
  cmp - <(dd if="\$DEV" bs=4096 skip=512 count=1 status=none)
nvme disconnect -n "\$NQN"
printf 'durable_restart_readback=true\nhost_device=%s\n' "\$DEV"
HOST
grep -q '^durable_restart_readback=true$' "${ARTIFACT_DIR}/v1-reconnect.txt"
grep -a -q 'durable recovered: recovered LSN=' "${ARTIFACT_DIR}/v1-restart.log"
write_summary "durable_restart_reconnect_verified=true"

start_volume v2 r2 s2 "${V2_DATA_PORT}" "${V2_CTRL_PORT}" "${V2_STATUS_PORT}" \
  "${RDMA_PORT2}" "${NQN2}" "${WORK}/v2-store" "${ARTIFACT_DIR}/v2.log"
V2_PID="${STARTED_PID}"
wait_log "${V2_PID}" "${ARTIFACT_DIR}/v2.log" '"phase":"nvme-listening"'
wait_status_ready "127.0.0.1:${V2_STATUS_PORT}" v2 "${ARTIFACT_DIR}/v2-status.json"
NBD2="$(cat "${CONFIGFS_ROOT}/subsystems/${NQN2}/namespaces/1/device_path")"
[[ "${NBD1_RESTART}" != "${NBD2}" ]]

ssh -i "${HOST_SSH_KEY}" -o BatchMode=yes "${HOST_SSH_USER}@${HOST_SSH_ADDR}" \
  "sudo bash -s" >"${ARTIFACT_DIR}/multi-target.txt" <<HOST
set -euo pipefail
NQN1='${NQN1}'
NQN2='${NQN2}'
find_dev() {
  local nqn="\$1"
  local p base candidate
  for _ in \$(seq 1 100); do
    for p in /sys/class/nvme-subsystem/*/subsysnqn; do
      [[ -e "\$p" ]] || continue
      [[ "\$(cat "\$p")" == "\$nqn" ]] || continue
      base="\$(dirname "\$p")"
      for candidate in "\$base"/nvme*n*; do
        [[ -e "\$candidate" ]] || continue
        echo "/dev/\$(basename "\$candidate")"
        return 0
      done
    done
    sleep 0.1
  done
  return 1
}
wait_size() {
  local dev="\$1" expected="\$2" size=''
  for _ in \$(seq 1 100); do
    size="\$(blockdev --getsize64 "\$dev" 2>/dev/null || true)"
    [[ "\$size" == "\$expected" ]] && { echo "\$size"; return 0; }
    sleep 0.1
  done
  return 1
}
nvme connect -t rdma -a '${TARGET_IP}' -s '${RDMA_PORT1}' -n "\$NQN1"
nvme connect -t rdma -a '${TARGET_IP}' -s '${RDMA_PORT2}' -n "\$NQN2"
DEV1="\$(find_dev "\$NQN1")"
DEV2="\$(find_dev "\$NQN2")"
[[ "\$DEV1" != "\$DEV2" ]]
SIZE1="\$(wait_size "\$DEV1" 33554432)"
SIZE2="\$(wait_size "\$DEV2" 33554432)"
printf 'v1_device=%s\nv1_size_bytes=%s\nv2_device=%s\nv2_size_bytes=%s\n' \
  "\$DEV1" "\$SIZE1" "\$DEV2" "\$SIZE2"
[[ "\$SIZE1" == '33554432' ]]
[[ "\$SIZE2" == '33554432' ]]
P1=/tmp/phase164-v1-${RUN_ID}.bin
P2=/tmp/phase164-v2-${RUN_ID}.bin
python3 -c 'import sys; sys.stdout.buffer.write((b"phase164-volume-one"*256)[:4096])' >"\$P1"
python3 -c 'import sys; sys.stdout.buffer.write((b"phase164-volume-two"*256)[:4096])' >"\$P2"
nvme write "\$DEV1" -n 1 -s 1024 -c 0 -z 4096 -d "\$P1" -f --force
nvme write "\$DEV2" -n 1 -s 1024 -c 0 -z 4096 -d "\$P2" -f --force
dd if="\$DEV1" bs=4096 skip=1024 count=1 status=none | cmp - "\$P1"
dd if="\$DEV2" bs=4096 skip=1024 count=1 status=none | cmp - "\$P2"
H1="\$(sha256sum "\$P1" | awk '{print \$1}')"
H2="\$(sha256sum "\$P2" | awk '{print \$1}')"
[[ "\$H1" != "\$H2" ]]
nvme disconnect -n "\$NQN1"
nvme disconnect -n "\$NQN2"
rm -f "\$P1" "\$P2"
printf 'multi_target_isolation=true\n'
HOST
grep -q '^multi_target_isolation=true$' "${ARTIFACT_DIR}/multi-target.txt"
write_summary "multi_target_isolation_verified=true"

ssh -i "${HOST_SSH_KEY}" -o BatchMode=yes "${HOST_SSH_USER}@${HOST_SSH_ADDR}" \
  "sudo bash -s" >"${ARTIFACT_DIR}/connect-churn.txt" <<HOST
set -euo pipefail
NQN='${NQN1}'
for cycle in \$(seq 1 5); do
  nvme connect -t rdma -a '${TARGET_IP}' -s '${RDMA_PORT1}' -n "\$NQN"
  DEV=''
  for _ in \$(seq 1 100); do
    for p in /sys/class/nvme-subsystem/*/subsysnqn; do
      [[ -e "\$p" ]] || continue
      [[ "\$(cat "\$p")" == "\$NQN" ]] || continue
      base="\$(dirname "\$p")"
      for candidate in "\$base"/nvme*n*; do
        [[ -e "\$candidate" ]] || continue
        DEV="/dev/\$(basename "\$candidate")"
        break 3
      done
    done
    sleep 0.1
  done
  [[ -b "\$DEV" ]]
  P="/tmp/phase164-churn-${RUN_ID}-\${cycle}.bin"
  head -c 4096 /dev/urandom >"\$P"
  LBA="\$((768 + cycle))"
  nvme write "\$DEV" -n 1 -s "\$LBA" -c 0 -z 4096 -d "\$P" -f --force
  dd if="\$DEV" bs=4096 skip="\$LBA" count=1 status=none | cmp - "\$P"
  nvme disconnect -n "\$NQN"
  rm -f "\$P"
  sleep 0.2
  printf 'cycle_%s=ok\n' "\$cycle"
done
printf 'bounded_connect_churn=true\n'
HOST
grep -q '^bounded_connect_churn=true$' "${ARTIFACT_DIR}/connect-churn.txt"
write_summary "bounded_connect_churn_verified=true"

disconnect_host_nqns
stop_pid "${V2_PID}"
V2_PID=""
stop_pid "${V1_PID}"
V1_PID=""
stop_pid "${MASTER_PID}"
MASTER_PID=""
wait_target_gone "${NQN2}" "${RDMA_PORT2}" "${NBD2}"
wait_target_gone "${NQN1}" "${RDMA_PORT1}" "${NBD1_RESTART}"
[[ "$(active_nbd_count)" == "${NBD_BASELINE}" ]]
if ssh -i "${HOST_SSH_KEY}" -o BatchMode=yes "${HOST_SSH_USER}@${HOST_SSH_ADDR}" \
    "sudo nvme list-subsys -o json" | grep -q -E "${NQN1}|${NQN2}"; then
  echo "Phase 164 host controller residue remains" >&2
  exit 1
fi

write_summary "capability_restart_honesty_verified=true"
write_summary "cleanup_status=ok"
write_summary "next_recommendation=phase165_nvme_rdma_kubernetes_publish_attach"
write_summary "phase164_nvme_rdma_standalone_hardening_status=ok"

trap - EXIT
case "${WORK}" in
  /tmp/sw-block-phase164-*) sudo rm -rf -- "${WORK}" ;;
  *) echo "refusing unsafe Phase 164 work cleanup: ${WORK}" >&2; exit 1 ;;
esac
