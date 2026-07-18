#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
RUN_ID="${SW_BLOCK_PHASE163_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase163-nvme-rdma-standalone-live-gate}"
SUMMARY="${ARTIFACT_DIR}/phase163-nvme-rdma-standalone-live-summary.txt"
WORK="/tmp/sw-block-phase163-${RUN_ID}"
BIN="${WORK}/bin"
NQN="${SW_BLOCK_PHASE163_NQN:-nqn.2026-07.io.seaweedfs:phase163-${RUN_ID}}"
TARGET_IP="${SW_BLOCK_PHASE163_TARGET_IP:-10.0.0.3}"
TARGET_PORT="${SW_BLOCK_PHASE163_TARGET_PORT:-11631}"
RDMA_DEVICE="${SW_BLOCK_PHASE163_RDMA_DEVICE:-rocep1s0}"
RDMA_NETDEV="${SW_BLOCK_PHASE163_RDMA_NETDEV:-enp1s0np0}"
HOST_SSH_ADDR="${SW_BLOCK_PHASE163_HOST_SSH_ADDR:-192.168.1.181}"
HOST_SSH_USER="${SW_BLOCK_PHASE163_HOST_SSH_USER:-testdev}"
HOST_SSH_KEY="${SW_BLOCK_PHASE163_HOST_SSH_KEY:-/opt/work/testdev_key}"

MASTER_PID=""
VOLUME_PID=""
NBD_PATH=""

mkdir -p "${ARTIFACT_DIR}" "${BIN}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

cleanup() {
  set +e
  ssh -i "${HOST_SSH_KEY}" -o BatchMode=yes -o ConnectTimeout=5 \
    "${HOST_SSH_USER}@${HOST_SSH_ADDR}" \
    "sudo nvme disconnect -n '${NQN}' >/dev/null 2>&1 || true" >/dev/null 2>&1 || true
  if [[ -n "${VOLUME_PID}" ]]; then
    sudo kill -TERM "${VOLUME_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${MASTER_PID}" ]]; then
    kill -TERM "${MASTER_PID}" >/dev/null 2>&1 || true
  fi
  for _ in $(seq 1 50); do
    volume_alive=false
    master_alive=false
    [[ -n "${VOLUME_PID}" ]] && sudo kill -0 "${VOLUME_PID}" >/dev/null 2>&1 && volume_alive=true
    [[ -n "${MASTER_PID}" ]] && kill -0 "${MASTER_PID}" >/dev/null 2>&1 && master_alive=true
    [[ "${volume_alive}" == false && "${master_alive}" == false ]] && break
    sleep 0.1
  done
  [[ -n "${VOLUME_PID}" ]] && sudo kill -KILL "${VOLUME_PID}" >/dev/null 2>&1 || true
  [[ -n "${MASTER_PID}" ]] && kill -KILL "${MASTER_PID}" >/dev/null 2>&1 || true
  case "${WORK}" in
    /tmp/sw-block-phase163-*) sudo rm -rf -- "${WORK}" ;;
    *) echo "refusing unsafe Phase 163 work cleanup: ${WORK}" >&2 ;;
  esac
}
trap cleanup EXIT

write_summary "phase163_nvme_rdma_standalone_listener_impl_spike_status=running"
write_summary "rdma_listener_impl_attempted=true"
write_summary "rdma_implementation_path=kernel_nvmet_rdma_nbd_bridge"
write_summary "rdma_bind_ip=${TARGET_IP}"
write_summary "rdma_device=${RDMA_DEVICE}"
write_summary "rdma_netdev=${RDMA_NETDEV}"
write_summary "k8s_publish_attach_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"

[[ -d "/sys/class/infiniband/${RDMA_DEVICE}" ]]
[[ -d "/sys/class/infiniband/${RDMA_DEVICE}/device/net/${RDMA_NETDEV}" ]]
ip -4 -o addr show dev "${RDMA_NETDEV}" | grep -q " ${TARGET_IP}/"
ip route get "${TARGET_IP}" >"${ARTIFACT_DIR}/target-route.txt"

cd "${ROOT}"
go test ./core/frontend/nbd ./core/frontend/nvmerdma ./core/frontend/nvme ./cmd/blockvolume \
  >"${ARTIFACT_DIR}/go-test.stdout.txt" 2>"${ARTIFACT_DIR}/go-test.stderr.txt"
write_summary "go_test_nbd_nvmerdma_nvme_blockvolume=ok"
write_summary "tcp_behavior_unchanged=true"
write_summary "rdma_not_published_to_csi=true"

go build -o "${BIN}/blockmaster" ./cmd/blockmaster
go build -o "${BIN}/blockvolume" ./cmd/blockvolume
printf '%s\n' \
  'volumes:' \
  '  - volume_id: v1' \
  '    slots:' \
  '      - replica_id: r1' \
  '        server_id: s1' >"${WORK}/topology.yaml"
mkdir -p "${WORK}/master-store" "${WORK}/volume-store"

"${BIN}/blockmaster" \
  --authority-store "${WORK}/master-store" \
  --listen 127.0.0.1:16331 \
  --topology "${WORK}/topology.yaml" \
  --expected-slots-per-volume 1 \
  --t0-print-ready \
  >"${ARTIFACT_DIR}/blockmaster.log" 2>&1 &
MASTER_PID=$!
for _ in $(seq 1 100); do
  grep -q '"phase":"listening"' "${ARTIFACT_DIR}/blockmaster.log" 2>/dev/null && break
  kill -0 "${MASTER_PID}"
  sleep 0.1
done
grep -q '"phase":"listening"' "${ARTIFACT_DIR}/blockmaster.log"

sudo "${BIN}/blockvolume" \
  --master 127.0.0.1:16331 \
  --server-id s1 \
  --volume-id v1 \
  --replica-id r1 \
  --data-addr 127.0.0.1:16332 \
  --ctrl-addr 127.0.0.1:16333 \
  --status-addr 127.0.0.1:16334 \
  --heartbeat-interval 200ms \
  --t1-readiness \
  --durable-root "${WORK}/volume-store" \
  --durable-impl smartwal \
  --durable-blocks 8192 \
  --durable-blocksize 4096 \
  --nvme-listen "${TARGET_IP}:${TARGET_PORT}" \
  --nvme-transport rdma \
  --allow-external-nvme-bind \
  --nvme-subsysnqn "${NQN}" \
  --nvme-ns 1 \
  >"${ARTIFACT_DIR}/blockvolume.log" 2>&1 &
VOLUME_PID=$!

for _ in $(seq 1 300); do
  if ! sudo kill -0 "${VOLUME_PID}" >/dev/null 2>&1; then
    cat "${ARTIFACT_DIR}/blockvolume.log" >&2
    exit 1
  fi
  grep -q '"phase":"nvme-listening"' "${ARTIFACT_DIR}/blockvolume.log" 2>/dev/null && break
  sleep 0.1
done
grep -q '"phase":"nvme-listening"' "${ARTIFACT_DIR}/blockvolume.log"

for _ in $(seq 1 300); do
  if curl -fsS 'http://127.0.0.1:16334/status?volume=v1' \
      >"${ARTIFACT_DIR}/status.json" 2>/dev/null &&
      grep -q '"FrontendPrimaryReady":true' "${ARTIFACT_DIR}/status.json"; then
    break
  fi
  sleep 0.1
done
grep -q '"FrontendPrimaryReady":true' "${ARTIFACT_DIR}/status.json"
curl -fsS 'http://127.0.0.1:16334/status/frontend-capabilities?volume=v1' \
  >"${ARTIFACT_DIR}/capabilities.json"

python3 - "${ARTIFACT_DIR}/capabilities.json" <<'PY'
import json, sys
doc = json.load(open(sys.argv[1]))
rdma = next(x for x in doc["frontendTransports"] if x["transport"] == "rdma")
tcp = next(x for x in doc["frontendTransports"] if x["transport"] == "tcp")
assert rdma["supported"] is True
assert rdma["listenerImplemented"] is True
assert rdma["listenerStarted"] is True
assert rdma["startAllowed"] is True
assert rdma["reason"] == "implemented"
assert tcp["supported"] is True
assert tcp["listenerStarted"] is False
PY
write_summary "capability_endpoint_reports_rdma_supported=true"
write_summary "rdma_listener_started=true"

NBD_PATH="$(cat "/sys/kernel/config/nvmet/subsystems/${NQN}/namespaces/1/device_path")"
[[ -b "${NBD_PATH}" ]]
write_summary "backend_bridge_device=${NBD_PATH}"

ssh -i "${HOST_SSH_KEY}" -o BatchMode=yes "${HOST_SSH_USER}@${HOST_SSH_ADDR}" \
  "sudo bash -s" >"${ARTIFACT_DIR}/host-io.txt" <<HOST
set -euo pipefail
NQN='${NQN}'
nvme disconnect -n "\$NQN" >/dev/null 2>&1 || true
modprobe nvme-rdma
ip route get '${TARGET_IP}'
nvme connect -t rdma -a '${TARGET_IP}' -s '${TARGET_PORT}' -n "\$NQN"
DEV=''
for _ in \$(seq 1 100); do
  for p in /sys/class/nvme-subsystem/*/subsysnqn; do
    [[ -e "\$p" ]] || continue
    if [[ "\$(cat "\$p")" == "\$NQN" ]]; then
      base="\$(dirname "\$p")"
      for candidate in "\$base"/nvme*n*; do
        [[ -e "\$candidate" ]] || continue
        DEV="/dev/\$(basename "\$candidate")"
        break 2
      done
    fi
  done
  sleep 0.1
done
[[ -b "\$DEV" ]]
EXPECTED="\$(python3 -c 'import hashlib; print(hashlib.sha256((b"phase163-seaweed-rdma"*256)[:4096]).hexdigest())')"
python3 -c 'import sys; sys.stdout.buffer.write((b"phase163-seaweed-rdma"*256)[:4096])' | \
  dd of="\$DEV" bs=4096 count=1 conv=fsync status=none
nvme flush "\$DEV" -n 1
ACTUAL="\$(dd if="\$DEV" bs=4096 count=1 status=none | sha256sum | awk '{print \$1}')"
[[ "\$ACTUAL" == "\$EXPECTED" ]]
nvme list-subsys -o json
nvme disconnect -n "\$NQN"
printf 'linux_nvme_connect_rdma_succeeded=true\nstandalone_write_read_verified=true\nflush_verified=true\nhost_device=%s\n' "\$DEV"
HOST

grep -q '^linux_nvme_connect_rdma_succeeded=true$' "${ARTIFACT_DIR}/host-io.txt"
grep -q '^standalone_write_read_verified=true$' "${ARTIFACT_DIR}/host-io.txt"
grep -q '^flush_verified=true$' "${ARTIFACT_DIR}/host-io.txt"
grep -a -q 'durable: write observer dispatch lba=0 lsn=' "${ARTIFACT_DIR}/blockvolume.log"
write_summary "linux_nvme_connect_rdma_succeeded=true"
write_summary "standalone_write_read_verified=true"
write_summary "flush_verified=true"
write_summary "seaweed_backend_write_observed=true"

cleanup
VOLUME_PID=""
MASTER_PID=""
trap - EXIT

[[ ! -e "/sys/kernel/config/nvmet/subsystems/${NQN}" ]]
[[ ! -e "/sys/kernel/config/nvmet/ports/${TARGET_PORT}" ]]
if [[ -n "${NBD_PATH}" && -e "/sys/block/$(basename "${NBD_PATH}")/pid" ]]; then
  [[ ! -s "/sys/block/$(basename "${NBD_PATH}")/pid" ]]
fi
if ssh -i "${HOST_SSH_KEY}" -o BatchMode=yes "${HOST_SSH_USER}@${HOST_SSH_ADDR}" \
    "sudo nvme list-subsys -o json" | grep -q "${NQN}"; then
  echo "Phase 163 host controller residue remains for ${NQN}" >&2
  exit 1
fi

write_summary "disconnect_cleanup_status=ok"
write_summary "cleanup_status=ok"
write_summary "phase163_decision=standalone_nvme_rdma_live_io_supported"
write_summary "next_recommendation=phase164_nvme_rdma_standalone_hardening"
write_summary "phase163_nvme_rdma_standalone_listener_impl_spike_status=ok"
