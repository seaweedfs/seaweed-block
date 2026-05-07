#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
WORK_DIR="${SW_BLOCK_NVME_WORK_DIR:-/tmp/sw-block-nvme-os}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${WORK_DIR}/runs/${RUN_ID}}"
SUBSYS_NQN="${SW_BLOCK_NVME_NQN:-nqn.2026-05.io.seaweedfs:os-smoke-v1}"
NSID="${SW_BLOCK_NVME_NSID:-1}"
MOUNT_DIR="${SW_BLOCK_NVME_MOUNT_DIR:-${WORK_DIR}/mnt}"
BLOCKS="${SW_BLOCK_DURABLE_BLOCKS:-65536}"      # 256 MiB at 4 KiB.
BLOCK_SIZE="${SW_BLOCK_DURABLE_BLOCKSIZE:-4096}"
DURABLE_IMPL="${SW_BLOCK_DURABLE_IMPL:-smartwal}"
ITERATIONS="${SW_BLOCK_NVME_ITERATIONS:-1}"
STRESS="${SW_BLOCK_NVME_STRESS:-none}"          # none | fio | dd
FIO_SIZE="${SW_BLOCK_NVME_FIO_SIZE:-32m}"
FIO_RUNTIME="${SW_BLOCK_NVME_FIO_RUNTIME:-10}"
FIO_BS="${SW_BLOCK_NVME_FIO_BS:-4k}"
DD_BS="${SW_BLOCK_NVME_DD_BS:-1M}"
DD_COUNT="${SW_BLOCK_NVME_DD_COUNT:-32}"
COLLECT_ANA="${SW_BLOCK_NVME_COLLECT_ANA:-0}"

BIN_DIR="${SW_BLOCK_BIN_DIR:-${WORK_DIR}/bin}"
RUN_DIR="${WORK_DIR}/run"

mkdir -p "$ARTIFACT_DIR" "$BIN_DIR" "$RUN_DIR" "$MOUNT_DIR"

_USED_PORTS=()

pick_free_port() {
  local candidate
  if command -v python3 >/dev/null 2>&1; then
    while true; do
      candidate="$(python3 - <<'PY'
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
      if [[ ! " ${_USED_PORTS[*]} " =~ " ${candidate} " ]]; then
        _USED_PORTS+=("$candidate")
        printf '%s\n' "$candidate"
        return
      fi
    done
  fi
  while true; do
    candidate="$(shuf -i 20000-60999 -n 1)"
    if [[ ! " ${_USED_PORTS[*]} " =~ " ${candidate} " ]]; then
      _USED_PORTS+=("$candidate")
      printf '%s\n' "$candidate"
      return
    fi
  done
}

PORT="${SW_BLOCK_NVME_PORT:-$(pick_free_port)}"
MASTER_ADDR="${SW_BLOCK_MASTER_ADDR:-127.0.0.1:$(pick_free_port)}"
DATA_ADDR="${SW_BLOCK_DATA_ADDR:-127.0.0.1:$(pick_free_port)}"
CTRL_ADDR="${SW_BLOCK_CTRL_ADDR:-127.0.0.1:$(pick_free_port)}"
STATUS_ADDR="${SW_BLOCK_STATUS_ADDR:-127.0.0.1:$(pick_free_port)}"
MASTER_PID=""
VOLUME_PID=""

log() {
  printf '[nvme-os] %s\n' "$*" | tee -a "$ARTIFACT_DIR/run.log"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

disconnect_nqn() {
  sudo nvme disconnect -n "$SUBSYS_NQN" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
}

cleanup() {
  set +e
  log "cleanup"
  mountpoint -q "$MOUNT_DIR" && sudo umount "$MOUNT_DIR" >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  disconnect_nqn
  [[ -n "$VOLUME_PID" ]] && kill "$VOLUME_PID" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  [[ -n "$MASTER_PID" ]] && kill "$MASTER_PID" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  pkill -TERM -f "${BIN_DIR}/blockvolume" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  pkill -TERM -f "${BIN_DIR}/blockmaster" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  sleep 1
  [[ -n "$VOLUME_PID" ]] && kill -KILL "$VOLUME_PID" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  [[ -n "$MASTER_PID" ]] && kill -KILL "$MASTER_PID" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  pkill -KILL -f "${BIN_DIR}/blockvolume" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  pkill -KILL -f "${BIN_DIR}/blockmaster" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.after.json" 2>&1 || true
  pgrep -af "${BIN_DIR}/blockmaster|${BIN_DIR}/blockvolume" >"$ARTIFACT_DIR/processes.after.txt" 2>&1 || true
}
trap cleanup EXIT

find_nvme_device() {
  local nqn="$1"
  local json
  json="$(sudo nvme list-subsys -o json 2>/dev/null || true)"
  if [[ -n "$json" ]] && command -v python3 >/dev/null 2>&1; then
    python3 -c '
import json, re, sys
nqn = sys.argv[1]
raw = sys.stdin.read().strip()
if not raw:
    sys.exit(1)
try:
    doc = json.loads(raw)
except Exception:
    sys.exit(1)
if isinstance(doc, dict):
    hosts = [doc]
else:
    hosts = doc
for host in hosts:
    for sub in host.get("Subsystems", []):
        if sub.get("NQN") != nqn:
            continue
        for path in sub.get("Paths", []):
            name = path.get("Name") or ""
            transport = (path.get("Transport") or "").lower()
            state = (path.get("State") or "").lower()
            if name and transport == "tcp" and state in ("live", "connecting", ""):
                base = name.split("/")[-1]
                if re.fullmatch(r"nvme[0-9]+n[0-9]+", base):
                    print("/dev/" + base)
                elif re.fullmatch(r"nvme[0-9]+", base):
                    print("/dev/" + base + "n1")
                else:
                    print("/dev/" + base)
                sys.exit(0)
sys.exit(1)
' "$nqn" <<<"$json"
    return
  fi
  sudo nvme list 2>/dev/null | awk '/SeaweedFS|BlockVol|Seaweed/ {print $1; exit}'
}

wait_nvme_device() {
  local dev
  for _ in $(seq 1 150); do
    dev="$(find_nvme_device "$SUBSYS_NQN" || true)"
    if [[ -n "$dev" && -b "$dev" ]]; then
      printf '%s\n' "$dev"
      return 0
    fi
    sleep 0.2
  done
  return 1
}

require_cmd sudo
require_cmd nvme
require_cmd mkfs.ext4
require_cmd mount
require_cmd sha256sum
if [[ "$STRESS" == "fio" ]]; then
  require_cmd fio
fi
if [[ "$COLLECT_ANA" == "1" ]]; then
  require_cmd python3
fi

log "run_id=$RUN_ID"
log "root=$ROOT"
log "artifact_dir=$ARTIFACT_DIR"
log "nqn=$SUBSYS_NQN"
log "nsid=$NSID"
log "portal=127.0.0.1:${PORT}"
log "master=${MASTER_ADDR}"
log "data_addr=${DATA_ADDR}"
log "ctrl_addr=${CTRL_ADDR}"
log "status_addr=${STATUS_ADDR}"
log "size_blocks=${BLOCKS} block_size=${BLOCK_SIZE}"
log "durable_impl=${DURABLE_IMPL}"
log "iterations=${ITERATIONS}"
log "stress=${STRESS}"
log "fio_bs=${FIO_BS}"
log "dd_bs=${DD_BS} dd_count=${DD_COUNT}"
log "collect_ana=${COLLECT_ANA}"

cd "$ROOT"
git rev-parse --short HEAD >"$ARTIFACT_DIR/git-head.txt" 2>/dev/null || true

if [[ -x "${BIN_DIR}/blockmaster" && -x "${BIN_DIR}/blockvolume" ]]; then
  log "use prebuilt binaries from ${BIN_DIR}"
else
  require_cmd go
  log "build binaries"
  go build -o "${BIN_DIR}/blockmaster" ./cmd/blockmaster
  go build -o "${BIN_DIR}/blockvolume" ./cmd/blockvolume
fi

cat >"$ARTIFACT_DIR/topology.yaml" <<YAML
volumes:
  - volume_id: v1
    slots:
      - replica_id: r1
        server_id: s1
YAML

rm -rf "${RUN_DIR}/master-store" "${RUN_DIR}/volume-store"
mkdir -p "${RUN_DIR}/master-store" "${RUN_DIR}/volume-store"
pkill -KILL -f "${BIN_DIR}/blockvolume" >/dev/null 2>&1 || true
pkill -KILL -f "${BIN_DIR}/blockmaster" >/dev/null 2>&1 || true
disconnect_nqn
sudo modprobe nvme_tcp >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.before.json" 2>&1 || true

log "start blockmaster"
"${BIN_DIR}/blockmaster" \
  --authority-store "${RUN_DIR}/master-store" \
  --listen "$MASTER_ADDR" \
  --topology "$ARTIFACT_DIR/topology.yaml" \
  --expected-slots-per-volume 1 \
  --t0-print-ready \
  >"$ARTIFACT_DIR/blockmaster.log" 2>&1 &
MASTER_PID=$!
for _ in $(seq 1 100); do
  if ! kill -0 "$MASTER_PID" >/dev/null 2>&1; then
    cat "$ARTIFACT_DIR/blockmaster.log" >&2 || true
    echo "blockmaster exited during startup" >&2
    exit 1
  fi
  if grep -q '"component":"blockmaster".*"phase":"listening"' "$ARTIFACT_DIR/blockmaster.log"; then
    break
  fi
  sleep 0.1
done
if ! grep -q '"component":"blockmaster".*"phase":"listening"' "$ARTIFACT_DIR/blockmaster.log"; then
  cat "$ARTIFACT_DIR/blockmaster.log" >&2 || true
  echo "blockmaster did not report ready" >&2
  exit 1
fi

log "start blockvolume NVMe target"
"${BIN_DIR}/blockvolume" \
  --master "$MASTER_ADDR" \
  --server-id s1 \
  --volume-id v1 \
  --replica-id r1 \
  --data-addr "$DATA_ADDR" \
  --ctrl-addr "$CTRL_ADDR" \
  --status-addr "$STATUS_ADDR" \
  --heartbeat-interval 200ms \
  --t1-readiness \
  --durable-root "${RUN_DIR}/volume-store" \
  --durable-impl "$DURABLE_IMPL" \
  --durable-blocks "$BLOCKS" \
  --durable-blocksize "$BLOCK_SIZE" \
  --nvme-listen "127.0.0.1:${PORT}" \
  --nvme-subsysnqn "$SUBSYS_NQN" \
  --nvme-ns "$NSID" \
  >"$ARTIFACT_DIR/blockvolume.log" 2>&1 &
VOLUME_PID=$!

log "wait NVMe listener"
for _ in $(seq 1 100); do
  if ! kill -0 "$VOLUME_PID" >/dev/null 2>&1; then
    cat "$ARTIFACT_DIR/blockvolume.log" >&2 || true
    echo "blockvolume exited during startup" >&2
    exit 1
  fi
  if bash -c "</dev/tcp/127.0.0.1/${PORT}" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
if ! bash -c "</dev/tcp/127.0.0.1/${PORT}" >/dev/null 2>&1; then
  cat "$ARTIFACT_DIR/blockvolume.log" >&2 || true
  echo "NVMe listener did not open" >&2
  exit 1
fi

for i in $(seq 1 "$ITERATIONS"); do
  log "iteration ${i}/${ITERATIONS}: nvme connect"
  disconnect_nqn
  sudo nvme connect -t tcp -a 127.0.0.1 -s "$PORT" -n "$SUBSYS_NQN" \
    >"$ARTIFACT_DIR/nvme-connect.iter${i}.log" 2>&1
  DEV="$(wait_nvme_device)"
  log "iteration ${i}/${ITERATIONS}: device=${DEV}"
  sudo nvme list >"$ARTIFACT_DIR/nvme-list.iter${i}.txt" 2>&1 || true
  sudo nvme id-ctrl "$DEV" >"$ARTIFACT_DIR/nvme-id-ctrl.iter${i}.txt" 2>&1 || true
  sudo nvme id-ns "$DEV" >"$ARTIFACT_DIR/nvme-id-ns.iter${i}.txt" 2>&1 || true
  if [[ "$COLLECT_ANA" == "1" ]]; then
    log "iteration ${i}/${ITERATIONS}: collect ANA log page"
    sudo nvme get-log "$DEV" -i 0x0c -l 40 -b \
      >"$ARTIFACT_DIR/nvme-ana-log.iter${i}.bin" \
      2>"$ARTIFACT_DIR/nvme-ana-log.iter${i}.stderr"
    python3 - "$ARTIFACT_DIR/nvme-ana-log.iter${i}.bin" "$NSID" \
      >"$ARTIFACT_DIR/nvme-ana-log.iter${i}.summary" <<'PY'
import struct
import sys

path = sys.argv[1]
want_nsid = int(sys.argv[2], 0)
data = open(path, "rb").read()
if len(data) < 40:
    raise SystemExit(f"ANA log too short: {len(data)} bytes, want at least 40")

state_names = {
    0x01: "optimized",
    0x02: "non_optimized",
    0x03: "inaccessible",
    0x04: "persistent_loss",
    0x0F: "change",
}

change_count = struct.unpack_from("<Q", data, 0)[0]
group_count = struct.unpack_from("<H", data, 8)[0]
group_id = struct.unpack_from("<I", data, 16)[0]
nsid_count = struct.unpack_from("<I", data, 20)[0]
group_change_count = struct.unpack_from("<Q", data, 24)[0]
state = data[32]
nsid = struct.unpack_from("<I", data, 36)[0]

print(f"ana_change_count={change_count}")
print(f"ana_group_count={group_count}")
print(f"ana_group_id={group_id}")
print(f"ana_nsid_count={nsid_count}")
print(f"ana_group_change_count={group_change_count}")
print(f"ana_state=0x{state:02x} {state_names.get(state, 'unknown')}")
print(f"ana_nsid={nsid}")

if group_count != 1:
    raise SystemExit(f"ANA group count={group_count}, want 1")
if group_id == 0:
    raise SystemExit("ANA group id is 0")
if nsid_count != 1:
    raise SystemExit(f"ANA NSID count={nsid_count}, want 1")
if nsid != want_nsid:
    raise SystemExit(f"ANA NSID={nsid}, want {want_nsid}")
if state not in state_names:
    raise SystemExit(f"ANA state=0x{state:02x} is not recognized")
PY
  fi

  log "iteration ${i}/${ITERATIONS}: mkfs/mount"
  sudo mkfs.ext4 -F "$DEV" >"$ARTIFACT_DIR/mkfs.iter${i}.log" 2>&1
  sudo mkdir -p "$MOUNT_DIR"
  sudo mount "$DEV" "$MOUNT_DIR"

  log "iteration ${i}/${ITERATIONS}: checksum write/read"
  dd if=/dev/urandom of="$ARTIFACT_DIR/payload.iter${i}.bin" bs=4096 count=1 status=none
  sha256sum "$ARTIFACT_DIR/payload.iter${i}.bin" | awk '{print $1"  /tmp/sw-block-nvme-payload.bin"}' >"$ARTIFACT_DIR/payload.iter${i}.sha256"
  sudo cp "$ARTIFACT_DIR/payload.iter${i}.bin" "$MOUNT_DIR/payload.bin"
  sudo sync
  sudo cp "$MOUNT_DIR/payload.bin" /tmp/sw-block-nvme-payload.bin
  sudo chown "$(id -u):$(id -g)" /tmp/sw-block-nvme-payload.bin
  sha256sum -c "$ARTIFACT_DIR/payload.iter${i}.sha256" >"$ARTIFACT_DIR/sha256.iter${i}.log"
  rm -f /tmp/sw-block-nvme-payload.bin

  case "$STRESS" in
    none)
      ;;
    dd)
      log "iteration ${i}/${ITERATIONS}: dd stress"
      sudo dd if=/dev/zero of="$MOUNT_DIR/dd-stress.bin" bs="$DD_BS" count="$DD_COUNT" conv=fsync \
        >"$ARTIFACT_DIR/dd.iter${i}.log" 2>&1
      ;;
    fio)
      log "iteration ${i}/${ITERATIONS}: fio stress"
      sudo fio --name=sw-block-nvme-fio \
        --directory="$MOUNT_DIR" \
        --rw=randrw \
        --rwmixread=50 \
        --bs="$FIO_BS" \
        --ioengine=psync \
        --iodepth=1 \
        --numjobs=1 \
        --size="$FIO_SIZE" \
        --runtime="$FIO_RUNTIME" \
        --time_based \
        --fsync=1 \
        --group_reporting \
        >"$ARTIFACT_DIR/fio.iter${i}.log" 2>&1
      ;;
    *)
      echo "unknown SW_BLOCK_NVME_STRESS=${STRESS}" >&2
      exit 2
      ;;
  esac

  log "iteration ${i}/${ITERATIONS}: unmount/disconnect"
  sudo umount "$MOUNT_DIR"
  disconnect_nqn
done

sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.final.json" 2>&1 || true
if grep -q "$SUBSYS_NQN" "$ARTIFACT_DIR/nvme-list-subsys.final.json"; then
  echo "NVMe subsystem still present after disconnect: $SUBSYS_NQN" >&2
  exit 1
fi

log "PASS: ${ITERATIONS} x nvme connect mkfs mount write/read disconnect"
log "artifacts=${ARTIFACT_DIR}"
