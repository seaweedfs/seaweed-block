#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
WORK_DIR="${SW_BLOCK_ISCSI_FAILOVER_WORK_DIR:-/tmp/sw-block-iscsi-failover}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${WORK_DIR}/runs/${RUN_ID}}"
IQN="${SW_BLOCK_ISCSI_IQN:-iqn.2026-05.io.seaweedfs:failover-v1}"
PORT1="${SW_BLOCK_ISCSI_PORT1:-3280}"
PORT2="${SW_BLOCK_ISCSI_PORT2:-3281}"
MASTER_ADDR="${SW_BLOCK_MASTER_ADDR:-127.0.0.1:19633}"
R1_DATA_ADDR="${SW_BLOCK_R1_DATA_ADDR:-127.0.0.1:19601}"
R1_CTRL_ADDR="${SW_BLOCK_R1_CTRL_ADDR:-127.0.0.1:19602}"
R1_STATUS_ADDR="${SW_BLOCK_R1_STATUS_ADDR:-127.0.0.1:19603}"
R2_DATA_ADDR="${SW_BLOCK_R2_DATA_ADDR:-127.0.0.1:19611}"
R2_CTRL_ADDR="${SW_BLOCK_R2_CTRL_ADDR:-127.0.0.1:19612}"
R2_STATUS_ADDR="${SW_BLOCK_R2_STATUS_ADDR:-127.0.0.1:19613}"
BLOCKS="${SW_BLOCK_DURABLE_BLOCKS:-65536}"
BLOCK_SIZE="${SW_BLOCK_DURABLE_BLOCKSIZE:-4096}"
BIN_DIR="${SW_BLOCK_BIN_DIR:-${WORK_DIR}/bin}"
RUN_DIR="${WORK_DIR}/run"
MOUNT_DIR="${SW_BLOCK_ISCSI_MOUNT_DIR:-${WORK_DIR}/mnt}"

mkdir -p "$ARTIFACT_DIR" "$BIN_DIR" "$RUN_DIR" "$MOUNT_DIR"

log() {
  printf '[iscsi-failover] %s\n' "$*" | tee -a "$ARTIFACT_DIR/run.log"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

portal_for() {
  printf '127.0.0.1:%s' "$1"
}

delete_iscsi_node() {
  local port="$1"
  sudo iscsiadm -m node -T "$IQN" -p "$(portal_for "$port")" --logout >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  sudo iscsiadm -m node -T "$IQN" -p "$(portal_for "$port")" -o delete >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  sudo iscsiadm -m discoverydb -t sendtargets -p "$(portal_for "$port")" -o delete >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
}

cleanup() {
  set +e
  log "cleanup"
  mountpoint -q "$MOUNT_DIR" && sudo umount "$MOUNT_DIR" >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  if [[ -s "$ARTIFACT_DIR/multipath-map-name.txt" ]]; then
    sudo multipath -f "$(cat "$ARTIFACT_DIR/multipath-map-name.txt")" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  fi
  delete_iscsi_node "$PORT1"
  delete_iscsi_node "$PORT2"
  pkill -TERM -f "${BIN_DIR}/blockvolume" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  pkill -TERM -f "${BIN_DIR}/blockmaster" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  sleep 1
  pkill -KILL -f "${BIN_DIR}/blockvolume" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  pkill -KILL -f "${BIN_DIR}/blockmaster" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  sudo multipath -ll >"$ARTIFACT_DIR/multipath.after.txt" 2>&1 || true
  sudo iscsiadm -m session >"$ARTIFACT_DIR/iscsi-sessions.after.txt" 2>&1 || true
}
trap cleanup EXIT

require_cmd sudo
require_cmd curl
require_cmd iscsiadm
require_cmd multipath
require_cmd sg_rtpg
require_cmd mkfs.ext4
require_cmd mount
require_cmd sha256sum
require_cmd python3

log "run_id=$RUN_ID"
log "root=$ROOT"
log "artifact_dir=$ARTIFACT_DIR"
log "iqn=$IQN"
log "portals=$(portal_for "$PORT1"),$(portal_for "$PORT2")"

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
      - replica_id: r2
        server_id: s2
YAML

rm -rf "${RUN_DIR}/master-store" "${RUN_DIR}/r1-store" "${RUN_DIR}/r2-store"
mkdir -p "${RUN_DIR}/master-store" "${RUN_DIR}/r1-store" "${RUN_DIR}/r2-store"
pkill -KILL -f "${BIN_DIR}/blockvolume" >/dev/null 2>&1 || true
pkill -KILL -f "${BIN_DIR}/blockmaster" >/dev/null 2>&1 || true
delete_iscsi_node "$PORT1"
delete_iscsi_node "$PORT2"
sudo iscsiadm -m session >"$ARTIFACT_DIR/iscsi-sessions.before.txt" 2>&1 || true
sudo multipath -ll >"$ARTIFACT_DIR/multipath.before.txt" 2>&1 || true

log "start blockmaster"
setsid -f "${BIN_DIR}/blockmaster" \
  --authority-store "${RUN_DIR}/master-store" \
  --listen "$MASTER_ADDR" \
  --topology "$ARTIFACT_DIR/topology.yaml" \
  --expected-slots-per-volume 2 \
  --freshness-window 800ms \
  --pending-grace 100ms \
  >"$ARTIFACT_DIR/blockmaster.log" 2>&1
sleep 1

start_blockvolume() {
  local replica="$1"
  local server="$2"
  local port="$3"
  local data_addr="$4"
  local ctrl_addr="$5"
  local status_addr="$6"
  local store="$7"
  local log_file="$8"

  setsid -f "${BIN_DIR}/blockvolume" \
    --master "$MASTER_ADDR" \
    --server-id "$server" \
    --volume-id v1 \
    --replica-id "$replica" \
    --data-addr "$data_addr" \
    --ctrl-addr "$ctrl_addr" \
    --status-addr "$status_addr" \
    --heartbeat-interval 200ms \
    --t1-readiness \
    --durable-root "$store" \
    --durable-impl smartwal \
    --durable-blocks "$BLOCKS" \
    --durable-blocksize "$BLOCK_SIZE" \
    --iscsi-listen "127.0.0.1:${port}" \
    --iscsi-iqn "$IQN" \
    >"$log_file" 2>&1
}

wait_port() {
  local port="$1"
  for _ in $(seq 1 100); do
    if bash -c "</dev/tcp/127.0.0.1/${port}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "timed out waiting for port ${port}" >&2
  exit 1
}

wait_status_healthy() {
  local status_addr="$1"
  local replica="$2"
  local min_epoch="$3"
  local out="$ARTIFACT_DIR/status-${replica}-healthy.json"
  for _ in $(seq 1 120); do
    if curl -fsS "http://${status_addr}/status?volume=v1" >"$out.tmp" 2>/dev/null; then
      if python3 - "$out.tmp" "$replica" "$min_epoch" <<'PY'
import json, sys
path, replica, min_epoch = sys.argv[1], sys.argv[2], int(sys.argv[3])
body = json.load(open(path))
healthy = bool(body.get("Healthy"))
rid = str(body.get("ReplicaID", ""))
epoch = int(body.get("Epoch", 0))
sys.exit(0 if healthy and rid == replica and epoch >= min_epoch else 1)
PY
      then
        mv "$out.tmp" "$out"
        return 0
      fi
    fi
    sleep 0.25
  done
  curl -fsS "http://${status_addr}/status?volume=v1" >"$ARTIFACT_DIR/status-${replica}-last.json" 2>/dev/null || true
  echo "timed out waiting for ${replica} Healthy epoch>=${min_epoch}" >&2
  exit 1
}

pid_for_replica() {
  local replica="$1"
  pgrep -f "${BIN_DIR}/blockvolume.*--replica-id ${replica}" | head -n1
}

log "start r1 active path"
start_blockvolume r1 s1 "$PORT1" "$R1_DATA_ADDR" "$R1_CTRL_ADDR" "$R1_STATUS_ADDR" \
  "${RUN_DIR}/r1-store" "$ARTIFACT_DIR/blockvolume-r1.log"
wait_port "$PORT1"
sleep 2

log "start r2 standby path"
start_blockvolume r2 s2 "$PORT2" "$R2_DATA_ADDR" "$R2_CTRL_ADDR" "$R2_STATUS_ADDR" \
  "${RUN_DIR}/r2-store" "$ARTIFACT_DIR/blockvolume-r2.log"
wait_port "$PORT2"
sleep 3
wait_status_healthy "$R1_STATUS_ADDR" r1 1

discover_login() {
  local port="$1"
  local name="$2"
  log "iscsi discovery/login ${name}"
  sudo iscsiadm -m discovery -t sendtargets -p "$(portal_for "$port")" | tee "$ARTIFACT_DIR/discovery-${name}.log"
  sudo iscsiadm -m node -T "$IQN" -p "$(portal_for "$port")" --login | tee "$ARTIFACT_DIR/login-${name}.log"
}

discover_login "$PORT1" r1
discover_login "$PORT2" r2
sudo iscsiadm -m session -P 3 >"$ARTIFACT_DIR/iscsi-session-P3.before-failover.txt" 2>&1 || true

find_devlink() {
  local port="$1"
  local pattern="/dev/disk/by-path/*ip-127.0.0.1:${port}-iscsi-${IQN}-lun-0"
  local dev=""
  for _ in $(seq 1 120); do
    dev="$(compgen -G "$pattern" | head -n1 || true)"
    if [[ -n "$dev" ]]; then
      printf '%s' "$dev"
      return 0
    fi
    sleep 0.1
  done
  ls -l /dev/disk/by-path >"$ARTIFACT_DIR/by-path.txt" 2>&1 || true
  (sudo dmesg | tail -n 160) >"$ARTIFACT_DIR/dmesg.tail.txt" 2>&1 || true
  echo "device not found for portal $(portal_for "$port")" >&2
  exit 1
}

dev1="$(find_devlink "$PORT1")"
dev2="$(find_devlink "$PORT2")"
real1="$(readlink -f "$dev1")"
real2="$(readlink -f "$dev2")"
base1="$(basename "$real1")"
base2="$(basename "$real2")"
log "r1_device=$dev1 real=$real1"
log "r2_device=$dev2 real=$real2"

sudo sg_rtpg "$real1" >"$ARTIFACT_DIR/sg_rtpg.r1.before.txt" 2>&1
sudo sg_rtpg "$real2" >"$ARTIFACT_DIR/sg_rtpg.r2.before.txt" 2>&1

log "ask multipath to group both paths"
sudo multipath "$real1" >"$ARTIFACT_DIR/multipath.real1.txt" 2>&1 || true
sudo multipath "$real2" >"$ARTIFACT_DIR/multipath.real2.txt" 2>&1 || true
sudo multipath -r >"$ARTIFACT_DIR/multipath.reload.before.txt" 2>&1 || true
sleep 2
sudo multipath -ll >"$ARTIFACT_DIR/multipath.ll.before.txt" 2>&1 || true

if ! awk -v a="$base1" -v b="$base2" '
  /^[[:alnum:]_.:-]+[[:space:]]+\(/ {
    if (seen && hasA && hasB) {
      print map
      found=1
      exit
    }
    seen=1
    map=$1
    hasA=0
    hasB=0
  }
  {
    if (index($0, a)) hasA=1
    if (index($0, b)) hasB=1
  }
  END {
    if (!found && seen && hasA && hasB) {
      print map
      found=1
    }
    exit(found ? 0 : 1)
  }
' "$ARTIFACT_DIR/multipath.ll.before.txt" >"$ARTIFACT_DIR/multipath-map-name.txt"; then
  cat "$ARTIFACT_DIR/multipath.ll.before.txt" >&2
  echo "multipath did not show one map containing both ${base1} and ${base2}" >&2
  exit 1
fi

map_name="$(cat "$ARTIFACT_DIR/multipath-map-name.txt")"
map_dev="/dev/mapper/${map_name}"
log "multipath_map=${map_name} dev=${map_dev}"
for _ in $(seq 1 80); do
  [[ -b "$map_dev" ]] && break
  sleep 0.1
done
if [[ ! -b "$map_dev" ]]; then
  echo "multipath map device ${map_dev} did not appear" >&2
  exit 1
fi

log "mkfs/mount multipath device"
sudo mkfs.ext4 -F "$map_dev" >"$ARTIFACT_DIR/mkfs.log" 2>&1
sudo mount "$map_dev" "$MOUNT_DIR"

log "write pre-failover payload"
sudo dd if=/dev/urandom of="$MOUNT_DIR/pre.bin" bs=4096 count=64 status=none
sync
sudo sha256sum "$MOUNT_DIR/pre.bin" | tee "$ARTIFACT_DIR/pre.sha256"

r1_pid="$(pid_for_replica r1 || true)"
if [[ -z "$r1_pid" ]]; then
  echo "could not find r1 blockvolume pid" >&2
  exit 1
fi
log "kill active r1 pid=${r1_pid}"
kill -TERM "$r1_pid"
sleep 1
if kill -0 "$r1_pid" >/dev/null 2>&1; then
  kill -KILL "$r1_pid" >/dev/null 2>&1 || true
fi

log "wait r2 failover"
wait_status_healthy "$R2_STATUS_ADDR" r2 2
sudo sg_rtpg "$real2" >"$ARTIFACT_DIR/sg_rtpg.r2.after.txt" 2>&1 || true
sudo multipath -r >"$ARTIFACT_DIR/multipath.reload.after.txt" 2>&1 || true
sudo multipath -ll >"$ARTIFACT_DIR/multipath.ll.after.txt" 2>&1 || true
sudo iscsiadm -m session -P 3 >"$ARTIFACT_DIR/iscsi-session-P3.after-failover.txt" 2>&1 || true

log "verify mounted workload after failover"
sudo sha256sum -c "$ARTIFACT_DIR/pre.sha256" | tee "$ARTIFACT_DIR/pre-check-after-failover.log"
sudo dd if=/dev/urandom of="$MOUNT_DIR/post.bin" bs=4096 count=64 status=none
sync
sudo sha256sum "$MOUNT_DIR/post.bin" | tee "$ARTIFACT_DIR/post.sha256"
sudo sha256sum -c "$ARTIFACT_DIR/post.sha256" | tee "$ARTIFACT_DIR/post-check.log"

log "unmount"
sudo umount "$MOUNT_DIR"

log "logout both paths"
delete_iscsi_node "$PORT1"
delete_iscsi_node "$PORT2"
sudo iscsiadm -m session >"$ARTIFACT_DIR/iscsi-sessions.final.txt" 2>&1 || true
if ! grep -q "No active sessions" "$ARTIFACT_DIR/iscsi-sessions.final.txt"; then
  cat "$ARTIFACT_DIR/iscsi-sessions.final.txt"
  exit 1
fi

log "PASS: mounted multipath workload read/wrote through r1->r2 failover"
log "artifacts=$ARTIFACT_DIR"
