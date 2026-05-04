#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
WORK_DIR="${SW_BLOCK_ISCSI_WORK_DIR:-/tmp/sw-block-iscsi-os}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${WORK_DIR}/runs/${RUN_ID}}"
IQN="${SW_BLOCK_ISCSI_IQN:-iqn.2026-05.io.seaweedfs:os-smoke-v1}"
PORT="${SW_BLOCK_ISCSI_PORT:-3267}"
MOUNT_DIR="${SW_BLOCK_ISCSI_MOUNT_DIR:-${WORK_DIR}/mnt}"
MASTER_ADDR="${SW_BLOCK_MASTER_ADDR:-127.0.0.1:19333}"
DATA_ADDR="${SW_BLOCK_DATA_ADDR:-127.0.0.1:19101}"
CTRL_ADDR="${SW_BLOCK_CTRL_ADDR:-127.0.0.1:19102}"
STATUS_ADDR="${SW_BLOCK_STATUS_ADDR:-127.0.0.1:19103}"
BLOCKS="${SW_BLOCK_DURABLE_BLOCKS:-65536}"      # 256 MiB at 4 KiB.
BLOCK_SIZE="${SW_BLOCK_DURABLE_BLOCKSIZE:-4096}"

BIN_DIR="${SW_BLOCK_BIN_DIR:-${WORK_DIR}/bin}"
RUN_DIR="${WORK_DIR}/run"
DEVLINK_GLOB="/dev/disk/by-path/*ip-127.0.0.1:${PORT}-iscsi-${IQN}-lun-0"

mkdir -p "$ARTIFACT_DIR" "$BIN_DIR" "$RUN_DIR" "$MOUNT_DIR"

log() {
  printf '[iscsi-os] %s\n' "$*" | tee -a "$ARTIFACT_DIR/run.log"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

cleanup() {
  set +e
  log "cleanup"
  mountpoint -q "$MOUNT_DIR" && sudo umount "$MOUNT_DIR" >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  sudo iscsiadm -m node -T "$IQN" -p "127.0.0.1:${PORT}" --logout >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  sudo iscsiadm -m node -T "$IQN" -p "127.0.0.1:${PORT}" -o delete >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  pkill -TERM -f "${BIN_DIR}/blockvolume" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  pkill -TERM -f "${BIN_DIR}/blockmaster" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  sleep 1
  pkill -KILL -f "${BIN_DIR}/blockvolume" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  pkill -KILL -f "${BIN_DIR}/blockmaster" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  sudo iscsiadm -m session >"$ARTIFACT_DIR/iscsi-sessions.after.txt" 2>&1 || true
}
trap cleanup EXIT

require_cmd sudo
require_cmd iscsiadm
require_cmd mkfs.ext4
require_cmd mount
require_cmd sha256sum

log "run_id=$RUN_ID"
log "root=$ROOT"
log "artifact_dir=$ARTIFACT_DIR"
log "iqn=$IQN"
log "portal=127.0.0.1:${PORT}"
log "size_blocks=${BLOCKS} block_size=${BLOCK_SIZE}"

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
sudo iscsiadm -m node -T "$IQN" -p "127.0.0.1:${PORT}" --logout >/dev/null 2>&1 || true
sudo iscsiadm -m node -T "$IQN" -p "127.0.0.1:${PORT}" -o delete >/dev/null 2>&1 || true
sudo iscsiadm -m session >"$ARTIFACT_DIR/iscsi-sessions.before.txt" 2>&1 || true

log "start blockmaster"
setsid -f "${BIN_DIR}/blockmaster" \
  --authority-store "${RUN_DIR}/master-store" \
  --listen "$MASTER_ADDR" \
  --topology "$ARTIFACT_DIR/topology.yaml" \
  --expected-slots-per-volume 1 \
  >"$ARTIFACT_DIR/blockmaster.log" 2>&1
sleep 1

log "start blockvolume iSCSI target"
setsid -f "${BIN_DIR}/blockvolume" \
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
  --durable-impl smartwal \
  --durable-blocks "$BLOCKS" \
  --durable-blocksize "$BLOCK_SIZE" \
  --iscsi-listen "127.0.0.1:${PORT}" \
  --iscsi-iqn "$IQN" \
  >"$ARTIFACT_DIR/blockvolume.log" 2>&1

log "wait iSCSI listener"
for _ in $(seq 1 100); do
  if bash -c "</dev/tcp/127.0.0.1/${PORT}" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
bash -c "</dev/tcp/127.0.0.1/${PORT}" >/dev/null 2>&1
sleep 2

log "iscsi discovery/login"
sudo iscsiadm -m discovery -t sendtargets -p "127.0.0.1:${PORT}" | tee "$ARTIFACT_DIR/discovery.log"
sudo iscsiadm -m node -T "$IQN" -p "127.0.0.1:${PORT}" --login | tee "$ARTIFACT_DIR/login.log"

log "wait kernel block device"
DEV=""
for _ in $(seq 1 100); do
  DEV="$(ls $DEVLINK_GLOB 2>/dev/null | head -n1 || true)"
  if [[ -n "$DEV" ]]; then
    break
  fi
  sleep 0.1
done
if [[ -z "$DEV" ]]; then
  ls -l /dev/disk/by-path >"$ARTIFACT_DIR/by-path.txt" 2>&1 || true
  dmesg | tail -n 120 >"$ARTIFACT_DIR/dmesg.tail.txt" 2>&1 || true
  echo "device not found for IQN ${IQN}" >&2
  exit 1
fi
REAL_DEV="$(readlink -f "$DEV")"
log "device=$DEV real=$REAL_DEV"
lsblk "$REAL_DEV" >"$ARTIFACT_DIR/lsblk.before.txt" 2>&1 || true

log "mkfs.ext4"
sudo mkfs.ext4 -F "$REAL_DEV" >"$ARTIFACT_DIR/mkfs.log" 2>&1

log "mount/write/read"
sudo mount "$REAL_DEV" "$MOUNT_DIR"
sudo dd if=/dev/urandom of="$MOUNT_DIR/payload.bin" bs=4096 count=8 status=none
sync
sudo sha256sum "$MOUNT_DIR/payload.bin" | tee "$ARTIFACT_DIR/payload.sha256"
sudo sha256sum -c "$ARTIFACT_DIR/payload.sha256" | tee "$ARTIFACT_DIR/sha256-check.log"
sudo umount "$MOUNT_DIR"

log "logout"
sudo iscsiadm -m node -T "$IQN" -p "127.0.0.1:${PORT}" --logout | tee "$ARTIFACT_DIR/logout.log"
sudo iscsiadm -m node -T "$IQN" -p "127.0.0.1:${PORT}" -o delete >>"$ARTIFACT_DIR/logout.log" 2>&1 || true
sudo iscsiadm -m session >"$ARTIFACT_DIR/iscsi-sessions.final.txt" 2>&1 || true
if ! grep -q "No active sessions" "$ARTIFACT_DIR/iscsi-sessions.final.txt"; then
  cat "$ARTIFACT_DIR/iscsi-sessions.final.txt"
  exit 1
fi

log "PASS: iscsiadm mkfs mount write/read logout"
log "artifacts=$ARTIFACT_DIR"
