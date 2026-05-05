#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
WORK_DIR="${SW_BLOCK_ISCSI_WORK_DIR:-/tmp/sw-block-iscsi-os}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${WORK_DIR}/runs/${RUN_ID}}"
IQN="${SW_BLOCK_ISCSI_IQN:-iqn.2026-05.io.seaweedfs:os-smoke-v1}"
PORT="${SW_BLOCK_ISCSI_PORT:-3267}"
PORTAL_ADDR="${SW_BLOCK_ISCSI_PORTAL_ADDR:-}"
DATAOUT_TIMEOUT="${SW_BLOCK_ISCSI_DATAOUT_TIMEOUT:-}"
CHAP_USERNAME="${SW_BLOCK_ISCSI_CHAP_USERNAME:-}"
CHAP_SECRET="${SW_BLOCK_ISCSI_CHAP_SECRET:-}"
CHAP_BAD_SECRET="${SW_BLOCK_ISCSI_CHAP_BAD_SECRET:-}"
MOUNT_DIR="${SW_BLOCK_ISCSI_MOUNT_DIR:-${WORK_DIR}/mnt}"
MASTER_ADDR="${SW_BLOCK_MASTER_ADDR:-127.0.0.1:19333}"
DATA_ADDR="${SW_BLOCK_DATA_ADDR:-127.0.0.1:19101}"
CTRL_ADDR="${SW_BLOCK_CTRL_ADDR:-127.0.0.1:19102}"
STATUS_ADDR="${SW_BLOCK_STATUS_ADDR:-127.0.0.1:19103}"
BLOCKS="${SW_BLOCK_DURABLE_BLOCKS:-65536}"      # 256 MiB at 4 KiB.
BLOCK_SIZE="${SW_BLOCK_DURABLE_BLOCKSIZE:-4096}"
ITERATIONS="${SW_BLOCK_ISCSI_ITERATIONS:-1}"
STRESS="${SW_BLOCK_ISCSI_STRESS:-none}"         # none | fio | dd
FIO_SIZE="${SW_BLOCK_ISCSI_FIO_SIZE:-32m}"
FIO_RUNTIME="${SW_BLOCK_ISCSI_FIO_RUNTIME:-10}"

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
  sudo iscsiadm -m discoverydb -t sendtargets -p "127.0.0.1:${PORT}" -o delete >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
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
if [[ "$STRESS" == "fio" ]]; then
  require_cmd fio
fi

log "run_id=$RUN_ID"
log "root=$ROOT"
log "artifact_dir=$ARTIFACT_DIR"
log "iqn=$IQN"
log "portal=127.0.0.1:${PORT}"
if [[ -n "$PORTAL_ADDR" ]]; then
  log "advertised_portal=$PORTAL_ADDR"
fi
if [[ -n "$DATAOUT_TIMEOUT" ]]; then
  log "dataout_timeout=$DATAOUT_TIMEOUT"
fi
if [[ -n "$CHAP_USERNAME" || -n "$CHAP_SECRET" ]]; then
  if [[ -z "$CHAP_USERNAME" || -z "$CHAP_SECRET" ]]; then
    echo "SW_BLOCK_ISCSI_CHAP_USERNAME and SW_BLOCK_ISCSI_CHAP_SECRET must be set together" >&2
    exit 2
  fi
  log "chap=enabled username=${CHAP_USERNAME}"
  if [[ -n "$CHAP_BAD_SECRET" ]]; then
    log "chap_negative_login=enabled"
  fi
fi
log "size_blocks=${BLOCKS} block_size=${BLOCK_SIZE}"
log "iterations=${ITERATIONS}"
log "stress=${STRESS}"

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
sudo iscsiadm -m discoverydb -t sendtargets -p "127.0.0.1:${PORT}" -o delete >/dev/null 2>&1 || true
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
blockvolume_iscsi_args=(
  --iscsi-listen "127.0.0.1:${PORT}"
  --iscsi-iqn "$IQN"
)
if [[ -n "$PORTAL_ADDR" ]]; then
  blockvolume_iscsi_args+=(--iscsi-portal-addr "$PORTAL_ADDR")
fi
if [[ -n "$DATAOUT_TIMEOUT" ]]; then
  blockvolume_iscsi_args+=(--iscsi-dataout-timeout "$DATAOUT_TIMEOUT")
fi
if [[ -n "$CHAP_SECRET" ]]; then
  blockvolume_iscsi_args+=(--iscsi-chap-username "$CHAP_USERNAME")
  blockvolume_iscsi_args+=(--iscsi-chap-secret "$CHAP_SECRET")
fi

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
  "${blockvolume_iscsi_args[@]}" \
  >"$ARTIFACT_DIR/blockvolume.log" 2>&1

log "wait iSCSI listener"
for _ in $(seq 1 100); do
  if bash -c "</dev/tcp/127.0.0.1/${PORT}" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
sleep 2

run_iteration() {
  local iter="$1"
  local suffix="iter${iter}"

  log "iteration ${iter}: iscsi discovery/login"
  if [[ -n "$CHAP_SECRET" ]]; then
    sudo iscsiadm -m discovery -t sendtargets -p "127.0.0.1:${PORT}" | tee "$ARTIFACT_DIR/discovery.${suffix}.log"
    sudo iscsiadm -m node -T "$IQN" -p "127.0.0.1:${PORT}" --op=update -n node.session.auth.authmethod -v CHAP >>"$ARTIFACT_DIR/node-auth.${suffix}.log" 2>&1
    sudo iscsiadm -m node -T "$IQN" -p "127.0.0.1:${PORT}" --op=update -n node.session.auth.username -v "$CHAP_USERNAME" >>"$ARTIFACT_DIR/node-auth.${suffix}.log" 2>&1
    sudo iscsiadm -m node -T "$IQN" -p "127.0.0.1:${PORT}" --op=update -n node.session.auth.password -v "$CHAP_SECRET" >>"$ARTIFACT_DIR/node-auth.${suffix}.log" 2>&1
    if [[ "$iter" == "1" && -n "$CHAP_BAD_SECRET" ]]; then
      log "iteration ${iter}: verify wrong CHAP secret fails"
      sudo iscsiadm -m node -T "$IQN" -p "127.0.0.1:${PORT}" --op=update -n node.session.auth.password -v "$CHAP_BAD_SECRET" >>"$ARTIFACT_DIR/node-auth-bad.${suffix}.log" 2>&1
      if sudo iscsiadm -m node -T "$IQN" -p "127.0.0.1:${PORT}" --login >"$ARTIFACT_DIR/login-bad.${suffix}.log" 2>&1; then
        echo "wrong CHAP secret unexpectedly logged in" >&2
        exit 1
      fi
      sudo iscsiadm -m session >"$ARTIFACT_DIR/iscsi-sessions.bad-auth.${suffix}.txt" 2>&1 || true
      sudo iscsiadm -m node -T "$IQN" -p "127.0.0.1:${PORT}" --op=update -n node.session.auth.password -v "$CHAP_SECRET" >>"$ARTIFACT_DIR/node-auth.${suffix}.log" 2>&1
    fi
  else
    sudo iscsiadm -m discovery -t sendtargets -p "127.0.0.1:${PORT}" | tee "$ARTIFACT_DIR/discovery.${suffix}.log"
  fi
  sudo iscsiadm -m node -T "$IQN" -p "127.0.0.1:${PORT}" --login | tee "$ARTIFACT_DIR/login.${suffix}.log"

  log "iteration ${iter}: wait kernel block device"
  local dev=""
  for _ in $(seq 1 100); do
    dev="$(ls $DEVLINK_GLOB 2>/dev/null | head -n1 || true)"
    if [[ -n "$dev" ]]; then
      break
    fi
    sleep 0.1
  done
  if [[ -z "$dev" ]]; then
    ls -l /dev/disk/by-path >"$ARTIFACT_DIR/by-path.${suffix}.txt" 2>&1 || true
    (sudo dmesg | tail -n 120) >"$ARTIFACT_DIR/dmesg.${suffix}.tail.txt" 2>&1 || true
    echo "device not found for IQN ${IQN} on iteration ${iter}" >&2
    exit 1
  fi
  local real_dev
  real_dev="$(readlink -f "$dev")"
  log "iteration ${iter}: device=$dev real=$real_dev"
  lsblk "$real_dev" >"$ARTIFACT_DIR/lsblk.${suffix}.txt" 2>&1 || true

  log "iteration ${iter}: mkfs.ext4"
  sudo mkfs.ext4 -F "$real_dev" >"$ARTIFACT_DIR/mkfs.${suffix}.log" 2>&1

  log "iteration ${iter}: mount/write/read"
  sudo mount "$real_dev" "$MOUNT_DIR"
  sudo dd if=/dev/urandom of="$MOUNT_DIR/payload.${suffix}.bin" bs=4096 count=8 status=none
  sync
  sudo sha256sum "$MOUNT_DIR/payload.${suffix}.bin" | tee "$ARTIFACT_DIR/payload.${suffix}.sha256"
  sudo sha256sum -c "$ARTIFACT_DIR/payload.${suffix}.sha256" | tee "$ARTIFACT_DIR/sha256-check.${suffix}.log"

  case "$STRESS" in
    none)
      ;;
    fio)
      log "iteration ${iter}: fio randrw stress"
      sudo fio \
        --name="sw-block-${suffix}" \
        --directory="$MOUNT_DIR" \
        --filename="fio-${suffix}.dat" \
        --size="$FIO_SIZE" \
        --rw=randrw \
        --rwmixread=50 \
        --bs=4k \
        --ioengine=sync \
        --direct=0 \
        --fsync=1 \
        --runtime="$FIO_RUNTIME" \
        --time_based \
        --group_reporting \
        --output="$ARTIFACT_DIR/fio.${suffix}.log"
      ;;
    dd)
      log "iteration ${iter}: dd sequential stress"
      sudo dd if=/dev/urandom of="$MOUNT_DIR/dd-${suffix}.bin" bs=1M count=32 conv=fsync status=none 2>"$ARTIFACT_DIR/dd.${suffix}.log"
      ;;
    *)
      echo "unsupported SW_BLOCK_ISCSI_STRESS=${STRESS} (want none, fio, or dd)" >&2
      exit 2
      ;;
  esac

  sudo umount "$MOUNT_DIR"

  log "iteration ${iter}: logout"
  sudo iscsiadm -m node -T "$IQN" -p "127.0.0.1:${PORT}" --logout | tee "$ARTIFACT_DIR/logout.${suffix}.log"
  sudo iscsiadm -m node -T "$IQN" -p "127.0.0.1:${PORT}" -o delete >>"$ARTIFACT_DIR/logout.${suffix}.log" 2>&1 || true
  sudo iscsiadm -m session >"$ARTIFACT_DIR/iscsi-sessions.${suffix}.txt" 2>&1 || true
  if ! grep -q "No active sessions" "$ARTIFACT_DIR/iscsi-sessions.${suffix}.txt"; then
    cat "$ARTIFACT_DIR/iscsi-sessions.${suffix}.txt"
    exit 1
  fi
}

for iter in $(seq 1 "$ITERATIONS"); do
  run_iteration "$iter"
done

cp "$ARTIFACT_DIR/iscsi-sessions.iter${ITERATIONS}.txt" "$ARTIFACT_DIR/iscsi-sessions.final.txt"

log "PASS: ${ITERATIONS} x iscsiadm mkfs mount write/read logout"
log "artifacts=$ARTIFACT_DIR"
