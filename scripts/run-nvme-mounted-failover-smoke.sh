#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
WORK_DIR="${SW_BLOCK_NVME_FAILOVER_WORK_DIR:-/tmp/sw-block-nvme-failover}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${WORK_DIR}/runs/${RUN_ID}}"
SOAK_ITERATIONS="${SW_BLOCK_NVME_SOAK_ITERATIONS:-0}"
if [[ "$SOAK_ITERATIONS" != "0" ]]; then
  SUMMARY="${ARTIFACT_DIR}/phase101-nvme-soak-summary.txt"
else
  SUMMARY="${ARTIFACT_DIR}/phase101-nvme-path-failure-summary.txt"
fi
SUBSYS_NQN="${SW_BLOCK_NVME_NQN:-nqn.2026-05.io.seaweedfs:failover-v1}"
NSID="${SW_BLOCK_NVME_NSID:-1}"
BLOCKS="${SW_BLOCK_DURABLE_BLOCKS:-65536}"
BLOCK_SIZE="${SW_BLOCK_DURABLE_BLOCKSIZE:-4096}"
DURABLE_IMPL="${SW_BLOCK_DURABLE_IMPL:-smartwal}"
BIN_DIR="${SW_BLOCK_BIN_DIR:-${WORK_DIR}/bin}"
RUN_DIR="${WORK_DIR}/run"
MOUNT_DIR="${SW_BLOCK_NVME_MOUNT_DIR:-${WORK_DIR}/mnt}"

mkdir -p "$ARTIFACT_DIR" "$BIN_DIR" "$RUN_DIR" "$MOUNT_DIR"
: >"$SUMMARY"

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

PORT1="${SW_BLOCK_NVME_PORT1:-$(pick_free_port)}"
PORT2="${SW_BLOCK_NVME_PORT2:-$(pick_free_port)}"
MASTER_ADDR="${SW_BLOCK_MASTER_ADDR:-127.0.0.1:$(pick_free_port)}"
R1_DATA_ADDR="${SW_BLOCK_R1_DATA_ADDR:-127.0.0.1:$(pick_free_port)}"
R1_CTRL_ADDR="${SW_BLOCK_R1_CTRL_ADDR:-127.0.0.1:$(pick_free_port)}"
R1_STATUS_ADDR="${SW_BLOCK_R1_STATUS_ADDR:-127.0.0.1:$(pick_free_port)}"
R2_DATA_ADDR="${SW_BLOCK_R2_DATA_ADDR:-127.0.0.1:$(pick_free_port)}"
R2_CTRL_ADDR="${SW_BLOCK_R2_CTRL_ADDR:-127.0.0.1:$(pick_free_port)}"
R2_STATUS_ADDR="${SW_BLOCK_R2_STATUS_ADDR:-127.0.0.1:$(pick_free_port)}"
MASTER_PID=""
R1_PID=""
R2_PID=""

log() {
  printf '[nvme-failover] %s\n' "$*" | tee -a "$ARTIFACT_DIR/run.log"
}

write_summary() {
  echo "$*" | tee -a "$SUMMARY" >/dev/null
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
  [[ -n "$R1_PID" ]] && kill "$R1_PID" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  [[ -n "$R2_PID" ]] && kill "$R2_PID" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  [[ -n "$MASTER_PID" ]] && kill "$MASTER_PID" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  pkill -TERM -f "${BIN_DIR}/blockvolume" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  pkill -TERM -f "${BIN_DIR}/blockmaster" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  sleep 1
  [[ -n "$R1_PID" ]] && kill -KILL "$R1_PID" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  [[ -n "$R2_PID" ]] && kill -KILL "$R2_PID" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  [[ -n "$MASTER_PID" ]] && kill -KILL "$MASTER_PID" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  pkill -KILL -f "${BIN_DIR}/blockvolume" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  pkill -KILL -f "${BIN_DIR}/blockmaster" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.after.json" 2>&1 || true
  pgrep -af "${BIN_DIR}/blockmaster|${BIN_DIR}/blockvolume" >"$ARTIFACT_DIR/processes.after.txt" 2>&1 || true
}
trap cleanup EXIT

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

wait_status_role() {
  local status_addr="$1"
  local replica="$2"
  local role="$3"
  local min_epoch="${4:-0}"
  local out="$ARTIFACT_DIR/status-${replica}-${role}.json"
  for _ in $(seq 1 240); do
    if curl -fsS "http://${status_addr}/status?volume=v1" >"$out.tmp" 2>/dev/null; then
      if python3 - "$out.tmp" "$replica" "$role" "$min_epoch" <<'PY'
import json, sys
path, replica, role, min_epoch = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
body = json.load(open(path))
if str(body.get("ReplicaID", "")) != replica:
    sys.exit(1)
epoch = int(body.get("Epoch", 0))
authority_role = str(body.get("AuthorityRole", ""))
frontend_ready = bool(body.get("FrontendPrimaryReady"))
replication_role = str(body.get("ReplicationRole", ""))
if role == "primary":
    sys.exit(0 if authority_role == "primary" and frontend_ready and epoch >= min_epoch else 1)
if role == "secondary":
    ok = authority_role != "primary" and replication_role in ("not_ready", "replica_ready", "recovering")
    sys.exit(0 if ok else 1)
sys.exit(1)
PY
      then
        mv "$out.tmp" "$out"
        return 0
      fi
    fi
    sleep 0.25
  done
  curl -fsS "http://${status_addr}/status?volume=v1" >"$ARTIFACT_DIR/status-${replica}-last.json" 2>/dev/null || true
  pgrep -af "${BIN_DIR}/blockmaster|${BIN_DIR}/blockvolume" >"$ARTIFACT_DIR/processes.${replica}.${role}.timeout.txt" 2>&1 || true
  echo "timed out waiting for ${replica} ${role} projection" >&2
  exit 1
}

wait_log_pattern() {
  local path="$1"
  local pattern="$2"
  local label="$3"
  for _ in $(seq 1 160); do
    if grep -q "$pattern" "$path" 2>/dev/null; then
      return 0
    fi
    sleep 0.25
  done
  echo "timed out waiting for ${label}" >&2
  exit 1
}

parse_nvme_subsys() {
  local field="$1"
  local raw
  raw="$(sudo nvme list-subsys -o json 2>/dev/null)"
  python3 - "$SUBSYS_NQN" "$field" "$raw" <<'PY'
import glob, json, os, re, sys
nqn, field, raw = sys.argv[1], sys.argv[2], sys.argv[3]
raw = raw.strip()
if not raw:
    sys.exit(1)
doc = json.loads(raw)
def iter_subsystems(node):
    if isinstance(node, dict):
        if "NQN" in node and "Paths" in node:
            yield node
        for sub in node.get("Subsystems", []):
            yield sub
    elif isinstance(node, list):
        for item in node:
            yield from iter_subsystems(item)

matched = False
all_paths = []
for sub in iter_subsystems(doc):
    if sub.get("NQN") != nqn:
        continue
    matched = True
    all_paths.extend(sub.get("Paths", []))
if not matched:
    sys.exit(1)

ns_devices = []
for nqn_file in glob.glob("/sys/class/nvme-subsystem/*/subsysnqn"):
    try:
        if open(nqn_file).read().strip() != nqn:
            continue
    except OSError:
        continue
    sub_dir = os.path.dirname(nqn_file)
    for entry in os.listdir(sub_dir):
        if re.fullmatch(r"nvme[0-9]+n[0-9]+", entry):
            ns_devices.append("/dev/" + entry)

if field == "path_count":
    print(len(all_paths))
elif field == "devices":
    print("\n".join(sorted(set(ns_devices))))
elif field == "paths":
    for path in all_paths:
        print(path)
else:
    sys.exit(1)
sys.exit(0)
PY
}

wait_nvme_paths() {
  for _ in $(seq 1 900); do
    if count="$(parse_nvme_subsys path_count 2>/dev/null)" && [[ "${count:-0}" -ge 2 ]]; then
      return 0
    fi
    sleep 0.2
  done
  sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.timeout.json" 2>&1 || true
  echo "timed out waiting for two NVMe paths for ${SUBSYS_NQN}" >&2
  exit 1
}

wait_grouped_device() {
  for _ in $(seq 1 900); do
    parse_nvme_subsys devices >"$ARTIFACT_DIR/devices.txt" 2>/dev/null || true
    local count
    count="$(grep -c '^/dev/' "$ARTIFACT_DIR/devices.txt" || true)"
    if [[ "$count" -eq 1 ]]; then
      local dev
      dev="$(head -n1 "$ARTIFACT_DIR/devices.txt")"
      if [[ -b "$dev" ]]; then
        printf '%s\n' "$dev"
        return 0
      fi
    fi
    sleep 0.2
  done
  sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.device-timeout.json" 2>&1 || true
  echo "timed out waiting for one grouped NVMe namespace device" >&2
  exit 1
}

capture_ops_cluster() {
  local label="$1"
  "${BIN_DIR}/sw-block" ops cluster --master-api "$MASTER_ADDR" -o json \
    >"$ARTIFACT_DIR/cluster-${label}.json"
}

managed_nvme_field() {
  local path="$1"
  local field="$2"
  python3 - "$path" "$field" <<'PY'
import json, sys
path, field = sys.argv[1], sys.argv[2]
doc = json.load(open(path))
managed = doc.get("managed_volumes") or []
if not managed:
    sys.exit(2)
volume = managed[0]
nvme = volume.get("nvme") or {}
if field == "status":
    print(volume.get("status", ""))
elif field == "reason":
    print(volume.get("reason_code", ""))
elif field == "ready_true":
    print("true" if any(c.get("type") == "Ready" and c.get("status") == "True" for c in volume.get("conditions") or []) else "false")
elif field == "nvme_reason":
    print(nvme.get("reason_code", ""))
elif field == "path_count":
    print(nvme.get("path_count", 0))
elif field == "multipath_observed":
    print(str(bool(nvme.get("multipath_observed", False))).lower())
elif field == "nqn":
    print(nvme.get("nqn", ""))
elif field == "nsid":
    print(nvme.get("nsid", 0))
elif field == "addrs":
    print(",".join(nvme.get("nvme_addrs") or []))
else:
    sys.exit(3)
PY
}

wait_managed_nvme_status() {
  local label="$1"
  local want_paths="$2"
  local want_reason="$3"
  local require_not_ready="${4:-false}"
  local json_path="$ARTIFACT_DIR/cluster-${label}.json"
  for _ in $(seq 1 120); do
    if capture_ops_cluster "$label" 2>"$ARTIFACT_DIR/cluster-${label}.stderr"; then
      local got_paths got_reason ready_true
      got_paths="$(managed_nvme_field "$json_path" path_count 2>/dev/null || true)"
      got_reason="$(managed_nvme_field "$json_path" nvme_reason 2>/dev/null || true)"
      ready_true="$(managed_nvme_field "$json_path" ready_true 2>/dev/null || true)"
      if [[ "$got_paths" == "$want_paths" && "$got_reason" == "$want_reason" ]]; then
        if [[ "$require_not_ready" != "true" || "$ready_true" != "true" ]]; then
          return 0
        fi
      fi
    fi
    sleep 0.25
  done
  echo "timed out waiting for managed NVMe status label=${label} paths=${want_paths} reason=${want_reason}" >&2
  [[ -f "$json_path" ]] && cat "$json_path" >&2
  exit 1
}

require_cmd sudo
require_cmd nvme
require_cmd curl
require_cmd python3
require_cmd mkfs.ext4
require_cmd mount
require_cmd sha256sum

if [[ "$SOAK_ITERATIONS" != "0" ]]; then
  write_summary "phase101_nvme_soak_status=running"
  write_summary "phase101_scope=standalone_nvme_bounded_writer_reader_soak"
  write_summary "soak_iterations=${SOAK_ITERATIONS}"
else
  write_summary "phase101_nvme_path_failure_status=running"
  write_summary "phase101_scope=standalone_nvme_multipath_one_path_loss_status_gate"
fi
log "run_id=$RUN_ID"
log "root=$ROOT"
log "artifact_dir=$ARTIFACT_DIR"
log "nqn=$SUBSYS_NQN"
log "portals=127.0.0.1:${PORT1},127.0.0.1:${PORT2}"
log "master=${MASTER_ADDR}"
log "size_blocks=${BLOCKS} block_size=${BLOCK_SIZE}"
log "durable_impl=${DURABLE_IMPL}"

if [[ -r /sys/module/nvme_core/parameters/multipath ]]; then
  cat /sys/module/nvme_core/parameters/multipath >"$ARTIFACT_DIR/nvme-core-multipath.txt"
  log "nvme_core_multipath=$(cat "$ARTIFACT_DIR/nvme-core-multipath.txt")"
else
  echo "missing" >"$ARTIFACT_DIR/nvme-core-multipath.txt"
  log "nvme_core_multipath=missing"
fi

cd "$ROOT"
git rev-parse --short HEAD >"$ARTIFACT_DIR/git-head.txt" 2>/dev/null || true

if [[ -x "${BIN_DIR}/blockmaster" && -x "${BIN_DIR}/blockvolume" ]]; then
  log "use prebuilt binaries from ${BIN_DIR}"
else
  require_cmd go
  log "build binaries"
  go build -o "${BIN_DIR}/blockmaster" ./cmd/blockmaster
  go build -o "${BIN_DIR}/blockvolume" ./cmd/blockvolume
  go build -o "${BIN_DIR}/sw-block" ./cmd/sw-block
fi
if [[ ! -x "${BIN_DIR}/sw-block" ]]; then
  require_cmd go
  go build -o "${BIN_DIR}/sw-block" ./cmd/sw-block
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

cat >"$ARTIFACT_DIR/placement-seed.json" <<JSON
[
  {
    "volume_id": "v1",
    "desired_rf": 2,
    "slots": [
      {
        "server_id": "s1",
        "replica_id": "r1",
        "source": "existing_replica"
      },
      {
        "server_id": "s2",
        "replica_id": "r2",
        "source": "existing_replica"
      }
    ]
  }
]
JSON

rm -rf "${RUN_DIR}/master-store" "${RUN_DIR}/lifecycle-store" "${RUN_DIR}/r1-store" "${RUN_DIR}/r2-store"
mkdir -p "${RUN_DIR}/master-store" "${RUN_DIR}/lifecycle-store" "${RUN_DIR}/r1-store" "${RUN_DIR}/r2-store"
pkill -KILL -f "${BIN_DIR}/blockvolume" >/dev/null 2>&1 || true
pkill -KILL -f "${BIN_DIR}/blockmaster" >/dev/null 2>&1 || true
disconnect_nqn
sudo modprobe nvme_tcp >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.before.json" 2>&1 || true
sudo dmesg >"$ARTIFACT_DIR/dmesg.before.txt" 2>&1 || true

log "start blockmaster"
"${BIN_DIR}/blockmaster" \
  --authority-store "${RUN_DIR}/master-store" \
  --lifecycle-store "${RUN_DIR}/lifecycle-store" \
  --lifecycle-placement-seed "$ARTIFACT_DIR/placement-seed.json" \
  --listen "$MASTER_ADDR" \
  --topology "$ARTIFACT_DIR/topology.yaml" \
  --expected-slots-per-volume 2 \
  --freshness-window 800ms \
  --pending-grace 100ms \
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

start_blockvolume() {
  local replica="$1"
  local server="$2"
  local port="$3"
  local data_addr="$4"
  local ctrl_addr="$5"
  local status_addr="$6"
  local store="$7"
  local log_file="$8"

  "${BIN_DIR}/blockvolume" \
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
    --durable-impl "$DURABLE_IMPL" \
    --durable-blocks "$BLOCKS" \
    --durable-blocksize "$BLOCK_SIZE" \
    --nvme-listen "127.0.0.1:${port}" \
    --nvme-subsysnqn "$SUBSYS_NQN" \
    --nvme-ns "$NSID" \
    >"$log_file" 2>&1 &
}

log "start r1 NVMe path"
start_blockvolume r1 s1 "$PORT1" "$R1_DATA_ADDR" "$R1_CTRL_ADDR" "$R1_STATUS_ADDR" \
  "${RUN_DIR}/r1-store" "$ARTIFACT_DIR/blockvolume-r1.log"
R1_PID=$!
wait_port "$PORT1"

log "start r2 NVMe path"
start_blockvolume r2 s2 "$PORT2" "$R2_DATA_ADDR" "$R2_CTRL_ADDR" "$R2_STATUS_ADDR" \
  "${RUN_DIR}/r2-store" "$ARTIFACT_DIR/blockvolume-r2.log"
R2_PID=$!
wait_port "$PORT2"

log "wait authority projections"
wait_status_role "$R1_STATUS_ADDR" r1 primary 1
wait_status_role "$R2_STATUS_ADDR" r2 secondary 0
wait_log_pattern "$ARTIFACT_DIR/blockvolume-r2.log" "using ANA metadata probe backend" "r2 ANA metadata probe readiness"

log "connect first NVMe path"
sudo nvme connect -t tcp -a 127.0.0.1 -s "$PORT1" -n "$SUBSYS_NQN" \
  >"$ARTIFACT_DIR/nvme-connect-r1.log" 2>&1
log "connect second NVMe path"
sudo nvme connect -t tcp -a 127.0.0.1 -s "$PORT2" -n "$SUBSYS_NQN" \
  >"$ARTIFACT_DIR/nvme-connect-r2.log" 2>&1

wait_nvme_paths
sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.before-failover.json" 2>&1 || true
parse_nvme_subsys paths >"$ARTIFACT_DIR/path-summary.before-failover.txt" || true
wait_managed_nvme_status before-failover 2 ""
before_status="$(managed_nvme_field "$ARTIFACT_DIR/cluster-before-failover.json" status)"
before_reason="$(managed_nvme_field "$ARTIFACT_DIR/cluster-before-failover.json" reason)"
before_path_count="$(managed_nvme_field "$ARTIFACT_DIR/cluster-before-failover.json" path_count)"
before_multipath_observed="$(managed_nvme_field "$ARTIFACT_DIR/cluster-before-failover.json" multipath_observed)"
before_nqn="$(managed_nvme_field "$ARTIFACT_DIR/cluster-before-failover.json" nqn)"
before_nsid="$(managed_nvme_field "$ARTIFACT_DIR/cluster-before-failover.json" nsid)"
before_addrs="$(managed_nvme_field "$ARTIFACT_DIR/cluster-before-failover.json" addrs)"
write_summary "before_status=${before_status}"
write_summary "before_reason=${before_reason:-none}"
write_summary "before_path_count=${before_path_count}"
write_summary "before_multipath_observed=${before_multipath_observed}"
write_summary "before_nqn=${before_nqn}"
write_summary "before_nsid=${before_nsid}"
write_summary "before_addrs=${before_addrs}"
map_dev="$(wait_grouped_device)"
log "nvme_namespace_device=${map_dev}"

log "mkfs/mount NVMe multipath namespace"
sudo mkfs.ext4 -F "$map_dev" >"$ARTIFACT_DIR/mkfs.log" 2>&1
sudo mount "$map_dev" "$MOUNT_DIR"

log "write pre-failover payload"
sudo dd if=/dev/urandom of="$MOUNT_DIR/pre.bin" bs=4096 count=64 status=none
sync
sudo sha256sum "$MOUNT_DIR/pre.bin" | tee "$ARTIFACT_DIR/pre.sha256"

if [[ "$SOAK_ITERATIONS" != "0" ]]; then
  if ! [[ "$SOAK_ITERATIONS" =~ ^[0-9]+$ ]]; then
    echo "SW_BLOCK_NVME_SOAK_ITERATIONS must be numeric, got ${SOAK_ITERATIONS}" >&2
    exit 2
  fi
  if [[ "$SOAK_ITERATIONS" -lt 1 ]]; then
    echo "SW_BLOCK_NVME_SOAK_ITERATIONS must be >= 1" >&2
    exit 2
  fi
  stable_nqn="$before_nqn"
  stable_nsid="$before_nsid"
  stable_addrs="$before_addrs"
  false_ready_count=0
  identity_drift_count=0
  for iter in $(seq 1 "$SOAK_ITERATIONS"); do
    log "soak iteration ${iter}"
    sudo dd if=/dev/urandom of="$MOUNT_DIR/soak-${iter}.bin" bs=4096 count=16 status=none
    sync
    sudo sha256sum "$MOUNT_DIR/soak-${iter}.bin" >"$ARTIFACT_DIR/soak-${iter}.sha256"
    sudo sha256sum -c "$ARTIFACT_DIR/soak-${iter}.sha256" >"$ARTIFACT_DIR/soak-${iter}.check"

    capture_ops_cluster "soak-${iter}"
    iter_path_count="$(managed_nvme_field "$ARTIFACT_DIR/cluster-soak-${iter}.json" path_count)"
    iter_nqn="$(managed_nvme_field "$ARTIFACT_DIR/cluster-soak-${iter}.json" nqn)"
    iter_nsid="$(managed_nvme_field "$ARTIFACT_DIR/cluster-soak-${iter}.json" nsid)"
    iter_addrs="$(managed_nvme_field "$ARTIFACT_DIR/cluster-soak-${iter}.json" addrs)"
    iter_ready_true="$(managed_nvme_field "$ARTIFACT_DIR/cluster-soak-${iter}.json" ready_true)"
    write_summary "soak_${iter}_path_count=${iter_path_count}"
    write_summary "soak_${iter}_ready_true=${iter_ready_true}"
    if [[ "$iter_path_count" != "2" || "$iter_nqn" != "$stable_nqn" || "$iter_nsid" != "$stable_nsid" || "$iter_addrs" != "$stable_addrs" ]]; then
      identity_drift_count=$((identity_drift_count + 1))
    fi
    if [[ "$iter_ready_true" == "true" && "$iter_path_count" != "2" ]]; then
      false_ready_count=$((false_ready_count + 1))
    fi
  done
  write_summary "soak_completed_iterations=${SOAK_ITERATIONS}"
  write_summary "soak_false_ready_count=${false_ready_count}"
  write_summary "soak_identity_drift_count=${identity_drift_count}"
  if [[ "$false_ready_count" != "0" || "$identity_drift_count" != "0" ]]; then
    echo "soak status invariant failed: false_ready=${false_ready_count} identity_drift=${identity_drift_count}" >&2
    exit 1
  fi

  log "unmount"
  sudo umount "$MOUNT_DIR"
  log "disconnect NVMe subsystem"
  disconnect_nqn
  sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.final.json" 2>&1 || true
  if grep -q "$SUBSYS_NQN" "$ARTIFACT_DIR/nvme-list-subsys.final.json"; then
    echo "NVMe subsystem still present after disconnect: $SUBSYS_NQN" >&2
    exit 1
  fi
  write_summary "final_nvme_residue_count=0"
  write_summary "phase101_nvme_soak_status=ok"
  log "PASS: bounded NVMe multipath writer/reader soak left zero residue"
  log "artifacts=$ARTIFACT_DIR"
  exit 0
fi

if [[ -z "$R1_PID" ]] || ! kill -0 "$R1_PID" >/dev/null 2>&1; then
  echo "could not find live r1 blockvolume pid" >&2
  exit 1
fi
log "kill active r1 pid=${R1_PID}"
kill -TERM "$R1_PID" || true
sleep 1
if kill -0 "$R1_PID" >/dev/null 2>&1; then
  kill -KILL "$R1_PID" >/dev/null 2>&1 || true
fi

log "wait r2 failover"
wait_status_role "$R2_STATUS_ADDR" r2 primary 2
sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.after-failover.json" 2>&1 || true
parse_nvme_subsys paths >"$ARTIFACT_DIR/path-summary.after-failover.txt" || true
wait_managed_nvme_status after-failover 1 "nvme_multipath_path_missing" true
after_status="$(managed_nvme_field "$ARTIFACT_DIR/cluster-after-failover.json" status)"
after_reason="$(managed_nvme_field "$ARTIFACT_DIR/cluster-after-failover.json" reason)"
after_nvme_reason="$(managed_nvme_field "$ARTIFACT_DIR/cluster-after-failover.json" nvme_reason)"
after_ready_true="$(managed_nvme_field "$ARTIFACT_DIR/cluster-after-failover.json" ready_true)"
after_path_count="$(managed_nvme_field "$ARTIFACT_DIR/cluster-after-failover.json" path_count)"
after_multipath_observed="$(managed_nvme_field "$ARTIFACT_DIR/cluster-after-failover.json" multipath_observed)"
after_addrs="$(managed_nvme_field "$ARTIFACT_DIR/cluster-after-failover.json" addrs)"
write_summary "after_status=${after_status}"
write_summary "after_reason=${after_reason}"
write_summary "after_nvme_reason=${after_nvme_reason}"
write_summary "after_ready_true=${after_ready_true}"
write_summary "after_path_count=${after_path_count}"
write_summary "after_multipath_observed=${after_multipath_observed}"
write_summary "after_addrs=${after_addrs}"
sleep 3

log "verify mounted workload after failover"
sudo sha256sum -c "$ARTIFACT_DIR/pre.sha256" | tee "$ARTIFACT_DIR/pre-check-after-failover.log"
sudo dd if=/dev/urandom of="$MOUNT_DIR/post.bin" bs=4096 count=64 status=none
sync
sudo sha256sum "$MOUNT_DIR/post.bin" | tee "$ARTIFACT_DIR/post.sha256"
sudo sha256sum -c "$ARTIFACT_DIR/post.sha256" | tee "$ARTIFACT_DIR/post-check.log"

log "unmount"
sudo umount "$MOUNT_DIR"

log "disconnect NVMe subsystem"
disconnect_nqn
sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.final.json" 2>&1 || true
if grep -q "$SUBSYS_NQN" "$ARTIFACT_DIR/nvme-list-subsys.final.json"; then
  echo "NVMe subsystem still present after disconnect: $SUBSYS_NQN" >&2
  exit 1
fi
write_summary "final_nvme_residue_count=0"

write_summary "phase101_nvme_path_failure_status=ok"
log "PASS: mounted NVMe multipath workload read/wrote through r1->r2 failover"
log "artifacts=$ARTIFACT_DIR"
