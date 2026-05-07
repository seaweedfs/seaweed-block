#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
WORK_DIR="${SW_BLOCK_NVME_MPATH_WORK_DIR:-/tmp/sw-block-nvme-mpath}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${WORK_DIR}/runs/${RUN_ID}}"
SUBSYS_NQN="${SW_BLOCK_NVME_NQN:-nqn.2026-05.io.seaweedfs:mpath-v1}"
NSID="${SW_BLOCK_NVME_NSID:-1}"
BLOCKS="${SW_BLOCK_DURABLE_BLOCKS:-65536}"
BLOCK_SIZE="${SW_BLOCK_DURABLE_BLOCKSIZE:-4096}"
DURABLE_IMPL="${SW_BLOCK_DURABLE_IMPL:-smartwal}"
BIN_DIR="${SW_BLOCK_BIN_DIR:-${WORK_DIR}/bin}"
RUN_DIR="${WORK_DIR}/run"

mkdir -p "$ARTIFACT_DIR" "$BIN_DIR" "$RUN_DIR"

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
  printf '[nvme-mpath] %s\n' "$*" | tee -a "$ARTIFACT_DIR/run.log"
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
  local out="$ARTIFACT_DIR/status-${replica}-${role}.json"
  for _ in $(seq 1 120); do
    if curl -fsS "http://${status_addr}/status?volume=v1" >"$out.tmp" 2>/dev/null; then
      if python3 - "$out.tmp" "$replica" "$role" <<'PY'
import json, sys
path, replica, role = sys.argv[1], sys.argv[2], sys.argv[3]
body = json.load(open(path))
if str(body.get("ReplicaID", "")) != replica:
    sys.exit(1)
authority_role = str(body.get("AuthorityRole", ""))
frontend_ready = bool(body.get("FrontendPrimaryReady"))
replication_role = str(body.get("ReplicationRole", ""))
if role == "primary":
    sys.exit(0 if authority_role == "primary" and frontend_ready else 1)
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

parse_nvme_subsys() {
  local field="$1"
  sudo nvme list-subsys -o json 2>/dev/null | python3 - "$SUBSYS_NQN" "$field" <<'PY'
import json, re, sys
nqn, field = sys.argv[1], sys.argv[2]
raw = sys.stdin.read().strip()
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
all_names = []
for sub in iter_subsystems(doc):
    if sub.get("NQN") != nqn:
        continue
    matched = True
    paths = sub.get("Paths", [])
    all_paths.extend(paths)
    for path in paths:
        name = path.get("Name") or ""
        base = name.split("/")[-1]
        if re.fullmatch(r"nvme[0-9]+n[0-9]+", base):
            all_names.append("/dev/" + base)
        elif re.fullmatch(r"nvme[0-9]+", base):
            all_names.append("/dev/" + base + "n1")
if not matched:
    sys.exit(1)
if field == "path_count":
    print(len(all_paths))
elif field == "devices":
    print("\n".join(sorted(set(all_names))))
elif field == "paths":
    for path in all_paths:
        print(path)
else:
    sys.exit(1)
sys.exit(0)
PY
}

wait_nvme_paths() {
  log "wait_nvme_paths: enter (180s budget)"
  local i
  for i in $(seq 1 900); do
    if count="$(parse_nvme_subsys path_count 2>/dev/null)" && [[ "${count:-0}" -ge 2 ]]; then
      log "wait_nvme_paths: ok at iter=$i count=$count"
      return 0
    fi
    if (( i % 25 == 0 )); then
      log "wait_nvme_paths: iter=$i count=${count:-empty}"
      sudo nvme list-subsys -o json >>"$ARTIFACT_DIR/wait-debug.json" 2>&1 || true
      printf '\n--- iter %s ---\n' "$i" >>"$ARTIFACT_DIR/wait-debug.json" 2>&1 || true
    fi
    sleep 0.2
  done
  log "wait_nvme_paths: timeout after 900 iters (last count=${count:-empty})"
  sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.timeout.json" 2>&1 || true
  if count="$(parse_nvme_subsys path_count 2>/dev/null)" && [[ "${count:-0}" -ge 2 ]]; then
    log "wait_nvme_paths: ok on post-timeout recheck count=$count"
    return 0
  fi
  echo "timed out waiting for two NVMe paths for ${SUBSYS_NQN}" >&2
  exit 1
}

summarize_ana_log() {
  local bin="$1"
  local out="$2"
  python3 - "$bin" "$NSID" >"$out" <<'PY'
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
if group_id != 1:
    raise SystemExit(f"ANA group id={group_id}, want 1 for current single-group model")
if nsid_count != 1:
    raise SystemExit(f"ANA NSID count={nsid_count}, want 1")
if nsid != want_nsid:
    raise SystemExit(f"ANA NSID={nsid}, want {want_nsid}")
if state not in state_names:
    raise SystemExit(f"ANA state=0x{state:02x} is not recognized")
PY
}

require_cmd sudo
require_cmd nvme
require_cmd curl
require_cmd python3

log "run_id=$RUN_ID"
log "root=$ROOT"
log "artifact_dir=$ARTIFACT_DIR"
log "nqn=$SUBSYS_NQN"
log "nsid=$NSID"
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
disconnect_nqn
sudo modprobe nvme_tcp >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.before.json" 2>&1 || true
sudo dmesg >"$ARTIFACT_DIR/dmesg.before.txt" 2>&1 || true

log "start blockmaster"
"${BIN_DIR}/blockmaster" \
  --authority-store "${RUN_DIR}/master-store" \
  --listen "$MASTER_ADDR" \
  --topology "$ARTIFACT_DIR/topology.yaml" \
  --expected-slots-per-volume 2 \
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
wait_status_role "$R1_STATUS_ADDR" r1 primary
wait_status_role "$R2_STATUS_ADDR" r2 secondary

log "connect first NVMe path"
set +e
sudo nvme connect -t tcp -a 127.0.0.1 -s "$PORT1" -n "$SUBSYS_NQN" \
  >"$ARTIFACT_DIR/nvme-connect-r1.log" 2>&1
rc1=$?
set -e
log "connect first NVMe path: rc=$rc1"
if [[ $rc1 -ne 0 ]]; then
  cat "$ARTIFACT_DIR/nvme-connect-r1.log" >&2 || true
  echo "first nvme connect failed rc=$rc1" >&2
  exit 1
fi
log "connect second NVMe path"
set +e
sudo nvme connect -t tcp -a 127.0.0.1 -s "$PORT2" -n "$SUBSYS_NQN" \
  >"$ARTIFACT_DIR/nvme-connect-r2.log" 2>&1
rc2=$?
set -e
log "connect second NVMe path: rc=$rc2"
if [[ $rc2 -ne 0 ]]; then
  cat "$ARTIFACT_DIR/nvme-connect-r2.log" >&2 || true
  sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.connect-failed.json" 2>&1 || true
  echo "second nvme connect failed rc=$rc2" >&2
  exit 1
fi
log "post-connect snapshot"
sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.post-connect.json" 2>&1 || true

wait_nvme_paths
sudo nvme list >"$ARTIFACT_DIR/nvme-list.txt" 2>&1 || true
sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.json" 2>&1 || true
parse_nvme_subsys paths >"$ARTIFACT_DIR/path-summary.txt" || true
parse_nvme_subsys devices >"$ARTIFACT_DIR/devices.txt"

path_count="$(parse_nvme_subsys path_count)"
device_count="$(grep -c '^/dev/' "$ARTIFACT_DIR/devices.txt" || true)"
log "nvme_path_count=$path_count"
log "nvme_namespace_devices=$device_count"
if [[ "$path_count" -lt 2 ]]; then
  echo "NVMe subsystem did not show two paths" >&2
  exit 1
fi
if [[ "$device_count" -lt 1 ]]; then
  cat "$ARTIFACT_DIR/nvme-list-subsys.json" >&2
  echo "NVMe subsystem reported paths but no namespace device" >&2
  exit 1
fi

if [[ "$(cat "$ARTIFACT_DIR/nvme-core-multipath.txt")" == "Y" && "$device_count" -ne 1 ]]; then
  cat "$ARTIFACT_DIR/nvme-list-subsys.json" >&2
  echo "native NVMe multipath is enabled but host exposed ${device_count} namespace devices; expected one grouped namespace" >&2
  exit 1
fi

idx=0
while IFS= read -r dev; do
  [[ -n "$dev" ]] || continue
  idx=$((idx + 1))
  log "capture identity and ANA for ${dev}"
  sudo nvme id-ctrl "$dev" >"$ARTIFACT_DIR/nvme-id-ctrl.dev${idx}.txt" 2>&1
  sudo nvme id-ns "$dev" >"$ARTIFACT_DIR/nvme-id-ns.dev${idx}.txt" 2>&1
  sudo nvme get-log "$dev" -i 0x0c -l 40 -b \
    >"$ARTIFACT_DIR/nvme-ana-log.dev${idx}.bin" \
    2>"$ARTIFACT_DIR/nvme-ana-log.dev${idx}.stderr"
  summarize_ana_log "$ARTIFACT_DIR/nvme-ana-log.dev${idx}.bin" "$ARTIFACT_DIR/nvme-ana-log.dev${idx}.summary"
done <"$ARTIFACT_DIR/devices.txt"

python3 - "$ARTIFACT_DIR" "$device_count" <<'PY'
import pathlib, re, sys
art = pathlib.Path(sys.argv[1])
device_count = int(sys.argv[2])
ids = []
for i in range(1, device_count + 1):
    body = (art / f"nvme-id-ns.dev{i}.txt").read_text(errors="replace")
    nguid = re.search(r"(?im)^\s*nguid\s*:\s*([0-9a-fA-F]+)", body)
    eui64 = re.search(r"(?im)^\s*eui64\s*:\s*([0-9a-fA-F]+)", body)
    anagrpid = re.search(r"(?im)^\s*anagrpid\s*:\s*(\S+)", body)
    if not nguid or not eui64 or not anagrpid:
        raise SystemExit(f"missing NGUID/EUI64/ANAGRPID in nvme-id-ns.dev{i}.txt")
    ids.append((nguid.group(1).lower(), eui64.group(1).lower(), anagrpid.group(1)))
if len(set(ids)) != 1:
    raise SystemExit(f"namespace identity differs across devices: {ids}")
(art / "identity-summary.txt").write_text(
    f"nguid={ids[0][0]}\neui64={ids[0][1]}\nanagrpid={ids[0][2]}\n"
)
PY

if grep -R "ana_state=0x03\\|ana_state=0x04\\|ana_state=0x0f" "$ARTIFACT_DIR"/nvme-ana-log.dev*.summary >/dev/null 2>&1; then
  grep -R "ana_state=" "$ARTIFACT_DIR"/nvme-ana-log.dev*.summary >&2 || true
  echo "ANA reported inaccessible/persistent-loss/change during steady two-path discovery" >&2
  exit 1
fi

sudo dmesg >"$ARTIFACT_DIR/dmesg.after.txt" 2>&1 || true
python3 - "$ARTIFACT_DIR/dmesg.before.txt" "$ARTIFACT_DIR/dmesg.after.txt" >"$ARTIFACT_DIR/dmesg.new.txt" <<'PY'
import sys
before = set(open(sys.argv[1], errors="replace").read().splitlines())
after = open(sys.argv[2], errors="replace").read().splitlines()
for line in after:
    if line not in before:
        print(line)
PY
if grep -Eiq "nvme_parse_ana_log|I/O error|reset controller" "$ARTIFACT_DIR/dmesg.new.txt"; then
  echo "dmesg contains new NVMe ANA/reset/I/O warning during run" >&2
  exit 1
fi

log "disconnect both NVMe paths"
disconnect_nqn
sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.final.json" 2>&1 || true
if grep -q "$SUBSYS_NQN" "$ARTIFACT_DIR/nvme-list-subsys.final.json"; then
  echo "NVMe subsystem still present after disconnect: $SUBSYS_NQN" >&2
  exit 1
fi

log "PASS: two NVMe/TCP paths expose one ANA-aware namespace"
log "artifacts=$ARTIFACT_DIR"
