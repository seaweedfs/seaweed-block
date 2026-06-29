#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
WORK_DIR="${SW_BLOCK_NVME_STAGE_WORK_DIR:-/tmp/sw-block-nvme-stage}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${WORK_DIR}/runs/${RUN_ID}}"
SUMMARY="${ARTIFACT_DIR}/phase101-nvme-stage-unstage-summary.txt"
SUBSYS_NQN="${SW_BLOCK_NVME_NQN:-nqn.2026-05.io.seaweedfs:stage-v1}"
NSID="${SW_BLOCK_NVME_NSID:-1}"
CYCLES="${SW_BLOCK_NVME_STAGE_CYCLES:-3}"
BLOCKS="${SW_BLOCK_DURABLE_BLOCKS:-4096}"
BLOCK_SIZE="${SW_BLOCK_DURABLE_BLOCKSIZE:-4096}"
DURABLE_IMPL="${SW_BLOCK_DURABLE_IMPL:-smartwal}"
BIN_DIR="${SW_BLOCK_BIN_DIR:-${WORK_DIR}/bin}"
RUN_DIR="${WORK_DIR}/run"

mkdir -p "$ARTIFACT_DIR" "$BIN_DIR" "$RUN_DIR"
: >"$SUMMARY"

_USED_PORTS=()

pick_free_port() {
  local candidate
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
  printf '[nvme-stage] %s\n' "$*" | tee -a "$ARTIFACT_DIR/run.log"
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
  disconnect_nqn
  [[ -n "$R1_PID" ]] && kill "$R1_PID" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  [[ -n "$R2_PID" ]] && kill "$R2_PID" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  [[ -n "$MASTER_PID" ]] && kill "$MASTER_PID" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  pkill -TERM -f "${BIN_DIR}/blockvolume" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  pkill -TERM -f "${BIN_DIR}/blockmaster" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  sleep 1
  pkill -KILL -f "${BIN_DIR}/blockvolume" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  pkill -KILL -f "${BIN_DIR}/blockmaster" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
  sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.after.json" 2>&1 || true
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
  echo "timed out waiting for ${replica} ${role} projection" >&2
  exit 1
}

wait_any_primary() {
  for _ in $(seq 1 240); do
    for item in "r1 $R1_STATUS_ADDR" "r2 $R2_STATUS_ADDR"; do
      set -- $item
      local replica="$1"
      local status_addr="$2"
      local out="$ARTIFACT_DIR/status-${replica}-primary-candidate.json"
      if curl -fsS "http://${status_addr}/status?volume=v1" >"$out.tmp" 2>/dev/null; then
        if python3 - "$out.tmp" "$replica" <<'PY'
import json, sys
path, replica = sys.argv[1], sys.argv[2]
body = json.load(open(path))
ok = (
    str(body.get("ReplicaID", "")) == replica and
    str(body.get("AuthorityRole", "")) == "primary" and
    bool(body.get("FrontendPrimaryReady"))
)
sys.exit(0 if ok else 1)
PY
        then
          mv "$out.tmp" "$ARTIFACT_DIR/status-${replica}-primary.json"
          echo "$replica"
          return 0
        fi
      fi
    done
    sleep 0.25
  done
  echo "timed out waiting for any primary projection" >&2
  exit 1
}

nvme_path_count() {
  local raw
  raw="$(sudo nvme list-subsys -o json 2>/dev/null || true)"
  python3 - "$SUBSYS_NQN" "$raw" <<'PY'
import json, sys
nqn, raw = sys.argv[1], sys.argv[2].strip()
if not raw:
    print(0)
    sys.exit(0)
try:
    doc = json.loads(raw)
except Exception:
    print(0)
    sys.exit(0)
def iter_subsystems(node):
    if isinstance(node, dict):
        if "NQN" in node and "Paths" in node:
            yield node
        for sub in node.get("Subsystems", []):
            yield sub
    elif isinstance(node, list):
        for item in node:
            yield from iter_subsystems(item)
for sub in iter_subsystems(doc):
    if sub.get("NQN") == nqn:
        print(len(sub.get("Paths", [])))
        sys.exit(0)
print(0)
PY
}

wait_nvme_path_count() {
  local want="$1"
  local label="$2"
  for _ in $(seq 1 300); do
    local got
    got="$(nvme_path_count)"
    if [[ "$got" == "$want" ]]; then
      return 0
    fi
    sleep 0.2
  done
  sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.${label}.timeout.json" 2>&1 || true
  echo "timed out waiting for ${want} NVMe paths at ${label}" >&2
  exit 1
}

require_cmd sudo
require_cmd nvme
require_cmd curl
require_cmd python3

write_summary "phase101_nvme_stage_unstage_status=running"
write_summary "phase101_scope=standalone_nvme_connect_disconnect_idempotency"
write_summary "cycles=${CYCLES}"
write_summary "nqn=${SUBSYS_NQN}"

cd "$ROOT"
if [[ -x "${BIN_DIR}/blockmaster" && -x "${BIN_DIR}/blockvolume" ]]; then
  log "use prebuilt binaries from ${BIN_DIR}"
else
  require_cmd go
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

cat >"$ARTIFACT_DIR/placement-seed.json" <<JSON
[
  {
    "volume_id": "v1",
    "desired_rf": 2,
    "slots": [
      {"server_id": "s1", "replica_id": "r1", "source": "existing_replica"},
      {"server_id": "s2", "replica_id": "r2", "source": "existing_replica"}
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

start_blockvolume r1 s1 "$PORT1" "$R1_DATA_ADDR" "$R1_CTRL_ADDR" "$R1_STATUS_ADDR" \
  "${RUN_DIR}/r1-store" "$ARTIFACT_DIR/blockvolume-r1.log"
R1_PID=$!
wait_port "$PORT1"
start_blockvolume r2 s2 "$PORT2" "$R2_DATA_ADDR" "$R2_CTRL_ADDR" "$R2_STATUS_ADDR" \
  "${RUN_DIR}/r2-store" "$ARTIFACT_DIR/blockvolume-r2.log"
R2_PID=$!
wait_port "$PORT2"

primary_replica="$(wait_any_primary)"
write_summary "primary_replica=${primary_replica}"

max_paths=0
for cycle in $(seq 1 "$CYCLES"); do
  log "cycle ${cycle}: connect"
  sudo nvme connect -t tcp -a 127.0.0.1 -s "$PORT1" -n "$SUBSYS_NQN" \
    >"$ARTIFACT_DIR/nvme-connect-r1-cycle-${cycle}.log" 2>&1
  sudo nvme connect -t tcp -a 127.0.0.1 -s "$PORT2" -n "$SUBSYS_NQN" \
    >"$ARTIFACT_DIR/nvme-connect-r2-cycle-${cycle}.log" 2>&1
  wait_nvme_path_count 2 "cycle-${cycle}-connected"
  sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.cycle-${cycle}.connected.json" 2>&1 || true
  write_summary "cycle_${cycle}_connected_path_count=2"
  max_paths=2

  log "cycle ${cycle}: disconnect"
  disconnect_nqn
  wait_nvme_path_count 0 "cycle-${cycle}-disconnected"
  sudo nvme list-subsys -o json >"$ARTIFACT_DIR/nvme-list-subsys.cycle-${cycle}.disconnected.json" 2>&1 || true
  write_summary "cycle_${cycle}_disconnected_path_count=0"
done

final_count="$(nvme_path_count)"
write_summary "max_connected_path_count=${max_paths}"
write_summary "final_nvme_residue_count=${final_count}"
if [[ "$final_count" != "0" ]]; then
  echo "NVMe subsystem still present after final disconnect: path_count=${final_count}" >&2
  exit 1
fi

write_summary "phase101_nvme_stage_unstage_status=ok"
log "PASS: repeated NVMe connect/disconnect cycles left zero residue"
