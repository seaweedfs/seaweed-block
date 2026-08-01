#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
M01_HOST="${SW_BLOCK_PHASE174_M01_HOST:-testdev@192.168.1.181}"
M02_HOST="${SW_BLOCK_PHASE174_M02_HOST:-testdev@192.168.1.184}"
TP01_HOST="${SW_BLOCK_PHASE174_TP01_HOST:-testdev@192.168.1.188}"
M01_DATA_IP="${SW_BLOCK_PHASE174_M01_DATA_IP:-192.168.1.181}"
TP01_DATA_IP="${SW_BLOCK_PHASE174_TP01_DATA_IP:-192.168.1.188}"
SSH_KEY="${SW_BLOCK_PHASE174_SSH_KEY:-/c/work/dev_server/testdev_key}"
PUBLISH_ROOT="${SW_BLOCK_PHASE174_PUBLISH_ROOT:-/mnt/smb/work/share/g15d-k8s}"
RUN_ID="${SW_BLOCK_PHASE174_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-phase174-d2-rf3-distinct}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-/tmp/${RUN_ID}}"
LOCAL_BINARY="/tmp/${RUN_ID}-replication.test"
PREBUILT_BINARY="${SW_BLOCK_PHASE174_PREBUILT_BINARY:-}"
REMOTE_ROOT="/tmp/${RUN_ID}"
REMOTE_BINARY="${REMOTE_ROOT}/replication.test"
M02_STORE_ROOT="/data/nvme/block/${RUN_ID}-stores"
RESULTS="${ARTIFACT_DIR}/phase174-distinct-node-rf3-results.jsonl"
SUMMARY="${ARTIFACT_DIR}/phase174-distinct-node-rf3-summary.txt"
M01_ADDR="${M01_DATA_IP}:17411"
TP01_ADDR="${TP01_DATA_IP}:17412"
RUNS=5
WRITERS=(1 4 8)
GATE_COMPLETE=false

SSH=(ssh -T -i "${SSH_KEY}" -o ConnectTimeout=10 -o StrictHostKeyChecking=no -o BatchMode=yes)
SCP=(scp -q -i "${SSH_KEY}" -o ConnectTimeout=10 -o StrictHostKeyChecking=no -o BatchMode=yes)

remote() {
  local host="$1"
  shift
  "${SSH[@]}" "${host}" "$@"
}

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

cleanup() {
  local exit_status=$?
  if [[ "${GATE_COMPLETE}" != "true" ]]; then
    mkdir -p "${ARTIFACT_DIR}/remote"
    for spec in "${M01_HOST}:m01" "${TP01_HOST}:tp01"; do
      local host="${spec%:*}" node="${spec##*:}"
      remote "${host}" "for pidfile in '${REMOTE_ROOT}'/w*-'${node}'/pid; do
        test -f \"\${pidfile}\" || continue
        touch \"\$(dirname \"\${pidfile}\")/stop\"
      done
      for pidfile in '${REMOTE_ROOT}'/w*-'${node}'/pid; do
        test -f \"\${pidfile}\" || continue
        pid=\$(cat \"\${pidfile}\")
        for _ in \$(seq 1 50); do kill -0 \"\${pid}\" 2>/dev/null || break; sleep 0.1; done
        if kill -0 \"\${pid}\" 2>/dev/null && tr '\\0' ' ' <\"/proc/\${pid}/cmdline\" | grep -Fq '${REMOTE_BINARY}'; then
          kill \"\${pid}\"
        fi
      done" >/dev/null 2>&1 || true
      for writers in "${WRITERS[@]}"; do
        local dir="${REMOTE_ROOT}/w${writers}-${node}"
        "${SCP[@]}" "${host}:${dir}/replica.log" "${ARTIFACT_DIR}/remote/w${writers}-${node}-failure.log" >/dev/null 2>&1 || true
        "${SCP[@]}" "${host}:${dir}/result.json" "${ARTIFACT_DIR}/remote/w${writers}-${node}-failure-result.json" >/dev/null 2>&1 || true
      done
    done
    write_summary "phase174_distinct_node_rf3_status=failed"
    write_summary "failure_artifact=${PUBLISH_ROOT}/${RUN_ID}.tar.gz"
    tar -C "$(dirname "${ARTIFACT_DIR}")" -czf "${ARTIFACT_DIR}.tar.gz" "$(basename "${ARTIFACT_DIR}")" >/dev/null 2>&1 || true
    sha256sum "${ARTIFACT_DIR}.tar.gz" >"${ARTIFACT_DIR}.tar.gz.sha256" 2>/dev/null || true
    "${SCP[@]}" "${ARTIFACT_DIR}.tar.gz" "${M02_HOST}:${PUBLISH_ROOT}/${RUN_ID}.tar.gz" >/dev/null 2>&1 || true
  fi
  rm -f "${LOCAL_BINARY}"
  for host in "${M01_HOST}" "${TP01_HOST}"; do
    remote "${host}" "test '${REMOTE_ROOT}' = '/tmp/${RUN_ID}'; rm -rf -- '${REMOTE_ROOT}'" >/dev/null 2>&1 || true
  done
  remote "${M02_HOST}" "test '${REMOTE_ROOT}' = '/tmp/${RUN_ID}'; test '${M02_STORE_ROOT}' = '/data/nvme/block/${RUN_ID}-stores'; rm -rf -- '${REMOTE_ROOT}' '${M02_STORE_ROOT}'" >/dev/null 2>&1 || true
  return "${exit_status}"
}
trap cleanup EXIT

wait_remote_file() {
  local host="$1" path="$2" label="$3"
  for _ in $(seq 1 150); do
    if remote "${host}" "test -s '${path}'"; then
      return 0
    fi
    sleep 0.2
  done
  echo "timed out waiting for ${label}: ${host}:${path}" >&2
  return 1
}

start_replica() {
  local host="$1" address="$2" writers="$3" node="$4"
  local dir="${REMOTE_ROOT}/w${writers}-${node}"
  remote "${host}" "set -e; mkdir -p '${dir}'; setsid nohup env \
SW_BLOCK_PHASE174_REPLICA_STORE='${dir}/replica.store' \
SW_BLOCK_PHASE174_REPLICA_LISTEN='${address}' \
SW_BLOCK_PHASE174_REPLICA_READY_FILE='${dir}/ready' \
SW_BLOCK_PHASE174_REPLICA_STOP_FILE='${dir}/stop' \
SW_BLOCK_PHASE174_REPLICA_RESULT_FILE='${dir}/result.json' \
SW_BLOCK_PHASE174_REPLICA_FINAL_SET=1 \
SW_BLOCK_PHASE174_REPLICA_FINAL_RUN=${RUNS} \
'${REMOTE_BINARY}' -test.run '^TestPhase174RemoteReplicaProcess$' -test.v -test.timeout=20m \
>'${dir}/replica.log' 2>&1 </dev/null & echo \$! >'${dir}/pid'"
  wait_remote_file "${host}" "${dir}/ready" "${node} replica readiness"
}

stop_replica() {
  local host="$1" writers="$2" node="$3"
  local dir="${REMOTE_ROOT}/w${writers}-${node}"
  remote "${host}" "touch '${dir}/stop'"
  wait_remote_file "${host}" "${dir}/result.json" "${node} replica result"
  "${SCP[@]}" "${host}:${dir}/result.json" "${ARTIFACT_DIR}/remote/w${writers}-${node}-result.json"
  "${SCP[@]}" "${host}:${dir}/replica.log" "${ARTIFACT_DIR}/remote/w${writers}-${node}.log"
}

mkdir -p "${ARTIFACT_DIR}/environment" "${ARTIFACT_DIR}/logs" "${ARTIFACT_DIR}/remote"
: >"${SUMMARY}"
: >"${RESULTS}"

required_commands=(ssh scp python3 tar)
if [[ -z "${PREBUILT_BINARY}" ]]; then
  required_commands+=(go)
fi
for command in "${required_commands[@]}"; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "missing required command: ${command}" >&2
    exit 2
  }
done

write_summary "phase174_distinct_node_rf3_status=running"
write_summary "contract=phase174-distinct-node-rf3-v1"
write_summary "source_commit=$(git -C "${ROOT}" rev-parse HEAD)"
write_summary "foreground_ack_profile=sync_quorum_rf3"
write_summary "post_measurement_drain=probe_catchup_then_sync_all"
write_summary "transport=tcp"
write_summary "network_class=management_lan"
write_summary "primary_node=m02"
write_summary "replica_nodes=m01,tp01"
write_summary "primary_management_ip=192.168.1.184"
write_summary "replica_addresses=${M01_ADDR},${TP01_ADDR}"
write_summary "tp01_roce_capable=false"
write_summary "cross_ack_profile_throughput_ratio_allowed=false"

for spec in "m01:${M01_HOST}" "m02:${M02_HOST}" "tp01:${TP01_HOST}"; do
  name="${spec%%:*}"
  host="${spec#*:}"
  remote "${host}" 'hostname; uname -a; uptime; lsblk -o NAME,TYPE,SIZE,ROTA,MODEL,FSTYPE,MOUNTPOINTS; ip -brief address' \
    >"${ARTIFACT_DIR}/environment/${name}.txt"
done
remote "${M02_HOST}" "test \"\$(findmnt -n -T /data/nvme/block -o SOURCE)\" = /dev/nvme0n1p1"
remote "${M01_HOST}" "findmnt -n -T /tmp -o SOURCE,FSTYPE" >"${ARTIFACT_DIR}/environment/m01-store.txt"
remote "${TP01_HOST}" "findmnt -n -T /tmp -o SOURCE,FSTYPE" >"${ARTIFACT_DIR}/environment/tp01-store.txt"
remote "${M01_HOST}" "! ss -ltnH | grep -q ':17411 '"
remote "${TP01_HOST}" "! ss -ltnH | grep -q ':17412 '"

cd "${ROOT}"
if [[ -n "${PREBUILT_BINARY}" ]]; then
  cp "${PREBUILT_BINARY}" "${LOCAL_BINARY}"
else
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test -tags swblock_testtools -c \
    -o "${LOCAL_BINARY}" ./core/replication
fi
for host in "${M01_HOST}" "${M02_HOST}" "${TP01_HOST}"; do
  remote "${host}" "test '${REMOTE_ROOT}' = '/tmp/${RUN_ID}'; rm -rf -- '${REMOTE_ROOT}'; mkdir -p '${REMOTE_ROOT}'"
  "${SCP[@]}" "${LOCAL_BINARY}" "${host}:${REMOTE_BINARY}"
  remote "${host}" "chmod 0755 '${REMOTE_BINARY}'"
done
remote "${M02_HOST}" "test '${M02_STORE_ROOT}' = '/data/nvme/block/${RUN_ID}-stores'; rm -rf -- '${M02_STORE_ROOT}'; mkdir -p '${M02_STORE_ROOT}'"

for writers in "${WRITERS[@]}"; do
  start_replica "${M01_HOST}" "${M01_ADDR}" "${writers}" m01
  start_replica "${TP01_HOST}" "${TP01_ADDR}" "${writers}" tp01

  for run in $(seq 0 "${RUNS}"); do
    log="${ARTIFACT_DIR}/logs/w${writers}-run${run}.log"
    remote "${M02_HOST}" "env \
SW_BLOCK_PHASE174_STORE_DIR='${M02_STORE_ROOT}/w${writers}' \
SW_BLOCK_PHASE174_LAYER=rf3_tcp \
SW_BLOCK_PHASE174_WRITERS=${writers} \
SW_BLOCK_PHASE174_SET=1 \
SW_BLOCK_PHASE174_RUN=${run} \
SW_BLOCK_PHASE174_REMOTE_REPLICAS='${M01_ADDR},${TP01_ADDR}' \
GOMAXPROCS=4 taskset -c 0,2,4,6 \
'${REMOTE_BINARY}' -test.run '^TestPhase174FixedWorkPipeline$' -test.v -test.count=1 -test.timeout=10m" \
      >"${log}" 2>&1
    if [[ "${run}" != "0" ]]; then
      sed -n 's/^phase174_fixed_work_result=//p' "${log}" >>"${RESULTS}"
    fi
  done

  stop_replica "${M01_HOST}" "${writers}" m01
  stop_replica "${TP01_HOST}" "${writers}" tp01
done

python3 - "${RESULTS}" "${ARTIFACT_DIR}/remote" "${SUMMARY}" "${RUNS}" <<'PY'
import glob
import json
import statistics
import sys

results_path, remote_dir, summary_path, runs_text = sys.argv[1:]
runs = int(runs_text)
rows = [json.loads(line) for line in open(results_path, encoding="utf-8") if line.strip()]
if len(rows) != 3 * runs:
    raise SystemExit(f"primary rows={len(rows)} want {3 * runs}")

queue_saturation_rows = 0
replica_probe_count = 0
replica_catchup_count = 0
replica_rebuild_count = 0
max_replica_lag_lsn = 0
for row in rows:
    if row["contract"] != "phase174-fixed-work-v1" or row["ack_profile"] != "sync_quorum_rf3":
        raise SystemExit(f"bad primary contract: {row}")
    if row["replica_count"] != 2 or not row["remote_replica_evidence_required"]:
        raise SystemExit(f"bad external replica contract: {row}")
    if not row["post_measurement_sync_all"] or row["post_measurement_sync_all_ns"] <= 0:
        raise SystemExit(f"missing post-measurement drain: {row}")
    if not row["post_measurement_recovery"] or row.get("replica_probe_count") != 2:
        raise SystemExit(f"missing post-measurement recovery evidence: {row}")
    if row["api_operations"] != 16384 or row["logical_bytes"] != 67108864:
        raise SystemExit(f"bad fixed work: {row}")
    if row["primary_wal_write_ops"] != 16384 or row["replication_write_ops"] != 16384:
        raise SystemExit(f"write counter mismatch: {row}")
    if not row["close_recover_complete"] or row["primary_stable_lsn"] != row["primary_head_lsn"]:
        raise SystemExit(f"primary recovery mismatch: {row}")
    if row.get("peer_queue_saturated", 0) > 0:
        queue_saturation_rows += 1
    replica_probe_count += row["replica_probe_count"]
    replica_catchup_count += row.get("replica_catchup_count", 0)
    replica_rebuild_count += row.get("replica_rebuild_count", 0)
    max_replica_lag_lsn = max(max_replica_lag_lsn, row.get("max_replica_lag_lsn", 0))

remote_paths = sorted(glob.glob(f"{remote_dir}/w*-*-result.json"))
if len(remote_paths) != 6:
    raise SystemExit(f"remote results={len(remote_paths)} want 6")
for path in remote_paths:
    row = json.load(open(path, encoding="utf-8"))
    if row["status"] != "ok" or row["stable_lsn"] != row["head_lsn"]:
        raise SystemExit(f"bad remote recovery {path}: {row}")
    if row["head_lsn"] != row["expected_head_lsn"] or row["correctness_samples"] < 5:
        raise SystemExit(f"bad remote frontier/data {path}: {row}")

with open(summary_path, "a", encoding="utf-8") as out:
    for writers in (1, 4, 8):
        rates = [row["mib_per_second"] for row in rows if int(row["writers"]) == writers]
        out.write(f"writers_{writers}_median_mibps={statistics.median(rates):.3f}\n")
        out.write(f"writers_{writers}_max_min_ratio={max(rates) / min(rates):.3f}\n")
    out.write(f"primary_result_count={len(rows)}\n")
    out.write(f"remote_recovered_replica_count={len(remote_paths)}\n")
    out.write(f"peer_queue_saturation_row_count={queue_saturation_rows}\n")
    out.write(f"replica_probe_count={replica_probe_count}\n")
    out.write(f"replica_catchup_count={replica_catchup_count}\n")
    out.write(f"replica_rebuild_count={replica_rebuild_count}\n")
    out.write(f"max_replica_lag_lsn={max_replica_lag_lsn}\n")
    out.write("remote_replica_frontiers_and_bytes_equal=true\n")
    out.write("foreground_sync_quorum_preserved=true\n")
    out.write("post_measurement_recovery_verified=true\n")
    out.write("post_measurement_sync_all_verified=true\n")
    out.write("rf3_distinct_node_healthy=true\n")
    out.write("architecture_candidate_selected=false\n")
    out.write("product_mutation_present=false\n")
    out.write("phase174_distinct_node_rf3_status=ok\n")
PY

GATE_COMPLETE=true
cleanup
trap - EXIT
for host in "${M01_HOST}" "${M02_HOST}" "${TP01_HOST}"; do
  remote "${host}" "test ! -e '${REMOTE_ROOT}'"
done
remote "${M02_HOST}" "test ! -e '${M02_STORE_ROOT}'"
write_summary "remote_process_and_store_residue_count=0"
write_summary "published_artifact=${PUBLISH_ROOT}/${RUN_ID}.tar.gz"
tar -C "$(dirname "${ARTIFACT_DIR}")" -czf "${ARTIFACT_DIR}.tar.gz" "$(basename "${ARTIFACT_DIR}")"
"${SCP[@]}" "${ARTIFACT_DIR}.tar.gz" "${M02_HOST}:${PUBLISH_ROOT}/${RUN_ID}.tar.gz"
sha256sum "${ARTIFACT_DIR}.tar.gz" | tee "${ARTIFACT_DIR}.tar.gz.sha256"
cat "${SUMMARY}"
