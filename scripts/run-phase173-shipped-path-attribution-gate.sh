#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase173-shipped-path-attribution-gate}"
STORE_DIR="${SW_BLOCK_PHASE173_STORE_DIR:-${ARTIFACT_DIR}/stores}"
SUMMARY="${ARTIFACT_DIR}/phase173-shipped-path-attribution-summary.txt"
TEST_BINARY="${ARTIFACT_DIR}/storage.test"
SHAPE="mounted_mixed"
WRITERS=4
STORE_ID="d2-mounted-mixed-writers4"
TOOL_PID=""
TEST_PID=""

mkdir -p "${ARTIFACT_DIR}" "${STORE_DIR}" \
  "${ARTIFACT_DIR}/environment" "${ARTIFACT_DIR}/logs" \
  "${ARTIFACT_DIR}/profiles" "${ARTIFACT_DIR}/control"
: >"${SUMMARY}"

cleanup() {
  if [[ -n "${TOOL_PID}" ]]; then
    sudo -n kill -INT "${TOOL_PID}" >/dev/null 2>&1 || true
    wait "${TOOL_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${TEST_PID}" ]]; then
    kill "${TEST_PID}" >/dev/null 2>&1 || true
    wait "${TEST_PID}" >/dev/null 2>&1 || true
  fi
  rm -rf "${ARTIFACT_DIR}/control/strace" "${ARTIFACT_DIR}/control/perf"
  rm -f "${STORE_DIR}"/phase173-*.store
}
trap cleanup EXIT

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

capture() {
  local name="$1"
  shift
  "$@" >"${ARTIFACT_DIR}/environment/${name}.txt" 2>&1 || true
}

wait_for_file() {
  local path="$1"
  local pid="$2"
  local attempts=300
  while (( attempts > 0 )); do
    if [[ -f "${path}" ]]; then
      return 0
    fi
    if ! kill -0 "${pid}" >/dev/null 2>&1; then
      echo "process ${pid} exited before ${path} appeared" >&2
      return 1
    fi
    sleep 0.1
    attempts=$((attempts - 1))
  done
  echo "timed out waiting for ${path}" >&2
  return 1
}

extract_result() {
  local log="$1"
  local output="$2"
  local result
  result="$(sed -n 's/^phase173_fixed_work_result=//p' "${log}")"
  if [[ -z "${result}" ]]; then
    echo "missing fixed-work result in ${log}" >&2
    return 1
  fi
  printf '%s\n' "${result}" >"${output}"
}

run_fixed_work() {
  local run_id="$1"
  local log="$2"
  shift 2
  env \
    SW_BLOCK_PHASE173_SHAPE="${SHAPE}" \
    SW_BLOCK_PHASE173_WRITERS="${WRITERS}" \
    SW_BLOCK_PHASE173_RUN_ID="${run_id}" \
    SW_BLOCK_PHASE173_STORE_ID="${STORE_ID}" \
    SW_BLOCK_PHASE173_REUSE_STORE="true" \
    SW_BLOCK_PHASE173_STORE_DIR="${STORE_DIR}" \
    "$@" \
    "${TEST_BINARY}" -test.run '^TestPhase173WALStoreFixedWork$' -test.v -test.count=1 \
    >"${log}" 2>&1
}

start_controlled_test() {
  local run_id="$1"
  local control_dir="$2"
  local log="$3"
  rm -rf "${control_dir}"
  mkdir -p "${control_dir}"
  env \
    SW_BLOCK_PHASE173_SHAPE="${SHAPE}" \
    SW_BLOCK_PHASE173_WRITERS="${WRITERS}" \
    SW_BLOCK_PHASE173_RUN_ID="${run_id}" \
    SW_BLOCK_PHASE173_STORE_ID="${STORE_ID}" \
    SW_BLOCK_PHASE173_REUSE_STORE="true" \
    SW_BLOCK_PHASE173_STORE_DIR="${STORE_DIR}" \
    SW_BLOCK_PHASE173_CONTROL_DIR="${control_dir}" \
    "${TEST_BINARY}" -test.run '^TestPhase173WALStoreFixedWork$' -test.v -test.count=1 \
    >"${log}" 2>&1 &
  TEST_PID=$!
  wait_for_file "${control_dir}/ready" "${TEST_PID}"
  local reported_pid
  reported_pid="$(cat "${control_dir}/ready")"
  if [[ "${reported_pid}" != "${TEST_PID}" ]]; then
    echo "control PID ${reported_pid} != test PID ${TEST_PID}" >&2
    return 1
  fi
}

finish_controlled_test() {
  local control_dir="$1"
  local log="$2"
  local result="$3"
  wait_for_file "${control_dir}/done" "${TEST_PID}"
  sudo -n kill -INT "${TOOL_PID}"
  wait "${TOOL_PID}" || true
  TOOL_PID=""
  : >"${control_dir}/detached"
  wait "${TEST_PID}"
  TEST_PID=""
  extract_result "${log}" "${result}"
}

for tool in go python3 findmnt lsblk iostat strace perf; do
  command -v "${tool}" >/dev/null 2>&1 || {
    echo "required tool ${tool} is unavailable" >&2
    exit 1
  }
done
sudo -n true

write_summary "phase173_shipped_path_attribution_status=running"
write_summary "contract=phase173-fixed-work-v1"
write_summary "scope=walstore_engine_checkpoint_path"
write_summary "shape=${SHAPE}"
write_summary "writers=${WRITERS}"
write_summary "measurement_window=post_warmup_foreground_through_final_drain_and_correctness"
write_summary "architecture_candidate_selected=false"
write_summary "optimization_code_present=false"

store_source="$(findmnt -n -T "${STORE_DIR}" -o SOURCE)"
store_filesystem="$(findmnt -n -T "${STORE_DIR}" -o FSTYPE)"
if [[ "${store_source}" != /dev/* ]]; then
  echo "store source ${store_source} is not a local block device" >&2
  exit 1
fi
store_device="$(lsblk -ndo PKNAME "${store_source}" | head -n1)"
if [[ -z "${store_device}" ]]; then
  store_device="$(basename "${store_source}")"
fi
write_summary "store_source=${store_source}"
write_summary "store_device=${store_device}"
write_summary "store_filesystem=${store_filesystem}"

capture kernel uname -a
capture go-version go version
capture cpu lscpu
capture affinity taskset -pc "$$"
capture scheduler chrt -p "$$"
capture filesystem findmnt -T "${STORE_DIR}" -o TARGET,SOURCE,FSTYPE,OPTIONS
capture block-devices lsblk -o NAME,TYPE,SIZE,ROTA,MODEL,FSTYPE,MOUNTPOINTS
capture free-space df -hT "${STORE_DIR}"
capture load uptime
capture strace-version strace -V
capture perf-version perf --version
capture perf-policy sh -c 'cat /proc/sys/kernel/perf_event_paranoid'

cd "${ROOT}"
go test ./core/storage -run '^(TestFlusherInstrumentation|TestPhase173)' -count=1 \
  >"${ARTIFACT_DIR}/local-tests.log" 2>&1
go vet ./core/storage >"${ARTIFACT_DIR}/go-vet.log" 2>&1
go test -c -o "${TEST_BINARY}" ./core/storage

precondition_log="${ARTIFACT_DIR}/logs/precondition.log"
run_fixed_work "d2-precondition" "${precondition_log}"
extract_result "${precondition_log}" "${ARTIFACT_DIR}/precondition-result.json"
sync

iostat -xz 1 3 >"${ARTIFACT_DIR}/iostat.txt" 2>&1 &
iostat_pid=$!
sleep 0.2
plain_log="${ARTIFACT_DIR}/logs/plain.log"
run_fixed_work "d2-plain" "${plain_log}"
extract_result "${plain_log}" "${ARTIFACT_DIR}/plain-result.json"
wait "${iostat_pid}"
grep -Eq "(^|[[:space:]])${store_device}([[:space:]]|$)" "${ARTIFACT_DIR}/iostat.txt"
write_summary "iostat_device_observed=true"
write_summary "iostat_evidence=iostat.txt"

profile_log="${ARTIFACT_DIR}/logs/profile.log"
profile_dir="${ARTIFACT_DIR}/profiles/measured"
rm -rf "${profile_dir}"
run_fixed_work "d2-profile" "${profile_log}" \
  SW_BLOCK_PHASE173_PROFILE_DIR="${profile_dir}"
extract_result "${profile_log}" "${ARTIFACT_DIR}/profile-result.json"
for profile in cpu heap allocs; do
  test -s "${profile_dir}/${profile}.pprof"
  go tool pprof -top -nodecount=40 "${TEST_BINARY}" \
    "${profile_dir}/${profile}.pprof" \
    >"${ARTIFACT_DIR}/profiles/${profile}-top.txt"
done
write_summary "cpu_profile=profiles/measured/cpu.pprof"
write_summary "heap_profile=profiles/measured/heap.pprof"
write_summary "allocs_profile=profiles/measured/allocs.pprof"
write_summary "profile_scope_exact=true"

strace_control="${ARTIFACT_DIR}/control/strace"
strace_log="${ARTIFACT_DIR}/logs/strace.log"
strace_output="${ARTIFACT_DIR}/strace.txt"
start_controlled_test "d2-strace" "${strace_control}" "${strace_log}"
sudo -n strace -f -qq -tt -T \
  -e trace=pread64,pwrite64,fsync,fdatasync \
  -p "${TEST_PID}" -o "${strace_output}" \
  2>"${ARTIFACT_DIR}/strace.stderr.txt" &
TOOL_PID=$!
sleep 0.5
sudo -n kill -0 "${TOOL_PID}"
: >"${strace_control}/go"
finish_controlled_test "${strace_control}" "${strace_log}" \
  "${ARTIFACT_DIR}/strace-result.json"
test -s "${strace_output}"
write_summary "strace_scope_exact=true"
write_summary "strace_evidence=strace.txt"

perf_control="${ARTIFACT_DIR}/control/perf"
perf_log="${ARTIFACT_DIR}/logs/perf.log"
perf_output="${ARTIFACT_DIR}/perf-stat.csv"
start_controlled_test "d2-perf" "${perf_control}" "${perf_log}"
sudo -n perf stat -x, \
  -e task-clock,cycles,instructions,cache-misses,context-switches,cpu-migrations,page-faults \
  -p "${TEST_PID}" -o "${perf_output}" \
  2>"${ARTIFACT_DIR}/perf.stderr.txt" &
TOOL_PID=$!
sleep 0.5
sudo -n kill -0 "${TOOL_PID}"
: >"${perf_control}/go"
finish_controlled_test "${perf_control}" "${perf_log}" \
  "${ARTIFACT_DIR}/perf-result.json"
test -s "${perf_output}"
write_summary "perf_scope_exact=true"
write_summary "perf_requires_sudo=true"
write_summary "perf_evidence=perf-stat.csv"

python3 - \
  "${ARTIFACT_DIR}/plain-result.json" \
  "${ARTIFACT_DIR}/profile-result.json" \
  "${ARTIFACT_DIR}/strace-result.json" \
  "${ARTIFACT_DIR}/perf-result.json" \
  "${strace_output}" "${perf_output}" "${SUMMARY}" <<'PY'
import json
import re
import sys

plain_path, profile_path, strace_path, perf_path, trace_path, perf_csv, summary_path = sys.argv[1:]
paths = (plain_path, profile_path, strace_path, perf_path)
rows = [json.load(open(path, encoding="utf-8")) for path in paths]

for row in rows:
    if row["contract"] != "phase173-fixed-work-v1":
        raise SystemExit(f"bad contract: {row}")
    if not row["store_reused"] or row["shape"] != "mounted_mixed" or row["writers"] != 4:
        raise SystemExit(f"bad fixed-work identity: {row}")
    logical_blocks = row["logical_blocks"]
    logical_bytes = row["logical_bytes"]
    if logical_blocks != 16000 or logical_bytes != logical_blocks * row["block_size"]:
        raise SystemExit(f"bad logical work: {row}")
    exact = {
        "wal_copy_ops": logical_blocks,
        "wal_copy_bytes": logical_bytes,
        "wal_encode_ops": logical_blocks,
        "wal_checksum_ops": logical_blocks,
        "commit_lock_wait_ops": row["api_operations"],
        "dirty_map_update_ops": logical_blocks,
        "flush_snapshot_entries": logical_blocks,
        "flush_header_reads": logical_blocks,
        "flush_record_reads": logical_blocks,
        "flush_record_decode_ops": logical_blocks,
        "validated_records": logical_blocks,
        "extent_write_ops": logical_blocks,
        "extent_write_bytes": logical_bytes,
        "final_sync_calls": 1,
        "dirty_entries": 0,
        "flush_record_decode_failures": 0,
        "validation_failures": 0,
        "superseded_entries": 0,
    }
    for key, want in exact.items():
        if row[key] != want:
            raise SystemExit(f"{row['run_id']} {key}={row[key]} want {want}")
    if row["wal_encode_bytes"] != row["flush_record_read_bytes"]:
        raise SystemExit(f"WAL encoded/read bytes disagree: {row}")
    if row["flush_record_decode_bytes"] != row["flush_record_read_bytes"]:
        raise SystemExit(f"WAL read/decode bytes disagree: {row}")
    if row["checkpoint_lsn"] != row["head_lsn"] or row["head_lsn"] != row["synced_lsn"]:
        raise SystemExit(f"incomplete checkpoint: {row}")
    if row["wal_writeat_calls"] < row["wal_append_ops"]:
        raise SystemExit(f"WAL WriteAt accounting failed: {row}")
    if row["checkpoint_write_ops"] != row["checkpoint_sync_ops"]:
        raise SystemExit(f"checkpoint write/sync accounting failed: {row}")
    if row["extent_sync_ops"] != row["flush_cycles"]:
        raise SystemExit(f"extent sync/cycle accounting failed: {row}")

trace = open(trace_path, encoding="utf-8", errors="replace").read()
counts = {name: len(re.findall(rf"\b{name}\(", trace)) for name in ("pread64", "pwrite64", "fsync", "fdatasync")}
traced = rows[2]
expected_pread = traced["flush_header_reads"] + traced["flush_record_reads"] + traced["correctness_samples"]
expected_pwrite = traced["wal_writeat_calls"] + traced["extent_write_ops"] + traced["checkpoint_write_ops"]
expected_sync = traced["extent_sync_ops"] + traced["checkpoint_sync_ops"] + traced["final_sync_calls"]
if counts["pread64"] != expected_pread:
    raise SystemExit(f"strace pread64={counts['pread64']} want {expected_pread}")
if counts["pwrite64"] != expected_pwrite:
    raise SystemExit(f"strace pwrite64={counts['pwrite64']} want {expected_pwrite}")
if counts["fsync"] + counts["fdatasync"] != expected_sync:
    raise SystemExit(f"strace sync={counts['fsync'] + counts['fdatasync']} want {expected_sync}")

perf_text = open(perf_csv, encoding="utf-8", errors="replace").read()
for event in ("task-clock", "cycles", "instructions", "cache-misses", "context-switches", "page-faults"):
    matching = [line for line in perf_text.splitlines() if event in line]
    if not matching or any("<not" in line for line in matching):
        raise SystemExit(f"perf event unavailable: {event}")

plain = rows[0]
known_flush = sum(plain[key] for key in (
    "flush_snapshot_ns",
    "flush_opportunity_ns",
    "flush_header_read_ns",
    "flush_record_read_ns",
    "flush_record_decode_ns",
    "extent_write_ns",
    "extent_sync_ns",
    "checkpoint_write_ns",
    "checkpoint_sync_ns",
))
flush_remainder = max(0, plain["flush_cycle_ns"] - known_flush)
summary = [
    "fixed_work_runs_reconciled=4",
    "product_counter_reconciliation=true",
    f"logical_operations={plain['api_operations']}",
    f"logical_blocks={plain['logical_blocks']}",
    f"logical_bytes={plain['logical_bytes']}",
    f"foreground_ns={plain['foreground_ns']}",
    f"foreground_mib_per_second={plain['foreground_mib_per_second']:.3f}",
    f"wal_copy_ops={plain['wal_copy_ops']}",
    f"wal_copy_bytes={plain['wal_copy_bytes']}",
    f"wal_copy_ns={plain['wal_copy_ns']}",
    f"wal_encode_ops={plain['wal_encode_ops']}",
    f"wal_encode_bytes={plain['wal_encode_bytes']}",
    f"wal_encode_ns={plain['wal_encode_ns']}",
    f"wal_checksum_ops={plain['wal_checksum_ops']}",
    f"wal_checksum_bytes={plain['wal_checksum_bytes']}",
    f"wal_checksum_ns={plain['wal_checksum_ns']}",
    f"wal_writeat_calls={plain['wal_writeat_calls']}",
    f"wal_writeat_bytes={plain['wal_writeat_bytes']}",
    f"wal_append_ns={plain['wal_append_ns']}",
    f"commit_lock_wait_ops={plain['commit_lock_wait_ops']}",
    f"commit_lock_wait_ns={plain['commit_lock_wait_ns']}",
    f"flush_cycles={plain['flush_cycles']}",
    f"flush_cycle_ns={plain['flush_cycle_ns']}",
    f"flush_snapshot_entries={plain['flush_snapshot_entries']}",
    f"flush_snapshot_ns={plain['flush_snapshot_ns']}",
    f"flush_header_reads={plain['flush_header_reads']}",
    f"flush_header_read_ns={plain['flush_header_read_ns']}",
    f"flush_record_reads={plain['flush_record_reads']}",
    f"flush_record_read_ns={plain['flush_record_read_ns']}",
    f"flush_record_decode_ops={plain['flush_record_decode_ops']}",
    f"flush_record_decode_ns={plain['flush_record_decode_ns']}",
    f"extent_write_ops={plain['extent_write_ops']}",
    f"extent_write_bytes={plain['extent_write_bytes']}",
    f"extent_write_ns={plain['extent_write_ns']}",
    f"extent_sync_ops={plain['extent_sync_ops']}",
    f"extent_sync_ns={plain['extent_sync_ns']}",
    f"checkpoint_write_ops={plain['checkpoint_write_ops']}",
    f"checkpoint_write_ns={plain['checkpoint_write_ns']}",
    f"checkpoint_sync_ops={plain['checkpoint_sync_ops']}",
    f"checkpoint_sync_ns={plain['checkpoint_sync_ns']}",
    f"flush_unattributed_remainder_ns={flush_remainder}",
    f"strace_pread64_calls={counts['pread64']}",
    f"strace_pwrite64_calls={counts['pwrite64']}",
    f"strace_sync_calls={counts['fsync'] + counts['fdatasync']}",
    "strace_product_counter_reconciliation=true",
    "perf_required_events_present=true",
    "checkpoint_frontiers_equal=true",
    "complete_drain=true",
    "architecture_candidate_selected=false",
    "phase173_shipped_path_attribution_status=ok",
]
with open(summary_path, "a", encoding="utf-8") as out:
    for line in summary:
        out.write(line + "\n")
PY

cleanup
TOOL_PID=""
TEST_PID=""
sync
find "${STORE_DIR}" -maxdepth 1 -type f -name 'phase173-*.store' -print -quit | grep -q . && {
  echo "phase173 attribution store residue remains under ${STORE_DIR}" >&2
  exit 1
}
write_summary "store_residue_count=0"
cat "${SUMMARY}"
