#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase174-nvme-tcp-fixed-work-gate}"
STORE_DIR="${SW_BLOCK_PHASE174_NVME_STORE_DIR:-${ARTIFACT_DIR}/stores}"
SUMMARY="${ARTIFACT_DIR}/phase174-nvme-tcp-fixed-work-summary.txt"
RESULTS="${ARTIFACT_DIR}/phase174-nvme-tcp-fixed-work-results.jsonl"
TEST_BINARY="${ARTIFACT_DIR}/phase174-nvme.test"
RUNS=5
MAX_RANGE="1.25"
CPUSET="${SW_BLOCK_PHASE174_CPUSET:-0,2,4,6}"
GOMAXPROCS_VALUE="${SW_BLOCK_PHASE174_GOMAXPROCS:-4}"
SOURCE_COMMIT="${SW_BLOCK_PHASE174_SOURCE_COMMIT:-$(git -C "${ROOT}" rev-parse HEAD 2>/dev/null || true)}"
WRITERS_FORWARD=(1 4 8)
WRITERS_REVERSE=(8 4 1)

mkdir -p "${ARTIFACT_DIR}/logs" "${ARTIFACT_DIR}/profiles" "${STORE_DIR}"
: >"${SUMMARY}"
: >"${RESULTS}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

capture() {
  local name="$1"
  shift
  "$@" >"${ARTIFACT_DIR}/${name}.txt" 2>&1 || true
}

cleanup() {
  find "${STORE_DIR}" -type f -name '*.store' -delete 2>/dev/null || true
  find "${STORE_DIR}" -depth -type d -empty -delete 2>/dev/null || true
  rm -f "${TEST_BINARY}"
}
trap cleanup EXIT

for command in go python3 taskset findmnt; do
  command -v "${command}" >/dev/null 2>&1 || {
    echo "missing required command: ${command}" >&2
    exit 2
  }
done
if [[ -z "${SOURCE_COMMIT}" ]]; then
  echo "phase174 source commit is required" >&2
  exit 2
fi

store_source="$(findmnt -n -o SOURCE -T "${STORE_DIR}")"
store_filesystem="$(findmnt -n -o FSTYPE -T "${STORE_DIR}")"
root_source="$(findmnt -n -o SOURCE -T /)"
if [[ -z "${store_source}" || "${store_source}" == "${root_source}" ]]; then
  echo "phase174 NVMe store must use a dedicated filesystem: store=${store_source:-missing} root=${root_source}" >&2
  exit 1
fi

write_summary "phase174_nvme_tcp_fixed_work_status=running"
write_summary "contract=phase174-fixed-work-v1"
write_summary "source_commit=${SOURCE_COMMIT}"
write_summary "layer=nvme_tcp_rf1"
write_summary "scope=nvme_tcp_target_durable_adapter"
write_summary "ack_profile=local_durable"
write_summary "transport=tcp"
write_summary "runs_per_set=${RUNS}"
write_summary "sets=2"
write_summary "writers=1,4,8"
write_summary "logical_block_bytes=4096"
write_summary "nvme_sector_bytes=512"
write_summary "api_operations_per_run=16384"
write_summary "max_min_limit=${MAX_RANGE}"
write_summary "store_source=${store_source}"
write_summary "store_filesystem=${store_filesystem}"
write_summary "dedicated_store_source=true"
write_summary "control_cpuset=${CPUSET}"
write_summary "control_gomaxprocs=${GOMAXPROCS_VALUE}"
write_summary "second_set_order=reversed"
write_summary "mounted_shape_comparable=false"
write_summary "mounted_throughput_ratio_allowed=false"

capture kernel uname -a
capture go-version go version
capture cpu lscpu
capture filesystem findmnt -T "${STORE_DIR}" -o TARGET,SOURCE,FSTYPE,OPTIONS
capture free-space df -hT "${STORE_DIR}"
capture load uptime

cd "${ROOT}"
go test -tags swblock_testtools ./core/frontend/nvme \
  -run '^TestPhase174NVMeFixedWorkContract$' -count=1 \
  >"${ARTIFACT_DIR}/contract-test.log" 2>&1
go test -tags swblock_testtools -c -o "${TEST_BINARY}" ./core/frontend/nvme

for set_id in 1 2; do
  sync
  sleep 5
  capture "set-${set_id}-start-load" uptime
  if [[ "${set_id}" == "1" ]]; then
    writers_values=("${WRITERS_FORWARD[@]}")
  else
    writers_values=("${WRITERS_REVERSE[@]}")
  fi
  for writers in "${writers_values[@]}"; do
    precondition_log="${ARTIFACT_DIR}/logs/set${set_id}-writers${writers}-precondition.log"
    SW_BLOCK_PHASE174_NVME_STORE_DIR="${STORE_DIR}" \
    SW_BLOCK_PHASE174_NVME_WRITERS="${writers}" \
    SW_BLOCK_PHASE174_NVME_SET="${set_id}" \
    SW_BLOCK_PHASE174_NVME_RUN=0 \
    GOMAXPROCS="${GOMAXPROCS_VALUE}" taskset -c "${CPUSET}" \
      "${TEST_BINARY}" -test.run '^TestPhase174NVMeFixedWorkPipeline$' -test.count=1 -test.timeout=10m \
      >"${precondition_log}" 2>&1
    for run in $(seq 1 "${RUNS}"); do
      sync
      sleep 0.25
      run_log="${ARTIFACT_DIR}/logs/set${set_id}-writers${writers}-run${run}.log"
      SW_BLOCK_PHASE174_NVME_STORE_DIR="${STORE_DIR}" \
      SW_BLOCK_PHASE174_NVME_WRITERS="${writers}" \
      SW_BLOCK_PHASE174_NVME_SET="${set_id}" \
      SW_BLOCK_PHASE174_NVME_RUN="${run}" \
      GOMAXPROCS="${GOMAXPROCS_VALUE}" taskset -c "${CPUSET}" \
        "${TEST_BINARY}" -test.run '^TestPhase174NVMeFixedWorkPipeline$' -test.count=1 -test.timeout=10m \
        >"${run_log}" 2>&1
      sed -n 's/^phase174_nvme_fixed_work_result=//p' "${run_log}" >>"${RESULTS}"
    done
  done
done

PROFILE_STORE="${STORE_DIR}/profiles"
mkdir -p "${PROFILE_STORE}"
SW_BLOCK_PHASE174_NVME_STORE_DIR="${PROFILE_STORE}" \
SW_BLOCK_PHASE174_NVME_WRITERS=4 \
SW_BLOCK_PHASE174_NVME_SET=1 \
SW_BLOCK_PHASE174_NVME_RUN=0 \
GOMAXPROCS="${GOMAXPROCS_VALUE}" taskset -c "${CPUSET}" \
  "${TEST_BINARY}" -test.run '^TestPhase174NVMeFixedWorkPipeline$' -test.count=1 -test.timeout=10m \
  >"${ARTIFACT_DIR}/profiles/precondition.log" 2>&1

cpu_profiles=()
block_profiles=()
mutex_profiles=()
for run in $(seq 1 "${RUNS}"); do
  prefix="${ARTIFACT_DIR}/profiles/run${run}"
  SW_BLOCK_PHASE174_NVME_STORE_DIR="${PROFILE_STORE}" \
  SW_BLOCK_PHASE174_NVME_WRITERS=4 \
  SW_BLOCK_PHASE174_NVME_SET=1 \
  SW_BLOCK_PHASE174_NVME_RUN="${run}" \
  GOMAXPROCS="${GOMAXPROCS_VALUE}" taskset -c "${CPUSET}" \
    "${TEST_BINARY}" -test.run '^TestPhase174NVMeFixedWorkPipeline$' -test.count=1 -test.timeout=10m \
    -test.cpuprofile="${prefix}-cpu.pprof" \
    -test.blockprofile="${prefix}-block.pprof" -test.blockprofilerate=1 \
    -test.mutexprofile="${prefix}-mutex.pprof" -test.mutexprofilefraction=1 \
    >"${prefix}.log" 2>&1
  cpu_profiles+=("${prefix}-cpu.pprof")
  block_profiles+=("${prefix}-block.pprof")
  mutex_profiles+=("${prefix}-mutex.pprof")
done
go tool pprof -top -nodecount=40 "${TEST_BINARY}" "${cpu_profiles[@]}" \
  >"${ARTIFACT_DIR}/profiles/cpu-top.txt" 2>&1
go tool pprof -top -nodecount=40 "${TEST_BINARY}" "${block_profiles[@]}" \
  >"${ARTIFACT_DIR}/profiles/block-top.txt" 2>&1
go tool pprof -top -nodecount=40 "${TEST_BINARY}" "${mutex_profiles[@]}" \
  >"${ARTIFACT_DIR}/profiles/mutex-top.txt" 2>&1

python3 - "${RESULTS}" "${SUMMARY}" "${RUNS}" "${MAX_RANGE}" <<'PY'
import json
import math
import statistics
import sys
from collections import defaultdict

results_path, summary_path, runs_text, max_range_text = sys.argv[1:]
runs = int(runs_text)
max_range = float(max_range_text)
rows = [json.loads(line) for line in open(results_path, encoding="utf-8") if line.strip()]
if len(rows) != 6 * runs:
    raise SystemExit(f"NVMe fixed-work rows={len(rows)} want {6 * runs}")

groups = defaultdict(list)
for row in rows:
    if row["contract"] != "phase174-fixed-work-v1":
        raise SystemExit(f"bad contract: {row}")
    if row["layer"] != "nvme_tcp_rf1" or row["ack_profile"] != "local_durable":
        raise SystemExit(f"bad layer contract: {row}")
    if row["api_operations"] != 16384 or row["logical_bytes"] != 67108864:
        raise SystemExit(f"bad fixed work: {row}")
    for key in (
        "nvme_write_commands", "nvme_r2t_write_commands", "nvme_h2c_data_pdus",
        "target_write_ops", "adapter_request_ops", "adapter_write_ops",
        "adapter_storage_write_calls", "adapter_storage_write_blocks",
        "primary_wal_write_ops",
    ):
        if row[key] != 16384:
            raise SystemExit(f"{key} mismatch: {row}")
    for key in (
        "nvme_r2t_write_bytes", "nvme_h2c_data_bytes", "target_write_bytes",
        "adapter_request_bytes", "adapter_write_bytes",
    ):
        if row[key] != 67108864:
            raise SystemExit(f"{key} mismatch: {row}")
    if row["client_write_latency_ns"] < row["target_write_ns"]:
        raise SystemExit(f"target duration exceeds client duration: {row}")
    if row["nvme_round_trip_nonbackend_ns"] != row["client_write_latency_ns"] - row["target_write_ns"]:
        raise SystemExit(f"non-backend duration mismatch: {row}")
    if row["primary_stable_lsn"] != row["primary_head_lsn"]:
        raise SystemExit(f"primary frontier mismatch: {row}")
    if not row["flusher_phase_reset"] or not row["close_recover_complete"]:
        raise SystemExit(f"recovery/reset missing: {row}")
    if row["correctness_samples"] < 5 or row["foreground_ns"] <= 0 or row["p99_ns"] <= 0:
        raise SystemExit(f"timing/correctness evidence missing: {row}")
    if row["mounted_shape_comparable"] or row["mounted_throughput_ratio_allowed"]:
        raise SystemExit(f"mounted diagnostic mislabelled comparable: {row}")
    groups[(int(row["writers"]), int(row["set"]))].append(row)

for key, values in groups.items():
    if len(values) != runs:
        raise SystemExit(f"group {key} rows={len(values)} want {runs}")

def all_rows(writers):
    return groups[(writers, 1)] + groups[(writers, 2)]

def median_rate(writers):
    return statistics.median(row["mib_per_second"] for row in all_rows(writers))

def value_range(values):
    return max(values) / min(values)

def rate_range(writers, set_id=None):
    values = all_rows(writers) if set_id is None else groups[(writers, set_id)]
    return value_range([row["mib_per_second"] for row in values])

def median_per_op(rows, field):
    return statistics.median(row[field] / row["api_operations"] for row in rows)

def correlation(rows, field):
    xs = [float(row["foreground_ns"]) for row in rows]
    ys = [float(row[field]) for row in rows]
    x_mean = statistics.mean(xs)
    y_mean = statistics.mean(ys)
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    x_norm = math.sqrt(sum((x - x_mean) ** 2 for x in xs))
    y_norm = math.sqrt(sum((y - y_mean) ** 2 for y in ys))
    if x_norm == 0 or y_norm == 0:
        return 0.0
    return numerator / (x_norm * y_norm)

four = all_rows(4)
eight = all_rows(8)
four_stable = all(rate_range(4, set_id) <= max_range for set_id in (1, 2)) and rate_range(4) <= max_range
all_shapes_stable = all(
    rate_range(writers, set_id) <= max_range
    for writers in (1, 4, 8)
    for set_id in (1, 2)
) and all(rate_range(writers) <= max_range for writers in (1, 4, 8))
target_per_op = median_per_op(four, "target_write_ns")
roundtrip_per_op = median_per_op(four, "nvme_round_trip_nonbackend_ns")
dominant = "target_backend_call" if target_per_op >= roundtrip_per_op else "nvme_tcp_round_trip_nonbackend"

with open(summary_path, "a", encoding="utf-8") as out:
    for writers in (1, 4, 8):
        out.write(f"nvme_tcp_rf1_writers_{writers}_median_mibps={median_rate(writers):.3f}\n")
        out.write(f"nvme_tcp_rf1_writers_{writers}_max_min_ratio={rate_range(writers):.3f}\n")
        out.write(f"nvme_tcp_rf1_writers_{writers}_set1_max_min_ratio={rate_range(writers, 1):.3f}\n")
        out.write(f"nvme_tcp_rf1_writers_{writers}_set2_max_min_ratio={rate_range(writers, 2):.3f}\n")
    for field in (
        "client_write_latency_ns", "nvme_round_trip_nonbackend_ns", "target_write_ns",
        "adapter_write_ns", "primary_write_commit_lock_wait_ns", "primary_wal_encode_ns",
        "primary_wal_append_ns", "primary_dirty_map_ns", "foreground_flusher_cycle_ns",
        "foreground_flusher_extent_write_ns", "foreground_flusher_extent_sync_ns",
    ):
        out.write(f"nvme_tcp_rf1_writers_4_{field}_per_op={median_per_op(four, field):.3f}\n")
    out.write(f"nvme_tcp_rf1_writers_4_flusher_foreground_correlation={correlation(four, 'foreground_flusher_cycle_ns'):.3f}\n")
    out.write(f"nvme_tcp_rf1_writers_8_flusher_cycle_ns_per_op={median_per_op(eight, 'foreground_flusher_cycle_ns'):.3f}\n")
    out.write(f"nvme_tcp_rf1_writers_8_flusher_foreground_correlation={correlation(eight, 'foreground_flusher_cycle_ns'):.3f}\n")
    out.write(f"nvme_tcp_rf1_dominant_accumulated_boundary={dominant}\n")
    out.write("nvme_tcp_rf1_counter_reconciliation=true\n")
    out.write("nvme_tcp_rf1_close_recover_verified=true\n")
    out.write(f"nvme_tcp_rf1_four_writer_stability_gate={'pass' if four_stable else 'hold'}\n")
    out.write(f"nvme_tcp_rf1_all_shapes_stability_gate={'pass' if all_shapes_stable else 'hold'}\n")
    out.write("mounted_shape_comparable=false\n")
    out.write("mounted_throughput_ratio_allowed=false\n")
    out.write("architecture_candidate_selected=false\n")
    out.write("phase174_nvme_tcp_fixed_work_status=ok\n")
PY

cleanup
find "${STORE_DIR}" -type f -name '*.store' -print -quit 2>/dev/null | grep -q . && {
  echo "phase174 NVMe store residue remains under ${STORE_DIR}" >&2
  exit 1
}
write_summary "store_residue_count=0"
cat "${SUMMARY}"
