#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase174-fixed-work-pipeline-gate}"
STORE_DIR="${SW_BLOCK_PHASE174_STORE_DIR:-${ARTIFACT_DIR}/stores}"
SUMMARY="${ARTIFACT_DIR}/phase174-fixed-work-pipeline-summary.txt"
RESULTS="${ARTIFACT_DIR}/phase174-fixed-work-results.jsonl"
TEST_BINARY="${ARTIFACT_DIR}/phase174-replication.test"
RUNS=5
MAX_RANGE="1.25"
CPUSET="${SW_BLOCK_PHASE174_CPUSET:-0,2,4,6}"
GOMAXPROCS_VALUE="${SW_BLOCK_PHASE174_GOMAXPROCS:-4}"

mkdir -p "${ARTIFACT_DIR}/logs" "${STORE_DIR}"
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

store_source="$(findmnt -n -o SOURCE -T "${STORE_DIR}")"
store_filesystem="$(findmnt -n -o FSTYPE -T "${STORE_DIR}")"
root_source="$(findmnt -n -o SOURCE -T /)"
if [[ -z "${store_source}" || "${store_source}" == "${root_source}" ]]; then
  echo "phase174 store must use a dedicated filesystem: store=${store_source:-missing} root=${root_source}" >&2
  exit 1
fi

write_summary "phase174_fixed_work_pipeline_status=running"
write_summary "contract=phase174-fixed-work-v1"
write_summary "runs_per_set=${RUNS}"
write_summary "sets=2"
write_summary "writers=1,4,8"
write_summary "logical_block_bytes=4096"
write_summary "api_operations_per_run=16384"
write_summary "max_min_limit=${MAX_RANGE}"
write_summary "store_source=${store_source}"
write_summary "store_filesystem=${store_filesystem}"
write_summary "dedicated_store_source=true"
write_summary "control_cpuset=${CPUSET}"
write_summary "control_gomaxprocs=${GOMAXPROCS_VALUE}"
write_summary "rf1_ack_profile=local_durable"
write_summary "rf3_ack_profile=sync_quorum_rf3"
write_summary "cross_ack_profile_throughput_ratio_allowed=false"

capture kernel uname -a
capture go-version go version
capture cpu lscpu
capture filesystem findmnt -T "${STORE_DIR}" -o TARGET,SOURCE,FSTYPE,OPTIONS
capture free-space df -hT "${STORE_DIR}"
capture load uptime

cd "${ROOT}"
go test -tags swblock_testtools ./core/replication -run '^TestPhase174FixedWorkContract$' -count=1 \
  >"${ARTIFACT_DIR}/contract-test.log" 2>&1
go test -tags swblock_testtools -c -o "${TEST_BINARY}" ./core/replication

for set_id in 1 2; do
  for layer in direct_walstore adapter_rf1 rf3_tcp; do
    for writers in 1 4 8; do
      if [[ "${layer}" != "rf3_tcp" ]]; then
        precondition_log="${ARTIFACT_DIR}/logs/set${set_id}-${layer}-writers${writers}-precondition.log"
        SW_BLOCK_PHASE174_STORE_DIR="${STORE_DIR}" \
        SW_BLOCK_PHASE174_LAYER="${layer}" \
        SW_BLOCK_PHASE174_WRITERS="${writers}" \
        SW_BLOCK_PHASE174_SET="${set_id}" \
        SW_BLOCK_PHASE174_RUN=0 \
        GOMAXPROCS="${GOMAXPROCS_VALUE}" taskset -c "${CPUSET}" \
          "${TEST_BINARY}" -test.run '^TestPhase174FixedWorkPipeline$' -test.v -test.count=1 \
          >"${precondition_log}" 2>&1
      fi
      for run in $(seq 1 "${RUNS}"); do
        sync
        sleep 0.25
        run_log="${ARTIFACT_DIR}/logs/set${set_id}-${layer}-writers${writers}-run${run}.log"
        SW_BLOCK_PHASE174_STORE_DIR="${STORE_DIR}" \
        SW_BLOCK_PHASE174_LAYER="${layer}" \
        SW_BLOCK_PHASE174_WRITERS="${writers}" \
        SW_BLOCK_PHASE174_SET="${set_id}" \
        SW_BLOCK_PHASE174_RUN="${run}" \
        GOMAXPROCS="${GOMAXPROCS_VALUE}" taskset -c "${CPUSET}" \
          "${TEST_BINARY}" -test.run '^TestPhase174FixedWorkPipeline$' -test.v -test.count=1 \
          >"${run_log}" 2>&1
        sed -n 's/^phase174_fixed_work_result=//p' "${run_log}" >>"${RESULTS}"
      done
    done
  done
done

python3 - "${RESULTS}" "${SUMMARY}" "${RUNS}" "${MAX_RANGE}" <<'PY'
import json
import statistics
import sys
from collections import defaultdict

results_path, summary_path, runs_text, max_range_text = sys.argv[1:]
runs = int(runs_text)
max_range = float(max_range_text)
rows = [json.loads(line) for line in open(results_path, encoding="utf-8") if line.strip()]
if len(rows) != 90:
    raise SystemExit(f"fixed-work rows={len(rows)} want 90")

groups = defaultdict(list)
for row in rows:
    if row["contract"] != "phase174-fixed-work-v1":
        raise SystemExit(f"bad contract: {row}")
    if row["api_operations"] != 16384 or row["logical_bytes"] != 16384 * 4096:
        raise SystemExit(f"bad fixed work: {row}")
    if row["primary_wal_write_ops"] != 16384:
        raise SystemExit(f"bad primary write count: {row}")
    if row["primary_stable_lsn"] != row["primary_head_lsn"]:
        raise SystemExit(f"primary frontier mismatch: {row}")
    if not row["flusher_phase_reset"]:
        raise SystemExit(f"flusher phase was not reset: {row}")
    if not row["close_recover_complete"] or row["correctness_samples"] < 5:
        raise SystemExit(f"close/recover correctness failed: {row}")
    if row["foreground_ns"] <= 0 or row["final_sync_ns"] <= 0 or row["p99_ns"] <= 0:
        raise SystemExit(f"missing timing evidence: {row}")
    if row["layer"] == "rf3_tcp":
        if row["ack_profile"] != "sync_quorum_rf3" or row["replication_write_ops"] != 16384:
            raise SystemExit(f"bad RF3 contract: {row}")
        if row["replica_count"] != 2 or row["replica_durable_count"] < 1:
            raise SystemExit(f"RF3 did not satisfy quorum durability: {row}")
    elif row["ack_profile"] != "local_durable" or row["replica_count"] != 0:
        raise SystemExit(f"bad RF1 contract: {row}")
    groups[(row["layer"], int(row["writers"]))].append(row)

for key, values in groups.items():
    if len(values) != 2 * runs:
        raise SystemExit(f"group {key} rows={len(values)} want {2 * runs}")

def median_rate(layer, writers):
    return statistics.median(row["mib_per_second"] for row in groups[(layer, writers)])

def rate_range(layer, writers):
    values = [row["mib_per_second"] for row in groups[(layer, writers)]]
    return max(values) / min(values)

def set_rate_range(layer, writers, set_id):
    values = [
        row["mib_per_second"]
        for row in groups[(layer, writers)]
        if int(row["set"]) == set_id
    ]
    if len(values) != runs:
        raise SystemExit(f"group {(layer, writers)} set={set_id} rows={len(values)} want {runs}")
    return max(values) / min(values)

with open(summary_path, "a", encoding="utf-8") as out:
    for layer in ("direct_walstore", "adapter_rf1", "rf3_tcp"):
        for writers in (1, 4, 8):
            out.write(f"{layer}_writers_{writers}_median_mibps={median_rate(layer, writers):.3f}\n")
            out.write(f"{layer}_writers_{writers}_max_min_ratio={rate_range(layer, writers):.3f}\n")

    direct_four = median_rate("direct_walstore", 4)
    adapter_four = median_rate("adapter_rf1", 4)
    direct_set_ranges = [set_rate_range("direct_walstore", 4, set_id) for set_id in (1, 2)]
    adapter_set_ranges = [set_rate_range("adapter_rf1", 4, set_id) for set_id in (1, 2)]
    rf1_stable = (
        all(value <= max_range for value in direct_set_ranges)
        and all(value <= max_range for value in adapter_set_ranges)
        and rate_range("direct_walstore", 4) <= max_range
        and rate_range("adapter_rf1", 4) <= max_range
    )
    rf3_rows = [row for row in rows if row["layer"] == "rf3_tcp"]
    rf3_healthy = all(
        row["replica_durable_count"] == 2
        and row["replica_frontiers_equal"]
        and row.get("peer_queue_saturated", 0) == 0
        for row in rf3_rows
    )
    queue_saturation_rows = sum(row.get("peer_queue_saturated", 0) > 0 for row in rf3_rows)
    out.write(f"direct_walstore_writers_4_set1_max_min_ratio={direct_set_ranges[0]:.3f}\n")
    out.write(f"direct_walstore_writers_4_set2_max_min_ratio={direct_set_ranges[1]:.3f}\n")
    out.write(f"adapter_rf1_writers_4_set1_max_min_ratio={adapter_set_ranges[0]:.3f}\n")
    out.write(f"adapter_rf1_writers_4_set2_max_min_ratio={adapter_set_ranges[1]:.3f}\n")
    out.write(f"rf1_direct_adapter_four_writer_ratio={adapter_four / direct_four:.3f}\n")
    out.write(f"rf1_local_stability_gate={'pass' if rf1_stable else 'hold'}\n")
    out.write(f"rf3_same_host_healthy_baseline={str(rf3_healthy).lower()}\n")
    out.write(f"rf3_queue_saturation_row_count={queue_saturation_rows}\n")
    out.write("rf3_same_host_admission_eligible=false\n")
    out.write("rf3_distinct_node_gate_required=true\n")
    out.write("d1_close_allowed=false\n")
    out.write("architecture_candidate_selected=false\n")
    out.write("product_mutation_present=false\n")
    if not rf1_stable:
        out.write("phase174_fixed_work_pipeline_status=hold\n")
        raise SystemExit("RF1 fixed-work admission shape is unstable")
    out.write("phase174_fixed_work_pipeline_status=ok\n")
PY

cleanup
find "${STORE_DIR}" -type f -name '*.store' -print -quit 2>/dev/null | grep -q . && {
  echo "phase174 store residue remains under ${STORE_DIR}" >&2
  exit 1
}
write_summary "store_residue_count=0"
cat "${SUMMARY}"
