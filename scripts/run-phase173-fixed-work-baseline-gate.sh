#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase173-fixed-work-baseline-gate}"
STORE_DIR="${SW_BLOCK_PHASE173_STORE_DIR:-${ARTIFACT_DIR}/stores}"
SUMMARY="${ARTIFACT_DIR}/phase173-fixed-work-baseline-summary.txt"
RESULTS="${ARTIFACT_DIR}/phase173-fixed-work-results.jsonl"
TEST_BINARY="${ARTIFACT_DIR}/storage.test"
SHAPES=(sequential_4k scattered_4k batch_16 mounted_mixed)
WRITERS=(1 2 4 8)
SETS=2
RUNS=5
DIAGNOSTIC_RUNS=1
MAX_RANGE="1.25"

mkdir -p "${ARTIFACT_DIR}" "${STORE_DIR}" "${ARTIFACT_DIR}/environment" "${ARTIFACT_DIR}/logs"
: >"${SUMMARY}"
: >"${RESULTS}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

capture() {
  local name="$1"
  shift
  "$@" >"${ARTIFACT_DIR}/environment/${name}.txt" 2>&1 || true
}

write_summary "phase173_fixed_work_baseline_status=running"
write_summary "contract=phase173-fixed-work-v1"
write_summary "sets=${SETS}"
write_summary "four_writer_runs_per_set=${RUNS}"
write_summary "diagnostic_runs_per_set=${DIAGNOSTIC_RUNS}"
write_summary "four_writer_max_min_limit=${MAX_RANGE}"
write_summary "flusher_interval_ms=100"
write_summary "page_cache_policy=warm_process_and_filesystem_no_drop_caches"
write_summary "go_benchmark_autocalibration_allowed=false"
write_summary "architecture_candidate_admission_allowed=false"

capture kernel uname -a
capture go-version go version
capture cpu lscpu
capture affinity taskset -pc "$$"
capture scheduler chrt -p "$$"
capture filesystem findmnt -T "${STORE_DIR}" -o TARGET,SOURCE,FSTYPE,OPTIONS
capture free-space df -hT "${STORE_DIR}"
capture block-devices lsblk -o NAME,TYPE,SIZE,ROTA,MODEL,FSTYPE,MOUNTPOINTS
capture load uptime
capture processes ps -eo pid,psr,ni,stat,comm,%cpu,%mem --sort=-%cpu
if command -v iostat >/dev/null 2>&1; then
  capture iostat iostat -xz 1 2
fi
if compgen -G '/sys/class/thermal/thermal_zone*/temp' >/dev/null; then
  for temp in /sys/class/thermal/thermal_zone*/temp; do
    printf '%s=' "${temp}" >>"${ARTIFACT_DIR}/environment/thermal.txt"
    cat "${temp}" >>"${ARTIFACT_DIR}/environment/thermal.txt" || true
  done
else
  echo "thermal_evidence=unavailable" >"${ARTIFACT_DIR}/environment/thermal.txt"
fi

cd "${ROOT}"
go test ./core/storage -run '^TestPhase173FixedWorkContract$' -count=1 \
  >"${ARTIFACT_DIR}/contract-test.log" 2>&1
go test -c -o "${TEST_BINARY}" ./core/storage

for set_id in $(seq 1 "${SETS}"); do
  for shape in "${SHAPES[@]}"; do
    for writers in "${WRITERS[@]}"; do
      point_runs="${DIAGNOSTIC_RUNS}"
      if [[ "${writers}" == "4" ]]; then
        point_runs="${RUNS}"
      fi
      for run_id in $(seq 1 "${point_runs}"); do
        id="set${set_id}-${shape}-writers${writers}-run${run_id}"
        log="${ARTIFACT_DIR}/logs/${id}.log"
        SW_BLOCK_PHASE173_SHAPE="${shape}" \
        SW_BLOCK_PHASE173_WRITERS="${writers}" \
        SW_BLOCK_PHASE173_RUN_ID="${id}" \
        SW_BLOCK_PHASE173_STORE_DIR="${STORE_DIR}" \
          "${TEST_BINARY}" -test.run '^TestPhase173WALStoreFixedWork$' -test.v -test.count=1 \
          >"${log}" 2>&1
        result="$(sed -n 's/^phase173_fixed_work_result=//p' "${log}")"
        if [[ -z "${result}" ]]; then
          echo "missing fixed-work result in ${log}" >&2
          exit 1
        fi
        printf '%s\n' "${result}" >>"${RESULTS}"
      done
    done
  done
done

python3 - "${RESULTS}" "${SUMMARY}" "${SETS}" "${RUNS}" "${MAX_RANGE}" <<'PY'
import json
import math
import statistics
import sys
from collections import defaultdict

results_path, summary_path, sets_text, runs_text, max_range_text = sys.argv[1:]
sets = int(sets_text)
runs = int(runs_text)
max_range = float(max_range_text)
rows = [json.loads(line) for line in open(results_path, encoding="utf-8") if line.strip()]
shapes = ("sequential_4k", "scattered_4k", "batch_16", "mounted_mixed")
writers_values = (1, 2, 4, 8)
expected = sets * len(shapes) * (runs + len(writers_values) - 1)
if len(rows) != expected:
    raise SystemExit(f"fixed-work rows={len(rows)} want {expected}")

grouped = defaultdict(list)
for row in rows:
    key = (row["shape"], int(row["writers"]))
    grouped[key].append(row)
    if row["contract"] != "phase173-fixed-work-v1":
        raise SystemExit(f"bad contract: {row}")
    if row["final_sync_calls"] != 1:
        raise SystemExit(f"final_sync_calls != 1: {row}")
    if row["dirty_entries"] != 0 or row["checkpoint_lsn"] != row["head_lsn"] or row["head_lsn"] != row["synced_lsn"]:
        raise SystemExit(f"incomplete drain: {row}")
    if row["wal_encode_ops"] != row["logical_blocks"]:
        raise SystemExit(f"logical/WAL record mismatch: {row}")
    if row["commit_lock_wait_ops"] != row["api_operations"]:
        raise SystemExit(f"API/commit operation mismatch: {row}")
    if row["wal_append_ops"] < row["api_operations"]:
        raise SystemExit(f"API/WAL physical append mismatch: {row}")
    extra_writeats = row["wal_writeat_calls"] - row["wal_append_ops"]
    if extra_writeats < 0 or extra_writeats > row["wal_wraps"]:
        raise SystemExit(f"WAL padding/writeat mismatch: {row}")
    if row["validation_failures"] != 0 or row["correctness_samples"] < 3:
        raise SystemExit(f"correctness evidence failed: {row}")
    if row["flusher_interval_ms"] != 100:
        raise SystemExit(f"unexpected flusher interval: {row}")

summary = []
gate_ok = True
for shape in shapes:
    for writers in writers_values:
        group = grouped[(shape, writers)]
        expected_group = sets * (runs if writers == 4 else 1)
        if len(group) != expected_group:
            raise SystemExit(f"shape={shape} writers={writers} rows={len(group)} want {expected_group}")
        values = [float(row["foreground_mib_per_second"]) for row in group]
        summary.append(f"{shape}_writers_{writers}_median_mibps={statistics.median(values):.3f}")
        summary.append(f"{shape}_writers_{writers}_median_p99_ns={statistics.median(row['p99_ns'] for row in group):.0f}")

    four_rows = grouped[(shape, 4)]
    all_values = [float(row["foreground_mib_per_second"]) for row in four_rows]
    overall_range = max(all_values) / min(all_values)
    summary.append(f"{shape}_writers_4_combined_max_min_ratio={overall_range:.3f}")
    if not math.isfinite(overall_range) or overall_range > max_range:
        gate_ok = False
    for set_id in range(1, sets + 1):
        prefix = f"set{set_id}-{shape}-writers4-"
        set_values = [
            float(row["foreground_mib_per_second"])
            for row in four_rows
            if row["run_id"].startswith(prefix)
        ]
        if len(set_values) != runs:
            raise SystemExit(f"shape={shape} set={set_id} four-writer rows={len(set_values)}")
        set_range = max(set_values) / min(set_values)
        summary.append(f"{shape}_writers_4_set_{set_id}_max_min_ratio={set_range:.3f}")
        if not math.isfinite(set_range) or set_range > max_range:
            gate_ok = False

with open(summary_path, "a", encoding="utf-8") as out:
    for line in summary:
        out.write(line + "\n")
    out.write(f"fixed_work_result_count={len(rows)}\n")
    out.write("fixed_work_counter_reconciliation=true\n")
    out.write("fixed_work_complete_drain=true\n")
    out.write("fixed_work_correctness_samples=true\n")
    out.write(f"four_writer_stability_gate={'pass' if gate_ok else 'fail'}\n")
    out.write(f"architecture_candidate_admission_allowed={'true' if gate_ok else 'false'}\n")
    out.write(f"phase173_fixed_work_baseline_status={'ok' if gate_ok else 'unstable'}\n")
if not gate_ok:
    raise SystemExit("four-writer stability exceeded the predeclared 1.25x range")
PY

find "${STORE_DIR}" -maxdepth 1 -type f -name 'phase173-*.store' -print -quit | grep -q . && {
  echo "fixed-work store residue remains under ${STORE_DIR}" >&2
  exit 1
}

cat "${SUMMARY}"
