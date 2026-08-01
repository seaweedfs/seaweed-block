#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase173-architecture-controls-gate}"
STORE_DIR="${SW_BLOCK_PHASE173_STORE_DIR:-${ARTIFACT_DIR}/stores}"
SUMMARY="${ARTIFACT_DIR}/phase173-architecture-controls-summary.txt"
RESULTS="${ARTIFACT_DIR}/phase173-architecture-control-results.jsonl"
TEST_BINARY="${ARTIFACT_DIR}/storage.test"
MAX_RANGE="1.25"
MATERIAL_RATIO="1.30"
RUNS=5
CONTROL_CPUSET="${SW_BLOCK_PHASE173_CONTROL_CPUSET:-}"

mkdir -p "${ARTIFACT_DIR}" "${STORE_DIR}" "${ARTIFACT_DIR}/environment" "${ARTIFACT_DIR}/logs"
: >"${SUMMARY}"
: >"${RESULTS}"

cleanup() {
  rm -f "${STORE_DIR}"/phase173-control-* "${STORE_DIR}"/*.scratch
  find "${STORE_DIR}" -mindepth 1 -maxdepth 1 -type d -name 'go-build*' -empty -delete 2>/dev/null || true
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

for tool in go python3 findmnt taskset nproc; do
  command -v "${tool}" >/dev/null 2>&1 || {
    echo "required tool ${tool} is unavailable" >&2
    exit 1
  }
done

if [[ -z "${CONTROL_CPUSET}" ]]; then
  echo "SW_BLOCK_PHASE173_CONTROL_CPUSET must name at least four comparable CPUs" >&2
  exit 1
fi
CONTROL_GOMAXPROCS="$(taskset -c "${CONTROL_CPUSET}" nproc)"
if (( CONTROL_GOMAXPROCS < 4 )); then
  echo "control cpuset ${CONTROL_CPUSET} exposes ${CONTROL_GOMAXPROCS} CPUs; need at least four" >&2
  exit 1
fi

store_source="$(findmnt -n -T "${STORE_DIR}" -o SOURCE)"
store_filesystem="$(findmnt -n -T "${STORE_DIR}" -o FSTYPE)"
root_source="$(findmnt -n -T / -o SOURCE)"
if [[ "${store_source}" != /dev/* ]]; then
  echo "store source ${store_source} is not a local block device" >&2
  exit 1
fi
if [[ "${store_source}" == "${root_source}" ]]; then
  echo "store source ${store_source} is the OS root filesystem" >&2
  exit 1
fi

write_summary "phase173_architecture_controls_status=running"
write_summary "contract=phase173-architecture-controls-v1"
write_summary "scope=test_only_diagnostic_controls"
write_summary "runs_per_control=${RUNS}"
write_summary "max_min_limit=${MAX_RANGE}"
write_summary "material_direction_ratio=${MATERIAL_RATIO}"
write_summary "store_source=${store_source}"
write_summary "store_filesystem=${store_filesystem}"
write_summary "root_source=${root_source}"
write_summary "dedicated_store_source=true"
write_summary "control_cpuset=${CONTROL_CPUSET}"
write_summary "control_gomaxprocs=${CONTROL_GOMAXPROCS}"
write_summary "architecture_candidate_selected=false"
write_summary "product_mutation_present=false"
write_summary "deferred_foreground_product_claim_allowed=false"
write_summary "split_file_scratch_product_claim_allowed=false"

capture kernel uname -a
capture go-version go version
capture cpu lscpu
capture filesystem findmnt -T "${STORE_DIR}" -o TARGET,SOURCE,FSTYPE,OPTIONS
capture free-space df -hT "${STORE_DIR}"
capture load uptime

cd "${ROOT}"
go test ./core/storage -run '^TestPhase173ArchitectureControlContract$' -count=1 \
  >"${ARTIFACT_DIR}/control-contract-test.log" 2>&1
go test -c -o "${TEST_BINARY}" ./core/storage

for control in \
  shipped_concurrent_writers_4 \
  deferred_foreground_writers_1 \
  deferred_foreground_writers_4 \
  shared_file_scratch \
  split_file_scratch; do
  if [[ "${control}" != *_scratch ]]; then
    precondition_log="${ARTIFACT_DIR}/logs/${control}-precondition.log"
    SW_BLOCK_PHASE173_ARCH_CONTROL_STORE_DIR="${STORE_DIR}" \
    SW_BLOCK_PHASE173_ARCH_CONTROL="${control}" \
    SW_BLOCK_PHASE173_ARCH_CONTROL_RUN="0" \
    GOMAXPROCS="${CONTROL_GOMAXPROCS}" taskset -c "${CONTROL_CPUSET}" \
      "${TEST_BINARY}" -test.run '^TestPhase173ArchitectureControls$' -test.v -test.count=1 \
      >"${precondition_log}" 2>&1
    sync
    sleep 0.25
  fi
  for run in $(seq 1 "${RUNS}"); do
    sync
    sleep 0.25
    control_log="${ARTIFACT_DIR}/logs/${control}-run${run}.log"
    SW_BLOCK_PHASE173_ARCH_CONTROL_STORE_DIR="${STORE_DIR}" \
    SW_BLOCK_PHASE173_ARCH_CONTROL="${control}" \
    SW_BLOCK_PHASE173_ARCH_CONTROL_RUN="${run}" \
    GOMAXPROCS="${CONTROL_GOMAXPROCS}" taskset -c "${CONTROL_CPUSET}" \
      "${TEST_BINARY}" -test.run '^TestPhase173ArchitectureControls$' -test.v -test.count=1 \
      >"${control_log}" 2>&1
    sed -n 's/^phase173_architecture_control_result=//p' "${control_log}" >>"${RESULTS}"
  done
done

mkdir -p "${STORE_DIR}/tmp"
TMPDIR="${STORE_DIR}/tmp" GOMAXPROCS="${CONTROL_GOMAXPROCS}" taskset -c "${CONTROL_CPUSET}" go test ./core/frontend/durable \
  -run '^$' -bench '^BenchmarkT3c_DurablePerf$/^walstore$' \
  -benchtime=8000x -count=1 \
  >"${ARTIFACT_DIR}/rf1-durable-adapter-benchmark.txt" 2>&1
TMPDIR="${STORE_DIR}/tmp" GOMAXPROCS="${CONTROL_GOMAXPROCS}" taskset -c "${CONTROL_CPUSET}" go test ./core/replication \
  -run '^$' -bench '^BenchmarkPhase167RF3SyncQuorumContention/(writers_1|writers_4)$' \
  -benchtime=2048x -count=1 \
  >"${ARTIFACT_DIR}/rf3-real-tcp-benchmark.txt" 2>&1
rm -rf "${STORE_DIR}/tmp"

python3 - "${RESULTS}" "${SUMMARY}" "${RUNS}" "${MAX_RANGE}" "${MATERIAL_RATIO}" <<'PY'
import json
import math
import statistics
import sys
from collections import defaultdict

results_path, summary_path, runs_text, max_range_text, material_text = sys.argv[1:]
runs = int(runs_text)
max_range = float(max_range_text)
material = float(material_text)
rows = [json.loads(line) for line in open(results_path, encoding="utf-8") if line.strip()]
if len(rows) != 30:
    raise SystemExit(f"control rows={len(rows)} want 30")

groups = defaultdict(list)
for row in rows:
    if row["contract"] != "phase173-architecture-controls-v1":
        raise SystemExit(f"bad contract: {row}")
    key = (row["control"], int(row["writers"]))
    groups[key].append(row)
    scratch = row["control"] in ("shared_file_scratch", "split_file_scratch")
    expected_blocks = 14500 * 3 if scratch else 14500
    if row["logical_blocks"] != expected_blocks or row["logical_bytes"] != expected_blocks * 4096:
        raise SystemExit(f"bad fixed work: {row}")
    if row["duration_ns"] <= 0 or row["mib_per_second"] <= 0 or row["correctness_samples"] < 3:
        raise SystemExit(f"empty duration/rate/correctness: {row}")
    if row["control"] in ("shipped_concurrent", "deferred_foreground"):
        if row["commit_lock_wait_ops"] != 2320:
            raise SystemExit(f"bad lock work: {row}")
        if row["flush_record_reads"] != 14500 or row["flush_decode_ops"] != 14500 or row["extent_write_ops"] != 14500:
            raise SystemExit(f"bad real flusher work: {row}")
        if row["extent_sync_ops"] != row["checkpoint_sync_ops"] or row["extent_sync_ops"] <= 0:
            raise SystemExit(f"bad flusher sync work: {row}")
    elif row["control"] == "prefilled_flusher":
        if row["writers"] != 0 or row["flush_record_reads"] != 14500 or row["extent_write_ops"] != 14500:
            raise SystemExit(f"bad prefilled flusher work: {row}")
    else:
        if row["scratch_pread_ops"] != 87000 or row["scratch_pwrite_ops"] != 43501 or row["scratch_sync_ops"] != 2:
            raise SystemExit(f"bad scratch I/O shape: {row}")

expected = {
    ("shipped_concurrent", 4),
    ("deferred_foreground", 1),
    ("deferred_foreground", 4),
    ("prefilled_flusher", 0),
    ("shared_file_scratch", 0),
    ("split_file_scratch", 0),
}
if set(groups) != expected:
    raise SystemExit(f"control groups={set(groups)} want {expected}")

summary = []
shipped_stable = True
counterfactual_stable = True
medians = {}
for key in sorted(expected):
    group = groups[key]
    if len(group) != runs:
        raise SystemExit(f"group={key} rows={len(group)} want {runs}")
    values = [float(row["mib_per_second"]) for row in group]
    ratio = max(values) / min(values)
    median = statistics.median(values)
    medians[key] = median
    prefix = f"{key[0]}_writers_{key[1]}"
    summary.append(f"{prefix}_median_mibps={median:.3f}")
    summary.append(f"{prefix}_max_min_ratio={ratio:.3f}")
    group_stable = math.isfinite(ratio) and ratio <= max_range
    if key == ("shipped_concurrent", 4):
        shipped_stable = group_stable
    elif not group_stable:
        counterfactual_stable = False
    if key[0] in ("shipped_concurrent", "deferred_foreground"):
        waits = [row["commit_lock_wait_ns"] / row["commit_lock_wait_ops"] for row in group]
        p99 = [row["p99_ns"] for row in group]
        gc_cycles = [row.get("measured_gc_cycles", 0) for row in group]
        gc_pause = [row.get("measured_gc_pause_ns", 0) for row in group]
        alloc_bytes = [row.get("measured_alloc_bytes", 0) for row in group]
        summary.append(f"{prefix}_median_commit_lock_wait_ns_per_op={statistics.median(waits):.0f}")
        summary.append(f"{prefix}_median_p99_ns={statistics.median(p99):.0f}")
        summary.append(f"{prefix}_median_measured_gc_cycles={statistics.median(gc_cycles):.0f}")
        summary.append(f"{prefix}_median_measured_gc_pause_ns={statistics.median(gc_pause):.0f}")
        summary.append(f"{prefix}_median_measured_alloc_bytes={statistics.median(alloc_bytes):.0f}")

shipped = medians[("shipped_concurrent", 4)]
deferred_one = medians[("deferred_foreground", 1)]
deferred_four = medians[("deferred_foreground", 4)]
shared = medians[("shared_file_scratch", 0)]
split = medians[("split_file_scratch", 0)]
deferred_vs_shipped = deferred_four / shipped
single_vs_four = deferred_one / deferred_four
split_vs_shared = split / shared
owner_signal = single_vs_four >= material
writeback_signal = deferred_vs_shipped >= material
media_signal = split_vs_shared >= material
if not counterfactual_stable:
    direction = "no_backend_change_unstable_counterfactuals"
elif media_signal and not owner_signal:
    direction = "wal_extent_media_separation"
elif owner_signal and not media_signal:
    direction = "owner_queue_redesign"
elif not owner_signal and not media_signal:
    direction = "no_backend_change_local_controls"
else:
    direction = "ambiguous_stop_before_candidate"

summary.extend([
    f"deferred_four_vs_shipped_four_ratio={deferred_vs_shipped:.3f}",
    f"deferred_one_vs_deferred_four_ratio={single_vs_four:.3f}",
    f"split_vs_shared_scratch_ratio={split_vs_shared:.3f}",
    f"owner_queue_signal={'true' if owner_signal else 'false'}",
    f"writeback_interference_signal={'true' if writeback_signal else 'false'}",
    f"media_separation_signal={'true' if media_signal else 'false'}",
    f"local_architecture_direction={direction}",
    f"shipped_control_stability_gate={'pass' if shipped_stable else 'fail'}",
    f"counterfactual_control_stability_gate={'pass' if counterfactual_stable else 'inconclusive'}",
    "architecture_candidate_selected=false",
])
with open(summary_path, "a", encoding="utf-8") as out:
    for line in summary:
        out.write(line + "\n")
if not shipped_stable:
    raise SystemExit("shipped-path control stability exceeded predeclared 1.25x range")
PY

benchmark_metric() {
  local file="$1"
  local prefix="$2"
  local metric="$3"
  awk -v prefix="${prefix}" -v metric="${metric}" '
    index($1, prefix) == 1 {
      for (i = 2; i <= NF; i++) {
        if ($(i + 1) == metric) {
          print $i
          exit
        }
      }
    }
  ' "${file}"
}

require_metric() {
  local file="$1"
  local prefix="$2"
  local metric="$3"
  local value
  value="$(benchmark_metric "${file}" "${prefix}" "${metric}")"
  if [[ -z "${value}" ]]; then
    echo "missing ${prefix} ${metric} in ${file}" >&2
    exit 1
  fi
  printf '%s' "${value}"
}

require_iteration_metric() {
  local file="$1"
  local iterations="$2"
  local metric="$3"
  local values
  values="$(awk -v iterations="${iterations}" -v metric="${metric}" '
    $1 == iterations {
      for (i = 2; i <= NF; i++) {
        if ($(i + 1) == metric) {
          print $i
        }
      }
    }
  ' "${file}")"
  if [[ -z "${values}" || "${values}" == *$'\n'* ]]; then
    echo "expected one ${iterations}-iteration ${metric} result in ${file}" >&2
    exit 1
  fi
  printf '%s' "${values}"
}

rf1_mibps="$(require_iteration_metric "${ARTIFACT_DIR}/rf1-durable-adapter-benchmark.txt" 8000 MB/s)"
rf1_ns="$(require_iteration_metric "${ARTIFACT_DIR}/rf1-durable-adapter-benchmark.txt" 8000 ns/op)"
write_summary "rf1_durable_adapter_fixed_iterations=8000"
write_summary "rf1_durable_adapter_mibps=${rf1_mibps}"
write_summary "rf1_durable_adapter_ns_per_op=${rf1_ns}"

for writers in 1 4; do
  prefix="BenchmarkPhase167RF3SyncQuorumContention/writers_${writers}-"
  write_summary "rf3_writers_${writers}_fixed_iterations=2048"
  write_summary "rf3_writers_${writers}_mibps=$(require_metric "${ARTIFACT_DIR}/rf3-real-tcp-benchmark.txt" "${prefix}" MB/s)"
  write_summary "rf3_writers_${writers}_ns_per_op=$(require_metric "${ARTIFACT_DIR}/rf3-real-tcp-benchmark.txt" "${prefix}" ns/op)"
  write_summary "rf3_writers_${writers}_fanout_ns_per_op=$(require_metric "${ARTIFACT_DIR}/rf3-real-tcp-benchmark.txt" "${prefix}" repl_fanout_ns/op)"
  write_summary "rf3_writers_${writers}_ack_wait_ns_per_op=$(require_metric "${ARTIFACT_DIR}/rf3-real-tcp-benchmark.txt" "${prefix}" repl_ack_wait_ns/op)"
  write_summary "rf3_writers_${writers}_queue_max_depth=$(require_metric "${ARTIFACT_DIR}/rf3-real-tcp-benchmark.txt" "${prefix}" peer_queue_max_depth)"
  queue_saturated="$(require_metric "${ARTIFACT_DIR}/rf3-real-tcp-benchmark.txt" "${prefix}" peer_queue_saturated)"
  if [[ "${queue_saturated}" != "0" ]]; then
    echo "RF3 writers=${writers} queue saturated ${queue_saturated} times" >&2
    exit 1
  fi
  write_summary "rf3_writers_${writers}_queue_saturated=${queue_saturated}"
done

write_summary "rf1_rf3_component_attribution=complete"
write_summary "mounted_nvme_tcp_control=pending_same_session_live_gate"
write_summary "d3_close_allowed=false"
write_summary "architecture_candidate_selected=false"
write_summary "phase173_architecture_controls_status=ok"

cleanup
find "${STORE_DIR}" -maxdepth 1 -type f \( -name 'phase173-control-*' -o -name '*.scratch' \) -print -quit | grep -q . && {
  echo "phase173 control residue remains under ${STORE_DIR}" >&2
  exit 1
}
write_summary "store_residue_count=0"
cat "${SUMMARY}"
