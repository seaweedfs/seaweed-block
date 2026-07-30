#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase168-native-wal-performance-gate}"
SUMMARY="${ARTIFACT_DIR}/phase168-native-wal-performance-summary.txt"
BENCH="${ARTIFACT_DIR}/contention-benchmark.txt"
METRICS="${ARTIFACT_DIR}/contention-metrics.tsv"
BENCHTIME="${SW_BLOCK_PHASE168_BENCHTIME:-1s}"
REPETITIONS="${SW_BLOCK_PHASE168_REPETITIONS:-5}"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"
: >"${BENCH}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

metric() {
  local implementation="$1"
  local shape="$2"
  local writers="$3"
  local unit="$4"
  local field="$5"
  awk -F '\t' \
    -v implementation="${implementation}" \
    -v shape="${shape}" \
    -v writers="${writers}" \
    -v unit="${unit}" \
    -v field="${field}" '
      $1 == implementation && $2 == shape && $3 == writers && $4 == unit {
        if (field == "median") print $5
        if (field == "min") print $6
        if (field == "max") print $7
      }
    ' "${METRICS}"
}

require_metric() {
  local value
  value="$(metric "$1" "$2" "$3" "$4" "$5")"
  if [[ -z "${value}" ]]; then
    echo "missing metric implementation=$1 shape=$2 writers=$3 unit=$4 field=$5" >&2
    exit 1
  fi
  printf '%s' "${value}"
}

ratio() {
  awk -v numerator="$1" -v denominator="$2" 'BEGIN {
    if (numerator <= 0 || denominator <= 0) exit 1
    printf "%.3f", numerator / denominator
  }'
}

benchmark_name() {
  local implementation="$1"
  local shape="$2"
  case "${implementation}/${shape}" in
    native/write) echo "BenchmarkPhase168NativeWALContention" ;;
    positioned/write) echo "BenchmarkPhase167ParallelWALContention" ;;
    legacy/write) echo "BenchmarkPhase167LegacyWALContentionControl" ;;
    native/batch) echo "BenchmarkPhase168NativeWALBatchContention" ;;
    positioned/batch) echo "BenchmarkPhase167ParallelWALBatchContention" ;;
    legacy/batch) echo "BenchmarkPhase167LegacyWALBatchContentionControl" ;;
    *) return 1 ;;
  esac
}

if [[ ! "${BENCHTIME}" =~ ^[1-5]s$ ]]; then
  echo "SW_BLOCK_PHASE168_BENCHTIME must be between 1s and 5s" >&2
  exit 1
fi
if (( REPETITIONS < 3 || REPETITIONS > 9 )); then
  echo "benchmark repetitions must be between 3 and 9" >&2
  exit 1
fi

write_summary "phase168_native_wal_performance_status=running"
write_summary "candidate=parallel-walstore/io_uring"
write_summary "candidate_default=false"
write_summary "mounted_nvme_claim_allowed=false"
write_summary "benchmark_benchtime=${BENCHTIME}"
write_summary "benchmark_repetitions=${REPETITIONS}"
write_summary "benchmark_order=rotated"

cd "${ROOT}"
if [[ "$(go env GOOS)/$(go env GOARCH)" != "linux/amd64" ]]; then
  write_summary "phase168_native_wal_performance_status=unsupported"
  write_summary "reason=gate_requires_linux_amd64"
  exit 2
fi

go test ./internal/iouring ./core/storage/parallelwal -count=1 \
  >"${ARTIFACT_DIR}/preflight-tests.log" 2>&1
write_summary "preflight_tests=pass"

test_binary="${ARTIFACT_DIR}/parallelwal-performance.test"
go test -c -o "${test_binary}" ./core/storage/parallelwal

run_benchmark() {
  local implementation="$1"
  local shape="$2"
  local name
  name="$(benchmark_name "${implementation}" "${shape}")"
  "${test_binary}" \
    -test.run '^$' \
    -test.bench "^${name}$" \
    -test.benchtime="${BENCHTIME}" \
    -test.count=1 \
    -test.benchmem >>"${BENCH}" 2>&1
}

for repetition in $(seq 1 "${REPETITIONS}"); do
  case $((repetition % 3)) in
    1) order=(native positioned legacy) ;;
    2) order=(positioned legacy native) ;;
    0) order=(legacy native positioned) ;;
  esac
  for implementation in "${order[@]}"; do
    run_benchmark "${implementation}" write
  done
  for implementation in "${order[@]}"; do
    run_benchmark "${implementation}" batch
  done
done

python3 - "${BENCH}" "${METRICS}" "${REPETITIONS}" <<'PY'
import re
import statistics
import sys

source, destination, expected_count = sys.argv[1], sys.argv[2], int(sys.argv[3])
names = {
    "BenchmarkPhase168NativeWALContention": ("native", "write"),
    "BenchmarkPhase167ParallelWALContention": ("positioned", "write"),
    "BenchmarkPhase167LegacyWALContentionControl": ("legacy", "write"),
    "BenchmarkPhase168NativeWALBatchContention": ("native", "batch"),
    "BenchmarkPhase167ParallelWALBatchContention": ("positioned", "batch"),
    "BenchmarkPhase167LegacyWALBatchContentionControl": ("legacy", "batch"),
}
write_units = {"MB/s", "p50_ns", "p95_ns", "p99_ns", "B/op", "allocs/op"}
batch_units = {"MB/s", "p99_ns", "B/op", "allocs/op", "batch_blocks"}
native_units = {
    "native_admitted", "native_rounds", "native_sqes",
    "native_submit_syscalls", "native_cqes", "native_queue_full",
    "native_short_cqes", "native_inflight_high_water",
    "native_buffer_allocations", "native_fallback",
}
samples = {}
pattern = re.compile(
    r"^(BenchmarkPhase(?:168NativeWAL(?:Batch)?Contention|"
    r"167ParallelWAL(?:Batch)?Contention|"
    r"167LegacyWAL(?:Batch)?ContentionControl))"
    r"/writers_(1|2|4|8)-\d+\s+"
)

with open(source, encoding="utf-8") as handle:
    for raw in handle:
        match = pattern.match(raw)
        if not match:
            continue
        implementation, shape = names[match.group(1)]
        writers = match.group(2)
        if shape == "batch" and writers not in {"1", "4"}:
            raise SystemExit(f"unexpected batch writers={writers}")
        fields = raw.split()
        iterations = int(fields[1])
        row = {}
        for index in range(2, len(fields) - 1):
            unit = fields[index + 1]
            try:
                row[unit] = float(fields[index])
            except ValueError:
                continue
        required = set(write_units if shape == "write" else batch_units)
        if implementation == "native":
            required |= native_units
            expected_admitted = iterations if shape == "write" else iterations * 16
            if row.get("native_admitted") != expected_admitted:
                raise SystemExit(
                    f"{shape}/writers_{writers}: admitted="
                    f"{row.get('native_admitted')} want={expected_admitted}"
                )
            if row.get("native_sqes") != row.get("native_cqes"):
                raise SystemExit(f"{shape}/writers_{writers}: SQE/CQE mismatch")
            for unit in ("native_fallback", "native_queue_full", "native_short_cqes"):
                if row.get(unit) != 0:
                    raise SystemExit(f"{shape}/writers_{writers}: {unit}={row.get(unit)}")
            allocations = row.get("native_buffer_allocations", 0)
            if allocations < 1 or allocations > 4:
                raise SystemExit(
                    f"{shape}/writers_{writers}: native_buffer_allocations={allocations}"
                )
        missing = required - row.keys()
        if missing:
            raise SystemExit(
                f"{implementation}/{shape}/writers_{writers}: missing={sorted(missing)}"
            )
        for unit in required:
            samples.setdefault((implementation, shape, writers, unit), []).append(row[unit])

with open(destination, "w", encoding="utf-8") as output:
    for key in sorted(samples):
        values = samples[key]
        if len(values) != expected_count:
            raise SystemExit(f"{key}: samples={len(values)} want={expected_count}")
        output.write(
            "\t".join(
                (
                    *key,
                    f"{statistics.median(values):.6f}",
                    f"{min(values):.6f}",
                    f"{max(values):.6f}",
                )
            )
            + "\n"
        )
PY

native_stable=true
native_p99_bounded=true
for writers in 1 2 4 8; do
  for implementation in native positioned legacy; do
    mibps="$(require_metric "${implementation}" write "${writers}" MB/s median)"
    mibps_min="$(require_metric "${implementation}" write "${writers}" MB/s min)"
    mibps_max="$(require_metric "${implementation}" write "${writers}" MB/s max)"
    p99="$(require_metric "${implementation}" write "${writers}" p99_ns median)"
    allocs="$(require_metric "${implementation}" write "${writers}" allocs/op median)"
    bytes="$(require_metric "${implementation}" write "${writers}" B/op median)"
    write_summary "${implementation}_writers_${writers}_mibps_median=${mibps}"
    write_summary "${implementation}_writers_${writers}_mibps_min=${mibps_min}"
    write_summary "${implementation}_writers_${writers}_mibps_max=${mibps_max}"
    write_summary "${implementation}_writers_${writers}_p99_ns_median=${p99}"
    write_summary "${implementation}_writers_${writers}_allocs_per_op=${allocs}"
    write_summary "${implementation}_writers_${writers}_bytes_per_op=${bytes}"
  done

  native_median="$(require_metric native write "${writers}" MB/s median)"
  native_min="$(require_metric native write "${writers}" MB/s min)"
  native_p99="$(require_metric native write "${writers}" p99_ns median)"
  stability="$(ratio "${native_min}" "${native_median}")"
  write_summary "native_writers_${writers}_min_vs_median_ratio=${stability}"
  if ! awk -v value="${stability}" 'BEGIN { exit !(value >= 0.70) }'; then
    native_stable=false
  fi
  if ! awk -v value="${native_p99}" 'BEGIN { exit !(value <= 100000000) }'; then
    native_p99_bounded=false
  fi

  for unit in native_admitted native_rounds native_sqes native_submit_syscalls \
    native_cqes native_inflight_high_water native_buffer_allocations; do
    value="$(require_metric native write "${writers}" "${unit}" median)"
    write_summary "native_writers_${writers}_${unit#native_}=${value}"
  done
done
write_summary "native_path_counter_integrity=pass"
write_summary "buffer_reuse_observed=true"
write_summary "fallback_count=0"
write_summary "queue_full_rejects=0"
write_summary "short_completions=0"
write_summary "native_all_writer_counts_stable=${native_stable}"
write_summary "native_all_writer_p99_bounded=${native_p99_bounded}"

for implementation in native positioned legacy; do
  batch_mibps="$(require_metric "${implementation}" batch 4 MB/s median)"
  batch_min="$(require_metric "${implementation}" batch 4 MB/s min)"
  batch_max="$(require_metric "${implementation}" batch 4 MB/s max)"
  batch_p99="$(require_metric "${implementation}" batch 4 p99_ns median)"
  batch_allocs="$(require_metric "${implementation}" batch 4 allocs/op median)"
  write_summary "${implementation}_batch_writers_4_mibps_median=${batch_mibps}"
  write_summary "${implementation}_batch_writers_4_mibps_min=${batch_min}"
  write_summary "${implementation}_batch_writers_4_mibps_max=${batch_max}"
  write_summary "${implementation}_batch_writers_4_p99_ns_median=${batch_p99}"
  write_summary "${implementation}_batch_writers_4_allocs_per_op=${batch_allocs}"
done
write_summary "write_sync_cadence=one_final_sync_per_benchmark_sample"
write_summary "batch_sync_cadence=one_final_sync_per_benchmark_sample"

if command -v strace >/dev/null 2>&1; then
  for implementation in native positioned legacy; do
    name="$(benchmark_name "${implementation}" write)"
    trace="${ARTIFACT_DIR}/${implementation}-writers-4.strace"
    timeout 120s strace -qq -f \
      -e trace=eventfd2,io_uring_setup,io_uring_register,io_uring_enter,pwrite64,pread64,read,ppoll,poll \
      -o "${trace}" \
      "${test_binary}" \
      -test.run '^$' \
      -test.bench "^${name}/writers_4$" \
      -test.benchtime=1024x \
      -test.count=1 >/dev/null 2>&1
    write_summary "${implementation}_writers_4_io_uring_enter=$(grep -c 'io_uring_enter(' "${trace}" || true)"
    write_summary "${implementation}_writers_4_eventfd2=$(grep -c 'eventfd2(' "${trace}" || true)"
    write_summary "${implementation}_writers_4_ppoll=$(grep -c 'ppoll(' "${trace}" || true)"
    write_summary "${implementation}_writers_4_read=$(grep -c 'read(' "${trace}" || true)"
    write_summary "${implementation}_writers_4_pwrite64=$(grep -c 'pwrite64(' "${trace}" || true)"
    write_summary "${implementation}_writers_4_pread64=$(grep -c 'pread64(' "${trace}" || true)"
    selected="$(grep -Ec '(eventfd2|io_uring_setup|io_uring_register|io_uring_enter|pwrite64|pread64|read|ppoll|poll)\(' "${trace}" || true)"
    write_summary "${implementation}_writers_4_selected_syscalls=${selected}"
  done
  native_enter="$(grep -c 'io_uring_enter(' "${ARTIFACT_DIR}/native-writers-4.strace" || true)"
  positioned_enter="$(grep -c 'io_uring_enter(' "${ARTIFACT_DIR}/positioned-writers-4.strace" || true)"
  legacy_enter="$(grep -c 'io_uring_enter(' "${ARTIFACT_DIR}/legacy-writers-4.strace" || true)"
  awk -v native="${native_enter}" -v positioned="${positioned_enter}" -v legacy="${legacy_enter}" '
    BEGIN { if (native <= 0 || positioned != 0 || legacy != 0) exit 1 }
  '
  write_summary "external_syscall_validation=strace_complete_wait_path"
else
  write_summary "external_syscall_validation=unavailable"
fi

go test ./core/storage/parallelwal \
  -run '^$' \
  -bench '^BenchmarkPhase168NativeWALContention/writers_4$' \
  -benchtime="${BENCHTIME}" \
  -count=1 \
  -cpuprofile="${ARTIFACT_DIR}/native-writers-4.cpu.pprof" \
  -memprofile="${ARTIFACT_DIR}/native-writers-4.mem.pprof" \
  >"${ARTIFACT_DIR}/native-writers-4-profile.log" 2>&1
go tool pprof -top "${ARTIFACT_DIR}/native-writers-4.cpu.pprof" \
  >"${ARTIFACT_DIR}/native-writers-4-cpu-top.txt" 2>&1
go tool pprof -top -alloc_space "${ARTIFACT_DIR}/native-writers-4.mem.pprof" \
  >"${ARTIFACT_DIR}/native-writers-4-alloc-top.txt" 2>&1
write_summary "cpu_profile=collected"
write_summary "allocation_profile=collected"

native_1="$(require_metric native write 1 MB/s median)"
native_4="$(require_metric native write 4 MB/s median)"
legacy_1="$(require_metric legacy write 1 MB/s median)"
legacy_4="$(require_metric legacy write 4 MB/s median)"
positioned_4="$(require_metric positioned write 4 MB/s median)"
native_batch_4="$(require_metric native batch 4 MB/s median)"
positioned_batch_4="$(require_metric positioned batch 4 MB/s median)"
single_ratio="$(ratio "${native_1}" "${legacy_1}")"
four_scaling="$(ratio "${native_4}" "${native_1}")"
four_vs_legacy="$(ratio "${native_4}" "${legacy_4}")"
four_vs_positioned="$(ratio "${native_4}" "${positioned_4}")"
batch_vs_positioned="$(ratio "${native_batch_4}" "${positioned_batch_4}")"
write_summary "candidate_single_writer_vs_legacy_ratio=${single_ratio}"
write_summary "candidate_four_writer_scaling_ratio=${four_scaling}"
write_summary "candidate_four_writer_vs_legacy_ratio=${four_vs_legacy}"
write_summary "candidate_four_writer_vs_positioned_ratio=${four_vs_positioned}"
write_summary "candidate_batch_four_writer_vs_positioned_ratio=${batch_vs_positioned}"

claim_allowed="$(
  awk -v single="${single_ratio}" \
    -v scaling="${four_scaling}" \
    -v positioned="${four_vs_positioned}" \
    -v batch="${batch_vs_positioned}" \
    -v stable="${native_stable}" \
    -v bounded="${native_p99_bounded}" '
      BEGIN {
        if (single >= 0.90 && scaling >= 1.50 &&
            positioned >= 1.00 && batch >= 1.00 &&
            stable == "true" && bounded == "true") {
          print "true"
        } else {
          print "false"
        }
      }
    '
)"
write_summary "performance_claim_allowed=${claim_allowed}"
if [[ "${claim_allowed}" == "true" ]]; then
  write_summary "next_recommendation=d6_recovery_and_mounted_admission"
else
  write_summary "next_recommendation=remove_native_candidate"
fi
write_summary "phase168_native_wal_performance_status=ok"
