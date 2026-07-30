#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase168-native-wal-performance-gate}"
SUMMARY="${ARTIFACT_DIR}/phase168-native-wal-performance-summary.txt"
BENCH="${ARTIFACT_DIR}/contention-benchmark.txt"
METRICS="${ARTIFACT_DIR}/contention-metrics.tsv"
BENCHTIME="${SW_BLOCK_PHASE168_BENCHTIME:-4096x}"
REPETITIONS="${SW_BLOCK_PHASE168_REPETITIONS:-5}"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

metric() {
  local implementation="$1"
  local writers="$2"
  local unit="$3"
  local field="$4"
  awk -F '\t' \
    -v implementation="${implementation}" \
    -v writers="${writers}" \
    -v unit="${unit}" \
    -v field="${field}" '
      $1 == implementation && $2 == writers && $3 == unit {
        if (field == "median") print $4
        if (field == "min") print $5
        if (field == "max") print $6
      }
    ' "${METRICS}"
}

require_metric() {
  local value
  value="$(metric "$1" "$2" "$3" "$4")"
  if [[ -z "${value}" ]]; then
    echo "missing metric implementation=$1 writers=$2 unit=$3 field=$4" >&2
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

if [[ ! "${BENCHTIME}" =~ ^[0-9]+x$ ]]; then
  echo "SW_BLOCK_PHASE168_BENCHTIME must be a fixed iteration count" >&2
  exit 1
fi
iterations="${BENCHTIME%x}"
if (( iterations < 1024 || iterations > 16384 )); then
  echo "benchmark iterations must be between 1024 and 16384" >&2
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
write_summary "benchmark_iterations=${iterations}"
write_summary "benchmark_repetitions=${REPETITIONS}"

cd "${ROOT}"
if [[ "$(go env GOOS)/$(go env GOARCH)" != "linux/amd64" ]]; then
  write_summary "phase168_native_wal_performance_status=unsupported"
  write_summary "reason=gate_requires_linux_amd64"
  exit 2
fi

go test ./internal/iouring ./core/storage/parallelwal -count=1 \
  >"${ARTIFACT_DIR}/preflight-tests.log" 2>&1
write_summary "preflight_tests=pass"

go test ./core/storage/parallelwal \
  -run '^$' \
  -bench '^BenchmarkPhase(168NativeWALContention|167ParallelWALContention|167LegacyWALContentionControl)$' \
  -benchtime="${BENCHTIME}" \
  -count="${REPETITIONS}" \
  -benchmem >"${BENCH}" 2>&1

python3 - "${BENCH}" "${METRICS}" "${REPETITIONS}" <<'PY'
import re
import statistics
import sys

source, destination, expected_count = sys.argv[1], sys.argv[2], int(sys.argv[3])
names = {
    "BenchmarkPhase168NativeWALContention": "native",
    "BenchmarkPhase167ParallelWALContention": "positioned",
    "BenchmarkPhase167LegacyWALContentionControl": "legacy",
}
required = {
    "native": {
        "MB/s", "p50_ns", "p95_ns", "p99_ns", "B/op", "allocs/op",
        "native_admitted", "native_rounds", "native_sqes",
        "native_submit_syscalls", "native_cqes", "native_queue_full",
        "native_short_cqes", "native_inflight_high_water", "native_fallback",
    },
    "positioned": {"MB/s", "p99_ns", "B/op", "allocs/op"},
    "legacy": {"MB/s", "p99_ns", "B/op", "allocs/op"},
}
samples = {}
pattern = re.compile(
    r"^(BenchmarkPhase(?:168NativeWALContention|167ParallelWALContention|"
    r"167LegacyWALContentionControl))/writers_(1|2|4|8)-\d+\s+"
)

with open(source, encoding="utf-8") as handle:
    for raw in handle:
        match = pattern.match(raw)
        if not match:
            continue
        implementation = names[match.group(1)]
        writers = match.group(2)
        fields = raw.split()
        for index in range(2, len(fields) - 1):
            unit = fields[index + 1]
            if unit not in required[implementation]:
                continue
            try:
                value = float(fields[index])
            except ValueError:
                continue
            samples.setdefault((implementation, writers, unit), []).append(value)

with open(destination, "w", encoding="utf-8") as output:
    for implementation in ("native", "positioned", "legacy"):
        for writers in ("1", "2", "4", "8"):
            for unit in sorted(required[implementation]):
                values = samples.get((implementation, writers, unit), [])
                if len(values) != expected_count:
                    raise SystemExit(
                        f"{implementation}/writers_{writers}/{unit}: "
                        f"samples={len(values)} want={expected_count}"
                    )
                output.write(
                    "\t".join(
                        (
                            implementation,
                            writers,
                            unit,
                            f"{statistics.median(values):.6f}",
                            f"{min(values):.6f}",
                            f"{max(values):.6f}",
                        )
                    )
                    + "\n"
                )
PY

for writers in 1 2 4 8; do
  for implementation in native positioned legacy; do
    mibps="$(require_metric "${implementation}" "${writers}" MB/s median)"
    mibps_min="$(require_metric "${implementation}" "${writers}" MB/s min)"
    mibps_max="$(require_metric "${implementation}" "${writers}" MB/s max)"
    p99="$(require_metric "${implementation}" "${writers}" p99_ns median)"
    allocs="$(require_metric "${implementation}" "${writers}" allocs/op median)"
    bytes="$(require_metric "${implementation}" "${writers}" B/op median)"
    write_summary "${implementation}_writers_${writers}_mibps_median=${mibps}"
    write_summary "${implementation}_writers_${writers}_mibps_min=${mibps_min}"
    write_summary "${implementation}_writers_${writers}_mibps_max=${mibps_max}"
    write_summary "${implementation}_writers_${writers}_p99_ns_median=${p99}"
    write_summary "${implementation}_writers_${writers}_allocs_per_op=${allocs}"
    write_summary "${implementation}_writers_${writers}_bytes_per_op=${bytes}"
  done

  native_fallback_max="$(require_metric native "${writers}" native_fallback max)"
  native_queue_full_max="$(require_metric native "${writers}" native_queue_full max)"
  native_short_max="$(require_metric native "${writers}" native_short_cqes max)"
  native_admitted="$(require_metric native "${writers}" native_admitted median)"
  native_rounds="$(require_metric native "${writers}" native_rounds median)"
  native_sqes="$(require_metric native "${writers}" native_sqes median)"
  native_cqes="$(require_metric native "${writers}" native_cqes median)"
  native_submits="$(require_metric native "${writers}" native_submit_syscalls median)"
  native_high_water="$(require_metric native "${writers}" native_inflight_high_water median)"
  awk -v fallback="${native_fallback_max}" \
    -v full="${native_queue_full_max}" \
    -v short="${native_short_max}" \
    -v admitted="${native_admitted}" \
    -v iterations="${iterations}" \
    -v sqes="${native_sqes}" \
    -v cqes="${native_cqes}" '
      BEGIN {
        if (fallback != 0 || full != 0 || short != 0) exit 1
        if (admitted != iterations || sqes != cqes) exit 1
      }
    '
  write_summary "native_writers_${writers}_admitted=${native_admitted}"
  write_summary "native_writers_${writers}_rounds=${native_rounds}"
  write_summary "native_writers_${writers}_sqes=${native_sqes}"
  write_summary "native_writers_${writers}_cqes=${native_cqes}"
  write_summary "native_writers_${writers}_submit_syscalls=${native_submits}"
  write_summary "native_writers_${writers}_inflight_high_water=${native_high_water}"
done
write_summary "native_path_counter_integrity=pass"
write_summary "fallback_count=0"
write_summary "queue_full_rejects=0"
write_summary "short_completions=0"

test_binary="${ARTIFACT_DIR}/parallelwal-performance.test"
go test -c -o "${test_binary}" ./core/storage/parallelwal
if command -v strace >/dev/null 2>&1; then
  for implementation in native positioned legacy; do
    case "${implementation}" in
      native) benchmark='BenchmarkPhase168NativeWALContention/writers_4' ;;
      positioned) benchmark='BenchmarkPhase167ParallelWALContention/writers_4' ;;
      legacy) benchmark='BenchmarkPhase167LegacyWALContentionControl/writers_4' ;;
    esac
    trace="${ARTIFACT_DIR}/${implementation}-writers-4.strace"
    timeout 120s strace -qq -f \
      -e trace=io_uring_setup,io_uring_register,io_uring_enter,pwrite64,pread64 \
      -o "${trace}" \
      "${test_binary}" \
      -test.run '^$' \
      -test.bench "^${benchmark}$" \
      -test.benchtime=1024x \
      -test.count=1 >/dev/null 2>&1
    write_summary "${implementation}_writers_4_io_uring_enter=$(grep -c 'io_uring_enter(' "${trace}" || true)"
    write_summary "${implementation}_writers_4_pwrite64=$(grep -c 'pwrite64(' "${trace}" || true)"
    write_summary "${implementation}_writers_4_pread64=$(grep -c 'pread64(' "${trace}" || true)"
  done
  native_enter="$(grep -c 'io_uring_enter(' "${ARTIFACT_DIR}/native-writers-4.strace" || true)"
  positioned_enter="$(grep -c 'io_uring_enter(' "${ARTIFACT_DIR}/positioned-writers-4.strace" || true)"
  legacy_enter="$(grep -c 'io_uring_enter(' "${ARTIFACT_DIR}/legacy-writers-4.strace" || true)"
  awk -v native="${native_enter}" -v positioned="${positioned_enter}" -v legacy="${legacy_enter}" '
    BEGIN {
      if (native <= 0 || positioned != 0 || legacy != 0) exit 1
    }
  '
  write_summary "external_syscall_validation=strace"
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

native_1="$(require_metric native 1 MB/s median)"
native_4="$(require_metric native 4 MB/s median)"
native_4_min="$(require_metric native 4 MB/s min)"
legacy_1="$(require_metric legacy 1 MB/s median)"
legacy_4="$(require_metric legacy 4 MB/s median)"
positioned_4="$(require_metric positioned 4 MB/s median)"
native_p50_4="$(require_metric native 4 p50_ns median)"
native_p99_4="$(require_metric native 4 p99_ns median)"
single_ratio="$(ratio "${native_1}" "${legacy_1}")"
four_scaling="$(ratio "${native_4}" "${native_1}")"
four_vs_legacy="$(ratio "${native_4}" "${legacy_4}")"
four_vs_positioned="$(ratio "${native_4}" "${positioned_4}")"
four_min_vs_median="$(ratio "${native_4_min}" "${native_4}")"
p99_over_p50="$(ratio "${native_p99_4}" "${native_p50_4}")"
write_summary "candidate_single_writer_vs_legacy_ratio=${single_ratio}"
write_summary "candidate_four_writer_scaling_ratio=${four_scaling}"
write_summary "candidate_four_writer_vs_legacy_ratio=${four_vs_legacy}"
write_summary "candidate_four_writer_vs_positioned_ratio=${four_vs_positioned}"
write_summary "candidate_four_writer_min_vs_median_ratio=${four_min_vs_median}"
write_summary "candidate_four_writer_p99_over_p50=${p99_over_p50}"

claim_allowed="$(
  awk -v single="${single_ratio}" \
    -v scaling="${four_scaling}" \
    -v stability="${four_min_vs_median}" \
    -v p99="${native_p99_4}" '
      BEGIN {
        if (single >= 0.90 && scaling >= 1.50 &&
            stability >= 0.70 && p99 <= 100000000) {
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
  write_summary "next_recommendation=reject_or_redesign_native_execution"
fi
write_summary "phase168_native_wal_performance_status=ok"
