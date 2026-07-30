#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase171-walstore-checkpoint-baseline-gate}"
SUMMARY="${ARTIFACT_DIR}/phase171-walstore-checkpoint-baseline-summary.txt"
BENCHTIME="${SW_BLOCK_PHASE171_BENCHTIME:-1s}"
REPETITIONS="${SW_BLOCK_PHASE171_REPETITIONS:-5}"
WRITERS=(1 2 4 8)
WORKLOADS=(sequential scattered batch)

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

benchmark_for_workload() {
  case "$1" in
    sequential) echo "BenchmarkPhase167WALStoreContention" ;;
    scattered) echo "BenchmarkPhase171WALStoreScatteredContention" ;;
    batch) echo "BenchmarkPhase170WALStoreBatchContention" ;;
    *) return 1 ;;
  esac
}

metric_from_log() {
  local file="$1"
  local benchmark="$2"
  local writers="$3"
  local metric="$4"
  awk -v prefix="${benchmark}/writers_${writers}-" -v metric="${metric}" '
    index($1, prefix) == 1 {
      for (i = 2; i < NF; i++) {
        if ($(i + 1) == metric) {
          print $i
          exit
        }
      }
    }
  ' "${file}"
}

median_file() {
  sort -n "$1" | awk 'NR == 3 { print; exit }'
}

minimum_file() {
  sort -n "$1" | awk 'NR == 1 { print; exit }'
}

maximum_file() {
  sort -n "$1" | awk 'END { print }'
}

ratio_for_summary() {
  awk -v numerator="$1" -v denominator="$2" 'BEGIN {
    if (numerator < 0 || denominator <= 0) exit 1
    printf "%.3f", numerator / denominator
  }'
}

at_least_value() {
  awk -v value="$1" -v minimum="$2" 'BEGIN {
    print (value >= minimum) ? "true" : "false"
  }'
}

at_most_value() {
  awk -v value="$1" -v maximum="$2" 'BEGIN {
    print (value <= maximum) ? "true" : "false"
  }'
}

equal_values() {
  awk -v left="$1" -v right="$2" 'BEGIN {
    print (left + 0 == right + 0) ? "true" : "false"
  }'
}

values_at_most_count() {
  awk -v maximum="$2" '$1 <= maximum { count++ } END { print count + 0 }' "$1"
}

values_at_least_count() {
  awk -v minimum="$2" '$1 >= minimum { count++ } END { print count + 0 }' "$1"
}

if [[ "${REPETITIONS}" != "5" ]]; then
  echo "SW_BLOCK_PHASE171_REPETITIONS must be exactly 5" >&2
  exit 1
fi
if [[ "${BENCHTIME}" != "1s" ]]; then
  echo "SW_BLOCK_PHASE171_BENCHTIME must be exactly 1s" >&2
  exit 1
fi

cd "${ROOT}"
write_summary "phase171_walstore_checkpoint_baseline_status=running"
write_summary "git_sha=$(git rev-parse HEAD)"
write_summary "git_dirty=$([[ -n "$(git status --porcelain)" ]] && echo true || echo false)"
write_summary "go_version=$(go version | tr ' ' '_')"
write_summary "kernel=$(uname -srvmo | tr ' ' '_')"
write_summary "benchmark_time=${BENCHTIME}"
write_summary "repetitions=${REPETITIONS}"
write_summary "writers=1,2,4,8"
write_summary "workloads=sequential_4k,scattered_4k,contiguous_16x4k_batch"
write_summary "background_flusher_interval=100ms"
write_summary "sync_cadence=one_final_explicit_sync_per_sample"
write_summary "timing_boundaries=foreground_then_final_sync_then_final_drain"
write_summary "d2_opportunity_thresholds=sequential_min_ops_per_entry_at_most_0.25,scattered_at_most_0.75,scattered_coalescible_entries_at_least_0.25"

go test ./core/storage -count=1 >"${ARTIFACT_DIR}/storage-regression.log" 2>&1
write_summary "storage_regression=pass"

metric_pairs=(
  "MB/s:mibps"
  "p50_ns:p50_ns"
  "p95_ns:p95_ns"
  "p99_ns:p99_ns"
  "B/op:bytes_per_api_call"
  "allocs/op:allocs_per_api_call"
  "foreground_ns:foreground_ns"
  "final_sync_ns:final_sync_ns"
  "final_drain_ns:final_drain_ns"
  "writeat_calls/entry:wal_writeat_ops_per_entry"
  "flush_snapshot_entries/entry:snapshot_entries_per_entry"
  "flush_snapshot_ns/entry:snapshot_ns_per_entry"
  "flush_opportunity_ns/entry:opportunity_analysis_ns_per_entry"
  "flush_validated_records/entry:validated_records_per_entry"
  "flush_superseded_entries/entry:superseded_entries_per_entry"
  "flush_header_reads/entry:wal_header_reads_per_entry"
  "flush_header_read_bytes/entry:wal_header_read_bytes_per_entry"
  "flush_header_read_ns/entry:wal_header_read_ns_per_entry"
  "flush_record_reads/entry:wal_record_reads_per_entry"
  "flush_record_read_bytes/entry:wal_record_read_bytes_per_entry"
  "flush_record_read_ns/entry:wal_record_read_ns_per_entry"
  "extent_write_ops/entry:extent_write_ops_per_entry"
  "extent_write_bytes/entry:extent_write_bytes_per_entry"
  "extent_write_ns/entry:extent_write_ns_per_entry"
  "extent_snapshot_min_write_ops/entry:extent_snapshot_min_write_ops_per_entry"
  "extent_snapshot_runs/entry:extent_snapshot_runs_per_entry"
  "extent_snapshot_singleton_runs/entry:extent_snapshot_singleton_runs_per_entry"
  "extent_snapshot_coalescible_entries/entry:extent_snapshot_coalescible_entries_per_entry"
  "extent_snapshot_max_run_blocks:extent_snapshot_max_run_blocks"
  "extent_written_min_write_ops/entry:extent_written_min_write_ops_per_entry"
  "extent_written_runs/entry:extent_written_runs_per_entry"
  "extent_written_singleton_runs/entry:extent_written_singleton_runs_per_entry"
  "extent_written_coalescible_entries/entry:extent_written_coalescible_entries_per_entry"
  "extent_written_max_run_blocks:extent_written_max_run_blocks"
  "extent_write_max_bytes:extent_write_max_bytes"
  "extent_sync_ops:extent_sync_ops"
  "extent_sync_ns/op:extent_sync_ns_per_op"
  "checkpoint_write_ops:checkpoint_write_ops"
  "checkpoint_write_ns/op:checkpoint_write_ns_per_op"
  "checkpoint_sync_ops:checkpoint_sync_ops"
  "checkpoint_sync_ns/op:checkpoint_sync_ns_per_op"
  "flush_cycles_started:flush_cycles_started"
  "flush_cycles_succeeded:flush_cycles_succeeded"
  "flush_cycle_ns/entry:flush_cycle_ns_per_entry"
  "flush_cycle_max_ns:flush_cycle_max_ns"
  "dirty_entries:dirty_entries"
  "checkpoint_lsn:checkpoint_lsn"
  "head_lsn:head_lsn"
  "checkpoint_coverage:checkpoint_coverage"
  "explicit_sync_calls:explicit_sync_calls"
  "entries/api_call:entries_per_api_call"
)

failure_metrics=(
  "flush_validation_failures"
  "flush_header_read_failures"
  "flush_record_read_failures"
  "extent_write_failures"
  "extent_sync_failures"
  "checkpoint_write_failures"
  "checkpoint_sync_failures"
  "flush_cycles_failed"
)

for workload in "${WORKLOADS[@]}"; do
  for writers in "${WRITERS[@]}"; do
    for pair in "${metric_pairs[@]}"; do
      : >"${ARTIFACT_DIR}/${workload}-writers-${writers}-${pair#*:}.values"
    done
  done
done

for repetition in 1 2 3 4 5; do
  case "${repetition}" in
    1|4) order=(sequential scattered batch) ;;
    2|5) order=(batch sequential scattered) ;;
    3) order=(scattered batch sequential) ;;
  esac
  write_summary "repetition_${repetition}_order=${order[*]}"
  for workload in "${order[@]}"; do
    benchmark="$(benchmark_for_workload "${workload}")"
    log="${ARTIFACT_DIR}/${repetition}-${workload}.log"
    go test ./core/storage -run '^$' \
      -bench "^${benchmark}/writers_(1|2|4|8)$" \
      -benchtime="${BENCHTIME}" -count=1 >"${log}" 2>&1

    for writers in "${WRITERS[@]}"; do
      for pair in "${metric_pairs[@]}"; do
        metric="${pair%%:*}"
        alias="${pair#*:}"
        value="$(metric_from_log "${log}" "${benchmark}" "${writers}" "${metric}")"
        if [[ -z "${value}" ]]; then
          echo "missing ${workload} writers=${writers} metric=${metric} in ${log}" >&2
          exit 1
        fi
        echo "${value}" >>"${ARTIFACT_DIR}/${workload}-writers-${writers}-${alias}.values"
      done

      for metric in "${failure_metrics[@]}"; do
        value="$(metric_from_log "${log}" "${benchmark}" "${writers}" "${metric}")"
        if [[ -z "${value}" || "$(equal_values "${value}" "0")" != "true" ]]; then
          echo "nonzero or missing ${workload} writers=${writers} ${metric}=${value:-missing} in ${log}" >&2
          exit 1
        fi
      done

      sync_calls="$(metric_from_log "${log}" "${benchmark}" "${writers}" "explicit_sync_calls")"
      dirty_entries="$(metric_from_log "${log}" "${benchmark}" "${writers}" "dirty_entries")"
      checkpoint_lsn="$(metric_from_log "${log}" "${benchmark}" "${writers}" "checkpoint_lsn")"
      head_lsn="$(metric_from_log "${log}" "${benchmark}" "${writers}" "head_lsn")"
      checkpoint_coverage="$(metric_from_log "${log}" "${benchmark}" "${writers}" "checkpoint_coverage")"
      cycles_started="$(metric_from_log "${log}" "${benchmark}" "${writers}" "flush_cycles_started")"
      cycles_succeeded="$(metric_from_log "${log}" "${benchmark}" "${writers}" "flush_cycles_succeeded")"
      if [[ "$(equal_values "${sync_calls}" "1")" != "true" ||
            "$(equal_values "${dirty_entries}" "0")" != "true" ||
            "$(equal_values "${checkpoint_lsn}" "${head_lsn}")" != "true" ||
            "$(equal_values "${checkpoint_coverage}" "1")" != "true" ||
            "$(equal_values "${cycles_started}" "${cycles_succeeded}")" != "true" ]]; then
        echo "unsettled or inconsistent pipeline ${workload} writers=${writers} in ${log}" >&2
        exit 1
      fi

      entries_per_call="$(metric_from_log "${log}" "${benchmark}" "${writers}" "entries/api_call")"
      if [[ "${workload}" == "batch" ]]; then
        batch_blocks="$(metric_from_log "${log}" "${benchmark}" "${writers}" "batch_blocks")"
        if [[ "$(equal_values "${batch_blocks}" "16")" != "true" ||
              "$(equal_values "${entries_per_call}" "16")" != "true" ]]; then
          echo "invalid batch shape writers=${writers} in ${log}" >&2
          exit 1
        fi
      elif [[ "$(equal_values "${entries_per_call}" "1")" != "true" ]]; then
        echo "invalid ordinary shape ${workload} writers=${writers} in ${log}" >&2
        exit 1
      fi
    done
  done
done

write_summary "all_samples_checkpoint_coverage_complete=true"
write_summary "all_samples_failed_cycles_zero=true"

summary_aliases=(
  mibps
  p50_ns
  p95_ns
  p99_ns
  bytes_per_api_call
  allocs_per_api_call
  foreground_ns
  final_sync_ns
  final_drain_ns
  snapshot_ns_per_entry
  opportunity_analysis_ns_per_entry
  wal_header_reads_per_entry
  wal_record_reads_per_entry
  extent_write_ops_per_entry
  extent_snapshot_min_write_ops_per_entry
  extent_snapshot_coalescible_entries_per_entry
  extent_snapshot_max_run_blocks
  extent_written_min_write_ops_per_entry
  extent_written_runs_per_entry
  extent_written_singleton_runs_per_entry
  extent_written_coalescible_entries_per_entry
  extent_written_max_run_blocks
  extent_sync_ns_per_op
  checkpoint_sync_ns_per_op
  flush_cycle_ns_per_entry
)

for workload in "${WORKLOADS[@]}"; do
  for writers in "${WRITERS[@]}"; do
    for alias in "${summary_aliases[@]}"; do
      file="${ARTIFACT_DIR}/${workload}-writers-${writers}-${alias}.values"
      write_summary "${workload}_writers_${writers}_${alias}_median=$(median_file "${file}")"
    done
    mibps_file="${ARTIFACT_DIR}/${workload}-writers-${writers}-mibps.values"
    write_summary "${workload}_writers_${writers}_mibps_min=$(minimum_file "${mibps_file}")"
    write_summary "${workload}_writers_${writers}_mibps_max=$(maximum_file "${mibps_file}")"
  done
done

sequential_min_file="${ARTIFACT_DIR}/sequential-writers-4-extent_written_min_write_ops_per_entry.values"
scattered_min_file="${ARTIFACT_DIR}/scattered-writers-4-extent_written_min_write_ops_per_entry.values"
scattered_coalescible_file="${ARTIFACT_DIR}/scattered-writers-4-extent_written_coalescible_entries_per_entry.values"
sequential_actual_file="${ARTIFACT_DIR}/sequential-writers-4-extent_write_ops_per_entry.values"
scattered_actual_file="${ARTIFACT_DIR}/scattered-writers-4-extent_write_ops_per_entry.values"

sequential_min_median="$(median_file "${sequential_min_file}")"
scattered_min_median="$(median_file "${scattered_min_file}")"
scattered_coalescible_median="$(median_file "${scattered_coalescible_file}")"
sequential_actual_median="$(median_file "${sequential_actual_file}")"
scattered_actual_median="$(median_file "${scattered_actual_file}")"

sequential_op_reduction_ratio="$(ratio_for_summary "${sequential_actual_median}" "${sequential_min_median}")"
scattered_op_reduction_ratio="$(ratio_for_summary "${scattered_actual_median}" "${scattered_min_median}")"
sequential_opportunity_pass_count="$(values_at_most_count "${sequential_min_file}" "0.25")"
scattered_opportunity_pass_count="$(values_at_most_count "${scattered_min_file}" "0.75")"
scattered_coalescible_pass_count="$(values_at_least_count "${scattered_coalescible_file}" "0.25")"

sequential_opportunity_stable="$(at_least_value "${sequential_opportunity_pass_count}" "4")"
scattered_opportunity_stable="$(at_least_value "${scattered_opportunity_pass_count}" "4")"
scattered_coalescible_stable="$(at_least_value "${scattered_coalescible_pass_count}" "4")"
sequential_current_amplification="$(at_least_value "${sequential_actual_median}" "0.75")"
scattered_current_amplification="$(at_least_value "${scattered_actual_median}" "0.75")"

d2_bounded_extent_candidate_admitted=false
if [[ "${sequential_opportunity_stable}" == "true" &&
      "${scattered_opportunity_stable}" == "true" &&
      "${scattered_coalescible_stable}" == "true" &&
      "${sequential_current_amplification}" == "true" &&
      "${scattered_current_amplification}" == "true" ]]; then
  d2_bounded_extent_candidate_admitted=true
fi

write_summary "sequential_writers_4_extent_op_reduction_ratio=${sequential_op_reduction_ratio}"
write_summary "scattered_writers_4_extent_op_reduction_ratio=${scattered_op_reduction_ratio}"
write_summary "sequential_opportunity_pass_count=${sequential_opportunity_pass_count}"
write_summary "scattered_opportunity_pass_count=${scattered_opportunity_pass_count}"
write_summary "scattered_coalescible_pass_count=${scattered_coalescible_pass_count}"
write_summary "sequential_opportunity_stable=${sequential_opportunity_stable}"
write_summary "scattered_opportunity_stable=${scattered_opportunity_stable}"
write_summary "scattered_coalescible_stable=${scattered_coalescible_stable}"
write_summary "sequential_current_amplification=${sequential_current_amplification}"
write_summary "scattered_current_amplification=${scattered_current_amplification}"
write_summary "d2_bounded_extent_candidate_admitted=${d2_bounded_extent_candidate_admitted}"

profile_log="${ARTIFACT_DIR}/sequential-writers-4-profile.log"
go test ./core/storage -run '^$' \
  -bench '^BenchmarkPhase167WALStoreContention/writers_4$' \
  -benchtime=1s -count=1 \
  -cpuprofile="${ARTIFACT_DIR}/sequential-writers-4.cpu.pprof" \
  -memprofile="${ARTIFACT_DIR}/sequential-writers-4.mem.pprof" \
  >"${profile_log}" 2>&1
go tool pprof -top -nodecount=25 \
  "${ARTIFACT_DIR}/sequential-writers-4.cpu.pprof" \
  >"${ARTIFACT_DIR}/sequential-writers-4.cpu-top.txt"
write_summary "cpu_profile=sequential-writers-4.cpu.pprof"
write_summary "memory_profile=sequential-writers-4.mem.pprof"
write_summary "cpu_top=sequential-writers-4.cpu-top.txt"

if command -v strace >/dev/null 2>&1; then
  go test -c ./core/storage -o "${ARTIFACT_DIR}/storage.test"
  if strace -f -c -e trace=pread64,pwrite64,fsync,fdatasync \
      -o "${ARTIFACT_DIR}/sequential-writers-4.strace.txt" \
      "${ARTIFACT_DIR}/storage.test" \
        -test.run '^$' \
        -test.bench '^BenchmarkPhase167WALStoreContention/writers_4$' \
        -test.benchtime=1s \
        -test.count=1 \
        >"${ARTIFACT_DIR}/sequential-writers-4.strace-benchmark.log" 2>&1; then
    write_summary "strace_available=true"
    write_summary "strace_summary=sequential-writers-4.strace.txt"
    write_summary "strace_scope=qualitative_whole_benchmark_process"
  else
    write_summary "strace_available=false"
    write_summary "strace_error=sequential-writers-4.strace-benchmark.log"
  fi
else
  write_summary "strace_available=false"
fi

if [[ "${d2_bounded_extent_candidate_admitted}" == "true" ]]; then
  write_summary "next_recommendation=implement_disabled_bounded_extent_writeback"
else
  write_summary "next_recommendation=stop_before_bounded_extent_writeback"
fi
write_summary "phase171_walstore_checkpoint_baseline_status=ok"
