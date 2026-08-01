#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase170-walstore-headroom-gate}"
SUMMARY="${ARTIFACT_DIR}/phase170-walstore-headroom-summary.txt"
BENCHTIME="${SW_BLOCK_PHASE170_BENCHTIME:-1s}"
REPETITIONS="${SW_BLOCK_PHASE170_REPETITIONS:-5}"
WRITERS=(1 2 4 8)

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

benchmark_for_mode() {
  case "$1" in
    ordinary) echo "BenchmarkPhase167WALStoreContention" ;;
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
    if (numerator <= 0 || denominator <= 0) exit 1
    printf "%.3f", numerator / denominator
  }'
}

ratio_at_least() {
  awk -v numerator="$1" -v denominator="$2" -v minimum="$3" 'BEGIN {
    if (numerator <= 0 || denominator <= 0) exit 1
    print (numerator / denominator >= minimum) ? "true" : "false"
  }'
}

ratio_at_most() {
  awk -v numerator="$1" -v denominator="$2" -v maximum="$3" 'BEGIN {
    if (numerator <= 0 || denominator <= 0) exit 1
    print (numerator / denominator <= maximum) ? "true" : "false"
  }'
}

ratio_below() {
  awk -v numerator="$1" -v denominator="$2" -v maximum="$3" 'BEGIN {
    if (numerator <= 0 || denominator <= 0) exit 1
    print (numerator / denominator < maximum) ? "true" : "false"
  }'
}

at_most() {
  awk -v value="$1" -v maximum="$2" 'BEGIN {
    print (value <= maximum) ? "true" : "false"
  }'
}

at_least_value() {
  awk -v value="$1" -v minimum="$2" 'BEGIN {
    print (value >= minimum) ? "true" : "false"
  }'
}

equal_values() {
  awk -v left="$1" -v right="$2" 'BEGIN {
    print (left + 0 == right + 0) ? "true" : "false"
  }'
}

paired_ratio_at_least_count() {
  paste "$1" "$2" | awk -v minimum="$3" '
    $1 > 0 && $2 > 0 && $1 / $2 >= minimum { count++ }
    END { print count + 0 }
  '
}

paired_ratio_below_count() {
  paste "$1" "$2" | awk -v maximum="$3" '
    $1 > 0 && $2 > 0 && $1 / $2 < maximum { count++ }
    END { print count + 0 }
  '
}

values_at_least_count() {
  awk -v minimum="$2" '$1 >= minimum { count++ } END { print count + 0 }' "$1"
}

values_at_most_count() {
  awk -v maximum="$2" '$1 <= maximum { count++ } END { print count + 0 }' "$1"
}

if [[ "${REPETITIONS}" != "5" ]]; then
  echo "SW_BLOCK_PHASE170_REPETITIONS must be exactly 5" >&2
  exit 1
fi
if [[ "${BENCHTIME}" != "1s" ]]; then
  echo "SW_BLOCK_PHASE170_BENCHTIME must be exactly 1s" >&2
  exit 1
fi

write_summary "phase170_walstore_headroom_status=running"
write_summary "benchmark_time=${BENCHTIME}"
write_summary "repetitions=${REPETITIONS}"
write_summary "writers=1,2,4,8"
write_summary "ordinary_shape=one_4k_write"
write_summary "batch_control_shape=sixteen_4k_writes"
write_summary "sync_cadence=one_final_explicit_sync_per_sample"
write_summary "legacy_background_flush_enabled=true"
write_summary "timed_close_shape=sync_then_final_flusher_drain"
write_summary "latency_and_alloc_denominator=api_call"
write_summary "record_stage_denominator=record_or_named_append_stage"
write_summary "stability_requirement=paired_4_of_5_and_range_ratio_at_most_1.35"

cd "${ROOT}"
go test ./core/storage -count=1 >"${ARTIFACT_DIR}/storage-regression.log" 2>&1
write_summary "storage_regression=pass"

metric_pairs=(
  "MB/s:mibps"
  "p50_ns:p50_ns_per_api_call"
  "p95_ns:p95_ns_per_api_call"
  "p99_ns:p99_ns"
  "B/op:bytes_per_api_call"
  "allocs/op:allocs_per_api_call"
  "wal_copy_ns/record:wal_copy_ns_per_record"
  "wal_encode_ns/record:wal_encode_ns_per_record"
  "wal_checksum_ns/record:wal_checksum_ns_per_record"
  "wal_append_ns/writeat:wal_append_ns_per_writeat"
  "wal_lock_wait_ns/append_call:wal_lock_wait_ns_per_append_call"
  "commit_lock_wait_ns/api_call:commit_lock_wait_ns_per_api_call"
  "dirty_map_ns/record:dirty_map_ns_per_record"
  "writeat_calls/entry:writeat_calls_per_entry"
  "writeat_bytes/entry:writeat_bytes_per_entry"
  "writeat_max_bytes:writeat_max_bytes"
  "flushes:flushes"
  "dirty_entries:dirty_entries"
  "checkpoint_lsn:checkpoint_lsn"
  "head_lsn:head_lsn"
  "checkpoint_coverage:checkpoint_coverage"
  "explicit_sync_calls:explicit_sync_calls"
  "entries/api_call:entries_per_api_call"
)

for mode in ordinary batch; do
  for writers in "${WRITERS[@]}"; do
    for pair in "${metric_pairs[@]}"; do
      alias="${pair#*:}"
      : >"${ARTIFACT_DIR}/${mode}-writers-${writers}-${alias}.values"
    done
  done
done

for repetition in 1 2 3 4 5; do
  if (( repetition % 2 == 1 )); then
    order=(ordinary batch)
  else
    order=(batch ordinary)
  fi
  write_summary "repetition_${repetition}_order=${order[*]}"
  for mode in "${order[@]}"; do
    benchmark="$(benchmark_for_mode "${mode}")"
    log="${ARTIFACT_DIR}/${repetition}-${mode}.log"
    go test ./core/storage -run '^$' \
      -bench "^${benchmark}/writers_(1|2|4|8)$" \
      -benchtime="${BENCHTIME}" -count=1 >"${log}" 2>&1
    for writers in "${WRITERS[@]}"; do
      for pair in "${metric_pairs[@]}"; do
        metric="${pair%%:*}"
        alias="${pair#*:}"
        value="$(metric_from_log "${log}" "${benchmark}" "${writers}" "${metric}")"
        if [[ -z "${value}" ]]; then
          echo "missing ${mode} writers=${writers} metric=${metric} in ${log}" >&2
          exit 1
        fi
        echo "${value}" >>"${ARTIFACT_DIR}/${mode}-writers-${writers}-${alias}.values"
      done
      sync_calls="$(metric_from_log "${log}" "${benchmark}" "${writers}" "explicit_sync_calls")"
      if [[ "${sync_calls}" != "1.000" ]]; then
        echo "invalid ${mode} writers=${writers} explicit Sync count in ${log}" >&2
        exit 1
      fi
      dirty_entries="$(metric_from_log "${log}" "${benchmark}" "${writers}" "dirty_entries")"
      checkpoint_lsn="$(metric_from_log "${log}" "${benchmark}" "${writers}" "checkpoint_lsn")"
      head_lsn="$(metric_from_log "${log}" "${benchmark}" "${writers}" "head_lsn")"
      checkpoint_coverage="$(metric_from_log "${log}" "${benchmark}" "${writers}" "checkpoint_coverage")"
      if [[ "$(equal_values "${dirty_entries}" "0")" != "true" ||
            "$(equal_values "${checkpoint_lsn}" "${head_lsn}")" != "true" ||
            "$(equal_values "${checkpoint_coverage}" "1")" != "true" ]]; then
        echo "unsettled flusher/checkpoint debt ${mode} writers=${writers} in ${log}" >&2
        exit 1
      fi
      entries_per_call="$(metric_from_log "${log}" "${benchmark}" "${writers}" "entries/api_call")"
      if [[ "${mode}" == "batch" ]]; then
        batch_blocks="$(metric_from_log "${log}" "${benchmark}" "${writers}" "batch_blocks")"
        if [[ "${batch_blocks}" != "16.00" || "${entries_per_call}" != "16.00" ]]; then
          echo "invalid batch block count writers=${writers} in ${log}" >&2
          exit 1
        fi
      elif [[ "${entries_per_call}" != "1.000" ]]; then
        echo "invalid ordinary entries/API call writers=${writers} in ${log}" >&2
        exit 1
      fi
    done
  done
done

write_summary "flusher_debt_settled_all_samples=true"

for mode in ordinary batch; do
  for writers in "${WRITERS[@]}"; do
    mibps_file="${ARTIFACT_DIR}/${mode}-writers-${writers}-mibps.values"
    write_summary "${mode}_writers_${writers}_mibps_median=$(median_file "${mibps_file}")"
    write_summary "${mode}_writers_${writers}_mibps_min=$(minimum_file "${mibps_file}")"
    write_summary "${mode}_writers_${writers}_mibps_max=$(maximum_file "${mibps_file}")"
    write_summary "${mode}_writers_${writers}_p50_ns_per_api_call_median=$(median_file \
      "${ARTIFACT_DIR}/${mode}-writers-${writers}-p50_ns_per_api_call.values")"
    write_summary "${mode}_writers_${writers}_p95_ns_per_api_call_median=$(median_file \
      "${ARTIFACT_DIR}/${mode}-writers-${writers}-p95_ns_per_api_call.values")"
    write_summary "${mode}_writers_${writers}_p99_ns_per_api_call_median=$(median_file \
      "${ARTIFACT_DIR}/${mode}-writers-${writers}-p99_ns.values")"
    write_summary "${mode}_writers_${writers}_bytes_per_api_call_median=$(median_file \
      "${ARTIFACT_DIR}/${mode}-writers-${writers}-bytes_per_api_call.values")"
    write_summary "${mode}_writers_${writers}_allocs_per_api_call_median=$(median_file \
      "${ARTIFACT_DIR}/${mode}-writers-${writers}-allocs_per_api_call.values")"
    write_summary "${mode}_writers_${writers}_writeat_calls_per_entry_median=$(median_file \
      "${ARTIFACT_DIR}/${mode}-writers-${writers}-writeat_calls_per_entry.values")"
  done
done

for writers in 1 4; do
  for alias in \
    wal_copy_ns_per_record \
    wal_encode_ns_per_record \
    wal_checksum_ns_per_record \
    wal_append_ns_per_writeat \
    wal_lock_wait_ns_per_append_call \
    commit_lock_wait_ns_per_api_call \
    dirty_map_ns_per_record \
    writeat_bytes_per_entry \
    writeat_max_bytes \
    flushes \
    checkpoint_lsn; do
    write_summary "ordinary_writers_${writers}_${alias}_median=$(median_file \
      "${ARTIFACT_DIR}/ordinary-writers-${writers}-${alias}.values")"
  done
done

ordinary_1="$(median_file "${ARTIFACT_DIR}/ordinary-writers-1-mibps.values")"
ordinary_4="$(median_file "${ARTIFACT_DIR}/ordinary-writers-4-mibps.values")"
batch_4="$(median_file "${ARTIFACT_DIR}/batch-writers-4-mibps.values")"
ordinary_writeat_4="$(median_file \
  "${ARTIFACT_DIR}/ordinary-writers-4-writeat_calls_per_entry.values")"
batch_writeat_4="$(median_file \
  "${ARTIFACT_DIR}/batch-writers-4-writeat_calls_per_entry.values")"
ordinary_4_min="$(minimum_file "${ARTIFACT_DIR}/ordinary-writers-4-mibps.values")"
ordinary_4_max="$(maximum_file "${ARTIFACT_DIR}/ordinary-writers-4-mibps.values")"
batch_4_min="$(minimum_file "${ARTIFACT_DIR}/batch-writers-4-mibps.values")"
batch_4_max="$(maximum_file "${ARTIFACT_DIR}/batch-writers-4-mibps.values")"

ordinary_scaling_ratio="$(ratio_for_summary "${ordinary_4}" "${ordinary_1}")"
batch_gain_ratio="$(ratio_for_summary "${batch_4}" "${ordinary_4}")"
ordinary_4_range_ratio="$(ratio_for_summary "${ordinary_4_max}" "${ordinary_4_min}")"
batch_4_range_ratio="$(ratio_for_summary "${batch_4_max}" "${batch_4_min}")"
ordinary_concurrency_deficit="$(ratio_below "${ordinary_4}" "${ordinary_1}" "1.200")"
batch_throughput_headroom="$(ratio_at_least "${batch_4}" "${ordinary_4}" "1.250")"
ordinary_one_writeat_per_entry="$(at_least_value "${ordinary_writeat_4}" "0.900")"
batch_writeat_coalescing="$(at_most "${batch_writeat_4}" "0.250")"
ordinary_range_bounded="$(ratio_at_most "${ordinary_4_max}" "${ordinary_4_min}" "1.350")"
batch_range_bounded="$(ratio_at_most "${batch_4_max}" "${batch_4_min}" "1.350")"
paired_batch_gain_pass_count="$(paired_ratio_at_least_count \
  "${ARTIFACT_DIR}/batch-writers-4-mibps.values" \
  "${ARTIFACT_DIR}/ordinary-writers-4-mibps.values" "1.250")"
paired_ordinary_deficit_count="$(paired_ratio_below_count \
  "${ARTIFACT_DIR}/ordinary-writers-4-mibps.values" \
  "${ARTIFACT_DIR}/ordinary-writers-1-mibps.values" "1.200")"
ordinary_writeat_pass_count="$(values_at_least_count \
  "${ARTIFACT_DIR}/ordinary-writers-4-writeat_calls_per_entry.values" "0.900")"
batch_writeat_pass_count="$(values_at_most_count \
  "${ARTIFACT_DIR}/batch-writers-4-writeat_calls_per_entry.values" "0.250")"
paired_batch_gain_stable="$(at_least_value "${paired_batch_gain_pass_count}" "4")"
writeat_shape_stable=false
if [[ "${ordinary_writeat_pass_count}" == "5" && "${batch_writeat_pass_count}" == "5" ]]; then
  writeat_shape_stable=true
fi

existing_format_headroom=false
if [[ "${batch_throughput_headroom}" == "true" &&
      "${ordinary_one_writeat_per_entry}" == "true" &&
      "${batch_writeat_coalescing}" == "true" &&
      "${writeat_shape_stable}" == "true" ]]; then
  existing_format_headroom=true
fi
d2_owner_admitted=false
if [[ "${existing_format_headroom}" == "true" &&
      "${paired_batch_gain_stable}" == "true" &&
      "${ordinary_range_bounded}" == "true" &&
      "${batch_range_bounded}" == "true" ]]; then
  d2_owner_admitted=true
fi

write_summary "ordinary_four_writer_scaling_ratio=${ordinary_scaling_ratio}"
write_summary "batch_four_vs_ordinary_four_ratio=${batch_gain_ratio}"
write_summary "ordinary_writers_4_range_ratio=${ordinary_4_range_ratio}"
write_summary "batch_writers_4_range_ratio=${batch_4_range_ratio}"
write_summary "ordinary_concurrency_deficit=${ordinary_concurrency_deficit}"
write_summary "paired_ordinary_deficit_count=${paired_ordinary_deficit_count}"
write_summary "batch_throughput_headroom=${batch_throughput_headroom}"
write_summary "paired_batch_gain_pass_count=${paired_batch_gain_pass_count}"
write_summary "paired_batch_gain_stable=${paired_batch_gain_stable}"
write_summary "ordinary_one_writeat_per_entry=${ordinary_one_writeat_per_entry}"
write_summary "batch_writeat_coalescing=${batch_writeat_coalescing}"
write_summary "ordinary_writeat_pass_count=${ordinary_writeat_pass_count}"
write_summary "batch_writeat_pass_count=${batch_writeat_pass_count}"
write_summary "writeat_shape_stable=${writeat_shape_stable}"
write_summary "ordinary_range_bounded=${ordinary_range_bounded}"
write_summary "batch_range_bounded=${batch_range_bounded}"
write_summary "existing_format_headroom=${existing_format_headroom}"
write_summary "d2_owner_admitted=${d2_owner_admitted}"

profile_log="${ARTIFACT_DIR}/ordinary-writers-4-profile.log"
go test ./core/storage -run '^$' \
  -bench '^BenchmarkPhase167WALStoreContention/writers_4$' \
  -benchtime=1s -count=1 \
  -cpuprofile="${ARTIFACT_DIR}/ordinary-writers-4.cpu.pprof" \
  -memprofile="${ARTIFACT_DIR}/ordinary-writers-4.mem.pprof" \
  >"${profile_log}" 2>&1
go tool pprof -top -nodecount=20 \
  "${ARTIFACT_DIR}/ordinary-writers-4.cpu.pprof" \
  >"${ARTIFACT_DIR}/ordinary-writers-4.cpu-top.txt"
write_summary "cpu_profile=ordinary-writers-4.cpu.pprof"
write_summary "memory_profile=ordinary-writers-4.mem.pprof"
write_summary "cpu_top=ordinary-writers-4.cpu-top.txt"

if command -v strace >/dev/null 2>&1; then
  go test -c ./core/storage -o "${ARTIFACT_DIR}/storage.test"
  if strace -f -c -e trace=pwrite64,fsync,fdatasync \
      -o "${ARTIFACT_DIR}/ordinary-writers-4.strace.txt" \
      "${ARTIFACT_DIR}/storage.test" \
        -test.run '^$' \
        -test.bench '^BenchmarkPhase167WALStoreContention/writers_4$' \
        -test.benchtime=1s \
        -test.count=1 \
        >"${ARTIFACT_DIR}/ordinary-writers-4.strace-benchmark.log" 2>&1; then
    write_summary "strace_available=true"
    write_summary "strace_summary=ordinary-writers-4.strace.txt"
    write_summary "strace_scope=qualitative_whole_benchmark_process"
  else
    write_summary "strace_available=false"
    write_summary "strace_error=ordinary-writers-4.strace-benchmark.log"
  fi
else
  write_summary "strace_available=false"
fi

if [[ "${d2_owner_admitted}" == "true" ]]; then
  write_summary "next_recommendation=implement_bounded_existing_format_commit_owner"
else
  write_summary "next_recommendation=stop_before_staged_owner"
fi
write_summary "phase170_walstore_headroom_status=ok"
