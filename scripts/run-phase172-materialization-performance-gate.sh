#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${TMPDIR:-/tmp}/phase172-materialization-performance-gate}"
SUMMARY="${ARTIFACT_DIR}/phase172-materialization-performance-summary.txt"
BENCHTIME="${SW_BLOCK_PHASE172_BENCHTIME:-1s}"
REPETITIONS="${SW_BLOCK_PHASE172_REPETITIONS:-5}"
MODES=(default candidate)
WORKLOADS=(sequential scattered batch multiblock)
WRITERS=(1 2 4 8)

SOURCE_DIRTY="$(git -C "${ROOT}" status --porcelain)"
if [[ -n "${SOURCE_DIRTY}" ]]; then
  echo "Phase 172 D5 requires an exact clean source worktree" >&2
  exit 1
fi

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
    multiblock) echo "BenchmarkPhase172WALStoreMultiBlockContention" ;;
    *) return 1 ;;
  esac
}

materialization_mode() {
  case "$1" in
    default) echo "default" ;;
    candidate) echo "shared-record" ;;
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

metric_from_test_log() {
  local file="$1"
  local key="$2"
  awk -F= -v key="${key}" '$1 ~ key "$" { print $2; exit }' "${file}"
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

values_csv() {
  paste -sd, "$1"
}

equal_values() {
  awk -v left="$1" -v right="$2" 'BEGIN {
    print (left + 0 == right + 0) ? "true" : "false"
  }'
}

greater_than_zero() {
  awk -v value="$1" 'BEGIN {
    print (value > 0) ? "true" : "false"
  }'
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

if [[ "${REPETITIONS}" != "5" ]]; then
  echo "SW_BLOCK_PHASE172_REPETITIONS must be exactly 5" >&2
  exit 1
fi
if [[ "${BENCHTIME}" != "1s" ]]; then
  echo "SW_BLOCK_PHASE172_BENCHTIME must be exactly 1s" >&2
  exit 1
fi
if ! command -v strace >/dev/null 2>&1; then
  echo "strace is required for the Phase 172 D5 admission gate" >&2
  exit 1
fi

cd "${ROOT}"
write_summary "phase172_materialization_performance_status=running"
write_summary "git_sha=$(git rev-parse HEAD)"
write_summary "git_dirty=false"
write_summary "go_version=$(go version | tr ' ' '_')"
write_summary "kernel=$(uname -srvmo | tr ' ' '_')"
write_summary "benchmark_time=${BENCHTIME}"
write_summary "repetitions=${REPETITIONS}"
write_summary "writers=1,2,4,8"
write_summary "modes=default,shared-record"
write_summary "workloads=sequential_4k,scattered_4k,explicit_16x4k_batch,multiblock_16x4k_opt_in"
write_summary "flusher_interval=100ms"
write_summary "sync_shape=one_final_explicit_sync_then_flusher_drain"
write_summary "admission_ordinary_read_reduction_minimum=45_percent"
write_summary "admission_ordinary_writers_1_floor=0.95x_default"
write_summary "admission_ordinary_writers_4_gain=1.15x_default"
write_summary "admission_candidate_writers_4_range_maximum=1.50x"
write_summary "admission_ordinary_writers_4_p99_maximum=1.10x_default"

go test ./core/storage -run '^TestPhase172BenchmarkMaterializationModes$' \
  -count=20 >"${ARTIFACT_DIR}/benchmark-mode-contract.log" 2>&1
write_summary "benchmark_mode_contract_repeat_20=pass"

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
  "flush_header_reads/validated_record:header_reads_per_validated_record"
  "flush_record_reads/validated_record:record_reads_per_validated_record"
  "flush_materialization_reads/validated_record:materialization_reads_per_validated_record"
  "flush_materialization_reads/entry:materialization_reads_per_entry"
  "flush_materialization_read_bytes/entry:materialization_read_bytes_per_entry"
  "flush_record_reuse_hits/validated_record:record_reuse_hits_per_validated_record"
  "checkpoint_coverage:checkpoint_coverage"
  "flush_cycles_started:cycles_started"
  "flush_cycles_succeeded:cycles_succeeded"
)
failure_metrics=(
  flush_validation_failures
  flush_header_read_failures
  flush_record_read_failures
  extent_write_failures
  extent_sync_failures
  checkpoint_write_failures
  checkpoint_sync_failures
  flush_cycles_failed
)

for mode in "${MODES[@]}"; do
  for workload in "${WORKLOADS[@]}"; do
    for writers in "${WRITERS[@]}"; do
      for pair in "${metric_pairs[@]}"; do
        : >"${ARTIFACT_DIR}/${mode}-${workload}-writers-${writers}-${pair#*:}.values"
      done
    done
  done
done

for repetition in 1 2 3 4 5; do
  case "${repetition}" in
    1) workload_order=(sequential scattered batch multiblock) ;;
    2) workload_order=(multiblock batch scattered sequential) ;;
    3) workload_order=(scattered sequential multiblock batch) ;;
    4) workload_order=(batch multiblock sequential scattered) ;;
    5) workload_order=(sequential batch scattered multiblock) ;;
  esac
  if (( repetition % 2 == 1 )); then
    mode_order=(default candidate)
  else
    mode_order=(candidate default)
  fi
  write_summary "repetition_${repetition}_workload_order=${workload_order[*]}"
  write_summary "repetition_${repetition}_mode_order=${mode_order[*]}"

  for workload in "${workload_order[@]}"; do
    benchmark="$(benchmark_for_workload "${workload}")"
    for mode in "${mode_order[@]}"; do
      benchmark_mode="$(materialization_mode "${mode}")"
      log="${ARTIFACT_DIR}/${repetition}-${workload}-${mode}.log"
      SW_BLOCK_PHASE172_MATERIALIZATION_MODE="${benchmark_mode}" \
        go test ./core/storage -run '^$' \
          -bench "^${benchmark}/writers_(1|2|4|8)$" \
          -benchtime="${BENCHTIME}" -count=1 >"${log}" 2>&1

      for writers in "${WRITERS[@]}"; do
        for pair in "${metric_pairs[@]}"; do
          metric="${pair%%:*}"
          alias="${pair#*:}"
          value="$(metric_from_log "${log}" "${benchmark}" "${writers}" "${metric}")"
          if [[ -z "${value}" ]]; then
            echo "missing mode=${mode} workload=${workload} writers=${writers} metric=${metric}" >&2
            exit 1
          fi
          echo "${value}" >>"${ARTIFACT_DIR}/${mode}-${workload}-writers-${writers}-${alias}.values"
        done

        for metric in "${failure_metrics[@]}"; do
          value="$(metric_from_log "${log}" "${benchmark}" "${writers}" "${metric}")"
          if [[ -z "${value}" || "$(equal_values "${value}" 0)" != "true" ]]; then
            echo "nonzero or missing mode=${mode} workload=${workload} writers=${writers} ${metric}=${value:-missing}" >&2
            exit 1
          fi
        done

        checkpoint="$(metric_from_log "${log}" "${benchmark}" "${writers}" checkpoint_coverage)"
        dirty="$(metric_from_log "${log}" "${benchmark}" "${writers}" dirty_entries)"
        checkpoint_lsn="$(metric_from_log "${log}" "${benchmark}" "${writers}" checkpoint_lsn)"
        head_lsn="$(metric_from_log "${log}" "${benchmark}" "${writers}" head_lsn)"
        sync_calls="$(metric_from_log "${log}" "${benchmark}" "${writers}" explicit_sync_calls)"
        cycles_started="$(metric_from_log "${log}" "${benchmark}" "${writers}" flush_cycles_started)"
        cycles_succeeded="$(metric_from_log "${log}" "${benchmark}" "${writers}" flush_cycles_succeeded)"
        if [[ "$(equal_values "${checkpoint}" 1)" != "true" ||
              "$(equal_values "${dirty}" 0)" != "true" ||
              "$(equal_values "${checkpoint_lsn}" "${head_lsn}")" != "true" ||
              "$(equal_values "${sync_calls}" 1)" != "true" ||
              "$(equal_values "${cycles_started}" "${cycles_succeeded}")" != "true" ]]; then
          echo "incomplete drain mode=${mode} workload=${workload} writers=${writers}" >&2
          exit 1
        fi

        single="$(metric_from_log "${log}" "${benchmark}" "${writers}" single_read_materialization)"
        shared="$(metric_from_log "${log}" "${benchmark}" "${writers}" shared_record_materialization)"
        if [[ "${mode}" == "default" ]]; then
          expected_single=0
          expected_shared=0
        else
          expected_single=1
          expected_shared=1
        fi
        if [[ "$(equal_values "${single}" "${expected_single}")" != "true" ||
              "$(equal_values "${shared}" "${expected_shared}")" != "true" ]]; then
          echo "wrong materialization mode=${mode} workload=${workload} writers=${writers}" >&2
          exit 1
        fi

        multi="$(metric_from_log "${log}" "${benchmark}" "${writers}" multi_block_records)"
        if [[ "${workload}" == "multiblock" ]]; then
          expected_multi=1
        else
          expected_multi=0
        fi
        if [[ "$(equal_values "${multi}" "${expected_multi}")" != "true" ]]; then
          echo "wrong multi-block mode=${mode} workload=${workload} writers=${writers}" >&2
          exit 1
        fi

        reuse="$(metric_from_log "${log}" "${benchmark}" "${writers}" flush_record_reuse_hits/validated_record)"
        if [[ "${mode}" == "candidate" && "${workload}" == "multiblock" ]]; then
          if [[ "$(greater_than_zero "${reuse}")" != "true" ]]; then
            echo "candidate multi-block reuse missing writers=${writers}" >&2
            exit 1
          fi
        elif [[ "$(equal_values "${reuse}" 0)" != "true" ]]; then
          echo "unexpected reuse mode=${mode} workload=${workload} writers=${writers} reuse=${reuse}" >&2
          exit 1
        fi
      done
    done
  done
done

write_summary "all_samples_checkpoint_coverage_complete=true"
write_summary "all_samples_failed_cycles_zero=true"
write_summary "all_samples_materialization_mode_verified=true"

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
  header_reads_per_validated_record
  record_reads_per_validated_record
  materialization_reads_per_validated_record
  materialization_reads_per_entry
  materialization_read_bytes_per_entry
  record_reuse_hits_per_validated_record
)
for mode in "${MODES[@]}"; do
  for workload in "${WORKLOADS[@]}"; do
    for writers in "${WRITERS[@]}"; do
      for alias in "${summary_aliases[@]}"; do
        values="${ARTIFACT_DIR}/${mode}-${workload}-writers-${writers}-${alias}.values"
        write_summary "${mode}_${workload}_writers_${writers}_${alias}_median=$(median_file "${values}")"
      done
    done
  done
done

default_one="$(median_file "${ARTIFACT_DIR}/default-sequential-writers-1-mibps.values")"
candidate_one="$(median_file "${ARTIFACT_DIR}/candidate-sequential-writers-1-mibps.values")"
default_four="$(median_file "${ARTIFACT_DIR}/default-sequential-writers-4-mibps.values")"
candidate_four="$(median_file "${ARTIFACT_DIR}/candidate-sequential-writers-4-mibps.values")"
candidate_four_min="$(minimum_file "${ARTIFACT_DIR}/candidate-sequential-writers-4-mibps.values")"
candidate_four_max="$(maximum_file "${ARTIFACT_DIR}/candidate-sequential-writers-4-mibps.values")"
default_four_p99="$(median_file "${ARTIFACT_DIR}/default-sequential-writers-4-p99_ns.values")"
candidate_four_p99="$(median_file "${ARTIFACT_DIR}/candidate-sequential-writers-4-p99_ns.values")"
default_reads="$(median_file "${ARTIFACT_DIR}/default-sequential-writers-4-materialization_reads_per_validated_record.values")"
candidate_reads="$(median_file "${ARTIFACT_DIR}/candidate-sequential-writers-4-materialization_reads_per_validated_record.values")"
default_multi_reads="$(median_file "${ARTIFACT_DIR}/default-multiblock-writers-4-materialization_reads_per_validated_record.values")"
candidate_multi_reads="$(median_file "${ARTIFACT_DIR}/candidate-multiblock-writers-4-materialization_reads_per_validated_record.values")"
candidate_multi_reuse="$(median_file "${ARTIFACT_DIR}/candidate-multiblock-writers-4-record_reuse_hits_per_validated_record.values")"

one_writer_floor="$(ratio_at_least "${candidate_one}" "${default_one}" 0.95)"
four_writer_gain="$(ratio_at_least "${candidate_four}" "${default_four}" 1.15)"
four_writer_range="$(ratio_at_most "${candidate_four_max}" "${candidate_four_min}" 1.50)"
p99_bounded="$(ratio_at_most "${candidate_four_p99}" "${default_four_p99}" 1.10)"
ordinary_reads_reduced="$(ratio_at_most "${candidate_reads}" "${default_reads}" 0.55)"
multi_reads_reduced="$(ratio_at_most "${candidate_multi_reads}" "${default_multi_reads}" 0.55)"
multi_reuse_present="$(greater_than_zero "${candidate_multi_reuse}")"

write_summary "ordinary_writers_1_candidate_vs_default_ratio=$(ratio_for_summary "${candidate_one}" "${default_one}")"
write_summary "ordinary_writers_1_floor_met=${one_writer_floor}"
write_summary "ordinary_writers_4_candidate_vs_default_ratio=$(ratio_for_summary "${candidate_four}" "${default_four}")"
write_summary "ordinary_writers_4_gain_met=${four_writer_gain}"
write_summary "candidate_ordinary_writers_4_range_ratio=$(ratio_for_summary "${candidate_four_max}" "${candidate_four_min}")"
write_summary "candidate_ordinary_writers_4_range_bounded=${four_writer_range}"
write_summary "ordinary_writers_4_p99_candidate_vs_default_ratio=$(ratio_for_summary "${candidate_four_p99}" "${default_four_p99}")"
write_summary "ordinary_writers_4_p99_bounded=${p99_bounded}"
write_summary "ordinary_materialization_reads_candidate_vs_default_ratio=$(ratio_for_summary "${candidate_reads}" "${default_reads}")"
write_summary "ordinary_materialization_read_reduction_met=${ordinary_reads_reduced}"
write_summary "multiblock_materialization_reads_candidate_vs_default_ratio=$(ratio_for_summary "${candidate_multi_reads}" "${default_multi_reads}")"
write_summary "multiblock_materialization_read_reduction_met=${multi_reads_reduced}"
write_summary "multiblock_candidate_reuse_present=${multi_reuse_present}"

go test -c ./core/storage -o "${ARTIFACT_DIR}/storage.test"
ordinary_strace_match=true
for mode in default candidate; do
  probe_path="${ARTIFACT_DIR}/phase172-${mode}-scoped-probe.store"
  probe_log="${ARTIFACT_DIR}/phase172-${mode}-scoped-probe.log"
  strace_log="${ARTIFACT_DIR}/phase172-${mode}-scoped-probe.strace.txt"
  probe_mode="$(materialization_mode "${mode}")"
  SW_BLOCK_PHASE172_SCOPED_PROBE_PATH="${probe_path}" \
  SW_BLOCK_PHASE172_SCOPED_PROBE_MODE="${probe_mode}" \
    strace -f -c -e trace=pread64 -P "${probe_path}" -o "${strace_log}" \
      "${ARTIFACT_DIR}/storage.test" \
        -test.run '^TestPhase172ScopedMaterializationProbe$' \
        -test.count=1 -test.v >"${probe_log}" 2>&1
  product_reads="$(metric_from_test_log "${probe_log}" phase172_probe_materialization_read_ops)"
  strace_reads="$(awk '$NF == "pread64" { print $4; exit }' "${strace_log}")"
  mode_match=false
  if [[ -n "${product_reads}" && -n "${strace_reads}" &&
        "$(equal_values "${product_reads}" "${strace_reads}")" == "true" ]]; then
    mode_match=true
  else
    ordinary_strace_match=false
  fi
  write_summary "${mode}_scoped_product_materialization_reads=${product_reads:-missing}"
  write_summary "${mode}_scoped_strace_pread64_calls=${strace_reads:-missing}"
  write_summary "${mode}_scoped_strace_matches_product_counter=${mode_match}"
done
write_summary "ordinary_scoped_strace_matches_product_counter=${ordinary_strace_match}"

multiblock_strace_match=true
for mode in default candidate; do
  probe_path="${ARTIFACT_DIR}/phase172-${mode}-shared-probe.store"
  probe_log="${ARTIFACT_DIR}/phase172-${mode}-shared-probe.log"
  strace_log="${ARTIFACT_DIR}/phase172-${mode}-shared-probe.strace.txt"
  probe_mode="$(materialization_mode "${mode}")"
  SW_BLOCK_PHASE172_SHARED_PROBE_PATH="${probe_path}" \
  SW_BLOCK_PHASE172_SHARED_PROBE_MODE="${probe_mode}" \
    strace -f -c -e trace=pread64 -P "${probe_path}" -o "${strace_log}" \
      "${ARTIFACT_DIR}/storage.test" \
        -test.run '^TestPhase172ScopedSharedRecordProbe$' \
        -test.count=1 -test.v >"${probe_log}" 2>&1
  product_reads="$(
    metric_from_test_log "${probe_log}" phase172_shared_probe_materialization_read_ops
  )"
  strace_reads="$(awk '$NF == "pread64" { print $4; exit }' "${strace_log}")"
  mode_match=false
  if [[ -n "${product_reads}" && -n "${strace_reads}" &&
        "$(equal_values "${product_reads}" "${strace_reads}")" == "true" ]]; then
    mode_match=true
  else
    multiblock_strace_match=false
  fi
  write_summary "${mode}_multiblock_scoped_product_materialization_reads=${product_reads:-missing}"
  write_summary "${mode}_multiblock_scoped_strace_pread64_calls=${strace_reads:-missing}"
  write_summary "${mode}_multiblock_scoped_strace_matches_product_counter=${mode_match}"
done
write_summary "multiblock_scoped_strace_matches_product_counter=${multiblock_strace_match}"
scoped_strace_match=false
if [[ "${ordinary_strace_match}" == "true" &&
      "${multiblock_strace_match}" == "true" ]]; then
  scoped_strace_match=true
fi
write_summary "scoped_strace_matches_product_counter=${scoped_strace_match}"

for mode in default candidate; do
  benchmark_mode="$(materialization_mode "${mode}")"
  profile_log="${ARTIFACT_DIR}/${mode}-ordinary-writers-4-profile.log"
  SW_BLOCK_PHASE172_MATERIALIZATION_MODE="${benchmark_mode}" \
    go test ./core/storage -run '^$' \
      -bench '^BenchmarkPhase167WALStoreContention/writers_4$' \
      -benchtime=1s -count=1 \
      -cpuprofile="${ARTIFACT_DIR}/${mode}-ordinary-writers-4.cpu.pprof" \
      -memprofile="${ARTIFACT_DIR}/${mode}-ordinary-writers-4.mem.pprof" \
      >"${profile_log}" 2>&1
  go tool pprof -top -nodecount=20 \
    "${ARTIFACT_DIR}/${mode}-ordinary-writers-4.cpu.pprof" \
    >"${ARTIFACT_DIR}/${mode}-ordinary-writers-4.cpu-top.txt"
done
write_summary "cpu_profiles_generated=true"
write_summary "memory_profiles_generated=true"

candidate_admitted=false
if [[ "${ordinary_reads_reduced}" == "true" &&
      "${scoped_strace_match}" == "true" &&
      "${one_writer_floor}" == "true" &&
      "${four_writer_gain}" == "true" &&
      "${four_writer_range}" == "true" &&
      "${p99_bounded}" == "true" &&
      "${multi_reads_reduced}" == "true" &&
      "${multi_reuse_present}" == "true" ]]; then
  candidate_admitted=true
fi

write_summary "d5_materialization_candidate_admitted=${candidate_admitted}"
if [[ "${candidate_admitted}" == "true" ]]; then
  write_summary "next_recommendation=run_mounted_rf1_rf3_close_gate"
else
  write_summary "next_recommendation=remove_materialization_candidate"
fi
write_summary "phase172_materialization_performance_status=ok"
