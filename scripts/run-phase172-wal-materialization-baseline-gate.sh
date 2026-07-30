#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase172-wal-materialization-baseline-gate}"
SUMMARY="${ARTIFACT_DIR}/phase172-wal-materialization-baseline-summary.txt"
BENCHTIME="${SW_BLOCK_PHASE172_BENCHTIME:-1s}"
REPETITIONS="${SW_BLOCK_PHASE172_REPETITIONS:-5}"
WORKLOADS=(sequential scattered batch multiblock)

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

metric_from_log() {
  local file="$1"
  local benchmark="$2"
  local metric="$3"
  awk -v prefix="${benchmark}/writers_4-" -v metric="${metric}" '
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

equal_values() {
  awk -v left="$1" -v right="$2" 'BEGIN {
    print (left + 0 == right + 0) ? "true" : "false"
  }'
}

value_in_range() {
  awk -v value="$1" -v minimum="$2" -v maximum="$3" 'BEGIN {
    print (value >= minimum && value <= maximum) ? "true" : "false"
  }'
}

value_at_least() {
  awk -v value="$1" -v minimum="$2" 'BEGIN {
    print (value >= minimum) ? "true" : "false"
  }'
}

sum_file() {
  awk '{ total += $1 } END { print total + 0 }' "$1"
}

median_file() {
  sort -n "$1" | awk 'NR == 3 { print; exit }'
}

values_csv() {
  paste -sd, "$1"
}

if [[ "${REPETITIONS}" != "5" ]]; then
  echo "SW_BLOCK_PHASE172_REPETITIONS must be exactly 5" >&2
  exit 1
fi
if [[ "${BENCHTIME}" != "1s" ]]; then
  echo "SW_BLOCK_PHASE172_BENCHTIME must be exactly 1s" >&2
  exit 1
fi

cd "${ROOT}"
write_summary "phase172_wal_materialization_baseline_status=running"
write_summary "git_sha=$(git rev-parse HEAD)"
write_summary "git_dirty=$([[ -n "$(git status --porcelain)" ]] && echo true || echo false)"
write_summary "go_version=$(go version | tr ' ' '_')"
write_summary "kernel=$(uname -srvmo | tr ' ' '_')"
write_summary "benchmark_time=${BENCHTIME}"
write_summary "repetitions=${REPETITIONS}"
write_summary "writers=4"
write_summary "workloads=sequential_4k,scattered_4k,explicit_16x4k_batch,multiblock_16x4k_opt_in"
write_summary "d2_admission=4_of_5_sequential_and_scattered_samples_each_header_and_record_reads_per_validated_record_in_0.95_to_1.05_and_combined_at_least_1.90"
write_summary "strace_scope=exact_store_file_path"

go test ./core/storage -count=1 >"${ARTIFACT_DIR}/storage-regression.log" 2>&1
write_summary "storage_regression=pass"

go test ./core/storage \
  -run 'Test(DirtyMapSnapshotCarriesRecordGeometry|WALStoreDirtyRecordGeometryAppendPaths|WALStoreRecoverReconstructsRecordGeometry|WALStoreRecoverReconstructsLegacyTrimRecordGeometry|WALStoreRecordGeometrySurvivesRingWrap)$' \
  -count=20 >"${ARTIFACT_DIR}/record-geometry-repeat.log" 2>&1
write_summary "record_geometry_repeat_20=pass"
write_summary "legacy_trim_recovery_fixture=pass"
write_summary "ring_wrap_geometry_fixture=pass"

metrics=(
  "MB/s:mibps"
  "p99_ns:p99_ns"
  "flush_snapshot_entries/entry:snapshot_entries_per_entry"
  "flush_unique_wal_records/snapshot_entry:unique_records_per_snapshot_entry"
  "flush_record_reuse_opportunities/snapshot_entry:reuse_opportunities_per_snapshot_entry"
  "flush_validated_records/entry:validated_records_per_entry"
  "flush_header_reads/validated_record:header_reads_per_validated_record"
  "flush_record_reads/validated_record:record_reads_per_validated_record"
  "flush_materialization_reads/validated_record:materialization_reads_per_validated_record"
  "flush_materialization_read_bytes/entry:materialization_read_bytes_per_entry"
  "flush_record_reuse_hits/validated_record:record_reuse_hits_per_validated_record"
  "wal_wraps:wal_wraps"
  "checkpoint_coverage:checkpoint_coverage"
  "flush_cycles_started:cycles_started"
  "flush_cycles_succeeded:cycles_succeeded"
  "multi_block_records:multi_block_records"
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

for workload in "${WORKLOADS[@]}"; do
  for pair in "${metrics[@]}"; do
    : >"${ARTIFACT_DIR}/${workload}-${pair#*:}.values"
  done
  : >"${ARTIFACT_DIR}/${workload}-admission.values"
done

for repetition in 1 2 3 4 5; do
  case "${repetition}" in
    1) order=(sequential scattered batch multiblock) ;;
    2) order=(multiblock batch scattered sequential) ;;
    3) order=(scattered sequential multiblock batch) ;;
    4) order=(batch multiblock sequential scattered) ;;
    5) order=(sequential batch scattered multiblock) ;;
  esac
  write_summary "repetition_${repetition}_order=${order[*]}"

  for workload in "${order[@]}"; do
    benchmark="$(benchmark_for_workload "${workload}")"
    log="${ARTIFACT_DIR}/${repetition}-${workload}.log"
    go test ./core/storage -run '^$' \
      -bench "^${benchmark}/writers_4$" \
      -benchtime="${BENCHTIME}" -count=1 >"${log}" 2>&1

    for pair in "${metrics[@]}"; do
      metric="${pair%%:*}"
      alias="${pair#*:}"
      value="$(metric_from_log "${log}" "${benchmark}" "${metric}")"
      if [[ -z "${value}" ]]; then
        echo "missing workload=${workload} metric=${metric} in ${log}" >&2
        exit 1
      fi
      echo "${value}" >>"${ARTIFACT_DIR}/${workload}-${alias}.values"
    done

    for metric in "${failure_metrics[@]}"; do
      value="$(metric_from_log "${log}" "${benchmark}" "${metric}")"
      if [[ -z "${value}" || "$(equal_values "${value}" 0)" != "true" ]]; then
        echo "nonzero or missing workload=${workload} ${metric}=${value:-missing}" >&2
        exit 1
      fi
    done

    checkpoint="$(metric_from_log "${log}" "${benchmark}" checkpoint_coverage)"
    cycles_started="$(metric_from_log "${log}" "${benchmark}" flush_cycles_started)"
    cycles_succeeded="$(metric_from_log "${log}" "${benchmark}" flush_cycles_succeeded)"
    if [[ "$(equal_values "${checkpoint}" 1)" != "true" ||
          "$(equal_values "${cycles_started}" "${cycles_succeeded}")" != "true" ]]; then
      echo "incomplete checkpoint workload=${workload} repetition=${repetition}" >&2
      exit 1
    fi
    reuse_hits="$(
      metric_from_log "${log}" "${benchmark}" \
        flush_record_reuse_hits/validated_record
    )"
    if [[ "$(equal_values "${reuse_hits}" 0)" != "true" ]]; then
      echo "unexpected materialization reuse workload=${workload} hits=${reuse_hits}" >&2
      exit 1
    fi

    mode="$(metric_from_log "${log}" "${benchmark}" multi_block_records)"
    if [[ "${workload}" == "multiblock" ]]; then
      expected_mode=1
    else
      expected_mode=0
    fi
    if [[ "$(equal_values "${mode}" "${expected_mode}")" != "true" ]]; then
      echo "wrong multi-block mode workload=${workload} got=${mode}" >&2
      exit 1
    fi

    header="$(metric_from_log "${log}" "${benchmark}" flush_header_reads/validated_record)"
    record="$(metric_from_log "${log}" "${benchmark}" flush_record_reads/validated_record)"
    combined="$(metric_from_log "${log}" "${benchmark}" flush_materialization_reads/validated_record)"
    admitted=0
    if [[ "$(value_in_range "${header}" 0.95 1.05)" == "true" &&
          "$(value_in_range "${record}" 0.95 1.05)" == "true" &&
          "$(value_at_least "${combined}" 1.90)" == "true" ]]; then
      admitted=1
    fi
    echo "${admitted}" >>"${ARTIFACT_DIR}/${workload}-admission.values"
  done
done

write_summary "all_samples_checkpoint_coverage_complete=true"
write_summary "all_samples_failed_cycles_zero=true"
for workload in "${WORKLOADS[@]}"; do
  for alias in \
    mibps p99_ns unique_records_per_snapshot_entry \
    reuse_opportunities_per_snapshot_entry header_reads_per_validated_record \
    record_reads_per_validated_record materialization_reads_per_validated_record \
    materialization_read_bytes_per_entry record_reuse_hits_per_validated_record \
    wal_wraps; do
    values="${ARTIFACT_DIR}/${workload}-${alias}.values"
    write_summary "${workload}_${alias}_samples=$(values_csv "${values}")"
    write_summary "${workload}_${alias}_median=$(median_file "${values}")"
  done
done

sequential_pass_count="$(sum_file "${ARTIFACT_DIR}/sequential-admission.values")"
scattered_pass_count="$(sum_file "${ARTIFACT_DIR}/scattered-admission.values")"
sequential_wrap_count="$(
  awk '$1 >= 1 { count++ } END { print count + 0 }' \
    "${ARTIFACT_DIR}/sequential-wal_wraps.values"
)"
write_summary "sequential_duplicate_read_pass_count=${sequential_pass_count}"
write_summary "scattered_duplicate_read_pass_count=${scattered_pass_count}"
write_summary "sequential_wrap_pass_count=${sequential_wrap_count}"

scoped_path="${ARTIFACT_DIR}/phase172-scoped-probe.store"
scoped_log="${ARTIFACT_DIR}/phase172-scoped-probe.log"
strace_log="${ARTIFACT_DIR}/phase172-scoped-probe.strace.txt"
scoped_match=false
if command -v strace >/dev/null 2>&1; then
  go test -c ./core/storage -o "${ARTIFACT_DIR}/storage.test"
  if SW_BLOCK_PHASE172_SCOPED_PROBE_PATH="${scoped_path}" \
    strace -f -c -e trace=pread64 -P "${scoped_path}" -o "${strace_log}" \
      "${ARTIFACT_DIR}/storage.test" \
        -test.run '^TestPhase172ScopedMaterializationProbe$' \
        -test.count=1 -test.v >"${scoped_log}" 2>&1; then
    product_reads="$(
      metric_from_test_log "${scoped_log}" phase172_probe_materialization_read_ops
    )"
    strace_reads="$(awk '$NF == "pread64" { print $4; exit }' "${strace_log}")"
    if [[ -n "${product_reads}" && -n "${strace_reads}" &&
          "$(equal_values "${product_reads}" "${strace_reads}")" == "true" ]]; then
      scoped_match=true
    fi
    write_summary "strace_available=true"
    write_summary "scoped_product_materialization_reads=${product_reads:-missing}"
    write_summary "scoped_strace_pread64_calls=${strace_reads:-missing}"
  else
    write_summary "strace_available=true"
    write_summary "scoped_strace_execution=failed"
  fi
else
  write_summary "strace_available=false"
fi
write_summary "scoped_strace_matches_product_counter=${scoped_match}"

d2_admitted=false
if [[ "${sequential_pass_count}" -ge 4 &&
      "${scattered_pass_count}" -ge 4 &&
      "${sequential_wrap_count}" -ge 4 &&
      "${scoped_match}" == "true" ]]; then
  d2_admitted=true
fi
write_summary "d2_single_read_candidate_admitted=${d2_admitted}"
if [[ "${d2_admitted}" == "true" ]]; then
  write_summary "next_recommendation=implement_disabled_single_read_materialization"
else
  write_summary "next_recommendation=stop_before_single_read_materialization"
fi
write_summary "phase172_wal_materialization_baseline_status=ok"
