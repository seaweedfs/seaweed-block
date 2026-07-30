#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase169-segment-pre-admission-performance-gate}"
SUMMARY="${ARTIFACT_DIR}/phase169-segment-pre-admission-performance-summary.txt"
BENCHTIME="${SW_BLOCK_PHASE169_BENCHTIME:-1s}"
REPETITIONS="${SW_BLOCK_PHASE169_REPETITIONS:-5}"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

benchmark_for_mode() {
  case "$1" in
    segmented) echo "BenchmarkPhase169SegmentedWALContention" ;;
    positioned) echo "BenchmarkPhase167ParallelWALContention" ;;
    legacy) echo "BenchmarkPhase167LegacyWALContentionControl" ;;
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

ratio_for_summary() {
  awk -v numerator="$1" -v denominator="$2" 'BEGIN {
    if (numerator <= 0 || denominator <= 0) exit 1
    printf "%.3f", numerator / denominator
  }'
}

at_least_value() {
  awk -v value="$1" -v minimum="$2" 'BEGIN {
    print (value >= minimum) ? "true" : "false"
  }'
}

ratio_at_least() {
  awk -v numerator="$1" -v denominator="$2" -v minimum="$3" 'BEGIN {
    if (numerator <= 0 || denominator <= 0) exit 1
    print (numerator / denominator >= minimum) ? "true" : "false"
  }'
}

if [[ "${REPETITIONS}" != "5" ]]; then
  echo "SW_BLOCK_PHASE169_REPETITIONS must be exactly 5" >&2
  exit 1
fi
if [[ "${BENCHTIME}" != "1s" ]]; then
  echo "SW_BLOCK_PHASE169_BENCHTIME must be exactly 1s" >&2
  exit 1
fi

write_summary "phase169_segment_pre_admission_performance_status=running"
write_summary "benchmark_time=${BENCHTIME}"
write_summary "repetitions=${REPETITIONS}"
write_summary "writers=1,4"
write_summary "sync_cadence=one_final_sync_per_sample"
write_summary "comparison_scope=optimistic_wal_append_core_upper_bound"
write_summary "legacy_background_flush_enabled=false"
write_summary "positioned_checkpoint_recycle_expected=false"

cd "${ROOT}"
for mode in segmented positioned legacy; do
  : >"${ARTIFACT_DIR}/${mode}-writers-1-mibps.values"
  : >"${ARTIFACT_DIR}/${mode}-writers-4-mibps.values"
done
: >"${ARTIFACT_DIR}/segmented-writers-4-entries-per-segment.values"

for repetition in 1 2 3 4 5; do
  case $((repetition % 3)) in
    1) order=(segmented positioned legacy) ;;
    2) order=(positioned legacy segmented) ;;
    0) order=(legacy segmented positioned) ;;
  esac
  write_summary "repetition_${repetition}_order=${order[*]}"
  for mode in "${order[@]}"; do
    benchmark="$(benchmark_for_mode "${mode}")"
    log="${ARTIFACT_DIR}/${repetition}-${mode}.log"
    go test ./core/storage/parallelwal -run '^$' \
      -bench "^${benchmark}/writers_(1|4)$" \
      -benchtime="${BENCHTIME}" -count=1 >"${log}" 2>&1
    for writers in 1 4; do
      mibps="$(metric_from_log "${log}" "${benchmark}" "${writers}" "MB/s")"
      sync_calls="$(metric_from_log "${log}" "${benchmark}" "${writers}" "sync_calls")"
      if [[ -z "${mibps}" ]]; then
        echo "missing ${mode} writers=${writers} MB/s in ${log}" >&2
        exit 1
      fi
      if [[ "${sync_calls}" != "1.000" ]]; then
        echo "invalid ${mode} writers=${writers} logical Sync count in ${log}" >&2
        exit 1
      fi
      echo "${mibps}" >>"${ARTIFACT_DIR}/${mode}-writers-${writers}-mibps.values"
    done
    if [[ "${mode}" == "segmented" ]]; then
      grouping="$(metric_from_log "${log}" "${benchmark}" 4 "entries/segment")"
      if [[ -z "${grouping}" ]]; then
        echo "missing segmented grouping metric in ${log}" >&2
        exit 1
      fi
      echo "${grouping}" >>"${ARTIFACT_DIR}/segmented-writers-4-entries-per-segment.values"
    fi
  done
done

segmented_1="$(median_file "${ARTIFACT_DIR}/segmented-writers-1-mibps.values")"
segmented_4="$(median_file "${ARTIFACT_DIR}/segmented-writers-4-mibps.values")"
positioned_4="$(median_file "${ARTIFACT_DIR}/positioned-writers-4-mibps.values")"
positioned_1="$(median_file "${ARTIFACT_DIR}/positioned-writers-1-mibps.values")"
legacy_1="$(median_file "${ARTIFACT_DIR}/legacy-writers-1-mibps.values")"
legacy_4="$(median_file "${ARTIFACT_DIR}/legacy-writers-4-mibps.values")"
grouping_4="$(median_file "${ARTIFACT_DIR}/segmented-writers-4-entries-per-segment.values")"

single_ratio="$(ratio_for_summary "${segmented_1}" "${legacy_1}")"
scaling_ratio="$(ratio_for_summary "${segmented_4}" "${segmented_1}")"
positioned_ratio="$(ratio_for_summary "${segmented_4}" "${positioned_4}")"
legacy_four_ratio="$(ratio_for_summary "${segmented_4}" "${legacy_4}")"
single_pass="$(ratio_at_least "${segmented_1}" "${legacy_1}" "0.900")"
scaling_pass="$(ratio_at_least "${segmented_4}" "${segmented_1}" "1.500")"
positioned_pass="$(ratio_at_least "${segmented_4}" "${positioned_4}" "1.000")"
absolute_positioned_pass="$(ratio_at_least "${segmented_4}" "${positioned_4}" "1.250")"
absolute_legacy_pass="$(ratio_at_least "${segmented_4}" "${legacy_4}" "1.250")"
grouping_pass="$(at_least_value "${grouping_4}" "1.001")"
absolute_gain_pass=false
if [[ "${absolute_positioned_pass}" == "true" && "${absolute_legacy_pass}" == "true" ]]; then
  absolute_gain_pass=true
fi
concurrency_gain_pass=false
if [[ "${scaling_pass}" == "true" || "${absolute_gain_pass}" == "true" ]]; then
  concurrency_gain_pass=true
fi

write_summary "segmented_writers_1_mibps_median=${segmented_1}"
write_summary "segmented_writers_4_mibps_median=${segmented_4}"
write_summary "positioned_writers_1_mibps_median=${positioned_1}"
write_summary "positioned_writers_4_mibps_median=${positioned_4}"
write_summary "legacy_writers_1_mibps_median=${legacy_1}"
write_summary "legacy_writers_4_mibps_median=${legacy_4}"
write_summary "segmented_writers_4_entries_per_segment_median=${grouping_4}"
write_summary "segmented_single_vs_legacy_ratio=${single_ratio}"
write_summary "segmented_four_writer_scaling_ratio=${scaling_ratio}"
write_summary "segmented_four_vs_positioned_ratio=${positioned_ratio}"
write_summary "segmented_four_vs_legacy_ratio=${legacy_four_ratio}"
write_summary "single_writer_threshold_pass=${single_pass}"
write_summary "four_writer_scaling_threshold_pass=${scaling_pass}"
write_summary "absolute_four_writer_gain_threshold_pass=${absolute_gain_pass}"
write_summary "concurrency_gain_threshold_pass=${concurrency_gain_pass}"
write_summary "positioned_threshold_pass=${positioned_pass}"
write_summary "grouping_threshold_pass=${grouping_pass}"

if [[ "${single_pass}" == "true" && "${concurrency_gain_pass}" == "true" &&
      "${positioned_pass}" == "true" && "${grouping_pass}" == "true" ]]; then
  write_summary "d4_full_engine_admitted=true"
  write_summary "next_recommendation=implement_checkpoint_rebuild_equivalence"
else
  write_summary "d4_full_engine_admitted=false"
  write_summary "next_recommendation=stop_before_full_engine_integration"
fi
write_summary "phase169_segment_pre_admission_performance_status=ok"
