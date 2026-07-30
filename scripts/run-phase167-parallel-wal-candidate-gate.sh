#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase167-parallel-wal-candidate-gate}"
SUMMARY="${ARTIFACT_DIR}/phase167-parallel-wal-candidate-summary.txt"
BENCH="${ARTIFACT_DIR}/parallel-wal-contention-benchmark.txt"
BENCHTIME="${SW_BLOCK_PHASE167_PARALLEL_WAL_BENCHTIME:-1000x}"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

metric() {
  local benchmark="$1"
  local writers="$2"
  local unit="$3"
  awk -v prefix="${benchmark}/writers_${writers}-" -v unit="${unit}" '
    index($1, prefix) == 1 {
      for (i = 2; i <= NF; i++) {
        if ($(i + 1) == unit) {
          print $i
          exit
        }
      }
    }
  ' "${BENCH}"
}

require_metric() {
  local value
  value="$(metric "$1" "$2" "$3")"
  if [[ -z "${value}" ]]; then
    echo "missing metric benchmark=$1 writers=$2 unit=$3" >&2
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
  echo "SW_BLOCK_PHASE167_PARALLEL_WAL_BENCHTIME must be a fixed iteration count" >&2
  exit 1
fi
iterations="${BENCHTIME%x}"
if (( iterations < 512 || iterations > 8192 )); then
  echo "benchmark iterations must be between 512 and 8192" >&2
  exit 1
fi

write_summary "phase167_parallel_wal_candidate_status=running"
write_summary "candidate=parallel-walstore"
write_summary "candidate_default=false"
write_summary "mounted_nvme_claim_allowed=false"
write_summary "performance_claim_allowed=false"

cd "${ROOT}"
go test ./core/storage ./core/storage/parallelwal ./core/recovery ./core/transport \
  ./core/frontend/durable ./core/launcher ./cmd/blockvolume ./cmd/blockmaster \
  -count=1 >"${ARTIFACT_DIR}/affected-tests.log" 2>&1
write_summary "affected_tests=pass"

if command -v helm >/dev/null 2>&1; then
  helm lint charts/seaweed-block \
    --set compat.launcherDurableImplFlag=true \
    --set blockmaster.durableImpl=parallel-walstore \
    >"${ARTIFACT_DIR}/helm-lint.log" 2>&1
  write_summary "helm_validation=lint"
else
  python - <<'PY' >"${ARTIFACT_DIR}/helm-lint.log"
import json

with open("charts/seaweed-block/values.schema.json", encoding="utf-8") as handle:
    schema = json.load(handle)
allowed = schema["properties"]["blockmaster"]["properties"]["durableImpl"]["enum"]
if "parallel-walstore" not in allowed:
    raise SystemExit("parallel-walstore is absent from blockmaster.durableImpl enum")
print("schema enum accepts parallel-walstore; helm binary unavailable")
PY
  write_summary "helm_validation=schema_only_no_helm"
fi
write_summary "helm_candidate_schema=pass"

go test ./core/storage/parallelwal \
  -run 'Test(CreateSyncRecoverAndScan|CrossLaneCompletionPublishesContiguousLSNs|SyncFencesWritesAdmittedBeforeCall|LowerLSNFailureBlocksCompletedHigherLane|CloseDrainsActiveAppenderAfterTerminalFailure|RecoverRejectsActiveAppender|UnsyncedTailIgnoredAfterCrash|OpenFallsBackFromCorruptLatestHeader|CommittedRecordCorruptionFailsClosed|RecoveryRejectsInvalidCommittedRecordSemantics|ApplyEntryAcceptsSourceFrontierJump|FailedApplyEntryDoesNotPublishSourceFrontierJump|SourceFrontierJumpPersistsWithoutFalseCheckpoint|ConcurrentSameLBAWritesRemainOrdered|WriteBatchDispatchesAcrossLanesBeforePublishing|RingWrapRecyclesOnlyCheckpointedPrefix|AdvancedWALTailBeyondHeadSurvivesRecovery|RecoverReplaysDurableWALBeforeCheckpoint|DirectExtentFrontierPersistsWithoutSyntheticWAL|RetainedPreCheckpointWALDoesNotOverrideRebuiltExtent|BaseExtentHeaderFailureKeepsPriorAcknowledgedExtent|BeginBaseInstallClearsAbortedStage|AdvanceFrontierWithoutBaseStageKeepsExistingData|NextBaseStagePreservesHeaderFallbackToCurrentExtent|RecycledSlotsRemainRecoverableThroughHeaderFallback|HeaderValidationRejectsWrappedRecordSize)$' \
  -count=50 >"${ARTIFACT_DIR}/correctness-stress.log" 2>&1
write_summary "correctness_stress=pass"
write_summary "dual_crc_header_fallback=pass"
write_summary "sync_admission_fence=pass"
write_summary "contiguous_completion_frontier=pass"
write_summary "lower_lsn_failure_fail_closed=pass"
write_summary "terminal_append_drain=pass"
write_summary "unsynced_tail_ignored=pass"
write_summary "committed_crc_corruption_fail_closed=pass"
write_summary "ring_wrap_retention=pass"
write_summary "stable_prefix_checkpoint=pass"
write_summary "rebuild_extent_precedence=pass"
write_summary "rebuild_cow_extent_commit=pass"
write_summary "failed_rebuild_keeps_acknowledged_extent=pass"
write_summary "aborted_rebuild_stage_reset=pass"
write_summary "reused_extent_header_fallback=pass"
write_summary "recycled_slot_header_fallback=pass"
write_summary "logical_storage_contract=pass"
write_summary "failed_source_jump_not_published=pass"
write_summary "source_jump_retention_floor=pass"
write_summary "record_semantics_validation=pass"
write_summary "persisted_geometry_overflow_validation=pass"
write_summary "partial_rmw_serialized=pass"

go test ./core/storage/parallelwal \
  -run '^$' \
  -bench '^BenchmarkPhase167(ParallelWALContention|LegacyWALContentionControl)$' \
  -benchtime="${BENCHTIME}" \
  -count=1 >"${BENCH}" 2>&1

for writers in 1 2 4 8; do
  candidate_mibps="$(require_metric BenchmarkPhase167ParallelWALContention "${writers}" MB/s)"
  candidate_p99="$(require_metric BenchmarkPhase167ParallelWALContention "${writers}" p99_ns)"
  active_lanes="$(require_metric BenchmarkPhase167ParallelWALContention "${writers}" active_lanes)"
  checkpoint_write_ops="$(require_metric BenchmarkPhase167ParallelWALContention "${writers}" checkpoint_write_ops)"
  wal_tail="$(require_metric BenchmarkPhase167ParallelWALContention "${writers}" wal_tail)"
  legacy_mibps="$(require_metric BenchmarkPhase167LegacyWALContentionControl "${writers}" MB/s)"
  write_summary "candidate_writers_${writers}_mibps=${candidate_mibps}"
  write_summary "candidate_writers_${writers}_p99_ns=${candidate_p99}"
  write_summary "candidate_writers_${writers}_active_lanes=${active_lanes}"
  write_summary "candidate_writers_${writers}_checkpoint_write_ops=${checkpoint_write_ops}"
  write_summary "candidate_writers_${writers}_wal_tail=${wal_tail}"
  write_summary "legacy_writers_${writers}_mibps=${legacy_mibps}"
done

active_four="$(require_metric BenchmarkPhase167ParallelWALContention 4 active_lanes)"
awk -v value="${active_four}" 'BEGIN { if (value < 2) exit 1 }'
write_summary "multiple_lanes_observed=true"
wal_tail_four="$(require_metric BenchmarkPhase167ParallelWALContention 4 wal_tail)"
awk -v value="${wal_tail_four}" 'BEGIN { if (value <= 1) exit 1 }'
write_summary "steady_state_recycle_observed=true"
checkpoint_ops_four="$(require_metric BenchmarkPhase167ParallelWALContention 4 checkpoint_write_ops)"
awk -v value="${checkpoint_ops_four}" -v writes="${iterations}" 'BEGIN {
  if (value <= 0 || value >= writes / 16) exit 1
}'
write_summary "checkpoint_write_coalescing_observed=true"

candidate_1="$(require_metric BenchmarkPhase167ParallelWALContention 1 MB/s)"
candidate_4="$(require_metric BenchmarkPhase167ParallelWALContention 4 MB/s)"
legacy_1="$(require_metric BenchmarkPhase167LegacyWALContentionControl 1 MB/s)"
legacy_4="$(require_metric BenchmarkPhase167LegacyWALContentionControl 4 MB/s)"
single_ratio="$(ratio "${candidate_1}" "${legacy_1}")"
four_scaling="$(ratio "${candidate_4}" "${candidate_1}")"
four_vs_legacy="$(ratio "${candidate_4}" "${legacy_4}")"
write_summary "candidate_single_writer_vs_legacy_ratio=${single_ratio}"
write_summary "candidate_four_writer_scaling_ratio=${four_scaling}"
write_summary "candidate_four_writer_vs_legacy_ratio=${four_vs_legacy}"

claim_allowed="$(
  awk -v single="${single_ratio}" -v scaling="${four_scaling}" 'BEGIN {
    if (single >= 0.90 && scaling >= 1.50) print "true"; else print "false"
  }'
)"
write_summary "performance_claim_allowed=${claim_allowed}"
if [[ "${claim_allowed}" == "true" ]]; then
  write_summary "next_recommendation=rf3_and_mounted_candidate_gate"
else
  write_summary "next_recommendation=local_execution_backend_redesign"
fi
write_summary "phase167_parallel_wal_candidate_status=ok"
