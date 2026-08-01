#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase167-ordered-async-replication-gate}"
SUMMARY="${ARTIFACT_DIR}/phase167-ordered-async-replication-summary.txt"
CONTENT_DIR="${ARTIFACT_DIR}/contention"
mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

write_summary "phase167_ordered_async_replication_status=running"
write_summary "scope=local_component_and_real_tcp_rf3"
write_summary "mounted_nvme_claim_allowed=false"

cd "${ROOT}"
go test ./core/replication \
  -run 'Test(WaitFor|PeerWorkQueue|ReplicationVolume_(ObserveBatchSyncQuorumDoesNotWaitForSlowNonQuorumPeer|SyncQuorumDoesNotWaitForSlowNonQuorumBarrier|SyncAllWaitsForSlowBarrier|QueueSaturationIsTypedAndCounted|HealthyRecoveryReplacesTerminalPeerQueue|RecoveryQueueResetCannotLoseConcurrentWrite|LineageReplacementWaitsForOldQueue))' \
  -count=25 \
  >"${ARTIFACT_DIR}/ordered-queue-tests.log" 2>&1
write_summary "ordered_queue_tests=pass"

go test ./core/frontend/durable \
  -run '^TestStorageBackend_(StrictAckUsesBatchObserver|PartialBatchObservesOnlyCommittedPrefix)$' \
  -count=25 \
  >"${ARTIFACT_DIR}/batch-observer-tests.log" 2>&1
write_summary "strict_batch_observer_tests=pass"

SW_BLOCK_ARTIFACT_DIR="${CONTENT_DIR}" \
  "${ROOT}/scripts/run-phase167-parallel-write-engine-local-baseline-gate.sh"

BASELINE_SUMMARY="${CONTENT_DIR}/phase167-parallel-write-engine-local-baseline-summary.txt"
grep -q '^phase167_parallel_write_engine_local_baseline_status=ok$' "${BASELINE_SUMMARY}"
for writers in 1 2 4 8; do
  grep -q "^rf3_writers_${writers}_queue_saturated=0$" "${BASELINE_SUMMARY}"
done

write_summary "real_tcp_rf3_contention_gate=pass"
write_summary "normal_path_queue_saturation=0"
write_summary "slow_non_quorum_progress=pass"
write_summary "sync_all_waits_all_peers=pass"
write_summary "queue_saturation_fail_closed=pass"
write_summary "terminal_queue_error_precedence=pass"
write_summary "healthy_recovery_queue_reset=pass"
write_summary "recovery_queue_reset_race=pass"
write_summary "lineage_replacement_drain=pass"
write_summary "batch_observer_path=pass"
write_summary "partial_batch_prefix_observed=pass"
write_summary "next_recommendation=parallel_local_wal_candidate"
write_summary "phase167_ordered_async_replication_status=ok"
