#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase60-rebuild-catchup-datapath-gate}"
SUMMARY="${ARTIFACT_DIR}/phase60-rebuild-catchup-datapath-summary.txt"
COMPONENT_LOG="${ARTIFACT_DIR}/component-go-test.log"
TRANSPORT_LOG="${ARTIFACT_DIR}/transport-go-test.log"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

run_go_test() {
  local name="$1"
  local pkg="$2"
  local pattern="$3"
  local log_file="$4"

  if (cd "${PRODUCT_ROOT}" && go test "${pkg}" -run "${pattern}" -count=1 -v) >"${log_file}" 2>&1; then
    write_summary "${name}=pass"
    return 0
  fi

  write_summary "${name}=fail"
  cat "${log_file}" >&2 || true
  return 1
}

require_log() {
  local name="$1"
  local pattern="$2"
  shift 2
  local found="false"
  local log_file
  for log_file in "$@"; do
    if grep -Eq -- "${pattern}" "${log_file}"; then
      found="true"
      break
    fi
  done
  write_summary "${name}=${found}"
  if [ "${found}" != "true" ]; then
    echo "missing evidence ${name}: pattern ${pattern}" >&2
    return 1
  fi
}

require_pass() {
  local name="$1"
  local test_name="$2"
  local log_file="$3"
  require_log "${name}" "^--- PASS: ${test_name}( |$|/)" "${log_file}"
}

write_summary "phase60_rebuild_catchup_datapath_status=running"
write_summary "phase60_scope=component_transport_datapath"
write_summary "kubernetes_executor_triggered=false"
write_summary "frontend_publication_allowed=false"
write_summary "failback_allowed=false"

COMPONENT_PATTERN="TestT4d4_RoundTrip_AssignmentToProbeToCatchUp_EngineDriven|TestDualLane_EngineDrivenRebuild_HappyPath|TestG9C_DualLaneRecoveredReplica_PublishesHealthyOnlyAfterPostCloseDurableAck|TestDualLane_EngineDrivenRebuild_WithPushLiveDuringSession|TestPillar3Slice2_EngineDriven_SameLBAArbitration"
TRANSPORT_PATTERN="TestT4d3_CatchUp_ScansFromReplicaR_NotGenesis|TestTransport_CatchUp_ShipsAndBarrierConfirms"

run_go_test "component_datapath_tests" "./core/replication/component" "${COMPONENT_PATTERN}" "${COMPONENT_LOG}"
run_go_test "transport_catchup_tests" "./core/transport" "${TRANSPORT_PATTERN}" "${TRANSPORT_LOG}"

require_pass "engine_catchup_roundtrip_test" "TestT4d4_RoundTrip_AssignmentToProbeToCatchUp_EngineDriven" "${COMPONENT_LOG}"
require_pass "dual_lane_rebuild_test" "TestDualLane_EngineDrivenRebuild_HappyPath" "${COMPONENT_LOG}"
require_pass "post_close_durable_ack_test" "TestG9C_DualLaneRecoveredReplica_PublishesHealthyOnlyAfterPostCloseDurableAck" "${COMPONENT_LOG}"
require_pass "live_write_during_rebuild_test" "TestDualLane_EngineDrivenRebuild_WithPushLiveDuringSession" "${COMPONENT_LOG}"
require_pass "same_lba_arbitration_test" "TestPillar3Slice2_EngineDriven_SameLBAArbitration" "${COMPONENT_LOG}"
require_pass "catchup_scans_from_replica_r_test" "TestT4d3_CatchUp_ScansFromReplicaR_NotGenesis" "${TRANSPORT_LOG}"
require_pass "catchup_barrier_confirms_test" "TestTransport_CatchUp_ShipsAndBarrierConfirms" "${TRANSPORT_LOG}"

require_log "start_catchup_observed" "command StartCatchUp|cmds=\\[StartCatchUp\\]|catch-up complete" "${COMPONENT_LOG}" "${TRANSPORT_LOG}"
require_log "catchup_session_completed_observed" "catch-up complete replica=" "${TRANSPORT_LOG}"
require_log "start_rebuild_observed" "command StartRebuild|cmds=\\[StartRebuild\\]|rebuild start replica=" "${COMPONENT_LOG}"
require_log "dual_lane_rebuild_observed" "rebuild start replica=.*\\(dual-lane\\)" "${COMPONENT_LOG}"
require_log "session_closed_completed_observed" "SessionClosedCompleted" "${COMPONENT_LOG}"
require_log "durable_ack_observed" "DurableAckObserved|post_close_durable_ack" "${COMPONENT_LOG}"
require_log "barrier_handshake_observed" "barrier handshake .* match=true|frameBarrierResp written" "${COMPONENT_LOG}"
require_log "live_wal_during_rebuild_observed" "walCount=[1-9][0-9]*|head=13|head=15" "${COMPONENT_LOG}"

# The byte-equality assertions are inside the named Go tests. If any equality
# check fails, the corresponding test fails before this point.
write_summary "byte_equal_assertions_passed=true"
write_summary "same_lba_last_write_wins_asserted=true"
write_summary "rebuild_traffic_started=true"
write_summary "catchup_traffic_started=true"
write_summary "authority_executor_datapath_callsite=false"
write_summary "phase60_rebuild_catchup_datapath_status=ok"
