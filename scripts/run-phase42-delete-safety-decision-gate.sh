#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-"$PRODUCT_ROOT/results/phase42-delete-safety-decision-gate"}"
SUMMARY="$ARTIFACT_DIR/phase42-delete-safety-decision-gate-summary.txt"
GO_BIN="${GO_BIN:-go}"

mkdir -p "$ARTIFACT_DIR"
: >"$SUMMARY"

write_summary() {
  echo "$*" | tee -a "$SUMMARY" >/dev/null
}

write_summary "phase42_delete_safety_decision_status=running"
write_summary "cleanup_execution_attempted=false"

cd "$PRODUCT_ROOT"

TEST_PATTERN='TestEvaluateSwBlockVolumeDeleteSafety|TestObservationBundle_DeleteSafety|TestOperatorStatusReconcilerProjectsDeleteSafetyWithoutFinalizerMutation|TestOperatorStatusReconcilerDeleteSafetyDoesNotContaminateOtherVolumes|TestPhase40D2VolumeStatusClearsStaleDeleteSafety'

if "$GO_BIN" test ./core/ops -count=1 -run "$TEST_PATTERN" >"$ARTIFACT_DIR/go-test-core-ops.log" 2>&1; then
  write_summary "go_test_core_ops=ok"
else
  cat "$ARTIFACT_DIR/go-test-core-ops.log" >&2 || true
  write_summary "go_test_core_ops=failed"
  write_summary "phase42_delete_safety_decision_status=failed"
  exit 1
fi

write_summary "clean_delete_safety_decision=allowed"
write_summary "blocked_delete_safety_decision=rejected"
write_summary "missing_delete_safety_decision=unknown"
write_summary "stale_delete_safety_decision=unknown"
write_summary "lifecycle_owner_action_type=safe_k8s.release_swblockvolume_finalizer"
write_summary "lifecycle_owner_action_mode=dry_run"
write_summary "lifecycle_owner_action_mutation_allowed=false"
write_summary "finalizer_patch_count=0"
write_summary "no_finalizer_mutation_events=true"
write_summary "multi_volume_delete_safety_isolation=true"
write_summary "stale_delete_safety_cleared_when_absent=true"
write_summary "phase42_delete_safety_decision_status=ok"
