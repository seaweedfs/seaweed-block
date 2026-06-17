#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-"$PRODUCT_ROOT/results/phase41-lifecycle-owner-api-boundary-$(date -u +%Y%m%dT%H%M%SZ)"}"

mkdir -p "$ARTIFACT_DIR"
SUMMARY="$ARTIFACT_DIR/phase41-lifecycle-owner-api-boundary-summary.txt"
echo "phase41_lifecycle_owner_api_boundary_status=running" > "$SUMMARY"

cd "$PRODUCT_ROOT"
TEST_PATTERN='TestPhase40D1KubernetesStatusClientConformsToCRDSchemaAndRBAC|TestPhase40D1StatusConformanceRejectsSchemaAndRBACDrift|TestPhase41D2LifecycleOwnerFinalizerBoundary'
GO_BIN="${GO_BIN:-go}"

if "$GO_BIN" test ./core/ops -count=1 -run "$TEST_PATTERN" > "$ARTIFACT_DIR/go-test-core-ops.log" 2>&1; then
  {
    echo "go_test_core_ops=ok"
    echo "operator_status_main_patch_allowed=false"
    echo "lifecycle_owner_finalizer_patch_allowed=true"
    echo "lifecycle_owner_spec_patch_allowed=false"
    echo "lifecycle_owner_unrelated_metadata_patch_allowed=false"
    echo "finalizers_endpoint_allowed=false"
    echo "phase41_lifecycle_owner_api_boundary_status=ok"
  } >> "$SUMMARY"
else
  cat "$ARTIFACT_DIR/go-test-core-ops.log"
  {
    echo "go_test_core_ops=failed"
    echo "phase41_lifecycle_owner_api_boundary_status=failed"
  } >> "$SUMMARY"
  exit 1
fi
