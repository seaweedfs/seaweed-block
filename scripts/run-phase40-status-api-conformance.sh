#!/usr/bin/env bash
set -euo pipefail

normalize_path() {
  local path="$1"
  if [[ "$path" =~ ^([A-Za-z]):/(.*)$ ]]; then
    local drive="${BASH_REMATCH[1],,}"
    if [[ -d "/mnt/$drive" ]]; then
      echo "/mnt/$drive/${BASH_REMATCH[2]}"
    else
      echo "/$drive/${BASH_REMATCH[2]}"
    fi
    return
  fi
  echo "$path"
}

PRODUCT_ROOT="$(normalize_path "${1:-$(pwd)}")"
ARTIFACT_DIR="$(normalize_path "${SW_BLOCK_ARTIFACT_DIR:-"$PRODUCT_ROOT/results/phase40-status-api-conformance-$(date -u +%Y%m%dT%H%M%SZ)"}")"
GO_BIN="${SW_BLOCK_GO_BIN:-go}"
HELM_BIN="${SW_BLOCK_HELM_BIN:-helm}"

go_minor_version() {
  local version
  version="$("$GO_BIN" version 2>/dev/null | awk '{print $3}' | sed -E 's/^go[0-9]+\.([0-9]+).*/\1/')"
  if [[ "$version" =~ ^[0-9]+$ ]]; then
    echo "$version"
  else
    echo 0
  fi
}

if [[ "${SW_BLOCK_GO_BIN:-}" == "" ]] && [[ "$(go_minor_version)" -lt 24 ]] && command -v go.exe >/dev/null 2>&1; then
  GO_BIN="$(command -v go.exe)"
fi

mkdir -p "$ARTIFACT_DIR"

SUMMARY="$ARTIFACT_DIR/phase40-status-api-conformance-summary.txt"
RESULT="$ARTIFACT_DIR/result.json"
LOG="$ARTIFACT_DIR/gate.log"
: > "$LOG"

status="ok"

run_step() {
  local name="$1"
  shift
  local stdout="$ARTIFACT_DIR/$name.stdout.txt"
  local stderr="$ARTIFACT_DIR/$name.stderr.txt"
  echo "[$(date -u +%H:%M:%S)] $name" | tee -a "$LOG"
  if "$@" >"$stdout" 2>"$stderr"; then
    echo "${name}=ok" >> "$SUMMARY"
  else
    echo "${name}=failed" >> "$SUMMARY"
    status="failed"
  fi
}

cd "$PRODUCT_ROOT"

echo "phase40_status_api_conformance_status=running" > "$SUMMARY"

run_step "status_conformance_go_test" "$GO_BIN" test ./core/ops -count=1 -run 'TestPhase40D1KubernetesStatusClientConformsToCRDSchemaAndRBAC|TestPhase40D1StatusConformanceRejectsSchemaAndRBACDrift|TestOperatorStatusReconcilerProjectsDeleteSafetyWithoutFinalizerMutation|TestOperatorStatusReconcilerDeleteSafetyDoesNotContaminateOtherVolumes|TestPhase40D2VolumeStatusClearsStaleDeleteSafety'

run_step "helm_operator_status_render" "$HELM_BIN" template sw-block charts/seaweed-block \
  --namespace kube-system \
  --include-crds \
  --set operatorStatus.create=true \
  --set operatorStatus.dryRun=false

{
  echo "phase40_status_api_conformance_status=$status"
  echo "casing_drift_gate=$status"
  echo "enum_drift_gate=$status"
  echo "wrong_endpoint_gate=$status"
  echo "rbac_boundary_gate=$status"
  echo "delete_safety_status_gate=$status"
  echo "operator_status_mutation_scope=status_events_only"
  echo "finalizer_mutation_allowed=false"
} >> "$SUMMARY"

cat > "$RESULT" <<JSON
{
  "schema_version": "1.0",
  "scenario": "phase40-status-api-conformance",
  "status": "$status",
  "summary": "Phase 40 status API conformance gate $status",
  "artifact_dir": "$ARTIFACT_DIR"
}
JSON

if [[ "$status" != "ok" ]]; then
  exit 1
fi
