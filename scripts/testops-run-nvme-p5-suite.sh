#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
RUN_ID="${SW_BLOCK_RUN_ID:-nvme-p5-suite-$(date -u +%Y%m%dT%H%M%SZ)}"
ARTIFACT_ROOT="${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-testops-${RUN_ID}}"
COMMIT="${SW_BLOCK_TESTOPS_COMMIT:-$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)}"

mkdir -p "$ARTIFACT_ROOT"

log() {
  printf '[testops-suite] %s\n' "$*" | tee -a "$ARTIFACT_ROOT/suite.log"
}

run_scenario() {
  local scenario="$1"
  local step="$2"
  shift 2
  local step_dir="$ARTIFACT_ROOT/$step"
  mkdir -p "$step_dir"

  log "run scenario=$scenario step=$step"
  set +e
  (
    cd "$ROOT"
    go run ./cmd/sw-testops \
      --repo-root "$ROOT" \
      --scenario "$scenario" \
      --commit "$COMMIT" \
      --run-id "${RUN_ID}-${step}" \
      --artifact-dir "$step_dir" \
      "$@"
  ) >"$step_dir/sw-testops.stdout.log" 2>"$step_dir/sw-testops.stderr.log"
  local rc=$?
  set -e
  echo "$rc" >"$step_dir/exit_code.txt"
  if [[ "$rc" -ne 0 ]]; then
    log "FAIL scenario=$scenario step=$step rc=$rc"
    return "$rc"
  fi
  log "PASS scenario=$scenario step=$step"
}

write_result() {
  local status="$1"
  local summary="$2"
  python3 - "$ARTIFACT_ROOT/result.json" "$RUN_ID" "$COMMIT" "$status" "$summary" "$ARTIFACT_ROOT" <<'PY'
import json
import pathlib
import sys

result_path, run_id, commit, status, summary, artifact_root = sys.argv[1:]
root = pathlib.Path(artifact_root)
steps = []
for name in ("pin-build", "nvme-dynamic", "iscsi-regression"):
    step_dir = root / name
    exit_path = step_dir / "exit_code.txt"
    if not exit_path.exists():
        continue
    rc = exit_path.read_text(encoding="utf-8").strip()
    steps.append({
        "name": name,
        "status": "pass" if rc == "0" else "error",
        "artifact_dir": str(step_dir),
    })
res = {
    "schema_version": "1.0",
    "run_id": run_id,
    "scenario": "nvme-p5-csi-suite",
    "source_commit": commit,
    "status": status,
    "summary": summary,
    "phase_results": steps,
    "artifact_dir": artifact_root,
    "artifacts": {
        "suite_log": str(root / "suite.log"),
        "pin_build": str(root / "pin-build"),
        "nvme_dynamic": str(root / "nvme-dynamic"),
        "iscsi_regression": str(root / "iscsi-regression"),
    },
    "non_claims": [
        "Developer-owned suite orchestration around existing TestOps scenarios.",
        "Single-node k3s CSI protocol-selection gate only.",
        "Not an ANA multipath, mounted failover, soak, or performance gate.",
    ],
}
with open(result_path, "w", encoding="utf-8") as f:
    json.dump(res, f, indent=2)
    f.write("\n")
PY
}

log "run_id=$RUN_ID"
log "root=$ROOT"
log "commit=$COMMIT"
log "artifact_root=$ARTIFACT_ROOT"

if ! run_scenario alpha-images-pin-build pin-build; then
  write_result error "pin-build failed"
  exit 1
fi

PIN_ENV="$ARTIFACT_ROOT/pin-build/pin-build/alpha-images.env"
if [[ ! -f "$PIN_ENV" ]]; then
  log "FAIL pin env missing: $PIN_ENV"
  write_result error "pin-build did not produce alpha-images.env"
  exit 1
fi

if ! run_scenario nvme-p5-csi-dynamic nvme-dynamic --param "SW_BLOCK_ALPHA_IMAGES_ENV=$PIN_ENV"; then
  write_result error "nvme dynamic pvc failed"
  exit 1
fi

if ! run_scenario nvme-p5-default-iscsi-regression iscsi-regression --param "SW_BLOCK_ALPHA_IMAGES_ENV=$PIN_ENV"; then
  write_result error "default iscsi regression failed"
  exit 1
fi

write_result pass "nvme p5 csi suite passed"
log "PASS: nvme p5 csi suite"
log "artifacts=$ARTIFACT_ROOT"
