#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
RUN_ID="${SW_BLOCK_RUN_ID:-protocol-release-gate-$(date -u +%Y%m%dT%H%M%SZ)}"
ARTIFACT_ROOT="${SW_BLOCK_ARTIFACT_DIR:-$ROOT/results/$RUN_ID}"
RESULTS_ROOT="${SW_BLOCK_RESULTS_DIR:-$ARTIFACT_ROOT/runs}"
REMOTE_PRODUCT_ROOT="${SW_BLOCK_REMOTE_PRODUCT_ROOT:-/tmp/seaweed-block-nvme-p4l}"
SSH_KEY="${SW_BLOCK_SSH_KEY:-C:\\work\\dev_server\\testdev_key}"
SWBLOCK_CMD="${SWBLOCK_CMD:-swblock}"
SWBLOCK_RUNNER_ROOT="${SWBLOCK_RUNNER_ROOT:-}"
PRODUCT_COMMIT="$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
RUNNER_COMMIT="unknown"

to_bash_path() {
  local p="$1"
  if command -v cygpath >/dev/null 2>&1 && [[ "$p" =~ ^[A-Za-z]:\\ ]]; then
    cygpath -u "$p"
    return
  fi
  printf '%s\n' "$p"
}

if [[ -n "$SWBLOCK_RUNNER_ROOT" ]]; then
  SWBLOCK_RUNNER_ROOT="$(to_bash_path "$SWBLOCK_RUNNER_ROOT")"
  RUNNER_COMMIT="$(git -C "$SWBLOCK_RUNNER_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
fi

mkdir -p "$ARTIFACT_ROOT" "$RESULTS_ROOT"

log() {
  printf '[protocol-gate] %s\n' "$*" | tee -a "$ARTIFACT_ROOT/suite.log"
}

run_swblock() {
  if [[ -n "$SWBLOCK_RUNNER_ROOT" ]]; then
    (cd "$SWBLOCK_RUNNER_ROOT" && go run ./cmd/swblock "$@")
  else
    "$SWBLOCK_CMD" "$@"
  fi
}

json_escape() {
  python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$1"
}

write_result() {
  local status="$1"
  local summary="$2"
  python3 - "$ARTIFACT_ROOT" "$RUN_ID" "$status" "$summary" "$PRODUCT_COMMIT" "$RUNNER_COMMIT" "$REMOTE_PRODUCT_ROOT" <<'PY'
import json
import pathlib
import sys
from datetime import datetime, timezone

root = pathlib.Path(sys.argv[1])
run_id, status, summary, product_commit, runner_commit, remote_product_root = sys.argv[2:8]
steps = []
for child in root.glob("*/child-run.txt"):
    step_dir = child.parent
    child_run = child.read_text(encoding="utf-8").strip()
    status_path = step_dir / "runs" / child_run / "status.json"
    child_status = "error"
    phases_done = None
    phases_total = None
    if status_path.exists():
        data = json.loads(status_path.read_text(encoding="utf-8"))
        child_status = data.get("state", child_status)
        phases_done = data.get("phases_done")
        phases_total = data.get("phases_total")
    steps.append({
        "name": step_dir.name,
        "status": child_status,
        "run_id": child_run,
        "artifact_dir": str(step_dir),
        "run_dir": str(step_dir / "runs" / child_run),
        "phases_done": phases_done,
        "phases_total": phases_total,
    })
steps.sort(key=lambda s: [
    "iscsi-p6-alua-failover",
    "nvme-p4-multipath-failover",
    "nvme-p5-csi-protocol",
    "iscsi-p8-compat-soak",
].index(s["name"]) if s["name"] in {
    "iscsi-p6-alua-failover",
    "nvme-p4-multipath-failover",
    "nvme-p5-csi-protocol",
    "iscsi-p8-compat-soak",
} else 999)
res = {
    "schema_version": "1.0",
    "run_id": run_id,
    "scenario": "protocol-release-gate-suite",
    "source_commit": product_commit,
    "product_commit": product_commit,
    "runner_commit": runner_commit,
    "remote_product_root": remote_product_root,
    "status": status,
    "summary": summary,
    "phase_results": steps,
    "artifact_dir": str(root),
    "artifacts": {
        "suite_log": str(root / "suite.log"),
        "runs_root": str(root / "runs"),
    },
    "non_claims": [
        "Single-node lab release gate over existing runner-native chains.",
        "Does not claim multi-node Kubernetes, RoCE, long soak, or production HA.",
        "Each child chain owns its own product-level assertions and artifacts.",
    ],
}
(root / "result.json").write_text(json.dumps(res, indent=2) + "\n", encoding="utf-8")
terminal = {"pass", "fail", "cancelled", "error"}
status_doc = {
    "schema_version": 1,
    "run_id": run_id,
    "scenario": "protocol-release-gate-suite",
    "state": "pass" if status == "pass" else "fail",
    "current_phase": "",
    "phases_total": 4,
    "phases_done": sum(1 for s in steps if s["status"] in terminal),
    "phases": steps,
    "product_commit": product_commit,
    "runner_commit": runner_commit,
    "remote_product_root": remote_product_root,
    "updated_at": datetime.now(timezone.utc).isoformat(),
    "artifact_dir": str(root),
    "error_summary": None if status == "pass" else summary,
}
(root / "status.json").write_text(json.dumps(status_doc, indent=2) + "\n", encoding="utf-8")
PY
}

run_chain() {
  local step="$1"
  local scenario="$2"
  local step_dir="$ARTIFACT_ROOT/$step"
  local step_results="$step_dir/runs"
  mkdir -p "$step_dir" "$step_results"

  log "run step=$step scenario=$scenario"
  set +e
  run_swblock run \
    --env "product_root=$REMOTE_PRODUCT_ROOT" \
    --env "ssh_key=$SSH_KEY" \
    --results-dir "$step_results" \
    "$ROOT/$scenario" \
    >"$step_dir/swblock.stdout.log" \
    2>"$step_dir/swblock.stderr.log"
  local rc=$?
  set -e
  printf '%s\n' "$rc" >"$step_dir/exit_code.txt"

  local child_run=""
  if [[ -f "$step_results/latest" ]]; then
    child_run="$(tr -d '\r\n' < "$step_results/latest")"
    printf '%s\n' "$child_run" >"$step_dir/child-run.txt"
  fi

  if [[ "$rc" -ne 0 ]]; then
    log "FAIL step=$step rc=$rc child_run=${child_run:-unknown}"
    write_result fail "release gate failed at $step"
    return "$rc"
  fi
  log "PASS step=$step child_run=${child_run:-unknown}"
}

log "run_id=$RUN_ID"
log "root=$ROOT"
log "artifact_root=$ARTIFACT_ROOT"
log "results_root=$RESULTS_ROOT"
log "remote_product_root=$REMOTE_PRODUCT_ROOT"
log "product_commit=$PRODUCT_COMMIT"
log "runner_commit=$RUNNER_COMMIT"

run_chain "iscsi-p6-alua-failover" "testops/scenarios/iscsi-p6-alua-failover-chain.yaml"
run_chain "nvme-p4-multipath-failover" "testops/scenarios/nvme-p4-multipath-failover-chain.yaml"
run_chain "nvme-p5-csi-protocol" "testops/scenarios/nvme-p5-csi-protocol-chain.yaml"
run_chain "iscsi-p8-compat-soak" "testops/scenarios/iscsi-p8-compat-soak-chain.yaml"

write_result pass "protocol release gate passed"
log "PASS: protocol release gate"
log "artifacts=$ARTIFACT_ROOT"
