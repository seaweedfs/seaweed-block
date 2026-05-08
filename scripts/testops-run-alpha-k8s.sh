#!/usr/bin/env bash
set -euo pipefail

REQUEST_JSON="${1:?usage: testops-run-alpha-k8s.sh <run-request.json>}"
WORKLOAD_SCRIPT="${SW_BLOCK_TESTOPS_WORKLOAD_SCRIPT:?SW_BLOCK_TESTOPS_WORKLOAD_SCRIPT is required}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

eval "$(
  python3 - "$REQUEST_JSON" <<'PY'
import json
import shlex
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    req = json.load(f)

print(f"RUN_ID={shlex.quote(req['run_id'])}")
print(f"SCENARIO={shlex.quote(req['scenario'])}")
print(f"SOURCE_COMMIT={shlex.quote(req['source']['commit'])}")
print(f"ARTIFACT_DIR={shlex.quote(req['artifact_dir'])}")
PY
)"

mkdir -p "$ARTIFACT_DIR"
started_at="$(date +%s)"
status="pass"
summary="$SCENARIO completed"
rc=0

set +e
SW_BLOCK_ARTIFACT_DIR="$ARTIFACT_DIR" \
  bash "$ROOT/$WORKLOAD_SCRIPT" "$ROOT" \
  >"$ARTIFACT_DIR/workload.stdout.log" 2>"$ARTIFACT_DIR/workload.stderr.log"
rc=$?
set -e

if [[ "$rc" -ne 0 ]]; then
  status="error"
  summary="$SCENARIO failed; see workload logs"
fi

ended_at="$(date +%s)"
duration="$((ended_at - started_at))"

python3 - "$ARTIFACT_DIR/result.json" "$RUN_ID" "$SCENARIO" "$SOURCE_COMMIT" "$status" "$summary" "$duration" "$ARTIFACT_DIR" "$WORKLOAD_SCRIPT" <<'PY'
import json
import sys

result_path, run_id, scenario, source_commit, status, summary, duration, artifact_dir, workload_script = sys.argv[1:]
res = {
    "schema_version": "1.0",
    "run_id": run_id,
    "scenario": scenario,
    "source_commit": source_commit,
    "status": status,
    "summary": summary,
    "wall_clock_s": float(duration),
    "phase_results": [
        {"name": "workload", "status": status, "duration_s": float(duration)}
    ],
    "artifact_dir": artifact_dir,
    "artifacts": {
        "run_log": f"{artifact_dir}/run.log",
        "alpha_images_env": f"{artifact_dir}/alpha-images.env",
        "generated_blockvolume": f"{artifact_dir}/generated-blockvolume.yaml",
        "lifecycle_volumes": f"{artifact_dir}/lifecycle-volumes.json",
        "pod_log": f"{artifact_dir}/pod.log",
        "workload_stdout": f"{artifact_dir}/workload.stdout.log",
        "workload_stderr": f"{artifact_dir}/workload.stderr.log"
    },
    "non_claims": [
        "This wrapper only normalizes TestOps result output around the existing alpha K8s workload script.",
        f"Workload script: {workload_script}"
    ],
}
with open(result_path, "w", encoding="utf-8") as f:
    json.dump(res, f, indent=2)
    f.write("\n")
PY

exit "$rc"
