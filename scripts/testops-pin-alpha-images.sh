#!/usr/bin/env bash
set -euo pipefail

REQUEST_JSON="${1:?usage: testops-pin-alpha-images.sh <run-request.json>}"
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
BUILD_DIR="$ARTIFACT_DIR/pin-build"
mkdir -p "$BUILD_DIR"

started_at="$(date +%s)"
status="pass"
summary="alpha images built, recorded, and imported"
rc=0

set +e
SW_BLOCK_ARTIFACT_DIR="$BUILD_DIR" SW_BLOCK_IMPORT_K3S="${SW_BLOCK_IMPORT_K3S:-1}" \
  bash "$ROOT/scripts/build-alpha-images.sh" "$ROOT" \
  >"$BUILD_DIR/build.stdout.log" 2>"$BUILD_DIR/build.stderr.log"
rc=$?
set -e
if [[ "$rc" -ne 0 ]]; then
  status="error"
  summary="alpha image pin-build/import failed"
fi

ended_at="$(date +%s)"
duration="$((ended_at - started_at))"

python3 - "$ARTIFACT_DIR/result.json" "$RUN_ID" "$SCENARIO" "$SOURCE_COMMIT" "$status" "$summary" "$duration" "$ARTIFACT_DIR" "$BUILD_DIR" <<'PY'
import json
import sys

result_path, run_id, scenario, source_commit, status, summary, duration, artifact_dir, build_dir = sys.argv[1:]
res = {
    "schema_version": "1.0",
    "run_id": run_id,
    "scenario": scenario,
    "source_commit": source_commit,
    "status": status,
    "summary": summary,
    "wall_clock_s": float(duration),
    "phase_results": [
        {"name": "pin_build_alpha_images", "status": status, "duration_s": float(duration)}
    ],
    "artifact_dir": artifact_dir,
    "artifacts": {
        "pin_build": build_dir,
        "image_env": f"{build_dir}/alpha-images.env",
        "blockmaster_version": f"{build_dir}/blockmaster.version.txt",
        "blockvolume_version": f"{build_dir}/blockvolume.version.txt",
        "blockcsi_version": f"{build_dir}/blockcsi.version.txt",
        "build_stdout": f"{build_dir}/build.stdout.log",
        "build_stderr": f"{build_dir}/build.stderr.log"
    },
    "non_claims": [
        "Build/import provenance only; no Kubernetes workload or storage data-path claim.",
        "k3s import is controlled by SW_BLOCK_IMPORT_K3S and defaults to enabled for this TestOps scenario."
    ],
}
with open(result_path, "w", encoding="utf-8") as f:
    json.dump(res, f, indent=2)
    f.write("\n")
PY

exit "$rc"
