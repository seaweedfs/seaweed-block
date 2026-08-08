#!/usr/bin/env bash
set -euo pipefail

REQ="${1:?run-request.json path required}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

read_req_field() {
  local field="$1"
  python3 - "$REQ" "$field" <<'PY'
import json
import sys

path, field = sys.argv[1], sys.argv[2]
with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)
value = data
for part in field.split("."):
    value = value.get(part, "")
    if value == "":
        break
print(value)
PY
}

ARTIFACT_DIR="$(read_req_field artifact_dir)"
RUN_ID="$(read_req_field run_id)"
SOURCE_COMMIT="$(read_req_field source.commit)"
mkdir -p "$ARTIFACT_DIR"

status="pass"
summary="alpha-large Kubernetes smoke passed"
if ! G15D_ARTIFACT_DIR="$ARTIFACT_DIR" SW_BLOCK_RUN_LABEL="alpha-large" bash "$ROOT/scripts/run-k8s-alpha-large.sh" "$ROOT"; then
  status="fail"
  summary="alpha-large Kubernetes smoke failed"
fi

python3 - "$ARTIFACT_DIR/result.json" "$RUN_ID" "$SOURCE_COMMIT" "$status" "$summary" "$ARTIFACT_DIR" <<'PY'
import json
import sys

path, run_id, commit, status, summary, artifact_dir = sys.argv[1:]
with open(path, "w", encoding="utf-8") as f:
    json.dump(
        {
            "schema_version": "1.0",
            "run_id": run_id,
            "scenario": "alpha-k8s-large",
            "source_commit": commit,
            "status": status,
            "summary": summary,
            "artifact_dir": artifact_dir,
        },
        f,
        indent=2,
    )
    f.write("\n")
PY

if [[ "$status" != "pass" ]]; then
  exit 1
fi
