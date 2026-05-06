#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
WORK_DIR="${SW_BLOCK_BACKEND_MATRIX_WORK_DIR:-/tmp/sw-block-backend-matrix}"
ARTIFACT_ROOT="${SW_BLOCK_ARTIFACT_DIR:-${WORK_DIR}/runs/${RUN_ID}}"
BIN_DIR="${SW_BLOCK_BIN_DIR:-${WORK_DIR}/bin}"
BACKENDS="${SW_BLOCK_BACKEND_MATRIX:-walstore smartwal}"
FIO_RUNTIME="${SW_BLOCK_BACKEND_FIO_RUNTIME:-60}"
FIO_SIZE="${SW_BLOCK_BACKEND_FIO_SIZE:-128m}"
BASE_ISCSI_PORT="${SW_BLOCK_BACKEND_BASE_ISCSI_PORT:-3290}"
BASE_MASTER_PORT="${SW_BLOCK_BACKEND_BASE_MASTER_PORT:-19730}"
BASE_DATA_PORT="${SW_BLOCK_BACKEND_BASE_DATA_PORT:-19740}"

mkdir -p "$ARTIFACT_ROOT" "$BIN_DIR"

log() {
  printf '[backend-matrix] %s\n' "$*" | tee -a "$ARTIFACT_ROOT/run.log"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

require_cmd bash
require_cmd awk
require_cmd grep
require_cmd tee

log "run_id=$RUN_ID"
log "root=$ROOT"
log "artifact_root=$ARTIFACT_ROOT"
log "backends=$BACKENDS"
log "fio_runtime=${FIO_RUNTIME}s fio_size=$FIO_SIZE"

summary="$ARTIFACT_ROOT/summary.md"
cat >"$summary" <<EOF
# iSCSI Backend FIO Matrix

Run ID: \`$RUN_ID\`
Root: \`$ROOT\`
FIO runtime: \`${FIO_RUNTIME}s\`
FIO size: \`$FIO_SIZE\`

This is an experimental comparison harness. It records comparable evidence; it
does not make a product performance claim.

| Backend | Result | Artifact | FIO summary |
|---|---|---|---|
EOF

idx=0
overall=0
for backend in $BACKENDS; do
  case "$backend" in
    walstore|smartwal)
      ;;
    *)
      echo "unsupported backend ${backend}; want walstore or smartwal" >&2
      exit 2
      ;;
  esac

  idx=$((idx + 1))
  iscsi_port=$((BASE_ISCSI_PORT + idx - 1))
  master_port=$((BASE_MASTER_PORT + idx - 1))
  data_port=$((BASE_DATA_PORT + (idx - 1) * 10))
  child_art="$ARTIFACT_ROOT/${backend}"
  child_work="$WORK_DIR/work-${backend}"
  mkdir -p "$child_art" "$child_work"

  log "run backend=${backend} iscsi_port=${iscsi_port}"
  if RUN_ID="${RUN_ID}-${backend}" \
    SW_BLOCK_ARTIFACT_DIR="$child_art" \
    SW_BLOCK_ISCSI_WORK_DIR="$child_work" \
    SW_BLOCK_BIN_DIR="$BIN_DIR" \
    SW_BLOCK_DURABLE_IMPL="$backend" \
    SW_BLOCK_ISCSI_STRESS=fio \
    SW_BLOCK_ISCSI_FIO_RUNTIME="$FIO_RUNTIME" \
    SW_BLOCK_ISCSI_FIO_SIZE="$FIO_SIZE" \
    SW_BLOCK_ISCSI_IQN="iqn.2026-05.io.seaweedfs:p7-${backend}" \
    SW_BLOCK_ISCSI_PORT="$iscsi_port" \
    SW_BLOCK_MASTER_ADDR="127.0.0.1:${master_port}" \
    SW_BLOCK_DATA_ADDR="127.0.0.1:${data_port}" \
    SW_BLOCK_CTRL_ADDR="127.0.0.1:$((data_port + 1))" \
    SW_BLOCK_STATUS_ADDR="127.0.0.1:$((data_port + 2))" \
    bash "$ROOT/scripts/run-iscsi-os-smoke.sh" "$ROOT" >"$child_art/matrix-child.stdout" 2>"$child_art/matrix-child.stderr"; then
    result="PASS"
  else
    result="FAIL"
    overall=1
  fi

  fio_log="$(ls "$child_art"/fio.iter*.log 2>/dev/null | head -n1 || true)"
  if [[ -n "$fio_log" ]]; then
    fio_summary="$(grep -E "^[[:space:]]*(READ:|WRITE:|read:|write:)" "$fio_log" 2>/dev/null | tr '\n' ';' | sed 's/|/\\|/g' || true)"
    if [[ -z "$fio_summary" ]]; then
      fio_summary="see fio log"
    fi
  else
    fio_summary="missing fio log"
  fi
  printf '| `%s` | `%s` | `%s` | %s |\n' "$backend" "$result" "$child_art" "$fio_summary" >>"$summary"
done

log "summary=$summary"
if [[ "$overall" -ne 0 ]]; then
  log "FAIL: at least one backend run failed"
  exit "$overall"
fi

log "PASS: backend fio matrix completed"
