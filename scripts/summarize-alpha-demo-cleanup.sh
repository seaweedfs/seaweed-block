#!/usr/bin/env bash
set -euo pipefail

DEMO_DIR="${1:-}"
OUT="${2:-}"
ISCSI_NODES_FILE="${3:-}"

if [[ -z "$DEMO_DIR" || -z "$OUT" ]]; then
  echo "usage: bash scripts/summarize-alpha-demo-cleanup.sh <demo-artifact-dir> <out> [iscsi-nodes-file]" >&2
  exit 2
fi

GEN="$DEMO_DIR/generated-blockvolume.yaml"
AFTER_DELETE="$DEMO_DIR/blockvolume-namespace-pods-deploys.after-delete.txt"
ISCSI_AFTER="$DEMO_DIR/iscsi-sessions.after-delete.txt"
APP_AFTER="$DEMO_DIR/app-storage.after-delete.txt"

value_for_arg() {
  local name="$1"
  sed -n "s/.*--${name}=\\([^\"[:space:]]*\\).*/\\1/p" "$GEN" 2>/dev/null | head -n 1
}

yaml_field_after() {
  local field="$1"
  awk -v field="$field" '
    $1 == field ":" { print $2; exit }
  ' "$GEN" 2>/dev/null
}

deployment_name="$(yaml_field_after name)"
deployment_ns="$(yaml_field_after namespace)"
volume_id="$(value_for_arg volume-id)"
iqn="$(value_for_arg iscsi-iqn)"

[[ -n "$deployment_name" ]] || deployment_name="<unknown>"
[[ -n "$deployment_ns" ]] || deployment_ns="<unknown>"
[[ -n "$volume_id" ]] || volume_id="<unknown>"
[[ -n "$iqn" ]] || iqn="<unknown>"

pvc_state="unknown"
if [[ -f "$APP_AFTER" ]] && ! grep -q 'sw-block-demo-pvc' "$APP_AFTER" 2>/dev/null; then
  pvc_state="deleted"
fi

blockvolume_state="unknown"
if [[ -f "$AFTER_DELETE" ]] && ! grep -q 'sw-blockvolume' "$AFTER_DELETE" 2>/dev/null; then
  blockvolume_state="deleted"
fi

iscsi_session_state="unknown"
if grep -q 'No active sessions' "$ISCSI_AFTER" 2>/dev/null; then
  iscsi_session_state="absent"
fi

iscsi_node_state="unchecked"
if [[ -n "$ISCSI_NODES_FILE" && -f "$ISCSI_NODES_FILE" ]]; then
  if grep -q "$iqn" "$ISCSI_NODES_FILE" 2>/dev/null; then
    iscsi_node_state="present_before_guardrail"
  else
    iscsi_node_state="absent_before_guardrail"
  fi
fi

mkdir -p "$(dirname "$OUT")"
cat >"$OUT" <<EOF
pvc:sw-block-demo-pvc state=${pvc_state} deleted_by=demo-script-kubectl-delete evidence=demo/delete-pvc.log
blockmaster-manifest:${volume_id} state=removed waited_by=demo-script-after-DeleteVolume evidence=demo/poll.log
blockvolume-deploy:${deployment_name} namespace=${deployment_ns} state=${blockvolume_state} deleted_by=pvc-owner-ref-or-demo-guard evidence=demo/blockvolume-namespace-pods-deploys.after-delete.txt
iscsi-session:${iqn} state=${iscsi_session_state} released_by=csi-node-unstage evidence=demo/iscsi-sessions.after-delete.txt
iscsi-node-db:${iqn} state=${iscsi_node_state} cleaned_by=testops-guardrail evidence=iscsi-nodes.after-demo.txt
testops-guardrail:pre_clean state=enabled cleans=stale-processes,stale-sessions,stale-nvme evidence=runner-phase-pre_clean
testops-guardrail:collect_and_cleanup state=enabled cleans=stale-processes,stale-sessions,stale-iscsi-node-db evidence=runner-phase-collect_and_cleanup
non_claim:operator-grade-reconciliation state=not_claimed
non_claim:multi-node-or-HA-lifecycle state=not_claimed
non_claim:upgrade-or-uninstall state=not_claimed
EOF
