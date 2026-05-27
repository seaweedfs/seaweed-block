#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
SOURCE_DIR="${SW_BLOCK_SUPPORT_BUNDLE_SOURCE:-${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-support-bundle}}"
ARTIFACT_DIR="${SW_BLOCK_SUPPORT_BUNDLE_OUT:-$SOURCE_DIR/support-bundle}"
NAMESPACE="${SW_BLOCK_APP_NAMESPACE:-default}"
HELM_RELEASE="${SW_BLOCK_HELM_RELEASE:-sw-block}"
HELM_NAMESPACE="${SW_BLOCK_HELM_NAMESPACE:-kube-system}"
VOLUME_ID="${SW_BLOCK_VOLUME_ID:-}"

mkdir -p "$ARTIFACT_DIR"/{helm,k8s,logs,iscsi,replayed-report}
SUPPORT_BUNDLE_CAPTURE_FAILED=0

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

sw_block_cmd() {
  if [[ -n "${SW_BLOCK_CLI:-}" ]]; then
    "$SW_BLOCK_CLI" "$@"
  elif command -v sw-block >/dev/null 2>&1; then
    sw-block "$@"
  elif [[ -x ./sw-block ]]; then
    ./sw-block "$@"
  elif [[ -x "$ROOT/sw-block" ]]; then
    "$ROOT/sw-block" "$@"
  else
    go run ./cmd/sw-block "$@"
  fi
}

capture() {
  local out="$1"
  shift
  if ! "$@" >"$out" 2>&1; then
    SUPPORT_BUNDLE_CAPTURE_FAILED=1
    {
      echo
      echo "[support-bundle] command failed: $*"
    } >>"$out"
    return 1
  fi
}

summary_value() {
  local value="$1"
  if [[ -n "$value" ]]; then
    printf '%s' "$value"
  else
    printf 'unknown'
  fi
}

if [[ -z "$VOLUME_ID" && -f "$SOURCE_DIR/first-volume-summary.txt" ]]; then
  VOLUME_ID="$(sed -n 's/^volume_id=//p' "$SOURCE_DIR/first-volume-summary.txt" | head -1)"
fi
if [[ -z "$VOLUME_ID" && -f "$SOURCE_DIR/multi-volume-summary.txt" ]]; then
  VOLUME_ID="$(sed -n 's/^volume_id=//p' "$SOURCE_DIR/multi-volume-summary.txt" | head -1)"
fi
if [[ -z "$VOLUME_ID" ]]; then
  VOLUME_ID="$(find "$SOURCE_DIR" -name volume-inventory-summary.txt -print -quit 2>/dev/null \
    | xargs -r sed -n 's/^volume:.* id=\([^ ]*\).*/\1/p' \
    | head -1 || true)"
fi

capture "$ARTIFACT_DIR/helm/status.txt" helm status "$HELM_RELEASE" --namespace "$HELM_NAMESPACE" || true
capture "$ARTIFACT_DIR/helm/values.txt" helm get values "$HELM_RELEASE" --namespace "$HELM_NAMESPACE" --all || true
capture "$ARTIFACT_DIR/helm/manifest.yaml" helm get manifest "$HELM_RELEASE" --namespace "$HELM_NAMESPACE" || true

capture "$ARTIFACT_DIR/k8s/nodes.txt" kubectl get nodes -o wide || true
capture "$ARTIFACT_DIR/k8s/pods.txt" kubectl get pods -A -o wide || true
capture "$ARTIFACT_DIR/k8s/pv.txt" kubectl get pv -o wide || true
capture "$ARTIFACT_DIR/k8s/pvc.txt" kubectl get pvc -A -o wide || true
capture "$ARTIFACT_DIR/k8s/events.txt" kubectl get events -A --sort-by=.lastTimestamp || true
capture "$ARTIFACT_DIR/k8s/volumeattachments.txt" kubectl get volumeattachments -o wide || true

capture "$ARTIFACT_DIR/logs/blockmaster.log" kubectl -n "$HELM_NAMESPACE" logs deploy/sw-blockmaster --all-containers --tail=300 || true
capture "$ARTIFACT_DIR/logs/csi-controller.log" kubectl -n "$HELM_NAMESPACE" logs deploy/sw-block-csi-controller --all-containers --tail=300 || true
capture "$ARTIFACT_DIR/logs/csi-node.log" kubectl -n "$HELM_NAMESPACE" logs ds/sw-block-csi-node --all-containers --tail=300 || true
capture "$ARTIFACT_DIR/logs/blockvolume.log" kubectl -n "$NAMESPACE" logs -l app=sw-blockvolume --all-containers --tail=300 || true

capture "$ARTIFACT_DIR/iscsi/sessions.txt" sudo -n iscsiadm -m session || true
capture "$ARTIFACT_DIR/iscsi/nodes.txt" sudo -n iscsiadm -m node || true

report_status=ok
explain_status=ok
timeline_status=ok

if ! sw_block_cmd ops report --from-bundle "$SOURCE_DIR" --out "$ARTIFACT_DIR/replayed-report" \
  >"$ARTIFACT_DIR/replayed-report.stdout.txt" 2>"$ARTIFACT_DIR/replayed-report.stderr.txt"; then
  report_status=failed
fi

if [[ -n "$VOLUME_ID" && "$VOLUME_ID" != "unknown" ]]; then
  if ! sw_block_cmd ops explain volume --from-bundle "$SOURCE_DIR" "$VOLUME_ID" \
    >"$ARTIFACT_DIR/explain.txt" 2>"$ARTIFACT_DIR/explain.stderr.txt"; then
    explain_status=failed
  fi
  if ! sw_block_cmd ops timeline volume --from-bundle "$SOURCE_DIR" "$VOLUME_ID" -o jsonl \
    >"$ARTIFACT_DIR/timeline.replayed.jsonl" 2>"$ARTIFACT_DIR/timeline.stderr.txt"; then
    timeline_status=failed
  fi
else
  explain_status=failed
  timeline_status=failed
  echo "volume id unavailable" >"$ARTIFACT_DIR/explain.stderr.txt"
  echo "volume id unavailable" >"$ARTIFACT_DIR/timeline.stderr.txt"
fi

support_bundle_status=ok
for required in \
  "$ARTIFACT_DIR/replayed-report/index.html" \
  "$ARTIFACT_DIR/replayed-report/cluster-evidence.json" \
  "$ARTIFACT_DIR/replayed-report/timeline.jsonl" \
  "$ARTIFACT_DIR/replayed-report/summary.txt" \
  "$ARTIFACT_DIR/explain.txt" \
  "$ARTIFACT_DIR/k8s/pods.txt" \
  "$ARTIFACT_DIR/helm/status.txt"; do
  if [[ ! -s "$required" ]]; then
    support_bundle_status=failed
  fi
done
if [[ "$SUPPORT_BUNDLE_CAPTURE_FAILED" != "0" || "$report_status" != "ok" || "$explain_status" != "ok" || "$timeline_status" != "ok" ]]; then
  support_bundle_status=failed
fi

{
  echo "support_bundle_status=$support_bundle_status"
  echo "source_bundle=$SOURCE_DIR"
  echo "volume_id=$(summary_value "$VOLUME_ID")"
  echo "helm_release=$HELM_RELEASE"
  echo "helm_namespace=$HELM_NAMESPACE"
  echo "report_status=$report_status"
  echo "explain_status=$explain_status"
  echo "timeline_status=$timeline_status"
  echo "capture_status=$([[ "$SUPPORT_BUNDLE_CAPTURE_FAILED" == "0" ]] && echo ok || echo failed)"
  echo "read_only=true"
  echo "report=replayed-report/index.html"
  echo "cluster_evidence=replayed-report/cluster-evidence.json"
  echo "timeline=replayed-report/timeline.jsonl"
  echo "explain=explain.txt"
} >"$ARTIFACT_DIR/support-bundle-summary.txt"

cat "$ARTIFACT_DIR/support-bundle-summary.txt"
test "$support_bundle_status" = "ok"
