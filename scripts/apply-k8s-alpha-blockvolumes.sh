#!/usr/bin/env bash
set -euo pipefail

ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-apply-volumes-$(date -u +%Y%m%dT%H%M%SZ)}"
POLL_LOG="$ARTIFACT_DIR/poll.log"

mkdir -p "$ARTIFACT_DIR"

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

log() {
  printf '[alpha-volumes] %s\n' "$*" | tee -a "$ARTIFACT_DIR/apply.log"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

require_cmd kubectl

log "artifact_dir=$ARTIFACT_DIR"
log "wait for launcher-generated blockvolume manifests"
for _ in $(seq 1 180); do
  if kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c 'ls /manifests/*.yaml >/dev/null 2>&1' >>"$POLL_LOG" 2>&1; then
    break
  fi
  sleep 1
done
if ! kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c 'ls /manifests/*.yaml >/dev/null 2>&1' >>"$POLL_LOG" 2>&1; then
  echo "launcher did not write blockvolume manifests" >&2
  exit 1
fi

kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c 'cat /manifests/*.yaml' >"$ARTIFACT_DIR/generated-blockvolume.yaml"

log "apply generated blockvolume workloads"
kubectl apply -f "$ARTIFACT_DIR/generated-blockvolume.yaml" | tee "$ARTIFACT_DIR/apply-generated-blockvolume.log"
kubectl -n kube-system wait --for=condition=available deploy -l app=sw-blockvolume --timeout=120s

log "PASS: generated blockvolume workloads are available"
log "artifacts=$ARTIFACT_DIR"
