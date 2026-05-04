#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"

export SW_BLOCK_RUN_LABEL="${SW_BLOCK_RUN_LABEL:-alpha}"
export G15D_ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${G15D_ARTIFACT_DIR:-}}"
if [[ -z "$G15D_ARTIFACT_DIR" ]]; then
  unset G15D_ARTIFACT_DIR
fi

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

exec bash "$ROOT/scripts/run-g15d-k8s-dynamic.sh" "$ROOT"

