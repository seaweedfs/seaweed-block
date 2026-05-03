#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

exec bash "$ROOT/scripts/run-g15d-k8s-dynamic.sh" "$ROOT"
