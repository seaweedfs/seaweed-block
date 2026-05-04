#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"

export SW_BLOCK_RUN_LABEL="${SW_BLOCK_RUN_LABEL:-alpha-fio}"
export SW_BLOCK_DYNAMIC_PVC_MANIFEST="${SW_BLOCK_DYNAMIC_PVC_MANIFEST:-$ROOT/deploy/k8s/alpha/demo-dynamic-pvc-fio-pod.yaml}"

exec bash "$ROOT/scripts/run-alpha-k8s-dynamic.sh" "$ROOT"
