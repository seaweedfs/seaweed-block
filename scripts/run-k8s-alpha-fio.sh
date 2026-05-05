#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"

export SW_BLOCK_RUN_LABEL="${SW_BLOCK_RUN_LABEL:-alpha-fio}"
export SW_BLOCK_DYNAMIC_PVC_MANIFEST="${SW_BLOCK_DYNAMIC_PVC_MANIFEST:-$ROOT/deploy/k8s/alpha/demo-dynamic-pvc-fio-pod.yaml}"
export SW_BLOCK_LAUNCHER_PVC_OWNER_REF="${SW_BLOCK_LAUNCHER_PVC_OWNER_REF:-1}"

exec bash "$ROOT/scripts/run-alpha-k8s-dynamic.sh" "$ROOT"
