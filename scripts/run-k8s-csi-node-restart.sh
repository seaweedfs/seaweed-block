#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"

export SW_BLOCK_RUN_LABEL="${SW_BLOCK_RUN_LABEL:-csi-node-restart}"
export SW_BLOCK_LAUNCHER_PVC_OWNER_REF="${SW_BLOCK_LAUNCHER_PVC_OWNER_REF:-1}"
export SW_BLOCK_RESTART_CSI_NODE_BEFORE_READER=1
export SW_BLOCK_DEMO_APP_MANIFEST="${SW_BLOCK_DEMO_APP_MANIFEST:-$ROOT/deploy/k8s/alpha/demo-app-pvc-writer-hold.yaml}"

exec bash "$ROOT/scripts/run-alpha-app-demo.sh" "$ROOT"
