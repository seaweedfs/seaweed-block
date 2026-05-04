#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"

export SW_BLOCK_IMAGE="${SW_BLOCK_IMAGE:-ghcr.io/seaweedfs/seaweed-block:alpha}"
export SW_BLOCK_CSI_IMAGE="${SW_BLOCK_CSI_IMAGE:-ghcr.io/seaweedfs/seaweed-block-csi:alpha}"

exec bash "$ROOT/scripts/run-k8s-demo.sh" "$ROOT"
