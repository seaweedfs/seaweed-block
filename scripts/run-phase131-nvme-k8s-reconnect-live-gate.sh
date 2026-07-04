#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"

export SW_BLOCK_NVME_PATH_LOSS_PHASE_STATUS_KEY="phase131_nvme_k8s_reconnect_live_status"
export SW_BLOCK_NVME_PATH_LOSS_SUMMARY_NAME="phase131-nvme-k8s-reconnect-live-summary.txt"
export SW_BLOCK_NVME_MOUNTED_IO="1"
export SW_BLOCK_NVME_MOUNTED_POD="${SW_BLOCK_NVME_MOUNTED_POD:-sw-block-phase131-mounted}"
export SW_BLOCK_NVME_RECONNECT_OWNER="1"
export SW_BLOCK_NVME_RECONNECT_INTERVAL="${SW_BLOCK_NVME_RECONNECT_INTERVAL:-5s}"
export SW_BLOCK_NVME_HOST_PATH_DISCONNECT="1"
export SW_BLOCK_NVME_FORCE_STAGE2_MULTIPATH="1"

exec bash "${ROOT}/scripts/run-phase111-nvme-k8s-path-loss-crd-gate.sh" "${ROOT}"
