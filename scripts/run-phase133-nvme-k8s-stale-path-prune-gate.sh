#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"

export SW_BLOCK_NVME_PATH_LOSS_PHASE_STATUS_KEY="phase133_nvme_k8s_stale_path_prune_status"
export SW_BLOCK_NVME_PATH_LOSS_SUMMARY_NAME="phase133-nvme-k8s-stale-path-prune-summary.txt"
export SW_BLOCK_NVME_MOUNTED_IO="1"
export SW_BLOCK_NVME_MOUNTED_POD="${SW_BLOCK_NVME_MOUNTED_POD:-sw-block-phase133-mounted}"
export SW_BLOCK_NVME_RECONNECT_OWNER="1"
export SW_BLOCK_NVME_RECONNECT_INTERVAL="${SW_BLOCK_NVME_RECONNECT_INTERVAL:-5s}"
export SW_BLOCK_NVME_FORCE_STAGE2_MULTIPATH="1"
export SW_BLOCK_NVME_DESIRED_PATH_CHANGE="1"
export SW_BLOCK_NVME_REQUIRE_STALE_PATH_PRUNE="1"

exec bash "${ROOT}/scripts/run-phase111-nvme-k8s-path-loss-crd-gate.sh" "${ROOT}"
