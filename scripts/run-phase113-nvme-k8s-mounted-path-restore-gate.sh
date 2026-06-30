#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"

export SW_BLOCK_NVME_PATH_LOSS_PHASE_STATUS_KEY="phase113_nvme_k8s_mounted_path_restore_status"
export SW_BLOCK_NVME_PATH_LOSS_SUMMARY_NAME="phase113-nvme-k8s-mounted-path-restore-summary.txt"
export SW_BLOCK_NVME_MOUNTED_IO="1"
export SW_BLOCK_NVME_RESTORE_PATH="1"
export SW_BLOCK_NVME_MOUNTED_POD="${SW_BLOCK_NVME_MOUNTED_POD:-sw-block-phase113-mounted}"

exec bash "${ROOT}/scripts/run-phase111-nvme-k8s-path-loss-crd-gate.sh" "${ROOT}"
