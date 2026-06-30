#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"

export SW_BLOCK_NVME_PATH_LOSS_PHASE_STATUS_KEY="phase112_nvme_k8s_mounted_path_loss_io_status"
export SW_BLOCK_NVME_PATH_LOSS_SUMMARY_NAME="phase112-nvme-k8s-mounted-path-loss-io-summary.txt"
export SW_BLOCK_NVME_MOUNTED_IO="1"

exec bash "${ROOT}/scripts/run-phase111-nvme-k8s-path-loss-crd-gate.sh" "${ROOT}"
