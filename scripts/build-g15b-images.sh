#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
export SW_BLOCK_IMAGE="${SW_BLOCK_IMAGE:-${G15B_IMAGE:-sw-block:local}}"
export SW_BLOCK_CSI_IMAGE="${SW_BLOCK_CSI_IMAGE:-${G15B_CSI_IMAGE:-sw-block-csi:local}}"

if [[ -n "${G15B_KIND_CLUSTER:-}" && -z "${SW_BLOCK_KIND_CLUSTER:-}" ]]; then
  export SW_BLOCK_KIND_CLUSTER="$G15B_KIND_CLUSTER"
fi

exec bash "$ROOT/scripts/build-alpha-images.sh" "$ROOT"
