#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
IMAGE="${SW_BLOCK_IMAGE:-sw-block:local}"
CSI_IMAGE="${SW_BLOCK_CSI_IMAGE:-sw-block-csi:local}"

echo "[alpha-build] root=$ROOT"
echo "[alpha-build] image=$IMAGE"
echo "[alpha-build] csi_image=$CSI_IMAGE"

docker build -t "$IMAGE" -f "$ROOT/deploy/k8s/g15b/Dockerfile.sw-block" "$ROOT"
docker build -t "$CSI_IMAGE" -f "$ROOT/deploy/k8s/g15b/Dockerfile.blockcsi" "$ROOT"

if [[ -n "${SW_BLOCK_KIND_CLUSTER:-}" ]]; then
  if ! command -v kind >/dev/null 2>&1; then
    echo "SW_BLOCK_KIND_CLUSTER is set but kind is not installed" >&2
    exit 2
  fi
  kind load docker-image --name "$SW_BLOCK_KIND_CLUSTER" "$IMAGE"
  kind load docker-image --name "$SW_BLOCK_KIND_CLUSTER" "$CSI_IMAGE"
fi

