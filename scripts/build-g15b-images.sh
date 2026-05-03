#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
IMAGE="${G15B_IMAGE:-sw-block:local}"
CSI_IMAGE="${G15B_CSI_IMAGE:-sw-block-csi:local}"

echo "[g15b-build] root=$ROOT"
echo "[g15b-build] image=$IMAGE"
echo "[g15b-build] csi_image=$CSI_IMAGE"

docker build -t "$IMAGE" -f "$ROOT/deploy/k8s/g15b/Dockerfile.sw-block" "$ROOT"
docker build -t "$CSI_IMAGE" -f "$ROOT/deploy/k8s/g15b/Dockerfile.blockcsi" "$ROOT"

if [[ -n "${G15B_KIND_CLUSTER:-}" ]]; then
  if ! command -v kind >/dev/null 2>&1; then
    echo "G15B_KIND_CLUSTER is set but kind is not installed" >&2
    exit 2
  fi
  kind load docker-image --name "$G15B_KIND_CLUSTER" "$IMAGE"
  kind load docker-image --name "$G15B_KIND_CLUSTER" "$CSI_IMAGE"
fi
