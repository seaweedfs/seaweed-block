#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
IMAGE="${SW_BLOCK_IMAGE:-sw-block:local}"
CSI_IMAGE="${SW_BLOCK_CSI_IMAGE:-sw-block-csi:local}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${SW_BLOCK_BUILD_ARTIFACT_DIR:-}}"
IMPORT_K3S="${SW_BLOCK_IMPORT_K3S:-0}"

if [[ -n "$ARTIFACT_DIR" ]]; then
  mkdir -p "$ARTIFACT_DIR"
fi

echo "[alpha-build] root=$ROOT"
echo "[alpha-build] image=$IMAGE"
echo "[alpha-build] csi_image=$CSI_IMAGE"
echo "[alpha-build] import_k3s=$IMPORT_K3S"

docker build -t "$IMAGE" -f "$ROOT/deploy/k8s/g15b/Dockerfile.sw-block" "$ROOT"
docker build -t "$CSI_IMAGE" -f "$ROOT/deploy/k8s/g15b/Dockerfile.blockcsi" "$ROOT"

record_build_evidence() {
  [[ -n "$ARTIFACT_DIR" ]] || return 0

  {
    echo "SW_BLOCK_IMAGE=$IMAGE"
    echo "SW_BLOCK_CSI_IMAGE=$CSI_IMAGE"
    echo "SW_BLOCK_IMAGE_ID=$(docker image inspect "$IMAGE" --format '{{.Id}}')"
    echo "SW_BLOCK_CSI_IMAGE_ID=$(docker image inspect "$CSI_IMAGE" --format '{{.Id}}')"
    if git -C "$ROOT" rev-parse --git-dir >/dev/null 2>&1; then
      echo "GIT_REVISION=$(git -C "$ROOT" rev-parse HEAD)"
      echo "GIT_DIRTY=$(if git -C "$ROOT" diff --quiet --ignore-submodules HEAD --; then echo false; else echo true; fi)"
    else
      echo "GIT_REVISION="
      echo "GIT_DIRTY=unknown"
    fi
  } >"$ARTIFACT_DIR/alpha-images.env"

  docker run --rm "$IMAGE" /usr/local/bin/blockmaster --version >"$ARTIFACT_DIR/blockmaster.version.txt"
  docker run --rm "$IMAGE" /usr/local/bin/blockvolume --version >"$ARTIFACT_DIR/blockvolume.version.txt"
  docker run --rm --entrypoint /usr/local/bin/blockcsi "$CSI_IMAGE" --version >"$ARTIFACT_DIR/blockcsi.version.txt"
}

import_k3s_image() {
  local image="$1"
  local log_name="$2"
  if ! command -v k3s >/dev/null 2>&1 && ! command -v sudo >/dev/null 2>&1; then
    echo "SW_BLOCK_IMPORT_K3S=1 requires k3s or sudo k3s on PATH" >&2
    exit 2
  fi

  local -a ctr=(k3s ctr images import -)
  if command -v sudo >/dev/null 2>&1; then
    ctr=(sudo k3s ctr images import -)
  fi

  echo "[alpha-build] k3s_import image=$image"
  if [[ -n "$ARTIFACT_DIR" ]]; then
    docker save "$image" | "${ctr[@]}" >"$ARTIFACT_DIR/$log_name" 2>&1
  else
    docker save "$image" | "${ctr[@]}"
  fi
}

record_build_evidence

if [[ -n "${SW_BLOCK_KIND_CLUSTER:-}" ]]; then
  if ! command -v kind >/dev/null 2>&1; then
    echo "SW_BLOCK_KIND_CLUSTER is set but kind is not installed" >&2
    exit 2
  fi
  kind load docker-image --name "$SW_BLOCK_KIND_CLUSTER" "$IMAGE"
  kind load docker-image --name "$SW_BLOCK_KIND_CLUSTER" "$CSI_IMAGE"
fi

if [[ "$IMPORT_K3S" == "1" || "$IMPORT_K3S" == "true" ]]; then
  import_k3s_image "$IMAGE" "k3s-import-sw-block.log"
  import_k3s_image "$CSI_IMAGE" "k3s-import-sw-block-csi.log"
fi
