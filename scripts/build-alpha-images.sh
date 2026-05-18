#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
IMAGE="${SW_BLOCK_IMAGE:-sw-block:local}"
CSI_IMAGE="${SW_BLOCK_CSI_IMAGE:-sw-block-csi:local}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${SW_BLOCK_BUILD_ARTIFACT_DIR:-}}"
IMPORT_K3S="${SW_BLOCK_IMPORT_K3S:-0}"
IMPORT_K3S_NODES="${SW_BLOCK_IMPORT_K3S_NODES:-}"
IMPORT_K3S_SSH_USER="${SW_BLOCK_IMPORT_K3S_SSH_USER:-${USER:-}}"
IMPORT_K3S_SSH_KEY="${SW_BLOCK_IMPORT_K3S_SSH_KEY:-}"
IMPORT_K3S_REMOTE_DIR="${SW_BLOCK_IMPORT_K3S_REMOTE_DIR:-/tmp/sw-block-alpha-images}"

if [[ -n "$ARTIFACT_DIR" ]]; then
  mkdir -p "$ARTIFACT_DIR"
fi

echo "[alpha-build] root=$ROOT"
echo "[alpha-build] image=$IMAGE"
echo "[alpha-build] csi_image=$CSI_IMAGE"
echo "[alpha-build] import_k3s=$IMPORT_K3S"
echo "[alpha-build] import_k3s_nodes=${IMPORT_K3S_NODES:-<local>}"

docker build -t "$IMAGE" -f "$ROOT/deploy/k8s/g15b/Dockerfile.sw-block" "$ROOT"
docker build -t "$CSI_IMAGE" -f "$ROOT/deploy/k8s/g15b/Dockerfile.blockcsi" "$ROOT"

record_build_evidence() {
  [[ -n "$ARTIFACT_DIR" ]] || return 0

  {
    echo "SW_BLOCK_IMAGE=$IMAGE"
    echo "SW_BLOCK_CSI_IMAGE=$CSI_IMAGE"
    echo "SW_BLOCK_IMPORT_K3S=$IMPORT_K3S"
    echo "SW_BLOCK_IMPORT_K3S_NODES=$IMPORT_K3S_NODES"
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
  local -a ctr=()
  if command -v sudo >/dev/null 2>&1 && sudo -n k3s --version >/dev/null 2>&1; then
    ctr=(sudo -n k3s ctr -n k8s.io images import -)
  elif command -v k3s >/dev/null 2>&1 && k3s --version >/dev/null 2>&1; then
    ctr=(k3s ctr -n k8s.io images import -)
  else
    echo "SW_BLOCK_IMPORT_K3S=1 requires k3s or passwordless sudo k3s on PATH" >&2
    exit 2
  fi

  echo "[alpha-build] k3s_import image=$image"
  if [[ -n "$ARTIFACT_DIR" ]]; then
    docker save "$image" | "${ctr[@]}" >"$ARTIFACT_DIR/$log_name" 2>&1
  else
    docker save "$image" | "${ctr[@]}"
  fi
}

local_k3s_available() {
  if command -v sudo >/dev/null 2>&1 && sudo -n k3s --version >/dev/null 2>&1; then
    return 0
  fi
  command -v k3s >/dev/null 2>&1 && k3s --version >/dev/null 2>&1
}

verify_local_k3s_image() {
  local image="$1"
  local log_name="$2"
  local -a ctr=()
  if command -v sudo >/dev/null 2>&1 && sudo -n k3s --version >/dev/null 2>&1; then
    ctr=(sudo -n k3s ctr -n k8s.io images ls -q)
  elif command -v k3s >/dev/null 2>&1 && k3s --version >/dev/null 2>&1; then
    ctr=(k3s ctr -n k8s.io images ls -q)
  else
    echo "SW_BLOCK_IMPORT_K3S=1 requires k3s or passwordless sudo k3s on PATH" >&2
    exit 2
  fi

  echo "[alpha-build] k3s_verify image=$image node=local"
  if [[ -n "$ARTIFACT_DIR" ]]; then
    "${ctr[@]}" >"$ARTIFACT_DIR/$log_name" 2>&1
    awk -v image="$image" '$0 == image || $0 ~ "/" image "$" { found=1 } END { exit !found }' "$ARTIFACT_DIR/$log_name"
  else
    "${ctr[@]}" | awk -v image="$image" '$0 == image || $0 ~ "/" image "$" { found=1 } END { exit !found }'
  fi
}

ssh_target_for_node() {
  local node="$1"
  if [[ -n "$IMPORT_K3S_SSH_USER" ]]; then
    printf '%s@%s' "$IMPORT_K3S_SSH_USER" "$node"
  else
    printf '%s' "$node"
  fi
}

run_ssh() {
  local target="$1"
  shift
  local -a opts=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
  if [[ -n "$IMPORT_K3S_SSH_KEY" ]]; then
    opts=(-i "$IMPORT_K3S_SSH_KEY" "${opts[@]}")
  fi
  ssh "${opts[@]}" "$target" "$@"
}

import_k3s_image_to_node() {
  local image="$1"
  local node="$2"
  local safe_image
  local tar_name
  local target
  local log_name
  safe_image="$(printf '%s' "$image" | tr '/:' '__')"
  tar_name="${safe_image}.tar"
  target="$(ssh_target_for_node "$node")"
  log_name="k3s-import-${safe_image}-${node}.log"

  echo "[alpha-build] k3s_import image=$image node=$node"
  if [[ -n "$ARTIFACT_DIR" ]]; then
    {
      run_ssh "$target" "mkdir -p '$IMPORT_K3S_REMOTE_DIR'"
      docker save "$image" | run_ssh "$target" "cat > '$IMPORT_K3S_REMOTE_DIR/$tar_name'"
      run_ssh "$target" "sudo -n k3s ctr -n k8s.io images import '$IMPORT_K3S_REMOTE_DIR/$tar_name'"
      run_ssh "$target" "sudo -n k3s ctr -n k8s.io images ls -q | awk -v image='$image' '\$0 == image || \$0 ~ \"/\" image \"\$\" { found=1 } END { exit !found }'"
      run_ssh "$target" "rm -f '$IMPORT_K3S_REMOTE_DIR/$tar_name'"
    } >"$ARTIFACT_DIR/$log_name" 2>&1
  else
    run_ssh "$target" "mkdir -p '$IMPORT_K3S_REMOTE_DIR'"
    docker save "$image" | run_ssh "$target" "cat > '$IMPORT_K3S_REMOTE_DIR/$tar_name'"
    run_ssh "$target" "sudo -n k3s ctr -n k8s.io images import '$IMPORT_K3S_REMOTE_DIR/$tar_name'"
    run_ssh "$target" "sudo -n k3s ctr -n k8s.io images ls -q | awk -v image='$image' '\$0 == image || \$0 ~ \"/\" image \"\$\" { found=1 } END { exit !found }'"
    run_ssh "$target" "rm -f '$IMPORT_K3S_REMOTE_DIR/$tar_name'"
  fi
}

import_k3s_images_to_nodes() {
  local nodes_csv="$1"
  local node
  IFS=',' read -r -a nodes <<<"$nodes_csv"
  for node in "${nodes[@]}"; do
    node="${node//[[:space:]]/}"
    [[ -n "$node" ]] || continue
    import_k3s_image_to_node "$IMAGE" "$node"
    import_k3s_image_to_node "$CSI_IMAGE" "$node"
  done
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
  if [[ -n "$IMPORT_K3S_NODES" ]]; then
    if local_k3s_available; then
      import_k3s_image "$IMAGE" "k3s-import-local-sw-block.log"
      verify_local_k3s_image "$IMAGE" "k3s-images-local-sw-block.txt"
      import_k3s_image "$CSI_IMAGE" "k3s-import-local-sw-block-csi.log"
      verify_local_k3s_image "$CSI_IMAGE" "k3s-images-local-sw-block-csi.txt"
    fi
    import_k3s_images_to_nodes "$IMPORT_K3S_NODES"
  else
    import_k3s_image "$IMAGE" "k3s-import-sw-block.log"
    verify_local_k3s_image "$IMAGE" "k3s-images-sw-block.txt"
    import_k3s_image "$CSI_IMAGE" "k3s-import-sw-block-csi.log"
    verify_local_k3s_image "$CSI_IMAGE" "k3s-images-sw-block-csi.txt"
  fi
fi
