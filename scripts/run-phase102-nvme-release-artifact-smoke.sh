#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase102-nvme-release-artifact-smoke-${RUN_ID}}"
SUMMARY="${ARTIFACT_DIR}/phase102-nvme-release-artifact-smoke-summary.txt"
DEFAULT_TAG="sha-$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || true)"
IMAGE="${SW_BLOCK_RELEASE_IMAGE:-${SW_BLOCK_IMAGE:-ghcr.io/seaweedfs/seaweed-block:${DEFAULT_TAG}}}"
CSI_IMAGE="${SW_BLOCK_CSI_RELEASE_IMAGE:-${SW_BLOCK_CSI_IMAGE:-ghcr.io/seaweedfs/seaweed-block-csi:${DEFAULT_TAG}}}"
BIN_DIR="${ARTIFACT_DIR}/image-bin"
IMAGE_ENV="${ARTIFACT_DIR}/release-images.env"

mkdir -p "$ARTIFACT_DIR" "$BIN_DIR"
: >"$SUMMARY"

write_summary() {
  echo "$*" | tee -a "$SUMMARY" >/dev/null
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

run_step() {
  local name="$1"
  shift
  write_summary "${name}=running"
  if "$@" >"${ARTIFACT_DIR}/${name}.stdout.txt" 2>"${ARTIFACT_DIR}/${name}.stderr.txt"; then
    write_summary "${name}=ok"
    return 0
  fi
  write_summary "${name}=failed"
  return 1
}

manifest_exists() {
  local image="$1"
  docker manifest inspect "$image" >"${ARTIFACT_DIR}/manifest-$(printf '%s' "$image" | tr '/:@' '___').json" 2>"${ARTIFACT_DIR}/manifest-$(printf '%s' "$image" | tr '/:@' '___').stderr.txt"
}

extract_release_binaries() {
  local container
  container="$(docker create "$IMAGE")"
  trap 'docker rm -f "$container" >/dev/null 2>&1 || true' RETURN
  docker cp "${container}:/usr/local/bin/blockmaster" "${BIN_DIR}/blockmaster"
  docker cp "${container}:/usr/local/bin/blockvolume" "${BIN_DIR}/blockvolume"
  docker cp "${container}:/usr/local/bin/sw-block" "${BIN_DIR}/sw-block"
  chmod +x "${BIN_DIR}/blockmaster" "${BIN_DIR}/blockvolume" "${BIN_DIR}/sw-block"
}

write_summary "phase102_nvme_release_artifact_status=running"
write_summary "phase102_scope=published_image_nvme_smoke"

if [[ "$IMAGE" == *":sha-" && "$IMAGE" == "ghcr.io/seaweedfs/seaweed-block:sha-"* ]]; then
  write_summary "phase102_nvme_release_artifact_status=blocked_missing_release_images"
  write_summary "reason=git_head_unavailable"
  write_summary "example_SW_BLOCK_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block:sha-<commit>"
  write_summary "example_SW_BLOCK_CSI_RELEASE_IMAGE=ghcr.io/seaweedfs/seaweed-block-csi:sha-<same-commit>"
  exit 2
fi

write_summary "release_image=${IMAGE}"
write_summary "release_csi_image=${CSI_IMAGE}"

cd "$ROOT"

require_cmd docker
if ! manifest_exists "$IMAGE"; then
  write_summary "release_image_manifest=missing"
  write_summary "phase102_nvme_release_artifact_status=blocked_missing_release_images"
  write_summary "missing_image=${IMAGE}"
  exit 2
fi
write_summary "release_image_manifest=present"
if ! manifest_exists "$CSI_IMAGE"; then
  write_summary "release_csi_image_manifest=missing"
  write_summary "phase102_nvme_release_artifact_status=blocked_missing_release_images"
  write_summary "missing_image=${CSI_IMAGE}"
  exit 2
fi
write_summary "release_csi_image_manifest=present"

require_cmd bash
require_cmd go
require_cmd kubectl
require_cmd nvme

run_step "docker_pull_sw_block" docker pull "$IMAGE"
run_step "docker_pull_sw_block_csi" docker pull "$CSI_IMAGE"
run_step "extract_release_binaries" extract_release_binaries

"${BIN_DIR}/sw-block" --version >"${ARTIFACT_DIR}/sw-block.version.txt" 2>&1 || true
"${BIN_DIR}/blockmaster" --version >"${ARTIFACT_DIR}/blockmaster.version.txt" 2>&1 || true
"${BIN_DIR}/blockvolume" --version >"${ARTIFACT_DIR}/blockvolume.version.txt" 2>&1 || true

{
  echo "SW_BLOCK_IMAGE=${IMAGE}"
  echo "SW_BLOCK_CSI_IMAGE=${CSI_IMAGE}"
} >"$IMAGE_ENV"

run_step "go_test_phase102_scope" go test ./scripts ./internal/testops ./core/ops ./core/host/master ./cmd/sw-block ./core/frontend/nvme -count=1

SW_BLOCK_ALPHA_IMAGES_ENV="$IMAGE_ENV" \
SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/phase100-nvme-csi-multipath-live" \
  run_step "phase100_nvme_csi_multipath_live" \
  bash scripts/run-phase100-nvme-csi-multipath-live-gate.sh "$ROOT"

SW_BLOCK_BIN_DIR="$BIN_DIR" \
SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/phase101-nvme-path-failure" \
  run_step "phase101_nvme_path_failure" \
  bash scripts/run-nvme-mounted-failover-smoke.sh "$ROOT"

SW_BLOCK_BIN_DIR="$BIN_DIR" \
SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/phase101-nvme-stage-unstage" \
SW_BLOCK_NVME_STAGE_CYCLES="${SW_BLOCK_NVME_STAGE_CYCLES:-3}" \
  run_step "phase101_nvme_stage_unstage" \
  bash scripts/run-phase101-nvme-stage-unstage-gate.sh "$ROOT"

SW_BLOCK_BIN_DIR="$BIN_DIR" \
SW_BLOCK_ARTIFACT_DIR="${ARTIFACT_DIR}/phase101-nvme-soak" \
SW_BLOCK_NVME_SOAK_ITERATIONS="${SW_BLOCK_NVME_SOAK_ITERATIONS:-5}" \
  run_step "phase101_nvme_bounded_soak" \
  bash scripts/run-nvme-mounted-failover-smoke.sh "$ROOT"

write_summary "phase100_nvme_csi_multipath_live_status=$(grep -E '^phase100_nvme_csi_multipath_live_status=' "${ARTIFACT_DIR}/phase100-nvme-csi-multipath-live/phase100-nvme-csi-multipath-live-summary.txt" | tail -1 | cut -d= -f2-)"
write_summary "phase101_nvme_path_failure_status=$(grep -E '^phase101_nvme_path_failure_status=' "${ARTIFACT_DIR}/phase101-nvme-path-failure/phase101-nvme-path-failure-summary.txt" | tail -1 | cut -d= -f2-)"
write_summary "phase101_nvme_stage_unstage_status=$(grep -E '^phase101_nvme_stage_unstage_status=' "${ARTIFACT_DIR}/phase101-nvme-stage-unstage/phase101-nvme-stage-unstage-summary.txt" | tail -1 | cut -d= -f2-)"
write_summary "phase101_nvme_soak_status=$(grep -E '^phase101_nvme_soak_status=' "${ARTIFACT_DIR}/phase101-nvme-soak/phase101-nvme-soak-summary.txt" | tail -1 | cut -d= -f2-)"
write_summary "phase101_soak_false_ready_count=$(grep -E '^soak_false_ready_count=' "${ARTIFACT_DIR}/phase101-nvme-soak/phase101-nvme-soak-summary.txt" | tail -1 | cut -d= -f2-)"
write_summary "phase101_soak_identity_drift_count=$(grep -E '^soak_identity_drift_count=' "${ARTIFACT_DIR}/phase101-nvme-soak/phase101-nvme-soak-summary.txt" | tail -1 | cut -d= -f2-)"
write_summary "phase102_nvme_release_artifact_status=ok"
