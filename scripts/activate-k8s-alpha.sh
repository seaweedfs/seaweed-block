#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-activation-$(date -u +%Y%m%dT%H%M%SZ)}"
IMAGE_MODE="${SW_BLOCK_ACTIVATION_IMAGE_MODE:-local}"
case "$IMAGE_MODE" in
  local|published)
    ;;
  *)
    echo "SW_BLOCK_ACTIVATION_IMAGE_MODE must be local or published; got: $IMAGE_MODE" >&2
    exit 2
    ;;
esac
if [[ "$IMAGE_MODE" == "published" ]]; then
  IMAGE="${SW_BLOCK_IMAGE:-ghcr.io/seaweedfs/seaweed-block:alpha}"
  CSI_IMAGE="${SW_BLOCK_CSI_IMAGE:-ghcr.io/seaweedfs/seaweed-block-csi:alpha}"
  IMPORT_K3S="${SW_BLOCK_ACTIVATION_IMPORT_K3S:-${SW_BLOCK_IMPORT_K3S:-0}}"
else
  IMAGE="${SW_BLOCK_IMAGE:-sw-block:local}"
  CSI_IMAGE="${SW_BLOCK_CSI_IMAGE:-sw-block-csi:local}"
  IMPORT_K3S="${SW_BLOCK_ACTIVATION_IMPORT_K3S:-${SW_BLOCK_IMPORT_K3S:-1}}"
fi
STORAGECLASS_NAME="${SW_BLOCK_STORAGECLASS_NAME:-sw-block-dynamic}"
REPLICATION_ACK="${SW_BLOCK_ALPHA_REPLICATION_ACK:-best-effort}"
FRONTEND_PROTOCOL="${SW_BLOCK_FRONTEND_PROTOCOL:-iscsi}"

mkdir -p "$ARTIFACT_DIR"

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

log() {
  printf '[alpha-activate] %s\n' "$*" | tee -a "$ARTIFACT_DIR/activate.log"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    write_summary "failed" "missing-command-$1"
    echo "missing required command: $1" >&2
    exit 2
  fi
}

safe_kubectl() {
  kubectl "$@" 2>/dev/null || true
}

image_digest() {
  local image="$1"
  if command -v docker >/dev/null 2>&1; then
    docker manifest inspect "$image" 2>/dev/null \
      | sed -n 's/.*"digest": "\(sha256:[^"]*\)".*/\1/p' \
      | head -1
  fi
}

collect_failure_diagnostics() {
  local phase="$1"
  mkdir -p "$ARTIFACT_DIR/diagnostics"
  {
    echo "failed_phase=$phase"
    echo "image_mode=$IMAGE_MODE"
    echo "image=$IMAGE"
    echo "image_digest=$(image_digest "$IMAGE")"
    echo "csi_image=$CSI_IMAGE"
    echo "csi_image_digest=$(image_digest "$CSI_IMAGE")"
  } >"$ARTIFACT_DIR/diagnostics/failure-context.txt"
  kubectl -n kube-system get pods,deploy,ds,events -o wide >"$ARTIFACT_DIR/diagnostics/kube-system.txt" 2>&1 || true
  kubectl -n kube-system describe deploy sw-blockmaster >"$ARTIFACT_DIR/diagnostics/sw-blockmaster.describe.txt" 2>&1 || true
  kubectl -n kube-system logs deploy/sw-blockmaster --all-containers --tail=200 >"$ARTIFACT_DIR/diagnostics/sw-blockmaster.log" 2>&1 || true
  kubectl -n kube-system logs deploy/sw-blockmaster --all-containers --previous --tail=200 >"$ARTIFACT_DIR/diagnostics/sw-blockmaster.previous.log" 2>&1 || true
  kubectl -n kube-system describe deploy sw-block-csi-controller >"$ARTIFACT_DIR/diagnostics/sw-block-csi-controller.describe.txt" 2>&1 || true
  kubectl -n kube-system logs deploy/sw-block-csi-controller --all-containers --tail=200 >"$ARTIFACT_DIR/diagnostics/sw-block-csi-controller.log" 2>&1 || true
  kubectl -n kube-system describe ds sw-block-csi-node >"$ARTIFACT_DIR/diagnostics/sw-block-csi-node.describe.txt" 2>&1 || true
  if grep -R "flag provided but not defined" "$ARTIFACT_DIR/diagnostics" >/dev/null 2>&1; then
    {
      echo "activation_blocker=image_flag_mismatch"
      echo "reason=published image does not support a flag required by this source tree"
      echo "remediation=republish the image from this commit or use matching sha-<commit> image tags"
    } >>"$ARTIFACT_DIR/diagnostics/failure-context.txt"
  fi
}

summary_value() {
  local value="$1"
  if [[ -n "$value" ]]; then
    printf '%s' "$value"
  else
    printf 'unknown'
  fi
}

write_summary() {
  local status="$1"
  local failed_phase="${2:-}"
  local master_ready csi_controller_ready csi_node_ready storageclass_provider nodes_ready
  master_ready="$(safe_kubectl -n kube-system get deploy sw-blockmaster -o jsonpath='{.status.availableReplicas}')"
  csi_controller_ready="$(safe_kubectl -n kube-system get deploy sw-block-csi-controller -o jsonpath='{.status.availableReplicas}')"
  csi_node_ready="$(safe_kubectl -n kube-system get ds sw-block-csi-node -o jsonpath='{.status.numberReady}/{.status.desiredNumberScheduled}')"
  storageclass_provider="$(safe_kubectl get sc "$STORAGECLASS_NAME" -o jsonpath='{.provisioner}')"
  nodes_ready="$(safe_kubectl get nodes --no-headers | awk '$2 ~ /Ready/ {ready++} END {print ready+0}')"
  {
    echo "activation_status=$status"
    if [[ -n "$failed_phase" ]]; then
      echo "failed_phase=$failed_phase"
    fi
    echo "artifact_dir=$ARTIFACT_DIR"
    echo "root=$ROOT"
    echo "image_mode=$IMAGE_MODE"
    echo "image=$IMAGE"
    echo "image_digest=$(summary_value "$(image_digest "$IMAGE")")"
    echo "csi_image=$CSI_IMAGE"
    echo "csi_image_digest=$(summary_value "$(image_digest "$CSI_IMAGE")")"
    echo "protocol=$FRONTEND_PROTOCOL"
    echo "ack_profile=$REPLICATION_ACK"
    echo "ready_kubernetes_nodes=$(summary_value "$nodes_ready")"
    echo "master_ready_replicas=$(summary_value "$master_ready")"
    echo "csi_controller_ready_replicas=$(summary_value "$csi_controller_ready")"
    echo "csi_node_ready=$(summary_value "$csi_node_ready")"
    echo "storageclass=$STORAGECLASS_NAME"
    echo "storageclass_provider=$(summary_value "$storageclass_provider")"
    echo "next_create_volume=kubectl apply -f examples/kubernetes/basic-app/storageclass-pvc.yaml"
    echo "next_status=kubectl -n kube-system port-forward deploy/sw-blockmaster 9333:9333 && sw-block ops cluster --master-api 127.0.0.1:9333 -o json"
    echo "next_inventory=sw-block ops inventory --namespace default --master 127.0.0.1:9333 --out /tmp/sw-block-inventory"
    echo "non_claims=alpha_only,no_backup_restore,no_upgrade_safety,no_mutating_dashboard_actions,no_broad_performance_slo"
  } >"$ARTIFACT_DIR/activation-summary.txt"
}

run_phase() {
  local name="$1"
  shift
  local log_file="$ARTIFACT_DIR/${name}.log"
  log "phase_start=$name"
  set +e
  "$@" >"$log_file" 2>&1
  local rc=$?
  set -e
  cat "$log_file"
  if [[ "$rc" -ne 0 ]]; then
    log "phase_fail=$name exit=$rc"
    collect_failure_diagnostics "$name"
    write_summary "failed" "$name"
    exit "$rc"
  fi
  log "phase_pass=$name"
}

require_cmd bash
require_cmd kubectl
require_cmd docker

log "artifact_dir=$ARTIFACT_DIR"
log "root=$ROOT"
log "image_mode=$IMAGE_MODE"
log "image=$IMAGE"
log "csi_image=$CSI_IMAGE"
log "import_k3s=$IMPORT_K3S"
log "storageclass=$STORAGECLASS_NAME"
log "replication_ack=$REPLICATION_ACK"

if [[ "$IMAGE_MODE" == "published" ]]; then
  run_phase preflight bash "$ROOT/scripts/preflight-k8s-alpha.sh" --ghcr
else
  run_phase preflight bash "$ROOT/scripts/preflight-k8s-alpha.sh" --local-k3s
fi

if [[ "$IMAGE_MODE" == "local" ]]; then
  BUILD_DIR="$ARTIFACT_DIR/build"
  mkdir -p "$BUILD_DIR"
  run_phase build env \
    SW_BLOCK_ARTIFACT_DIR="$BUILD_DIR" \
    SW_BLOCK_IMAGE="$IMAGE" \
    SW_BLOCK_CSI_IMAGE="$CSI_IMAGE" \
    SW_BLOCK_IMPORT_K3S="$IMPORT_K3S" \
    SW_BLOCK_IMPORT_K3S_NODES="${SW_BLOCK_IMPORT_K3S_NODES:-}" \
    SW_BLOCK_IMPORT_K3S_SSH_USER="${SW_BLOCK_IMPORT_K3S_SSH_USER:-}" \
    SW_BLOCK_IMPORT_K3S_SSH_KEY="${SW_BLOCK_IMPORT_K3S_SSH_KEY:-}" \
    bash "$ROOT/scripts/build-alpha-images.sh" "$ROOT"

  if [[ -f "$BUILD_DIR/alpha-images.env" ]]; then
    set -a
    # shellcheck disable=SC1091
    . "$BUILD_DIR/alpha-images.env"
    set +a
    IMAGE="${SW_BLOCK_IMAGE:-$IMAGE}"
    CSI_IMAGE="${SW_BLOCK_CSI_IMAGE:-$CSI_IMAGE}"
  fi
else
  {
    echo "SW_BLOCK_IMAGE=$IMAGE"
    echo "SW_BLOCK_CSI_IMAGE=$CSI_IMAGE"
    echo "SW_BLOCK_ACTIVATION_IMAGE_MODE=published"
    echo "SW_BLOCK_IMPORT_K3S=$IMPORT_K3S"
  } >"$ARTIFACT_DIR/published-images.env"
  log "phase_skip=build reason=published-images image=$IMAGE csi_image=$CSI_IMAGE"
fi

INSTALL_DIR="$ARTIFACT_DIR/install"
mkdir -p "$INSTALL_DIR"
run_phase install env \
  SW_BLOCK_ARTIFACT_DIR="$INSTALL_DIR" \
  SW_BLOCK_IMAGE="$IMAGE" \
  SW_BLOCK_CSI_IMAGE="$CSI_IMAGE" \
  SW_BLOCK_ALPHA_REPLICATION_ACK="$REPLICATION_ACK" \
  bash "$ROOT/scripts/install-k8s-alpha.sh" "$ROOT"

STORAGECLASS_RENDERED="$ARTIFACT_DIR/storageclass.rendered.yaml"
sed "s/name: sw-block-dynamic/name: ${STORAGECLASS_NAME}/" \
  "$ROOT/deploy/k8s/alpha/storageclass.yaml" >"$STORAGECLASS_RENDERED"
run_phase storageclass kubectl apply -f "$STORAGECLASS_RENDERED"

{
  kubectl get nodes -o wide
  kubectl -n kube-system get deploy sw-blockmaster sw-block-csi-controller -o wide
  kubectl -n kube-system get ds sw-block-csi-node -o wide
  kubectl get sc "$STORAGECLASS_NAME" -o wide
} >"$ARTIFACT_DIR/readiness.txt" 2>&1 || true

write_summary "ok"
cat "$ARTIFACT_DIR/activation-summary.txt"
log "PASS: seaweed-block alpha activation complete"
