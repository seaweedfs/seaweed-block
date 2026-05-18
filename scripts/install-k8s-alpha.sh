#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-install-$(date -u +%Y%m%dT%H%M%SZ)}"
IMAGE="${SW_BLOCK_IMAGE:-ghcr.io/seaweedfs/seaweed-block:alpha}"
CSI_IMAGE="${SW_BLOCK_CSI_IMAGE:-ghcr.io/seaweedfs/seaweed-block-csi:alpha}"
LAUNCHER_PVC_OWNER_REF="${SW_BLOCK_LAUNCHER_PVC_OWNER_REF:-1}"
LAUNCHER_STATE_HOSTPATH="${SW_BLOCK_LAUNCHER_STATE_HOSTPATH:-}"
REPLICATION_ACK="${SW_BLOCK_ALPHA_REPLICATION_ACK:-best-effort}"
LOGICAL_SERVERS="${SW_BLOCK_ALPHA_LOGICAL_SERVERS:-1}"
NODE_SPECS="${SW_BLOCK_ALPHA_NODE_SPECS:-}"
EXPECTED_SLOTS_PER_VOLUME="${SW_BLOCK_ALPHA_EXPECTED_SLOTS_PER_VOLUME:-$LOGICAL_SERVERS}"
STAGE2_MULTIPATH="${SW_BLOCK_STAGE2_MULTIPATH:-0}"
REJECT_LOOPBACK_PUBLISH_TARGETS="${SW_BLOCK_REJECT_LOOPBACK_PUBLISH_TARGETS:-0}"
LAUNCHER_EXTERNAL_ISCSI="${SW_BLOCK_LAUNCHER_EXTERNAL_ISCSI:-0}"
LAUNCHER_EXTERNAL_STATUS="${SW_BLOCK_LAUNCHER_EXTERNAL_STATUS:-0}"
CHAP_SECRET_NAME="${SW_BLOCK_ISCSI_CHAP_SECRET_NAME:-sw-block-iscsi-chap}"
CHAP_USERNAME="${SW_BLOCK_ISCSI_CHAP_USERNAME:-}"
CHAP_SECRET="${SW_BLOCK_ISCSI_CHAP_SECRET:-}"
APP_NAMESPACE="${SW_BLOCK_APP_NAMESPACE:-default}"

mkdir -p "$ARTIFACT_DIR"

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

log() {
  printf '[alpha-install] %s\n' "$*" | tee -a "$ARTIFACT_DIR/install.log"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

sed_escape() {
  printf '%s' "$1" | sed 's/[\/&]/\\&/g'
}

render_cluster_spec_nodes() {
  local node_name="$1"
  local count="$2"
  local i server_id data_port ctrl_port pool_id
  echo "    nodes:"
  for i in $(seq 1 "$count"); do
    if [[ "$count" == "1" ]]; then
      server_id="$node_name"
      pool_id="default"
    else
      server_id="${node_name}-r${i}"
      pool_id="default-r${i}"
    fi
    data_port=$((19101 + (i - 1) * 2))
    ctrl_port=$((19102 + (i - 1) * 2))
    cat <<YAML
      - server_id: ${server_id}
        data_addr: 127.0.0.1:${data_port}
        ctrl_addr: 127.0.0.1:${ctrl_port}
        labels:
          kubernetes.io/hostname: ${node_name}
        pools:
          - pool_id: ${pool_id}
            total_bytes: 1073741824
            free_bytes: 1073741824
            block_size: 4096
YAML
  done
}

node_spec_count() {
  local specs="$1"
  local IFS=';'
  local entry count=0
  read -ra entries <<< "$specs"
  for entry in "${entries[@]}"; do
    [[ -n "$entry" ]] && count=$((count + 1))
  done
  echo "$count"
}

render_cluster_spec_node_specs() {
  local specs="$1"
  local IFS=';'
  local entry i=0 server_id node_name host pool_id data_port ctrl_port
  echo "    nodes:"
  read -ra entries <<< "$specs"
  for entry in "${entries[@]}"; do
    [[ -z "$entry" ]] && continue
    IFS='|' read -r server_id node_name host pool_id extra <<< "$entry"
    if [[ -z "$server_id" || -z "$node_name" || -z "$host" || -n "${extra:-}" ]]; then
      echo "SW_BLOCK_ALPHA_NODE_SPECS entries must be server_id|kubernetes_node|host_or_ip|pool_id; got: $entry" >&2
      exit 2
    fi
    case "$host" in
      localhost|localhost.*|127.*|0.0.0.0|::1|\[::1\])
        echo "SW_BLOCK_ALPHA_NODE_SPECS host_or_ip must be non-loopback and non-unspecified for node-loss gates; got: $host" >&2
        exit 2
        ;;
    esac
    if [[ -z "$pool_id" ]]; then
      pool_id="default-${server_id}"
    fi
    data_port=$((19101 + i * 2))
    ctrl_port=$((19102 + i * 2))
    cat <<YAML
      - server_id: ${server_id}
        data_addr: ${host}:${data_port}
        ctrl_addr: ${host}:${ctrl_port}
        labels:
          kubernetes.io/hostname: ${node_name}
        pools:
          - pool_id: ${pool_id}
            total_bytes: 1073741824
            free_bytes: 1073741824
            block_size: 4096
YAML
    i=$((i + 1))
  done
}

require_cmd kubectl

case "$LOGICAL_SERVERS" in
  ''|*[!0-9]*|0)
    echo "SW_BLOCK_ALPHA_LOGICAL_SERVERS must be a positive integer, got: $LOGICAL_SERVERS" >&2
    exit 2
    ;;
esac
case "$EXPECTED_SLOTS_PER_VOLUME" in
  ''|*[!0-9]*|0)
    echo "SW_BLOCK_ALPHA_EXPECTED_SLOTS_PER_VOLUME must be a positive integer, got: $EXPECTED_SLOTS_PER_VOLUME" >&2
    exit 2
    ;;
esac
case "$REPLICATION_ACK" in
  best-effort|sync-quorum|sync-all)
    ;;
  *)
    echo "SW_BLOCK_ALPHA_REPLICATION_ACK must be best-effort, sync-quorum, or sync-all; got: $REPLICATION_ACK" >&2
    exit 2
    ;;
esac

NODE_NAME="${SW_BLOCK_ALPHA_NODE_NAME:-$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')}"
if [[ -n "$NODE_SPECS" ]]; then
  NODE_SPEC_COUNT="$(node_spec_count "$NODE_SPECS")"
  if [[ "$NODE_SPEC_COUNT" == "0" ]]; then
    echo "SW_BLOCK_ALPHA_NODE_SPECS did not contain any node entries" >&2
    exit 2
  fi
  if [[ -z "${SW_BLOCK_ALPHA_EXPECTED_SLOTS_PER_VOLUME:-}" ]]; then
    EXPECTED_SLOTS_PER_VOLUME="$NODE_SPEC_COUNT"
  fi
fi
if [[ "$LAUNCHER_EXTERNAL_ISCSI" == "1" || "$LAUNCHER_EXTERNAL_ISCSI" == "true" ]]; then
  if [[ -z "$CHAP_USERNAME" || -z "$CHAP_SECRET" ]]; then
    echo "SW_BLOCK_LAUNCHER_EXTERNAL_ISCSI requires SW_BLOCK_ISCSI_CHAP_USERNAME and SW_BLOCK_ISCSI_CHAP_SECRET" >&2
    exit 2
  fi
fi
if [[ "$LAUNCHER_EXTERNAL_STATUS" == "1" || "$LAUNCHER_EXTERNAL_STATUS" == "true" ]]; then
  if [[ "$LAUNCHER_EXTERNAL_ISCSI" != "1" && "$LAUNCHER_EXTERNAL_ISCSI" != "true" ]]; then
    echo "SW_BLOCK_LAUNCHER_EXTERNAL_STATUS requires SW_BLOCK_LAUNCHER_EXTERNAL_ISCSI=1" >&2
    exit 2
  fi
fi
STACK_RENDERED="$ARTIFACT_DIR/block-stack.rendered.yaml"
CSI_CONTROLLER_RENDERED="$ARTIFACT_DIR/csi-controller.rendered.yaml"
CSI_NODE_RENDERED="$ARTIFACT_DIR/csi-node.rendered.yaml"
IMAGE_SED="$(sed_escape "$IMAGE")"
CSI_IMAGE_SED="$(sed_escape "$CSI_IMAGE")"

CLUSTER_SPEC_NODES="$ARTIFACT_DIR/cluster-spec-nodes.rendered.yaml"
if [[ -n "$NODE_SPECS" ]]; then
  render_cluster_spec_node_specs "$NODE_SPECS" >"$CLUSTER_SPEC_NODES"
else
  render_cluster_spec_nodes "$NODE_NAME" "$LOGICAL_SERVERS" >"$CLUSTER_SPEC_NODES"
fi

sed -e "s/__NODE_NAME__/${NODE_NAME}/g" \
  -e "s/__EXPECTED_SLOTS_PER_VOLUME__/${EXPECTED_SLOTS_PER_VOLUME}/g" \
  -e "s/__REPLICATION_ACK__/${REPLICATION_ACK}/g" \
  -e "s/sw-block:local/${IMAGE_SED}/g" \
  -e "s/imagePullPolicy: Never/imagePullPolicy: IfNotPresent/g" \
  "$ROOT/deploy/k8s/alpha/block-stack.yaml" >"$STACK_RENDERED.tmp"
awk -v nodes_file="$CLUSTER_SPEC_NODES" '
  /__CLUSTER_SPEC_NODES__/ {
    while ((getline line < nodes_file) > 0) print line
    close(nodes_file)
    next
  }
  { print }
' "$STACK_RENDERED.tmp" >"$STACK_RENDERED"
rm -f "$STACK_RENDERED.tmp"
if [[ "$LAUNCHER_PVC_OWNER_REF" == "1" || "$LAUNCHER_PVC_OWNER_REF" == "true" ]]; then
  awk '/--launcher-namespace=/{print; print "            - \"--launcher-pvc-owner-ref\""; next} {print}' "$STACK_RENDERED" >"$STACK_RENDERED.tmp"
  mv "$STACK_RENDERED.tmp" "$STACK_RENDERED"
  grep -q -- '--launcher-pvc-owner-ref' "$STACK_RENDERED" || { echo "failed to inject --launcher-pvc-owner-ref into $STACK_RENDERED" >&2; exit 1; }
fi
if [[ -n "$LAUNCHER_STATE_HOSTPATH" ]]; then
  SW_BLOCK_AWK_HOSTPATH="$LAUNCHER_STATE_HOSTPATH" awk 'BEGIN{hostpath=ENVIRON["SW_BLOCK_AWK_HOSTPATH"]; gsub(/\\/, "\\\\", hostpath); gsub(/"/, "\\\"", hostpath)} /--launcher-durable-root=/{print; print "            - \"--launcher-state-hostpath=" hostpath "\""; next} {print}' "$STACK_RENDERED" >"$STACK_RENDERED.tmp"
  mv "$STACK_RENDERED.tmp" "$STACK_RENDERED"
  grep -q -- '--launcher-state-hostpath=' "$STACK_RENDERED" || { echo "failed to inject --launcher-state-hostpath into $STACK_RENDERED" >&2; exit 1; }
fi
if [[ "$LAUNCHER_EXTERNAL_ISCSI" == "1" || "$LAUNCHER_EXTERNAL_ISCSI" == "true" ]]; then
  awk -v secret="$CHAP_SECRET_NAME" '/--launcher-iscsi-port-base=/{print; print "            - \"--launcher-external-iscsi\""; print "            - \"--launcher-iscsi-chap-secret-name=" secret "\""; next} {print}' "$STACK_RENDERED" >"$STACK_RENDERED.tmp"
  mv "$STACK_RENDERED.tmp" "$STACK_RENDERED"
  grep -q -- '--launcher-external-iscsi' "$STACK_RENDERED" || { echo "failed to inject --launcher-external-iscsi into $STACK_RENDERED" >&2; exit 1; }
  grep -q -- '--launcher-iscsi-chap-secret-name' "$STACK_RENDERED" || { echo "failed to inject --launcher-iscsi-chap-secret-name into $STACK_RENDERED" >&2; exit 1; }
fi
if [[ "$LAUNCHER_EXTERNAL_STATUS" == "1" || "$LAUNCHER_EXTERNAL_STATUS" == "true" ]]; then
  awk '/--launcher-status/{print; print "            - \"--launcher-external-status\""; next} {print}' "$STACK_RENDERED" >"$STACK_RENDERED.tmp"
  mv "$STACK_RENDERED.tmp" "$STACK_RENDERED"
  grep -q -- '--launcher-external-status' "$STACK_RENDERED" || { echo "failed to inject --launcher-external-status into $STACK_RENDERED" >&2; exit 1; }
fi
sed -e "s/sw-block-csi:local/${CSI_IMAGE_SED}/g" \
  -e "s/imagePullPolicy: Never/imagePullPolicy: IfNotPresent/g" \
  "$ROOT/deploy/k8s/alpha/csi-controller.yaml" >"$CSI_CONTROLLER_RENDERED"
if [[ "$LAUNCHER_PVC_OWNER_REF" == "1" || "$LAUNCHER_PVC_OWNER_REF" == "true" ]]; then
  awk '/--node-id=\$\(NODE_NAME\)/{print; print "            - \"--kubernetes-pvc-uid-lookup\""; next} {print}' "$CSI_CONTROLLER_RENDERED" >"$CSI_CONTROLLER_RENDERED.tmp"
  mv "$CSI_CONTROLLER_RENDERED.tmp" "$CSI_CONTROLLER_RENDERED"
  grep -q -- '--kubernetes-pvc-uid-lookup' "$CSI_CONTROLLER_RENDERED" || { echo "failed to inject --kubernetes-pvc-uid-lookup into $CSI_CONTROLLER_RENDERED" >&2; exit 1; }
fi
sed -e "s/sw-block-csi:local/${CSI_IMAGE_SED}/g" \
  -e "s/imagePullPolicy: Never/imagePullPolicy: IfNotPresent/g" \
  "$ROOT/deploy/k8s/alpha/csi-node.yaml" >"$CSI_NODE_RENDERED"
if [[ "$STAGE2_MULTIPATH" == "1" || "$STAGE2_MULTIPATH" == "true" ]]; then
  awk '/--node-id=\$\(NODE_NAME\)/{print; print "            - \"--stage2-multipath\""; next} {print}' "$CSI_CONTROLLER_RENDERED" >"$CSI_CONTROLLER_RENDERED.tmp"
  mv "$CSI_CONTROLLER_RENDERED.tmp" "$CSI_CONTROLLER_RENDERED"
  grep -q -- '--stage2-multipath' "$CSI_CONTROLLER_RENDERED" || { echo "failed to inject --stage2-multipath into $CSI_CONTROLLER_RENDERED" >&2; exit 1; }
  awk '/--node-id=\$\(NODE_NAME\)/{print; print "            - \"--stage2-multipath\""; next} {print}' "$CSI_NODE_RENDERED" >"$CSI_NODE_RENDERED.tmp"
  mv "$CSI_NODE_RENDERED.tmp" "$CSI_NODE_RENDERED"
  grep -q -- '--stage2-multipath' "$CSI_NODE_RENDERED" || { echo "failed to inject --stage2-multipath into $CSI_NODE_RENDERED" >&2; exit 1; }
fi
if [[ "$REJECT_LOOPBACK_PUBLISH_TARGETS" == "1" || "$REJECT_LOOPBACK_PUBLISH_TARGETS" == "true" ]]; then
  awk '/--node-id=\$\(NODE_NAME\)/{print; print "            - \"--reject-loopback-publish-targets\""; next} {print}' "$CSI_CONTROLLER_RENDERED" >"$CSI_CONTROLLER_RENDERED.tmp"
  mv "$CSI_CONTROLLER_RENDERED.tmp" "$CSI_CONTROLLER_RENDERED"
  grep -q -- '--reject-loopback-publish-targets' "$CSI_CONTROLLER_RENDERED" || { echo "failed to inject --reject-loopback-publish-targets into $CSI_CONTROLLER_RENDERED" >&2; exit 1; }
  awk '/--node-id=\$\(NODE_NAME\)/{print; print "            - \"--reject-loopback-publish-targets\""; next} {print}' "$CSI_NODE_RENDERED" >"$CSI_NODE_RENDERED.tmp"
  mv "$CSI_NODE_RENDERED.tmp" "$CSI_NODE_RENDERED"
  grep -q -- '--reject-loopback-publish-targets' "$CSI_NODE_RENDERED" || { echo "failed to inject --reject-loopback-publish-targets into $CSI_NODE_RENDERED" >&2; exit 1; }
fi

log "artifact_dir=$ARTIFACT_DIR"
log "root=$ROOT"
log "node=$NODE_NAME"
log "logical_servers=$LOGICAL_SERVERS"
log "node_specs=${NODE_SPECS:-<single-node>}"
log "expected_slots_per_volume=$EXPECTED_SLOTS_PER_VOLUME"
log "replication_ack=$REPLICATION_ACK"
log "image=$IMAGE"
log "csi_image=$CSI_IMAGE"
log "launcher_pvc_owner_ref=$LAUNCHER_PVC_OWNER_REF"
log "launcher_state_hostpath=${LAUNCHER_STATE_HOSTPATH:-<emptyDir>}"
log "launcher_external_iscsi=$LAUNCHER_EXTERNAL_ISCSI"
log "launcher_external_status=$LAUNCHER_EXTERNAL_STATUS"
log "chap_enabled=$([[ -n "$CHAP_SECRET" ]] && echo 1 || echo 0)"
log "stage2_multipath=$STAGE2_MULTIPATH"
log "reject_loopback_publish_targets=$REJECT_LOOPBACK_PUBLISH_TARGETS"

kubectl version --client=true >"$ARTIFACT_DIR/kubectl-version.txt" 2>&1 || true
kubectl get nodes -o wide >"$ARTIFACT_DIR/nodes.before.txt"

log "apply RBAC"
kubectl apply -f "$ROOT/deploy/k8s/alpha/rbac.yaml" | tee "$ARTIFACT_DIR/apply-rbac.log"

log "apply seaweed-block control plane"
kubectl apply -f "$STACK_RENDERED" | tee "$ARTIFACT_DIR/apply-block-stack.log"
kubectl -n kube-system wait --for=condition=available deploy/sw-blockmaster --timeout=120s

log "apply CSI components"
kubectl apply -f "$ROOT/deploy/k8s/alpha/csi-driver.yaml" | tee "$ARTIFACT_DIR/apply-csidriver.log"
kubectl apply -f "$CSI_CONTROLLER_RENDERED" | tee "$ARTIFACT_DIR/apply-csi-controller.log"
kubectl apply -f "$CSI_NODE_RENDERED" | tee "$ARTIFACT_DIR/apply-csi-node.log"
kubectl -n kube-system wait --for=condition=available deploy/sw-block-csi-controller --timeout=120s
kubectl -n kube-system rollout status ds/sw-block-csi-node --timeout=120s

if [[ -n "$CHAP_SECRET" ]]; then
  log "apply iSCSI CHAP Secret"
  for secret_ns in "$APP_NAMESPACE" kube-system; do
    kubectl -n "$secret_ns" create secret generic "$CHAP_SECRET_NAME" \
      --from-literal=chapUsername="$CHAP_USERNAME" \
      --from-literal=chapSecret="$CHAP_SECRET" \
      --dry-run=client -o yaml | kubectl apply -f - | tee -a "$ARTIFACT_DIR/apply-chap-secret.log"
  done
fi

log "PASS: seaweed-block alpha stack installed"
log "next: create a PVC; blockmaster reconciles the generated blockvolume workload"
log "artifacts=$ARTIFACT_DIR"
