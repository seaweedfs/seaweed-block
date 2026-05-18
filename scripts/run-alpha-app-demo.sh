#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
NAMESPACE="${SW_BLOCK_APP_NAMESPACE:-default}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-app-demo-$(date -u +%Y%m%dT%H%M%SZ)}"
POLL_LOG="$ARTIFACT_DIR/poll.log"
IMAGE="${SW_BLOCK_IMAGE:-sw-block:local}"
CSI_IMAGE="${SW_BLOCK_CSI_IMAGE:-sw-block-csi:local}"
LAUNCHER_PVC_OWNER_REF="${SW_BLOCK_LAUNCHER_PVC_OWNER_REF:-0}"
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
RESTART_CSI_NODE_BEFORE_READER="${SW_BLOCK_RESTART_CSI_NODE_BEFORE_READER:-0}"
RESTART_BLOCKVOLUME_BEFORE_READER="${SW_BLOCK_RESTART_BLOCKVOLUME_BEFORE_READER:-0}"
COLLECT_INVENTORY_AFTER_RESTART="${SW_BLOCK_COLLECT_INVENTORY_AFTER_RESTART:-0}"
COLLECT_INVENTORY_ON_FAILURE="${SW_BLOCK_COLLECT_INVENTORY_ON_FAILURE:-0}"
DEMO_STOP_AFTER="${SW_BLOCK_DEMO_STOP_AFTER:-}"
COLLECT_OPS_STATUS="${SW_BLOCK_DEMO_COLLECT_OPS_STATUS:-0}"
KEEP_ON_STOP="${SW_BLOCK_DEMO_KEEP_ON_STOP:-0}"
MANUAL_APPLY_BLOCKVOLUMES="${SW_BLOCK_DEMO_MANUAL_APPLY_BLOCKVOLUMES:-0}"
AFTER_BLOCKVOLUME_READY_CMD="${SW_BLOCK_DEMO_AFTER_BLOCKVOLUME_READY_CMD:-}"
BREAK_AFTER_BLOCKVOLUME_READY="${SW_BLOCK_DEMO_BREAK_AFTER_BLOCKVOLUME_READY:-}"
BREAK_AFTER_BLOCKVOLUME_RESTART="${SW_BLOCK_DEMO_BREAK_AFTER_BLOCKVOLUME_RESTART:-}"
FAIL_PRIMARY_BEFORE_READER="${SW_BLOCK_DEMO_FAIL_PRIMARY_BEFORE_READER:-}"
REQUIRE_PROMOTION_READY_BEFORE_FAILURE="${SW_BLOCK_DEMO_REQUIRE_PROMOTION_READY_BEFORE_FAILURE:-0}"
WRITER_TIMEOUT="${SW_BLOCK_DEMO_WRITER_TIMEOUT:-240}"
DELETE_ALL_BLOCKVOLUMES="${SW_BLOCK_UNINSTALL_DELETE_ALL_BLOCKVOLUMES:-0}"
PIN_APP_NODE="${SW_BLOCK_DEMO_PIN_APP_NODE:-1}"
DEFAULT_DEMO_APP_MANIFEST="$ROOT/deploy/k8s/alpha/demo-app-pvc.yaml"
DEMO_APP_MANIFEST="${SW_BLOCK_DEMO_APP_MANIFEST:-$DEFAULT_DEMO_APP_MANIFEST}"
BLOCKVOLUME_NAMESPACE="kube-system"
if [[ "$LAUNCHER_PVC_OWNER_REF" == "1" || "$LAUNCHER_PVC_OWNER_REF" == "true" ]]; then
  BLOCKVOLUME_NAMESPACE="$NAMESPACE"
fi
if [[ "$RESTART_CSI_NODE_BEFORE_READER" == "1" || "$RESTART_CSI_NODE_BEFORE_READER" == "true" ]]; then
  if [[ "$DEMO_APP_MANIFEST" == "$DEFAULT_DEMO_APP_MANIFEST" || "$(basename "$DEMO_APP_MANIFEST")" == "demo-app-pvc.yaml" ]]; then
    echo "restart mode requires a writer manifest that keeps the PVC mounted, e.g. deploy/k8s/alpha/demo-app-pvc-writer-hold.yaml" >&2
    exit 2
  fi
fi
if [[ "$RESTART_BLOCKVOLUME_BEFORE_READER" == "1" || "$RESTART_BLOCKVOLUME_BEFORE_READER" == "true" ]]; then
  if [[ "$DEMO_APP_MANIFEST" == "$DEFAULT_DEMO_APP_MANIFEST" || "$(basename "$DEMO_APP_MANIFEST")" == "demo-app-pvc.yaml" ]]; then
    echo "blockvolume restart mode requires a writer manifest that keeps the PVC mounted, e.g. deploy/k8s/alpha/demo-app-pvc-writer-hold.yaml" >&2
    exit 2
  fi
  if [[ -z "$LAUNCHER_STATE_HOSTPATH" ]]; then
    echo "blockvolume restart mode requires SW_BLOCK_LAUNCHER_STATE_HOSTPATH so generated blockvolume state is durable" >&2
    exit 2
  fi
fi
if [[ -n "$DEMO_STOP_AFTER" && "$DEMO_STOP_AFTER" != "blockvolume-ready" && "$DEMO_STOP_AFTER" != "writer-verified" && "$DEMO_STOP_AFTER" != "promotion-ready" && "$DEMO_STOP_AFTER" != "reader-verified" ]]; then
  echo "unsupported SW_BLOCK_DEMO_STOP_AFTER value: $DEMO_STOP_AFTER" >&2
  exit 2
fi
if [[ -n "$BREAK_AFTER_BLOCKVOLUME_READY" && "$BREAK_AFTER_BLOCKVOLUME_READY" != "delete-generated-blockvolume" ]]; then
  echo "unsupported SW_BLOCK_DEMO_BREAK_AFTER_BLOCKVOLUME_READY value: $BREAK_AFTER_BLOCKVOLUME_READY" >&2
  exit 2
fi
if [[ -n "$BREAK_AFTER_BLOCKVOLUME_RESTART" && "$BREAK_AFTER_BLOCKVOLUME_RESTART" != "scale-generated-blockvolume-to-zero" ]]; then
  echo "unsupported SW_BLOCK_DEMO_BREAK_AFTER_BLOCKVOLUME_RESTART value: $BREAK_AFTER_BLOCKVOLUME_RESTART" >&2
  exit 2
fi
if [[ -n "$FAIL_PRIMARY_BEFORE_READER" && "$FAIL_PRIMARY_BEFORE_READER" != "scale-primary-to-zero" && "$FAIL_PRIMARY_BEFORE_READER" != "cordon-node-scale-primary-to-zero" ]]; then
  echo "unsupported SW_BLOCK_DEMO_FAIL_PRIMARY_BEFORE_READER value: $FAIL_PRIMARY_BEFORE_READER" >&2
  exit 2
fi
if [[ -z "$DEMO_STOP_AFTER" && ( "$KEEP_ON_STOP" == "1" || "$KEEP_ON_STOP" == "true" ) ]]; then
  echo "SW_BLOCK_DEMO_KEEP_ON_STOP requires SW_BLOCK_DEMO_STOP_AFTER" >&2
  exit 2
fi

mkdir -p "$ARTIFACT_DIR"
OPS_STATUS_COLLECTION_ATTEMPTED=0

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

log() {
  printf '[app-demo] %s\n' "$*" | tee -a "$ARTIFACT_DIR/run.log"
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

render_pods_with_node_selector() {
  local input="$1"
  local output="$2"
  local node="$3"
  if [[ "$PIN_APP_NODE" != "1" && "$PIN_APP_NODE" != "true" ]]; then
    cp "$input" "$output"
    return
  fi
  awk -v node="$node" '
    /^[[:space:]]*kind:[[:space:]]*Pod[[:space:]]*$/ { in_pod=1 }
    {
      print
      if (in_pod && /^[[:space:]]*spec:[[:space:]]*$/) {
        print "  nodeSelector:"
        print "    kubernetes.io/hostname: " node
        in_pod=0
      }
    }
  ' "$input" >"$output"
}

inject_node_stage_secret_into_storageclass() {
  local input="$1"
  local output="$2"
  local secret="$3"
  local namespace="$4"
  awk -v secret="$secret" -v namespace="$namespace" '
    /^parameters:[[:space:]]*$/ {
      print
      print "  csi.storage.k8s.io/node-stage-secret-name: \"" secret "\""
      print "  csi.storage.k8s.io/node-stage-secret-namespace: \"" namespace "\""
      next
    }
    { print }
  ' "$input" >"$output"
}

capture_once() {
  local path="$1"
  shift
  if [[ -s "$path" ]]; then
    return
  fi
  "$@" >"$path" 2>&1 || true
}

collect_logs() {
  set +e
  capture_once "$ARTIFACT_DIR/writer.log" kubectl -n "$NAMESPACE" logs sw-block-demo-writer
  capture_once "$ARTIFACT_DIR/reader.log" kubectl -n "$NAMESPACE" logs sw-block-demo-reader
  capture_once "$ARTIFACT_DIR/writer.describe.txt" kubectl -n "$NAMESPACE" describe pod sw-block-demo-writer
  capture_once "$ARTIFACT_DIR/reader.describe.txt" kubectl -n "$NAMESPACE" describe pod sw-block-demo-reader
  capture_once "$ARTIFACT_DIR/blockmaster.log" kubectl -n kube-system logs deploy/sw-blockmaster -c blockmaster
  capture_once "$ARTIFACT_DIR/blockcsi-controller.log" kubectl -n kube-system logs deploy/sw-block-csi-controller -c block-csi
  capture_once "$ARTIFACT_DIR/blockcsi-node.log" kubectl -n kube-system logs ds/sw-block-csi-node -c block-csi --tail=-1
  capture_once "$ARTIFACT_DIR/csi-provisioner.log" kubectl -n kube-system logs deploy/sw-block-csi-controller -c csi-provisioner
  capture_once "$ARTIFACT_DIR/csi-attacher.log" kubectl -n kube-system logs deploy/sw-block-csi-controller -c csi-attacher
  capture_once "$ARTIFACT_DIR/blockvolume-generated.log" kubectl -n "$BLOCKVOLUME_NAMESPACE" logs -l sw-block.seaweedfs.com/volume -c blockvolume --tail=-1
  capture_once "$ARTIFACT_DIR/kube-system-pods-deploys.txt" kubectl -n kube-system get pods,deploy -o wide
  capture_once "$ARTIFACT_DIR/blockvolume-namespace-pods-deploys.txt" kubectl -n "$BLOCKVOLUME_NAMESPACE" get pods,deploy -o wide
  capture_once "$ARTIFACT_DIR/app-storage.txt" kubectl -n "$NAMESPACE" get sc,pv,pvc,pod -o wide
  if [[ ! -s "$ARTIFACT_DIR/generated-blockvolume.yaml" ]]; then
    kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c 'cat /manifests/*.yaml' >"$ARTIFACT_DIR/generated-blockvolume.yaml" 2>"$ARTIFACT_DIR/generated-blockvolume.err" || true
  fi
}

collect_post_delete_state() {
  set +e
  kubectl -n kube-system get pods,deploy -o wide >"$ARTIFACT_DIR/kube-system-pods-deploys.after-delete.txt" 2>&1 || true
  kubectl -n "$BLOCKVOLUME_NAMESPACE" get pods,deploy -o wide >"$ARTIFACT_DIR/blockvolume-namespace-pods-deploys.after-delete.txt" 2>&1 || true
  kubectl -n "$NAMESPACE" get sc,pv,pvc,pod -o wide >"$ARTIFACT_DIR/app-storage.after-delete.txt" 2>&1 || true
  if command -v sudo >/dev/null 2>&1; then
    sudo iscsiadm -m session >"$ARTIFACT_DIR/iscsi-sessions.after-delete.txt" 2>&1 || true
  elif command -v iscsiadm >/dev/null 2>&1; then
    iscsiadm -m session >"$ARTIFACT_DIR/iscsi-sessions.after-delete.txt" 2>&1 || true
  else
    echo "iscsiadm unavailable" >"$ARTIFACT_DIR/iscsi-sessions.after-delete.txt"
  fi
}

capture_iscsi_sessions_to() {
  local path="$1"
  if command -v sudo >/dev/null 2>&1; then
    sudo iscsiadm -m session >"$path" 2>&1 || true
  elif command -v iscsiadm >/dev/null 2>&1; then
    iscsiadm -m session >"$path" 2>&1 || true
  else
    echo "iscsiadm unavailable" >"$path"
  fi
}

capture_stage2_host_path_evidence() {
  local label="$1"
  capture_iscsi_sessions_to "$ARTIFACT_DIR/iscsi-sessions.${label}.txt"
  if command -v sudo >/dev/null 2>&1; then
    sudo multipath -ll >"$ARTIFACT_DIR/multipath.${label}.txt" 2>&1 || true
  elif command -v multipath >/dev/null 2>&1; then
    multipath -ll >"$ARTIFACT_DIR/multipath.${label}.txt" 2>&1 || true
  else
    echo "multipath unavailable" >"$ARTIFACT_DIR/multipath.${label}.txt"
  fi
  {
    found=0
    for d in /dev/disk/by-path/*io.seaweedfs*; do
      [ -e "$d" ] || continue
      found=1
      raw="$(readlink -f "$d")"
      echo "device=$raw source=$d"
      if command -v sudo >/dev/null 2>&1; then
        sudo sg_rtpg "$raw" || true
      elif command -v sg_rtpg >/dev/null 2>&1; then
        sg_rtpg "$raw" || true
      else
        echo "sg_rtpg unavailable"
      fi
    done
    if [ "$found" -eq 0 ]; then
      echo "no seaweed iSCSI by-path devices"
    fi
  } >"$ARTIFACT_DIR/sg-rtpg.${label}.txt" 2>&1
}

wait_no_swblock_iscsi_sessions() {
  local path="$1"
  local timeout_s="$2"
  for _ in $(seq 1 "$timeout_s"); do
    capture_iscsi_sessions_to "$path"
    if ! grep -q 'iqn.2026-05.io.seaweedfs' "$path" 2>/dev/null; then
      return
    fi
    sleep 1
  done
  capture_iscsi_sessions_to "$path"
  if grep -q 'iqn.2026-05.io.seaweedfs' "$path" 2>/dev/null; then
    echo "sw-block iSCSI session still active before blockvolume restart" >&2
    cat "$path" >&2 || true
    exit 1
  fi
}

capture_blockvolume_pod_ids() {
  local path="$1"
  kubectl -n "$BLOCKVOLUME_NAMESPACE" get pods -l sw-block.seaweedfs.com/volume -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.metadata.uid}{"\t"}{.status.startTime}{"\n"}{end}' >"$path" 2>&1 || true
}

wait_blockvolume_log_pattern() {
  local deploy="$1"
  local pattern="$2"
  local out="$3"
  local timeout_s="$4"
  for _ in $(seq 1 "$timeout_s"); do
    kubectl -n "$BLOCKVOLUME_NAMESPACE" logs "$deploy" -c blockvolume --tail=-1 >"$out" 2>&1 || true
    if grep -F -q "$pattern" "$out" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  kubectl -n "$BLOCKVOLUME_NAMESPACE" logs "$deploy" -c blockvolume --tail=-1 >"$out" 2>&1 || true
  echo "timed out waiting for blockvolume log pattern: $pattern" >&2
  cat "$out" >&2 || true
  exit 1
}

generated_blockvolume_arg() {
  local name="$1"
  sed -n "s/.*--${name}=\\([^\"[:space:]]*\\).*/\\1/p" "$ARTIFACT_DIR/generated-blockvolume.yaml" | head -n 1
}

generated_blockvolume_node() {
  sed -n 's/^[[:space:]]*kubernetes.io\/hostname:[[:space:]]*\([^[:space:]]*\).*/\1/p' "$ARTIFACT_DIR/generated-blockvolume.yaml" | head -n 1
}

is_loopback_endpoint() {
  local addr="$1"
  case "$addr" in
    127.0.0.1:*|localhost:*|\[::1\]:*) return 0 ;;
    *) return 1 ;;
  esac
}

check_same_node_loopback_contract() {
  local blockvolume_node
  local frontend
  local app_node
  local issue_file="$ARTIFACT_DIR/unsupported-cross-node-loopback-attach.txt"
  blockvolume_node="$(generated_blockvolume_node)"
  frontend="$(generated_blockvolume_arg "iscsi-listen")"
  app_node="$APP_NODE_NAME"
  if [[ "$PIN_APP_NODE" != "1" && "$PIN_APP_NODE" != "true" ]]; then
    app_node="$(kubectl -n "$NAMESPACE" get pod sw-block-demo-writer -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
    app_node="${app_node:-$APP_NODE_NAME}"
  fi
  if [[ -z "$blockvolume_node" || -z "$frontend" ]]; then
    return 0
  fi
  if is_loopback_endpoint "$frontend" && [[ "$app_node" != "$blockvolume_node" ]]; then
    {
      echo "issue=unsupported_cross_node_loopback_attach"
      echo "app_node=$app_node"
      echo "blockvolume_node=$blockvolume_node"
      echo "frontend=$frontend"
      echo "volume_id=$(generated_blockvolume_arg "volume-id")"
      echo "replica_id=$(generated_blockvolume_arg "replica-id")"
      echo "reason=loopback frontend requires app pod and blockvolume on the same node"
      echo "ops_inventory_dir=$ARTIFACT_DIR/ops-inventory-unsupported-placement"
    } >"$issue_file"
    log "unsupported cross-node loopback attach: app_node=$app_node blockvolume_node=$blockvolume_node frontend=$frontend"
    collect_ops_inventory_bundle "$ARTIFACT_DIR/ops-inventory-unsupported-placement" || true
    exit 45
  fi
}

wait_tcp_ready() {
  local host="$1"
  local port="$2"
  local timeout_s="$3"
  for _ in $(seq 1 "$timeout_s"); do
    if (echo >/dev/tcp/"$host"/"$port") >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

choose_local_port() {
  python3 - <<'PY'
import socket

s = socket.socket()
s.bind(("127.0.0.1", 0))
try:
    print(s.getsockname()[1])
finally:
    s.close()
PY
}

run_sw_block_ops_status() {
  local volume_id="$1"
  local master_addr="$2"
  local status_addr="$3"
  local out_dir="$4"
  local product_revision="${5:-unknown}"
  if command -v sw-block >/dev/null 2>&1; then
    sw-block ops status --volume "$volume_id" --master "$master_addr" --status-addr "$status_addr" --out "$out_dir" --product-revision "$product_revision" --timeout 15s
    return $?
  fi
  go run ./cmd/sw-block ops status --volume "$volume_id" --master "$master_addr" --status-addr "$status_addr" --out "$out_dir" --product-revision "$product_revision" --timeout 15s
}

run_sw_block_ops_inventory() {
  local namespace="$1"
  local master_addr="$2"
  local out_dir="$3"
  local extra=()
  local raw_frontiers="${SW_BLOCK_OPS_INVENTORY_REQUIRED_FRONTIERS:-}"
  local claim_profile="${SW_BLOCK_OPS_INVENTORY_CLAIM_PROFILE:-}"
  if [[ -n "$raw_frontiers" ]]; then
    local frontier
    IFS=',' read -r -a extra_frontiers <<<"$raw_frontiers"
    for frontier in "${extra_frontiers[@]}"; do
      frontier="${frontier#"${frontier%%[![:space:]]*}"}"
      frontier="${frontier%"${frontier##*[![:space:]]}"}"
      [[ -n "$frontier" ]] && extra+=(--required-frontier "$frontier")
    done
  fi
  if [[ -n "$claim_profile" ]]; then
    extra+=(--claim-profile "$claim_profile")
  fi
  if command -v sw-block >/dev/null 2>&1; then
    sw-block ops inventory --namespace "$namespace" --master "$master_addr" --out "$out_dir" --timeout 30s "${extra[@]}"
    return $?
  fi
  go run ./cmd/sw-block ops inventory --namespace "$namespace" --master "$master_addr" --out "$out_dir" --timeout 30s "${extra[@]}"
}

collect_ops_status_bundle() {
  set +e
  OPS_STATUS_COLLECTION_ATTEMPTED=1
  local out_dir="$ARTIFACT_DIR/ops-status"
  local work_dir
  local marker="$ARTIFACT_DIR/controlled-stop.txt"
  local volume_id
  local status_addr
  local product_revision
  local pf_port
  local pf_pid
  mkdir -p "$out_dir"
  volume_id="$(generated_blockvolume_arg "volume-id")"
  status_addr="$(generated_blockvolume_arg "status-addr")"
  product_revision="${GIT_REVISION:-}"
  if [[ -z "$product_revision" ]]; then
    product_revision="$(git -C "$ROOT" rev-parse HEAD 2>/dev/null || true)"
  fi
  product_revision="${product_revision:-${SW_BLOCK_IMAGE_ID:-unknown}}"
  work_dir="$(mktemp -d /tmp/sw-block-ops-status.XXXXXX)"
  pf_port="${SW_BLOCK_OPS_STATUS_MASTER_PORT:-$(choose_local_port)}"
  {
    echo "phase=blockvolume-ready"
    echo "volume_id=${volume_id:-<empty>}"
    echo "status_addr=${status_addr:-<empty>}"
    echo "ops_status_dir=$out_dir"
    echo "ops_status_work_dir=$work_dir"
  } >"$marker"
  if [[ -z "$volume_id" || -z "$status_addr" ]]; then
    echo "ops-status-unavailable: missing volume-id or status-addr" >>"$marker"
    rm -rf "$work_dir"
    return 0
  fi
  kubectl -n kube-system port-forward svc/blockmaster "${pf_port}:9333" >"$out_dir/blockmaster-port-forward.log" 2>&1 &
  pf_pid=$!
  echo "$pf_pid" >"$out_dir/blockmaster-port-forward.pid"
  if ! wait_tcp_ready 127.0.0.1 "$pf_port" 20 || ! kill -0 "$pf_pid" >/dev/null 2>&1; then
    echo "ops-status-unavailable: blockmaster port-forward did not become ready" >>"$marker"
    kill "$pf_pid" >/dev/null 2>&1 || true
    wait "$pf_pid" >/dev/null 2>&1 || true
    rm -rf "$work_dir"
    return 0
  fi
  (
    cd "$ROOT"
    run_sw_block_ops_status "$volume_id" "127.0.0.1:${pf_port}" "$status_addr" "$work_dir" "$product_revision"
  ) >"$work_dir/stdout.txt" 2>"$work_dir/stderr.txt"
  local rc=$?
  echo "$rc" >"$work_dir/exit_code.txt"
  cp -f "$work_dir"/* "$out_dir"/ 2>>"$marker" || true
  kill "$pf_pid" >/dev/null 2>&1 || true
  wait "$pf_pid" >/dev/null 2>&1 || true
  if [[ -s "$out_dir/ops-status-bundle.json" ]]; then
    echo "ops-status-collected: $out_dir exit_code=$rc" >>"$marker"
  else
    echo "ops-status-failed: exit_code=$rc dir=$out_dir" >>"$marker"
  fi
  rm -rf "$work_dir"
  return 0
}

collect_ops_inventory_bundle() {
  local out_dir="$1"
  local pf_port
  local pf_pid
  mkdir -p "$out_dir"
  pf_port="${SW_BLOCK_OPS_INVENTORY_MASTER_PORT:-$(choose_local_port)}"
  kubectl -n kube-system port-forward svc/blockmaster "${pf_port}:9333" >"$out_dir/blockmaster-port-forward.log" 2>&1 &
  pf_pid=$!
  echo "$pf_pid" >"$out_dir/blockmaster-port-forward.pid"
  if ! wait_tcp_ready 127.0.0.1 "$pf_port" 20 || ! kill -0 "$pf_pid" >/dev/null 2>&1; then
    echo "ops-inventory-after-restart-unavailable: blockmaster port-forward did not become ready" >"$out_dir/unavailable.txt"
    kill "$pf_pid" >/dev/null 2>&1 || true
    wait "$pf_pid" >/dev/null 2>&1 || true
    return 1
  fi
  (
    cd "$ROOT"
    run_sw_block_ops_inventory "$NAMESPACE" "127.0.0.1:${pf_port}" "$out_dir"
  ) >"$out_dir/stdout.txt" 2>"$out_dir/stderr.txt"
  local rc=$?
  echo "$rc" >"$out_dir/exit_code.txt"
  find "$out_dir/volumes" -name ops-status-bundle.json -exec cat {} \; >"$out_dir/nested-ops-status-bundles.json" 2>/dev/null || true
  kill "$pf_pid" >/dev/null 2>&1 || true
  wait "$pf_pid" >/dev/null 2>&1 || true
  return "$rc"
}

collect_ops_inventory_after_restart() {
  collect_ops_inventory_bundle "$ARTIFACT_DIR/ops-inventory-after-restart"
}

collect_ops_inventory_on_failure() {
  collect_ops_inventory_bundle "$ARTIFACT_DIR/ops-inventory-on-failure"
}

inventory_primary_replica() {
  local summary="$1"
  awk '
    /^replica: / && / role=primary / {
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^replica=/) {
          sub(/^replica=/, "", $i)
          print $i
          exit
        }
      }
    }
  ' "$summary"
}

inventory_non_primary_not_ready_evidence() {
  local summary="$1"
  local primary="$2"
  awk -v primary="$primary" '
    /^replica: / {
      replica = ""
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^replica=/) {
          replica = $i
          sub(/^replica=/, "", replica)
        }
      }
      if (replica != "" && replica != primary && ($0 ~ /status=unhealthy/ || $0 ~ /replication=not_ready/ || $0 ~ / role=unknown /)) {
        print
        exit
      }
    }
  ' "$summary"
}

inventory_non_primary_promotion_evidence() {
  local summary="$1"
  local primary="$2"
  awk -v primary="$primary" '
    /^promotion: / {
      replica = ""
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^replica=/) {
          replica = $i
          sub(/^replica=/, "", replica)
        }
      }
      if (replica != "" && replica != primary) {
        print
        exit
      }
    }
  ' "$summary"
}

promotion_evidence_field() {
  local line="$1"
  local key="$2"
  awk -v key="$key" '
    {
      for (i = 1; i <= NF; i++) {
        if ($i ~ "^" key "=") {
          sub("^" key "=", "", $i)
          print $i
          exit
        }
      }
    }
  ' <<<"$line"
}

inventory_actionable_issue_evidence() {
  local summary="$1"
  grep -E 'replica_degraded=|ops_status=unhealthy|status_endpoint_' "$summary" 2>/dev/null | head -n 1 || true
}

write_control_plane_timeline_event() {
  local timeline="$1"
  local event="$2"
  shift 2
  {
    printf 'event=%s' "$event"
    local field
    for field in "$@"; do
      printf ' %s' "$field"
    done
    printf '\n'
  } >>"$timeline"
}

marker_field() {
  local marker="$1"
  local key="$2"
  sed -n "s/^${key}=//p" "$marker" 2>/dev/null | tail -n 1
}

marker_colon_field() {
  local marker="$1"
  local key="$2"
  sed -n "s/^${key}: //p" "$marker" 2>/dev/null | tail -n 1
}

write_node_loss_recovery_summary() {
  local marker="$1"
  local out="$2"
  local failover_status
  local ack_profile
  local before_primary_replica
  local before_primary_node
  local failed_replica
  local failed_node
  local promoted_replica
  local promoted_replica_node
  local before_frontend
  local after_frontend
  local reader_verified
  local data_check
  local stale_success
  local pod_recreate
  local transparent_claim
  local physical_host_claim

  failover_status="$(marker_colon_field "$marker" "failover_status")"
  ack_profile="$(marker_colon_field "$marker" "ack_profile")"
  before_primary_replica="$(marker_field "$marker" "before_primary_replica")"
  before_primary_node="$(marker_field "$marker" "before_primary_node")"
  failed_replica="$(marker_field "$marker" "failed_replica")"
  failed_node="$(marker_field "$marker" "failed_node")"
  promoted_replica="$(marker_field "$marker" "promoted_replica")"
  promoted_replica_node="$(marker_field "$marker" "promoted_replica_node")"
  before_frontend="$(marker_field "$marker" "before_publish_target_frontend")"
  after_frontend="$(marker_field "$marker" "after_publish_target_frontend")"
  reader_verified="$(marker_field "$marker" "reader_verified")"
  data_check="$(marker_field "$marker" "data_check_after_node_loss")"
  stale_success="$(marker_field "$marker" "old_primary_stale_io_success_count")"
  pod_recreate="$(marker_field "$marker" "pod_recreate_used")"
  transparent_claim="$(marker_field "$marker" "transparent_failover_claimed")"
  physical_host_claim="$(marker_field "$marker" "physical_host_loss_claimed")"

  {
    echo "node_loss_recovery_summary_version=1"
    echo "result=${failover_status:-unavailable}"
    echo "ack_profile=${ack_profile:-unavailable}"
    echo "before_primary=${before_primary_replica:-unavailable}@${before_primary_node:-unavailable}"
    echo "failed=${failed_replica:-unavailable}@${failed_node:-unavailable}"
    echo "promoted=${promoted_replica:-unavailable}@${promoted_replica_node:-unavailable}"
    echo "before_frontend=${before_frontend:-unavailable}"
    echo "after_frontend=${after_frontend:-unavailable}"
    echo "pod_recreate_used=${pod_recreate:-unavailable}"
    echo "reader_verified=${reader_verified:-false}"
    echo "data_check_after_node_loss=${data_check:-unavailable}"
    echo "old_primary_stale_io_success_count=${stale_success:-unavailable}"
    echo "transparent_failover_claimed=${transparent_claim:-false}"
    echo "physical_host_loss_claimed=${physical_host_claim:-false}"
    echo "source_marker=${marker}"
  } >"$out"
}

inventory_replica_line() {
  local summary="$1"
  local replica="$2"
  awk -v replica="$replica" '
    /^replica: / {
      for (i = 1; i <= NF; i++) {
        if ($i == "replica=" replica) {
          print
          exit
        }
      }
    }
  ' "$summary"
}

inventory_primary_required_frontier_arg() {
  local summary="$1"
  local inventory_dir="$2"
  python3 - "$summary" "$inventory_dir" <<'PY'
import json
import pathlib
import re
import sys

summary_path = pathlib.Path(sys.argv[1])
inventory_dir = pathlib.Path(sys.argv[2])
try:
    summary = summary_path.read_text(encoding="utf-8", errors="replace")
except FileNotFoundError:
    sys.exit(0)

primary_line = None
for line in summary.splitlines():
    if line.startswith("replica: ") and " role=primary " in line:
        primary_line = line
        break
if not primary_line:
    sys.exit(0)

volume = re.search(r"\bvolume=([^ ]+)", primary_line)
replica = re.search(r"\breplica=([^ ]+)", primary_line)
if not volume or not replica:
    sys.exit(0)
volume_id = volume.group(1)
replica_id = replica.group(1)
report_path = inventory_dir / "volumes" / volume_id / replica_id / "volume-status-report.json"
try:
    report = json.loads(report_path.read_text(encoding="utf-8"))
except Exception:
    sys.exit(0)

for durable in report.get("durable") or []:
    if durable.get("replica_id") != replica_id:
        continue
    if durable.get("frontier_known") is not True:
        continue
    lsn = durable.get("durable_lsn")
    if isinstance(lsn, int) and lsn >= 0:
        print(f"{volume_id}={lsn}")
        sys.exit(0)
PY
}

wait_promotion_ready_before_reader() {
  local out_dir="$ARTIFACT_DIR/ops-inventory-promotion-ready"
  local summary="$out_dir/volume-inventory-summary.txt"
  local marker="$ARTIFACT_DIR/promotion-ready.txt"
  local primary
  local required_frontier_arg
  local candidate_promotion_evidence
  local promotion_candidate_ready
  local promotion_reason

  log "collect inventory before promotion-ready wait"
  collect_ops_inventory_bundle "$out_dir" || true
  required_frontier_arg="$(inventory_primary_required_frontier_arg "$summary" "$out_dir")"
  if [[ -z "$required_frontier_arg" ]]; then
    echo "required_frontier_missing: unable to derive primary durable frontier from mounted writer path" >"$ARTIFACT_DIR/mounted-writer-required-frontier.txt"
    echo "unable to derive required frontier from mounted writer path" >&2
    cat "$summary" >&2 || true
    exit 1
  fi
  echo "$required_frontier_arg" >"$ARTIFACT_DIR/mounted-writer-required-frontier.txt"
  log "mounted writer required frontier: $required_frontier_arg"

  for _ in $(seq 1 90); do
    SW_BLOCK_OPS_INVENTORY_REQUIRED_FRONTIERS="$required_frontier_arg" collect_ops_inventory_bundle "$out_dir" || true
    primary="$(inventory_primary_replica "$summary")"
    if [[ -n "$primary" ]]; then
      candidate_promotion_evidence="$(inventory_non_primary_promotion_evidence "$summary" "$primary")"
      promotion_candidate_ready="$(promotion_evidence_field "$candidate_promotion_evidence" "candidate_ready")"
      promotion_reason="$(promotion_evidence_field "$candidate_promotion_evidence" "reason")"
      if [[ "$promotion_candidate_ready" == "true" ]]; then
        log "controlled stop after promotion-ready"
        {
          echo "promotion_ready_status: ready"
          echo "ack_profile: ${REPLICATION_ACK}"
          echo "required_frontier=${required_frontier_arg}"
          echo "primary_replica=${primary}"
          echo "candidate_promotion_evidence=${candidate_promotion_evidence}"
          echo "candidate_ready=true"
          echo "reason=${promotion_reason:-promotion_ready}"
          echo "inventory=${out_dir}"
          echo "data_check_after_failover=not_claimed"
        } >"$marker"
        exit 47
      fi
    fi
    sleep 2
  done

  {
    echo "promotion_ready_status: timeout"
    echo "ack_profile: ${REPLICATION_ACK}"
    echo "required_frontier=${required_frontier_arg}"
    echo "primary_replica=${primary:-unavailable}"
    echo "candidate_promotion_evidence=${candidate_promotion_evidence:-unavailable}"
    echo "candidate_ready=${promotion_candidate_ready:-false}"
    echo "reason=${promotion_reason:-promotion_ready_timeout}"
    echo "inventory=${out_dir}"
    echo "data_check_after_failover=not_claimed"
  } >"$marker"
  echo "non-primary candidate did not become promotion-ready" >&2
  cat "$summary" >&2 || true
  exit 1
}

fail_primary_before_reader() {
  local before_dir="$ARTIFACT_DIR/ops-inventory-before-primary-failure"
  local after_dir="$ARTIFACT_DIR/ops-inventory-after-primary-failure"
  local before_summary="$before_dir/volume-inventory-summary.txt"
  local after_summary="$after_dir/volume-inventory-summary.txt"
  local marker="$ARTIFACT_DIR/primary-failure-safe-refusal.txt"
  local recovery_marker="$ARTIFACT_DIR/primary-failure-recovery.txt"
  local timeline="$ARTIFACT_DIR/control-plane-timeline.txt"
  local primary
  local primary_count
  local after_primary_count
  local before_primary_line
  local candidate_evidence
  local candidate_promotion_evidence
  local candidate_replica
  local promotion_candidate_ready
  local promotion_reason
  local after_issue_evidence
  local after_primary
  local failed_after_line
  local promoted_line
  local promoted_node
  local deploy
  local ready
  local required_frontier_arg
  local failure_class="primary-blockvolume-controlled-stop"
  local node_loss_mode=0
  local before_primary_node=""
  local before_primary_frontend=""
  local promoted_frontend=""

  if [[ "$FAIL_PRIMARY_BEFORE_READER" == "cordon-node-scale-primary-to-zero" ]]; then
    failure_class="primary-kubernetes-node-cordoned-blockvolume-stop"
    node_loss_mode=1
  fi

  log "collect inventory before primary failure"
  collect_ops_inventory_bundle "$before_dir" || true
  required_frontier_arg="$(inventory_primary_required_frontier_arg "$before_summary" "$before_dir")"
  if [[ -n "$required_frontier_arg" ]]; then
    echo "$required_frontier_arg" >"$ARTIFACT_DIR/mounted-writer-required-frontier.txt"
    log "mounted writer required frontier: $required_frontier_arg"
    SW_BLOCK_OPS_INVENTORY_REQUIRED_FRONTIERS="$required_frontier_arg" collect_ops_inventory_bundle "$before_dir" || true
  else
    echo "required_frontier_missing: unable to derive primary durable frontier from mounted writer path" >"$ARTIFACT_DIR/mounted-writer-required-frontier.txt"
    log "mounted writer required frontier unavailable"
  fi
  primary_count="$(grep -c '^replica: .* role=primary ' "$before_summary" 2>/dev/null || true)"
  if [[ "$primary_count" != "1" ]]; then
    echo "expected exactly one primary before failure, found ${primary_count}" >&2
    cat "$before_summary" >&2 || true
    exit 1
  fi
  primary="$(inventory_primary_replica "$before_summary")"
  before_primary_line="$(inventory_replica_line "$before_summary" "$primary" 2>/dev/null || true)"
  before_primary_node="$(promotion_evidence_field "$before_primary_line" "node")"
  before_primary_frontend="$(promotion_evidence_field "$before_primary_line" "frontend")"
  if [[ "$node_loss_mode" == "1" && ( -z "$before_primary_node" || "$before_primary_node" == "unavailable" ) ]]; then
    echo "node-loss failure mode requires primary replica node evidence" >&2
    cat "$before_summary" >&2 || true
    exit 1
  fi
  if [[ "$node_loss_mode" == "1" && "$READER_NODE_NAME" == "$before_primary_node" ]]; then
    echo "node-loss failure mode requires SW_BLOCK_DEMO_READER_NODE_NAME to target a survivor node; reader_node=${READER_NODE_NAME} primary_node=${before_primary_node}" >&2
    cat "$before_summary" >&2 || true
    exit 1
  fi
  candidate_evidence="$(inventory_non_primary_not_ready_evidence "$before_summary" "$primary")"
  candidate_promotion_evidence="$(inventory_non_primary_promotion_evidence "$before_summary" "$primary")"
  if [[ -z "$candidate_promotion_evidence" ]]; then
    echo "no non-primary promotion-readiness evidence before failure" >&2
    cat "$before_summary" >&2 || true
    exit 1
  fi
  promotion_candidate_ready="$(promotion_evidence_field "$candidate_promotion_evidence" "candidate_ready")"
  promotion_reason="$(promotion_evidence_field "$candidate_promotion_evidence" "reason")"
  candidate_replica="$(promotion_evidence_field "$candidate_promotion_evidence" "replica")"
  if [[ "$promotion_candidate_ready" != "true" && ( "$REQUIRE_PROMOTION_READY_BEFORE_FAILURE" == "1" || "$REQUIRE_PROMOTION_READY_BEFORE_FAILURE" == "true" ) ]]; then
    log "wait for promotion-ready candidate before primary failure"
    for _ in $(seq 1 120); do
      if [[ -n "$required_frontier_arg" ]]; then
        SW_BLOCK_OPS_INVENTORY_REQUIRED_FRONTIERS="$required_frontier_arg" collect_ops_inventory_bundle "$before_dir" || true
      else
        collect_ops_inventory_bundle "$before_dir" || true
      fi
      candidate_promotion_evidence="$(inventory_non_primary_promotion_evidence "$before_summary" "$primary")"
      promotion_candidate_ready="$(promotion_evidence_field "$candidate_promotion_evidence" "candidate_ready")"
      promotion_reason="$(promotion_evidence_field "$candidate_promotion_evidence" "reason")"
      candidate_replica="$(promotion_evidence_field "$candidate_promotion_evidence" "replica")"
      if [[ "$promotion_candidate_ready" == "true" ]]; then
        break
      fi
      sleep 2
    done
  fi
  if [[ "$promotion_candidate_ready" != "true" && -z "$candidate_evidence" ]]; then
    echo "no non-primary not-ready candidate evidence before failure" >&2
    cat "$before_summary" >&2 || true
    exit 1
  fi
  deploy="$(kubectl -n "$BLOCKVOLUME_NAMESPACE" get deploy -l "app=sw-blockvolume,sw-block.seaweedfs.com/replica=${primary}" -o name | head -n 1)"
  if [[ -z "$deploy" ]]; then
    echo "primary blockvolume Deployment not found for replica ${primary}" >&2
    exit 1
  fi
  rm -f "$timeline"
  write_control_plane_timeline_event "$timeline" "primary_observed" "replica=${primary}" "evidence=${before_primary_line:-unavailable}" "inventory=${before_dir}"
  write_control_plane_timeline_event "$timeline" "candidate_evaluated" "replica=${candidate_replica:-unavailable}" "candidate_ready=${promotion_candidate_ready:-false}" "reason=${promotion_reason:-unavailable}" "evidence=${candidate_promotion_evidence:-${candidate_evidence:-unavailable}}"

  if [[ "$node_loss_mode" == "1" ]]; then
    echo "$before_primary_node" >"$ARTIFACT_DIR/cordoned-primary-node.txt"
  fi

  if [[ "$promotion_candidate_ready" == "true" ]]; then
    {
      echo "failover_status: promotion_pending"
      echo "ack_profile: ${REPLICATION_ACK}"
      echo "failure_class=${failure_class}"
      echo "before_primary_replica=${primary}"
      echo "before_primary_node=${before_primary_node:-unavailable}"
      echo "promotion_candidate_replica=${candidate_replica}"
      echo "failed_replica=${primary}"
      echo "failed_node=${before_primary_node:-unavailable}"
      echo "required_frontier=${required_frontier_arg:-unavailable}"
      echo "candidate_ready=true"
      echo "candidate_promotion_evidence=${candidate_promotion_evidence}"
      echo "before_publish_target_evidence=${before_primary_line}"
      echo "before_publish_target_frontend=${before_primary_frontend:-unavailable}"
      echo "data_check_after_failover=pending_reader"
      if [[ "$node_loss_mode" == "1" ]]; then
        echo "kubernetes_node_loss_claimed=true"
        echo "physical_host_loss_claimed=false"
        echo "transparent_failover_claimed=false"
        echo "pod_recreate_used=pending"
        echo "node_loss_recovery_claimed=pending"
        echo "data_check_after_node_loss=pending_reader"
      fi
      echo "control_plane_timeline=$timeline"
      echo "before_inventory=${before_dir}"
      echo "after_inventory=${after_dir}"
      echo "target_deployment=${deploy}"
    } >"$recovery_marker"

    if [[ "$node_loss_mode" == "1" ]]; then
      log "controlled node-loss failure: cordon ${before_primary_node}"
      kubectl cordon "$before_primary_node" | tee "$ARTIFACT_DIR/cordon-primary-node.log"
    fi
    log "controlled primary failure: scale ${deploy} to zero"
    write_control_plane_timeline_event "$timeline" "primary_failure_injected" "replica=${primary}" "node=${before_primary_node:-unavailable}" "deployment=${deploy}" "failure_class=${failure_class}"
    kubectl -n "$BLOCKVOLUME_NAMESPACE" get "$deploy" -o yaml >"$ARTIFACT_DIR/primary-deployment.before-failure.yaml" 2>&1 || true
    kubectl -n "$BLOCKVOLUME_NAMESPACE" scale "$deploy" --replicas=0 | tee "$ARTIFACT_DIR/scale-primary-zero.log"
    for _ in $(seq 1 60); do
      ready="$(kubectl -n "$BLOCKVOLUME_NAMESPACE" get "$deploy" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
      if [[ -z "$ready" || "$ready" == "0" ]]; then
        break
      fi
      sleep 1
    done
    kubectl -n "$BLOCKVOLUME_NAMESPACE" get deploy -l app=sw-blockvolume -o wide >"$ARTIFACT_DIR/blockvolume-pods.after-primary-failure.txt" 2>&1 || true
    ready="$(kubectl -n "$BLOCKVOLUME_NAMESPACE" get "$deploy" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
    echo "target_ready_replicas=${ready:-0}" >>"$recovery_marker"
    echo "old_primary_stale_io_success_count=0" >>"$recovery_marker"
    echo "stale_primary_fence_evidence=target_ready_replicas=${ready:-0}" >>"$recovery_marker"

    log "wait for promoted primary ${candidate_replica} after controlled failure"
    for _ in $(seq 1 120); do
      if [[ -n "$required_frontier_arg" ]]; then
        SW_BLOCK_OPS_INVENTORY_REQUIRED_FRONTIERS="$required_frontier_arg" collect_ops_inventory_bundle "$after_dir" || true
      else
        collect_ops_inventory_bundle "$after_dir" || true
      fi
      after_primary="$(inventory_primary_replica "$after_summary" 2>/dev/null || true)"
      after_primary_count="$(grep -c '^replica: .* role=primary ' "$after_summary" 2>/dev/null || true)"
      promoted_line="$(inventory_replica_line "$after_summary" "$candidate_replica" 2>/dev/null || true)"
      promoted_node="$(promotion_evidence_field "$promoted_line" "node")"
      promoted_frontend="$(promotion_evidence_field "$promoted_line" "frontend")"
      failed_after_line="$(inventory_replica_line "$after_summary" "$primary" 2>/dev/null || true)"
      frontend_ready_issue_count="$(grep -c 'frontend_primary_ready=true' "$after_summary" 2>/dev/null || true)"
      if [[ "$after_primary" == "$candidate_replica" && "$after_primary_count" == "1" && "$promoted_line" == *" role=primary "* && -n "$failed_after_line" && "$failed_after_line" != *" role=primary "* && "$frontend_ready_issue_count" == "0" ]]; then
        {
          echo "failover_status: promoted"
          echo "post_failure_primary_count=${after_primary_count}"
          echo "frontend_primary_ready_issue_count=${frontend_ready_issue_count}"
          echo "after_primary_replica=${after_primary}"
          echo "promoted_replica=${candidate_replica}"
          echo "promoted_replica_node=${promoted_node:-unavailable}"
          echo "promoted_replica_evidence=${promoted_line}"
          echo "failed_replica_after_evidence=${failed_after_line}"
          echo "after_publish_target_evidence=${promoted_line}"
          echo "after_publish_target_frontend=${promoted_frontend:-unavailable}"
        } >>"$recovery_marker"
        write_control_plane_timeline_event "$timeline" "authority_published" "from=${primary}" "to=${candidate_replica}" "primary=${after_primary}" "primary_count=${after_primary_count}" "evidence=${promoted_line}"
        return 0
      fi
      sleep 2
    done

    {
      echo "failover_status: promotion_timeout"
      echo "post_failure_primary_count=${after_primary_count:-0}"
      echo "frontend_primary_ready_issue_count=${frontend_ready_issue_count:-0}"
      echo "after_primary_replica=${after_primary:-unavailable}"
      echo "reason=promotion_not_observed"
    } >>"$recovery_marker"
    write_control_plane_timeline_event "$timeline" "promotion_timeout" "candidate=${candidate_replica:-unavailable}" "after_primary=${after_primary:-unavailable}" "primary_count=${after_primary_count:-0}" "reason=promotion_not_observed"
    echo "promotion-ready candidate did not become primary after controlled failure" >&2
    cat "$after_summary" >&2 || true
    exit 1
  fi

  {
    echo "failover_status: refused"
    echo "ack_profile: ${REPLICATION_ACK}"
    echo "failure_class=${failure_class}"
    echo "before_primary_replica=${primary}"
    echo "before_primary_node=${before_primary_node:-unavailable}"
    echo "failed_replica=${primary}"
    echo "failed_node=${before_primary_node:-unavailable}"
    echo "old_primary_safe=unknown"
    echo "required_frontier=${required_frontier_arg:-unavailable}"
    echo "candidate_ready=${promotion_candidate_ready:-false}"
    echo "candidate_evidence=${candidate_evidence}"
    echo "candidate_promotion_evidence=${candidate_promotion_evidence}"
    echo "data_check_after_failover=not_claimed"
    if [[ "$node_loss_mode" == "1" ]]; then
      echo "kubernetes_node_loss_claimed=true"
      echo "physical_host_loss_claimed=false"
      echo "transparent_failover_claimed=false"
      echo "pod_recreate_used=false"
      echo "node_loss_recovery_claimed=false"
      echo "data_check_after_node_loss=not_claimed"
    fi
    echo "reason=${promotion_reason:-candidate_not_ready_for_primary}"
    echo "control_plane_timeline=$timeline"
    echo "before_inventory=${before_dir}"
    echo "after_inventory=${after_dir}"
    echo "target_deployment=${deploy}"
  } >"$marker"

  if [[ "$node_loss_mode" == "1" ]]; then
    log "controlled node-loss failure: cordon ${before_primary_node}"
    kubectl cordon "$before_primary_node" | tee "$ARTIFACT_DIR/cordon-primary-node.log"
  fi
  log "controlled primary failure: scale ${deploy} to zero"
  write_control_plane_timeline_event "$timeline" "primary_failure_injected" "replica=${primary}" "node=${before_primary_node:-unavailable}" "deployment=${deploy}" "failure_class=${failure_class}"
  kubectl -n "$BLOCKVOLUME_NAMESPACE" get "$deploy" -o yaml >"$ARTIFACT_DIR/primary-deployment.before-failure.yaml" 2>&1 || true
  kubectl -n "$BLOCKVOLUME_NAMESPACE" scale "$deploy" --replicas=0 | tee "$ARTIFACT_DIR/scale-primary-zero.log"
  for _ in $(seq 1 60); do
    ready="$(kubectl -n "$BLOCKVOLUME_NAMESPACE" get "$deploy" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
    if [[ -z "$ready" || "$ready" == "0" ]]; then
      break
    fi
    sleep 1
  done
  kubectl -n "$BLOCKVOLUME_NAMESPACE" get deploy -l app=sw-blockvolume -o wide >"$ARTIFACT_DIR/blockvolume-pods.after-primary-failure.txt" 2>&1 || true
  ready="$(kubectl -n "$BLOCKVOLUME_NAMESPACE" get "$deploy" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
  echo "target_ready_replicas=${ready:-0}" >>"$marker"
  echo "old_primary_stale_io_success_count=0" >>"$marker"
  echo "stale_primary_fence_evidence=target_ready_replicas=${ready:-0}" >>"$marker"

  log "collect inventory after primary failure"
  collect_ops_inventory_bundle "$after_dir" || true
  after_issue_evidence="$(inventory_actionable_issue_evidence "$after_summary")"
  if [[ -z "$after_issue_evidence" ]]; then
    echo "after-failure inventory did not include an actionable degradation issue" >&2
    cat "$after_summary" >&2 || true
    exit 1
  fi
  echo "after_issue_evidence=${after_issue_evidence}" >>"$marker"
  write_control_plane_timeline_event "$timeline" "safe_refusal" "replica=${candidate_replica:-unavailable}" "candidate_ready=${promotion_candidate_ready:-false}" "reason=${promotion_reason:-candidate_not_ready_for_primary}" "evidence=${after_issue_evidence}"
  exit 46
}

record_ops_status_unavailable() {
  local reason="$1"
  mkdir -p "$ARTIFACT_DIR"
  {
    echo "phase=failure"
    echo "volume_id=<unavailable>"
    echo "status_addr=<unavailable>"
    echo "ops-status-unavailable: $reason"
  } >"$ARTIFACT_DIR/controlled-stop.txt"
}

wait_blockvolume_durable_status() {
  local volume_id="$1"
  local status_addr="$2"
  local out="$3"
  local timeout_s="$4"
  local tmp="${out}.tmp"
  if [[ -z "$volume_id" || -z "$status_addr" ]]; then
    echo "missing blockvolume durable status input: volume_id=${volume_id:-<empty>} status_addr=${status_addr:-<empty>}" >&2
    exit 1
  fi
  for _ in $(seq 1 "$timeout_s"); do
    rm -f "$tmp"
    if python3 - "$volume_id" "http://${status_addr}/status/durable?volume=${volume_id}" "$tmp" <<'PY'
import json
import sys
import urllib.request

volume_id, url, out = sys.argv[1:4]
try:
    with urllib.request.urlopen(url, timeout=2) as resp:
        raw = resp.read()
except Exception as exc:
    print(f"status query failed: {exc}", file=sys.stderr)
    sys.exit(1)

with open(out, "wb") as f:
    f.write(raw)
body = json.loads(raw.decode("utf-8"))
if body.get("VolumeID") != volume_id:
    print(f"status volume mismatch: {body.get('VolumeID')} != {volume_id}", file=sys.stderr)
    sys.exit(2)
for vol in body.get("Volumes") or []:
    if (
        vol.get("VolumeID") == volume_id
        and vol.get("Latched") is True
        and vol.get("Operational") is True
        and int(vol.get("Epoch") or 0) >= 1
        and int(vol.get("EndpointVersion") or 0) >= 1
    ):
        sys.exit(0)
print(f"durable status not ready: {body}", file=sys.stderr)
sys.exit(3)
PY
    then
      mv "$tmp" "$out"
      return 0
    fi
    sleep 1
  done
  rm -f "$tmp"
  python3 - "http://${status_addr}/status/durable?volume=${volume_id}" "$tmp" <<'PY' || true
import sys
import urllib.request

url, out = sys.argv[1:3]
try:
    with urllib.request.urlopen(url, timeout=2) as resp:
        raw = resp.read()
    with open(out, "wb") as f:
        f.write(raw)
    print(raw.decode("utf-8", errors="replace"), file=sys.stderr)
except Exception as exc:
    print(f"final status query failed: {exc}", file=sys.stderr)
PY
  [[ -s "$tmp" ]] && mv "$tmp" "$out"
  echo "timed out waiting for blockvolume durable status at ${status_addr}" >&2
  exit 1
}

blockvolume_pod_uids() {
  local path="$1"
  awk 'NF >= 2 { print $2 }' "$path" | sort
}

cleanup() {
  (
  set +e
  kubectl delete -f "$ROOT/deploy/k8s/alpha/demo-app-reader-pod.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$DEMO_APP_MANIFEST" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl -n "$NAMESPACE" delete pod sw-block-demo-reader sw-block-demo-writer --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl -n "$NAMESPACE" delete pvc sw-block-demo-pvc --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  if [[ -s "$ARTIFACT_DIR/generated-blockvolume.yaml" ]]; then
    kubectl delete -f "$ARTIFACT_DIR/generated-blockvolume.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  elif [[ "$DELETE_ALL_BLOCKVOLUMES" == "1" || "$DELETE_ALL_BLOCKVOLUMES" == "true" ]]; then
    kubectl -n kube-system delete deploy -l app=sw-blockvolume --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
    kubectl -n "$NAMESPACE" delete deploy -l app=sw-blockvolume --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  else
    echo "skip broad app=sw-blockvolume cleanup; no generated manifest captured" >>"$ARTIFACT_DIR/cleanup.log"
  fi
  kubectl delete -f "$CSI_NODE_RENDERED" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$CSI_CONTROLLER_RENDERED" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/alpha/csi-driver.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$ROOT/deploy/k8s/alpha/rbac.yaml" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl delete -f "$STACK_RENDERED" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  if [[ -s "$ARTIFACT_DIR/cordoned-primary-node.txt" ]]; then
    primary_node="$(cat "$ARTIFACT_DIR/cordoned-primary-node.txt" 2>/dev/null || true)"
    if [[ -n "$primary_node" ]]; then
      kubectl uncordon "$primary_node" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
    fi
  fi
  kubectl -n "$NAMESPACE" delete secret "$CHAP_SECRET_NAME" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  kubectl -n "$BLOCKVOLUME_NAMESPACE" delete secret "$CHAP_SECRET_NAME" --ignore-not-found=true >>"$ARTIFACT_DIR/cleanup.log" 2>&1
  if [[ "$RESTART_BLOCKVOLUME_BEFORE_READER" == "1" || "$RESTART_BLOCKVOLUME_BEFORE_READER" == "true" || -n "$FAIL_PRIMARY_BEFORE_READER" || "$DEMO_STOP_AFTER" == "promotion-ready" ]]; then
    case "$LAUNCHER_STATE_HOSTPATH" in
      /var/lib/sw-block/testops-*)
        if command -v sudo >/dev/null 2>&1; then
          sudo rm -rf -- "$LAUNCHER_STATE_HOSTPATH" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
        else
          rm -rf -- "$LAUNCHER_STATE_HOSTPATH" >>"$ARTIFACT_DIR/cleanup.log" 2>&1 || true
        fi
        ;;
    esac
  fi
  )
}

wait_pod_succeeded() {
  local pod="$1"
  local timeout_s="$2"
  for _ in $(seq 1 "$timeout_s"); do
    phase="$(kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if [[ "$phase" == "Succeeded" ]]; then
      return 0
    fi
    if [[ "$phase" == "Failed" ]]; then
      echo "pod $pod failed" >&2
      return 1
    fi
    sleep 1
  done
  echo "pod $pod did not complete before timeout" >&2
  return 1
}

wait_pod_log_contains() {
  local pod="$1"
  local pattern="$2"
  local timeout_s="$3"
  for _ in $(seq 1 "$timeout_s"); do
    if kubectl -n "$NAMESPACE" logs "$pod" 2>/dev/null | grep -F -q "$pattern"; then
      return 0
    fi
    phase="$(kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
    if [[ "$phase" == "Failed" ]]; then
      echo "pod $pod failed before log pattern $pattern" >&2
      return 1
    fi
    sleep 1
  done
  echo "pod $pod did not emit log pattern $pattern before timeout" >&2
  return 1
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
APP_NODE_NAME="${SW_BLOCK_DEMO_APP_NODE_NAME:-$NODE_NAME}"
READER_NODE_NAME="${SW_BLOCK_DEMO_READER_NODE_NAME:-$APP_NODE_NAME}"
STACK_RENDERED="$ARTIFACT_DIR/block-stack.rendered.yaml"
CSI_CONTROLLER_RENDERED="$ARTIFACT_DIR/csi-controller.rendered.yaml"
CSI_NODE_RENDERED="$ARTIFACT_DIR/csi-node.rendered.yaml"
DEMO_APP_RENDERED="$ARTIFACT_DIR/demo-app.rendered.yaml"
READER_RENDERED="$ARTIFACT_DIR/demo-app-reader.rendered.yaml"
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
if [[ "$BLOCKVOLUME_NAMESPACE" != "kube-system" ]]; then
  awk '/--launcher-namespace=/{print; print "            - \"--launcher-pvc-owner-ref\""; next} {print}' "$STACK_RENDERED" >"$STACK_RENDERED.tmp"
  mv "$STACK_RENDERED.tmp" "$STACK_RENDERED"
  grep -q -- '--launcher-pvc-owner-ref' "$STACK_RENDERED" || { echo "failed to inject --launcher-pvc-owner-ref into $STACK_RENDERED" >&2; exit 1; }
fi
if [[ -n "$LAUNCHER_STATE_HOSTPATH" ]]; then
  awk -v hostpath="$LAUNCHER_STATE_HOSTPATH" '/--launcher-durable-root=/{print; print "            - \"--launcher-state-hostpath=" hostpath "\""; next} {print}' "$STACK_RENDERED" >"$STACK_RENDERED.tmp"
  mv "$STACK_RENDERED.tmp" "$STACK_RENDERED"
  grep -q -- '--launcher-state-hostpath=' "$STACK_RENDERED" || { echo "failed to inject --launcher-state-hostpath into $STACK_RENDERED" >&2; exit 1; }
fi
if [[ "$LAUNCHER_EXTERNAL_ISCSI" == "1" || "$LAUNCHER_EXTERNAL_ISCSI" == "true" ]]; then
  awk -v secret="$CHAP_SECRET_NAME" '/--launcher-iscsi-port-base=/{print; print "            - \"--launcher-external-iscsi\""; print "            - \"--launcher-iscsi-chap-secret-name=" secret "\""; next} {print}' "$STACK_RENDERED" >"$STACK_RENDERED.tmp"
  mv "$STACK_RENDERED.tmp" "$STACK_RENDERED"
  grep -q -- '--launcher-external-iscsi' "$STACK_RENDERED" || { echo "failed to inject --launcher-external-iscsi into $STACK_RENDERED" >&2; exit 1; }
  grep -q -- '--launcher-iscsi-chap-secret-name' "$STACK_RENDERED" || { echo "failed to inject --launcher-iscsi-chap-secret-name into $STACK_RENDERED" >&2; exit 1; }
fi
if [[ "$RESTART_BLOCKVOLUME_BEFORE_READER" == "1" || "$RESTART_BLOCKVOLUME_BEFORE_READER" == "true" || "$COLLECT_OPS_STATUS" == "1" || "$COLLECT_OPS_STATUS" == "true" || -n "$DEMO_STOP_AFTER" ]]; then
  awk '/--launcher-durable-root=/{print; print "            - \"--launcher-status\""; next} {print}' "$STACK_RENDERED" >"$STACK_RENDERED.tmp"
  mv "$STACK_RENDERED.tmp" "$STACK_RENDERED"
  grep -q -- '--launcher-status' "$STACK_RENDERED" || { echo "failed to inject --launcher-status into $STACK_RENDERED" >&2; exit 1; }
fi
if [[ "$LAUNCHER_EXTERNAL_STATUS" == "1" || "$LAUNCHER_EXTERNAL_STATUS" == "true" ]]; then
  grep -q -- '--launcher-status' "$STACK_RENDERED" || {
    awk '/--launcher-durable-root=/{print; print "            - \"--launcher-status\""; next} {print}' "$STACK_RENDERED" >"$STACK_RENDERED.tmp"
    mv "$STACK_RENDERED.tmp" "$STACK_RENDERED"
  }
  awk '/--launcher-status/{print; print "            - \"--launcher-external-status\""; next} {print}' "$STACK_RENDERED" >"$STACK_RENDERED.tmp"
  mv "$STACK_RENDERED.tmp" "$STACK_RENDERED"
  grep -q -- '--launcher-external-status' "$STACK_RENDERED" || { echo "failed to inject --launcher-external-status into $STACK_RENDERED" >&2; exit 1; }
fi
sed -e "s/sw-block-csi:local/${CSI_IMAGE_SED}/g" \
  -e "s/imagePullPolicy: Never/imagePullPolicy: IfNotPresent/g" \
  "$ROOT/deploy/k8s/alpha/csi-controller.yaml" >"$CSI_CONTROLLER_RENDERED"
if [[ "$BLOCKVOLUME_NAMESPACE" != "kube-system" ]]; then
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
render_pods_with_node_selector "$DEMO_APP_MANIFEST" "$DEMO_APP_RENDERED" "$APP_NODE_NAME"
render_pods_with_node_selector "$ROOT/deploy/k8s/alpha/demo-app-reader-pod.yaml" "$READER_RENDERED" "$READER_NODE_NAME"
if [[ "$LAUNCHER_EXTERNAL_ISCSI" == "1" || "$LAUNCHER_EXTERNAL_ISCSI" == "true" ]]; then
  inject_node_stage_secret_into_storageclass "$DEMO_APP_RENDERED" "$DEMO_APP_RENDERED.tmp" "$CHAP_SECRET_NAME" "$NAMESPACE"
  mv "$DEMO_APP_RENDERED.tmp" "$DEMO_APP_RENDERED"
fi

log "artifact_dir=$ARTIFACT_DIR"
log "root=$ROOT"
log "namespace=$NAMESPACE"
log "node=$NODE_NAME"
log "app_node=$APP_NODE_NAME"
log "reader_node=$READER_NODE_NAME"
log "pin_app_node=$PIN_APP_NODE"
log "logical_servers=$LOGICAL_SERVERS"
log "node_specs=${NODE_SPECS:-<single-node>}"
log "expected_slots_per_volume=$EXPECTED_SLOTS_PER_VOLUME"
log "replication_ack=$REPLICATION_ACK"
log "image=$IMAGE"
log "csi_image=$CSI_IMAGE"
log "blockvolume_namespace=$BLOCKVOLUME_NAMESPACE"
log "launcher_pvc_owner_ref=$LAUNCHER_PVC_OWNER_REF"
log "launcher_state_hostpath=${LAUNCHER_STATE_HOSTPATH:-<emptyDir>}"
log "launcher_external_iscsi=$LAUNCHER_EXTERNAL_ISCSI"
log "launcher_external_status=$LAUNCHER_EXTERNAL_STATUS"
log "chap_enabled=$([[ -n "$CHAP_SECRET" ]] && echo 1 || echo 0)"
log "stage2_multipath=$STAGE2_MULTIPATH"
log "reject_loopback_publish_targets=$REJECT_LOOPBACK_PUBLISH_TARGETS"
log "restart_csi_node_before_reader=$RESTART_CSI_NODE_BEFORE_READER"
log "restart_blockvolume_before_reader=$RESTART_BLOCKVOLUME_BEFORE_READER"
log "collect_inventory_after_restart=$COLLECT_INVENTORY_AFTER_RESTART"
log "collect_inventory_on_failure=$COLLECT_INVENTORY_ON_FAILURE"
log "demo_stop_after=${DEMO_STOP_AFTER:-<none>}"
log "collect_ops_status=$COLLECT_OPS_STATUS"
log "keep_on_stop=$KEEP_ON_STOP"
log "manual_apply_blockvolumes=$MANUAL_APPLY_BLOCKVOLUMES"
log "after_blockvolume_ready_cmd=${AFTER_BLOCKVOLUME_READY_CMD:-<none>}"
log "break_after_blockvolume_ready=${BREAK_AFTER_BLOCKVOLUME_READY:-<none>}"
log "break_after_blockvolume_restart=${BREAK_AFTER_BLOCKVOLUME_RESTART:-<none>}"
log "fail_primary_before_reader=${FAIL_PRIMARY_BEFORE_READER:-<none>}"
log "writer_timeout=$WRITER_TIMEOUT"
log "demo_app_manifest=$DEMO_APP_MANIFEST"
kubectl version --client=true >"$ARTIFACT_DIR/kubectl-version.txt" 2>&1 || true
kubectl get nodes -o wide >"$ARTIFACT_DIR/nodes.before.txt"

on_exit() {
  local rc=$?
  if [[ "$rc" -ne 0 && ( "$COLLECT_OPS_STATUS" == "1" || "$COLLECT_OPS_STATUS" == "true" ) && "$OPS_STATUS_COLLECTION_ATTEMPTED" -eq 0 ]]; then
    if [[ -s "$ARTIFACT_DIR/generated-blockvolume.yaml" ]]; then
      log "collect ops status after failure"
      collect_ops_status_bundle
    else
      record_ops_status_unavailable "no volume id reached"
    fi
  fi
  if [[ "$rc" -ne 0 && ( "$COLLECT_INVENTORY_ON_FAILURE" == "1" || "$COLLECT_INVENTORY_ON_FAILURE" == "true" ) && -s "$ARTIFACT_DIR/generated-blockvolume.yaml" ]]; then
    log "collect inventory after failure"
    collect_ops_inventory_on_failure || true
  fi
  collect_logs
  cleanup
  exit "$rc"
}

cleanup
trap on_exit EXIT

log "apply RBAC"
kubectl apply -f "$ROOT/deploy/k8s/alpha/rbac.yaml" | tee "$ARTIFACT_DIR/apply-rbac.log"

log "apply seaweed-block service stack"
kubectl apply -f "$STACK_RENDERED" | tee "$ARTIFACT_DIR/apply-block-stack.log"
kubectl -n kube-system wait --for=condition=available deploy/sw-blockmaster --timeout=120s

log "apply CSI manifests"
kubectl apply -f "$ROOT/deploy/k8s/alpha/csi-driver.yaml" | tee "$ARTIFACT_DIR/apply-csidriver.log"
kubectl apply -f "$CSI_CONTROLLER_RENDERED" | tee "$ARTIFACT_DIR/apply-csi-controller.log"
kubectl apply -f "$CSI_NODE_RENDERED" | tee "$ARTIFACT_DIR/apply-csi-node.log"
kubectl -n kube-system wait --for=condition=available deploy/sw-block-csi-controller --timeout=120s
kubectl -n kube-system rollout status ds/sw-block-csi-node --timeout=120s

if [[ -n "$CHAP_SECRET" ]]; then
  log "apply iSCSI CHAP Secret"
  for secret_ns in "$NAMESPACE" "$BLOCKVOLUME_NAMESPACE"; do
    kubectl -n "$secret_ns" create secret generic "$CHAP_SECRET_NAME" \
      --from-literal=chapUsername="$CHAP_USERNAME" \
      --from-literal=chapSecret="$CHAP_SECRET" \
      --dry-run=client -o yaml | kubectl apply -f - | tee -a "$ARTIFACT_DIR/apply-chap-secret.log"
  done
fi

log "apply demo app PVC and writer pod"
kubectl apply -f "$DEMO_APP_RENDERED" | tee "$ARTIFACT_DIR/apply-demo-app.log"

log "wait for launcher-generated blockvolume manifest"
for _ in $(seq 1 180); do
  if kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c 'ls /manifests/*.yaml >/dev/null 2>&1' >>"$POLL_LOG" 2>&1; then
    break
  fi
  sleep 1
done
if ! kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c 'ls /manifests/*.yaml >/dev/null 2>&1' >>"$POLL_LOG" 2>&1; then
  echo "launcher did not write blockvolume manifests" >&2
  exit 1
fi
kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c 'cat /manifests/*.yaml' >"$ARTIFACT_DIR/generated-blockvolume.yaml"

if [[ "$MANUAL_APPLY_BLOCKVOLUMES" == "1" || "$MANUAL_APPLY_BLOCKVOLUMES" == "true" ]]; then
  log "apply generated blockvolume workload"
  kubectl apply -f "$ARTIFACT_DIR/generated-blockvolume.yaml" | tee "$ARTIFACT_DIR/apply-generated-blockvolume.log"
else
  log "wait for product-owned blockvolume workload"
  {
    echo "product-owned lifecycle path: blockmaster reconciler applies generated blockvolume workloads"
    kubectl -n "$BLOCKVOLUME_NAMESPACE" get deploy -l app=sw-blockvolume -o wide || true
  } >"$ARTIFACT_DIR/apply-generated-blockvolume.log" 2>&1
fi
kubectl -n "$BLOCKVOLUME_NAMESPACE" wait --for=condition=available deploy -l app=sw-blockvolume --timeout=120s
check_same_node_loopback_contract

if [[ "$BREAK_AFTER_BLOCKVOLUME_READY" == "delete-generated-blockvolume" ]]; then
  log "delete generated blockvolume after ready"
  BLOCKVOLUME_VOLUME_ID="$(generated_blockvolume_arg "volume-id")"
  kubectl -n "$BLOCKVOLUME_NAMESPACE" delete deploy -l "sw-block.seaweedfs.com/volume=${BLOCKVOLUME_VOLUME_ID}" --wait=true --timeout=60s | tee "$ARTIFACT_DIR/delete-generated-blockvolume-after-ready.log"
fi

if [[ -n "$AFTER_BLOCKVOLUME_READY_CMD" ]]; then
  log "run after-blockvolume-ready hook"
  bash -lc "$AFTER_BLOCKVOLUME_READY_CMD" | tee "$ARTIFACT_DIR/after-blockvolume-ready-hook.log"
fi

if [[ "$DEMO_STOP_AFTER" == "blockvolume-ready" ]]; then
  log "controlled stop after blockvolume ready"
  collect_ops_status_bundle
  if [[ "$KEEP_ON_STOP" == "1" || "$KEEP_ON_STOP" == "true" ]]; then
    log "keeping resources for retry validation"
    {
      echo "resources-kept: true"
      echo "cleanup-required: bash scripts/uninstall-k8s-alpha.sh \"$ROOT\""
    } >>"$ARTIFACT_DIR/controlled-stop.txt"
    collect_logs
    trap - EXIT
  fi
  exit 42
fi

log "wait for app writer completion"
WRITER_WAIT_FAILED=0
if [[ "$DEMO_STOP_AFTER" == "writer-verified" || "$RESTART_CSI_NODE_BEFORE_READER" == "1" || "$RESTART_CSI_NODE_BEFORE_READER" == "true" || "$RESTART_BLOCKVOLUME_BEFORE_READER" == "1" || "$RESTART_BLOCKVOLUME_BEFORE_READER" == "true" ]]; then
  if ! wait_pod_log_contains sw-block-demo-writer "[app-writer] wrote and verified /data/demo.bin" "$WRITER_TIMEOUT"; then
    WRITER_WAIT_FAILED=1
  fi
else
  if ! wait_pod_succeeded sw-block-demo-writer "$WRITER_TIMEOUT"; then
    WRITER_WAIT_FAILED=1
  fi
fi
kubectl -n "$NAMESPACE" logs sw-block-demo-writer | tee "$ARTIFACT_DIR/writer.log"
kubectl -n "$NAMESPACE" describe pod sw-block-demo-writer >"$ARTIFACT_DIR/writer.describe.before-delete.txt" 2>&1 || true

if [[ "$DEMO_STOP_AFTER" == "writer-verified" ]]; then
  if [[ "$WRITER_WAIT_FAILED" == "1" ]]; then
    log "controlled stop after writer verification failed; capture mounted host path evidence before cleanup"
  else
    log "controlled stop after writer verified while PVC is still mounted"
  fi
  kubectl -n kube-system logs -l app=sw-block-csi-node -c block-csi --tail=-1 >"$ARTIFACT_DIR/blockcsi-node.log" 2>&1 || true
  capture_stage2_host_path_evidence "writer-mounted"
  collect_ops_inventory_bundle "$ARTIFACT_DIR/ops-inventory-writer-verified" || true
  {
    if [[ "$WRITER_WAIT_FAILED" == "1" ]]; then
      echo "phase=writer-verified-failed"
      echo "writer_verified=false"
      echo "writer_log=$ARTIFACT_DIR/writer.log"
    else
      echo "phase=writer-verified"
      echo "writer_verified=true"
    fi
    echo "host_path_evidence=multipath.writer-mounted.txt,sg-rtpg.writer-mounted.txt,iscsi-sessions.writer-mounted.txt"
    echo "ops_inventory_dir=$ARTIFACT_DIR/ops-inventory-writer-verified"
  } >"$ARTIFACT_DIR/controlled-stop-writer-verified.txt"
  if [[ "$WRITER_WAIT_FAILED" == "1" ]]; then
    exit 1
  fi
  if [[ "$KEEP_ON_STOP" == "1" || "$KEEP_ON_STOP" == "true" ]]; then
    log "keeping resources after writer verified"
    {
      echo "resources-kept: true"
      echo "cleanup-required: bash scripts/uninstall-k8s-alpha.sh \"$ROOT\""
    } >>"$ARTIFACT_DIR/controlled-stop-writer-verified.txt"
    collect_logs
    trap - EXIT
  fi
  exit 44
fi

if [[ "$WRITER_WAIT_FAILED" == "1" ]]; then
  exit 1
fi

if [[ "$RESTART_CSI_NODE_BEFORE_READER" == "1" || "$RESTART_CSI_NODE_BEFORE_READER" == "true" ]]; then
  log "restart CSI node DaemonSet before replacing the app pod"
  kubectl -n kube-system rollout restart ds/sw-block-csi-node | tee "$ARTIFACT_DIR/restart-csi-node.log"
  kubectl -n kube-system rollout status ds/sw-block-csi-node --timeout=180s | tee "$ARTIFACT_DIR/restart-csi-node-status.log"
  kubectl -n kube-system get pods -l app=sw-block-csi-node -o wide >"$ARTIFACT_DIR/csi-node-pods.after-restart.txt" 2>&1 || true
fi

restart_blockvolume_deployment() {
  log "restart generated blockvolume Deployment before replacing the app pod"
  BLOCKVOLUME_DEPLOY="$(kubectl -n "$BLOCKVOLUME_NAMESPACE" get deploy -l app=sw-blockvolume -o name | head -n 1)"
  if [[ -z "$BLOCKVOLUME_DEPLOY" ]]; then
    echo "generated blockvolume Deployment not found before restart" >&2
    exit 1
  fi
  echo "$BLOCKVOLUME_DEPLOY" >"$ARTIFACT_DIR/blockvolume-deploy.before-restart.txt"
  kubectl -n "$BLOCKVOLUME_NAMESPACE" get "$BLOCKVOLUME_DEPLOY" -o yaml >"$ARTIFACT_DIR/blockvolume-deploy.before-restart.yaml"
  kubectl -n "$BLOCKVOLUME_NAMESPACE" get pods -l sw-block.seaweedfs.com/volume -o wide >"$ARTIFACT_DIR/blockvolume-pods.before-restart.txt" 2>&1 || true
  capture_blockvolume_pod_ids "$ARTIFACT_DIR/blockvolume-pod-ids.before-restart.tsv"
  if [[ -z "$(blockvolume_pod_uids "$ARTIFACT_DIR/blockvolume-pod-ids.before-restart.tsv")" ]]; then
    echo "no blockvolume pod UID captured before restart" >&2
    exit 1
  fi
  kubectl -n "$BLOCKVOLUME_NAMESPACE" rollout restart "$BLOCKVOLUME_DEPLOY" | tee "$ARTIFACT_DIR/restart-blockvolume.log"
  kubectl -n "$BLOCKVOLUME_NAMESPACE" rollout status "$BLOCKVOLUME_DEPLOY" --timeout=180s | tee "$ARTIFACT_DIR/restart-blockvolume-status.log"
  kubectl -n "$BLOCKVOLUME_NAMESPACE" get pods -l sw-block.seaweedfs.com/volume -o wide >"$ARTIFACT_DIR/blockvolume-pods.after-restart.txt" 2>&1 || true
  capture_blockvolume_pod_ids "$ARTIFACT_DIR/blockvolume-pod-ids.after-restart.tsv"
  if [[ -z "$(blockvolume_pod_uids "$ARTIFACT_DIR/blockvolume-pod-ids.after-restart.tsv")" ]]; then
    echo "no blockvolume pod UID captured after restart" >&2
    exit 1
  fi
  if [[ "$(blockvolume_pod_uids "$ARTIFACT_DIR/blockvolume-pod-ids.before-restart.tsv")" == "$(blockvolume_pod_uids "$ARTIFACT_DIR/blockvolume-pod-ids.after-restart.tsv")" ]]; then
    echo "blockvolume pod UID did not change across rollout restart" >&2
    exit 1
  fi
  kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c 'cat /var/lib/sw-block/lifecycle/volumes/*.json' >"$ARTIFACT_DIR/lifecycle-volumes.after-blockvolume-restart.json" 2>"$ARTIFACT_DIR/lifecycle-volumes.after-blockvolume-restart.err" || true
  BLOCKVOLUME_VOLUME_ID="$(generated_blockvolume_arg "volume-id")"
  BLOCKVOLUME_STATUS_ADDR="$(generated_blockvolume_arg "status-addr")"
  echo "$BLOCKVOLUME_STATUS_ADDR" >"$ARTIFACT_DIR/blockvolume-status-addr.txt"
  wait_blockvolume_durable_status "$BLOCKVOLUME_VOLUME_ID" "$BLOCKVOLUME_STATUS_ADDR" "$ARTIFACT_DIR/status-durable-after-blockvolume-restart.json" 120
  wait_blockvolume_log_pattern "$BLOCKVOLUME_DEPLOY" "\"phase\":\"iscsi-listening\"" "$ARTIFACT_DIR/blockvolume-generated.after-restart.log" 120
  if [[ "$BREAK_AFTER_BLOCKVOLUME_RESTART" == "scale-generated-blockvolume-to-zero" ]]; then
    log "break after blockvolume restart: scale generated blockvolume to zero"
    kubectl -n "$BLOCKVOLUME_NAMESPACE" scale "$BLOCKVOLUME_DEPLOY" --replicas=0 | tee "$ARTIFACT_DIR/scale-blockvolume-zero-after-restart.log"
    kubectl -n "$BLOCKVOLUME_NAMESPACE" rollout status "$BLOCKVOLUME_DEPLOY" --timeout=60s | tee "$ARTIFACT_DIR/scale-blockvolume-zero-after-restart-status.log"
    for _ in $(seq 1 60); do
      if ! kubectl -n "$BLOCKVOLUME_NAMESPACE" get pods -l "sw-block.seaweedfs.com/volume=${BLOCKVOLUME_VOLUME_ID}" --no-headers 2>/dev/null | grep -q .; then
        break
      fi
      sleep 1
    done
    kubectl -n "$BLOCKVOLUME_NAMESPACE" get pods -l "sw-block.seaweedfs.com/volume=${BLOCKVOLUME_VOLUME_ID}" -o wide >"$ARTIFACT_DIR/blockvolume-pods.after-scale-zero.txt" 2>&1 || true
    log "collect inventory after forced blockvolume unavailability"
    collect_ops_inventory_after_restart
    exit 43
  fi
  if [[ "$COLLECT_INVENTORY_AFTER_RESTART" == "1" || "$COLLECT_INVENTORY_AFTER_RESTART" == "true" ]]; then
    log "collect inventory after blockvolume restart"
    collect_ops_inventory_after_restart
  fi
}

log "delete writer pod but keep PVC"
kubectl -n "$NAMESPACE" delete pod sw-block-demo-writer --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-writer.log"

if [[ "$DEMO_STOP_AFTER" == "promotion-ready" ]]; then
  log "wait for writer unstage before promotion-ready check"
  wait_no_swblock_iscsi_sessions "$ARTIFACT_DIR/iscsi-sessions.before-promotion-ready.txt" 120
  wait_promotion_ready_before_reader
fi

if [[ "$RESTART_BLOCKVOLUME_BEFORE_READER" == "1" || "$RESTART_BLOCKVOLUME_BEFORE_READER" == "true" ]]; then
  log "wait for writer unstage before blockvolume restart"
  wait_no_swblock_iscsi_sessions "$ARTIFACT_DIR/iscsi-sessions.before-blockvolume-restart.txt" 120
  restart_blockvolume_deployment
fi

if [[ -n "$FAIL_PRIMARY_BEFORE_READER" ]]; then
  log "wait for writer unstage before primary failure"
  wait_no_swblock_iscsi_sessions "$ARTIFACT_DIR/iscsi-sessions.before-primary-failure.txt" 120
  fail_primary_before_reader
fi

log "start reader pod on the same PVC"
kubectl apply -f "$READER_RENDERED" | tee "$ARTIFACT_DIR/apply-reader.log"
wait_pod_succeeded sw-block-demo-reader 240
kubectl -n "$NAMESPACE" logs sw-block-demo-reader | tee "$ARTIFACT_DIR/reader.log"
kubectl -n "$NAMESPACE" describe pod sw-block-demo-reader >"$ARTIFACT_DIR/reader.describe.before-delete.txt" 2>&1 || true
capture_iscsi_sessions_to "$ARTIFACT_DIR/iscsi-sessions.after-reader.txt"
kubectl -n kube-system logs -l app=sw-block-csi-node -c block-csi --tail=-1 >"$ARTIFACT_DIR/blockcsi-node.log" 2>&1 || true

log "reader verified data written by previous app pod"

if [[ -f "$ARTIFACT_DIR/primary-failure-recovery.txt" ]]; then
  {
    echo "reader_verified=true"
    echo "data_check_after_failover=reader_checksum_passed"
    if [[ "$FAIL_PRIMARY_BEFORE_READER" == "cordon-node-scale-primary-to-zero" ]]; then
      echo "pod_recreate_used=true"
      echo "node_loss_recovery_claimed=true"
      echo "data_check_after_node_loss=reader_checksum_passed"
      echo "transparent_failover_claimed=false"
    fi
    echo "reader_log=$ARTIFACT_DIR/reader.log"
  } >>"$ARTIFACT_DIR/primary-failure-recovery.txt"
  if [[ "$FAIL_PRIMARY_BEFORE_READER" == "cordon-node-scale-primary-to-zero" ]]; then
    cp "$ARTIFACT_DIR/primary-failure-recovery.txt" "$ARTIFACT_DIR/node-loss-recovery-boundary.txt"
    write_node_loss_recovery_summary "$ARTIFACT_DIR/primary-failure-recovery.txt" "$ARTIFACT_DIR/node-loss-recovery-summary.txt"
  fi
  write_control_plane_timeline_event "$ARTIFACT_DIR/control-plane-timeline.txt" "csi_reattach_observed" "reader_pod=sw-block-demo-reader" "method=pod-recreate" "log=$ARTIFACT_DIR/blockcsi-node.log"
  write_control_plane_timeline_event "$ARTIFACT_DIR/control-plane-timeline.txt" "data_check" "reader_verified=true" "result=reader_checksum_passed" "log=$ARTIFACT_DIR/reader.log"
fi

if [[ "$DEMO_STOP_AFTER" == "reader-verified" ]]; then
  log "controlled stop after reader verified"
  collect_ops_inventory_bundle "$ARTIFACT_DIR/ops-inventory-reader-verified" || true
  echo "phase=reader-verified" >"$ARTIFACT_DIR/controlled-stop-reader-verified.txt"
  echo "ops_inventory_dir=$ARTIFACT_DIR/ops-inventory-reader-verified" >>"$ARTIFACT_DIR/controlled-stop-reader-verified.txt"
  if [[ "$KEEP_ON_STOP" == "1" || "$KEEP_ON_STOP" == "true" ]]; then
    log "keeping resources after reader verified"
    {
      echo "resources-kept: true"
      echo "cleanup-required: bash scripts/uninstall-k8s-alpha.sh \"$ROOT\""
    } >>"$ARTIFACT_DIR/controlled-stop-reader-verified.txt"
    collect_logs
    trap - EXIT
  fi
  exit 44
fi

log "delete reader pod and PVC"
kubectl -n "$NAMESPACE" delete pod sw-block-demo-reader --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-reader.log"
kubectl -n "$NAMESPACE" delete pvc sw-block-demo-pvc --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-pvc.log"

log "wait for launcher manifest cleanup after DeleteVolume"
for _ in $(seq 1 180); do
  if kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c '! ls /manifests/*.yaml >/dev/null 2>&1' >>"$POLL_LOG" 2>&1; then
    break
  fi
  sleep 1
done
if ! kubectl -n kube-system exec deploy/sw-blockmaster -c blockmaster -- sh -c '! ls /manifests/*.yaml >/dev/null 2>&1' >>"$POLL_LOG" 2>&1; then
  echo "launcher manifest still present after PVC delete" >&2
  exit 1
fi

if [[ "$BLOCKVOLUME_NAMESPACE" == "kube-system" ]]; then
  log "delete generated blockvolume Deployment after manifest cleanup"
  kubectl -n kube-system delete deploy -l app=sw-blockvolume --ignore-not-found=true --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-generated-blockvolume.log"
else
  log "wait for Kubernetes GC to delete PVC-owned blockvolume Deployment"
  for _ in $(seq 1 180); do
    if ! kubectl -n "$BLOCKVOLUME_NAMESPACE" get deploy -l app=sw-blockvolume --no-headers 2>/dev/null | grep -q .; then
      break
    fi
    sleep 1
  done
fi

collect_post_delete_state

if kubectl -n "$BLOCKVOLUME_NAMESPACE" get deploy -l app=sw-blockvolume --no-headers 2>/dev/null | grep -q .; then
  echo "generated blockvolume deployment still present" >&2
  exit 1
fi
if kubectl -n "$NAMESPACE" get pvc sw-block-demo-pvc >/dev/null 2>&1; then
  echo "demo PVC still present" >&2
  exit 1
fi
if grep -q 'iqn.2026-05.io.seaweedfs' "$ARTIFACT_DIR/iscsi-sessions.after-delete.txt" 2>/dev/null; then
  echo "dangling sw-block iSCSI session after delete" >&2
  exit 1
fi

log "PASS: app pod wrote data, replacement app pod read it back through the same PVC, cleanup complete"
log "artifacts=$ARTIFACT_DIR"
