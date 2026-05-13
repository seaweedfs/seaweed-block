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
RESTART_CSI_NODE_BEFORE_READER="${SW_BLOCK_RESTART_CSI_NODE_BEFORE_READER:-0}"
RESTART_BLOCKVOLUME_BEFORE_READER="${SW_BLOCK_RESTART_BLOCKVOLUME_BEFORE_READER:-0}"
DEMO_STOP_AFTER="${SW_BLOCK_DEMO_STOP_AFTER:-}"
COLLECT_OPS_STATUS="${SW_BLOCK_DEMO_COLLECT_OPS_STATUS:-0}"
KEEP_ON_STOP="${SW_BLOCK_DEMO_KEEP_ON_STOP:-0}"
AFTER_BLOCKVOLUME_READY_CMD="${SW_BLOCK_DEMO_AFTER_BLOCKVOLUME_READY_CMD:-}"
BREAK_AFTER_BLOCKVOLUME_READY="${SW_BLOCK_DEMO_BREAK_AFTER_BLOCKVOLUME_READY:-}"
WRITER_TIMEOUT="${SW_BLOCK_DEMO_WRITER_TIMEOUT:-240}"
DELETE_ALL_BLOCKVOLUMES="${SW_BLOCK_UNINSTALL_DELETE_ALL_BLOCKVOLUMES:-0}"
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
if [[ -n "$DEMO_STOP_AFTER" && "$DEMO_STOP_AFTER" != "blockvolume-ready" ]]; then
  echo "unsupported SW_BLOCK_DEMO_STOP_AFTER value: $DEMO_STOP_AFTER" >&2
  exit 2
fi
if [[ -n "$BREAK_AFTER_BLOCKVOLUME_READY" && "$BREAK_AFTER_BLOCKVOLUME_READY" != "delete-generated-blockvolume" ]]; then
  echo "unsupported SW_BLOCK_DEMO_BREAK_AFTER_BLOCKVOLUME_READY value: $BREAK_AFTER_BLOCKVOLUME_READY" >&2
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
  if [[ "$RESTART_BLOCKVOLUME_BEFORE_READER" == "1" || "$RESTART_BLOCKVOLUME_BEFORE_READER" == "true" ]]; then
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

NODE_NAME="$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')"
STACK_RENDERED="$ARTIFACT_DIR/block-stack.rendered.yaml"
CSI_CONTROLLER_RENDERED="$ARTIFACT_DIR/csi-controller.rendered.yaml"
CSI_NODE_RENDERED="$ARTIFACT_DIR/csi-node.rendered.yaml"
IMAGE_SED="$(sed_escape "$IMAGE")"
CSI_IMAGE_SED="$(sed_escape "$CSI_IMAGE")"
sed -e "s/__NODE_NAME__/${NODE_NAME}/g" \
  -e "s/sw-block:local/${IMAGE_SED}/g" \
  -e "s/imagePullPolicy: Never/imagePullPolicy: IfNotPresent/g" \
  "$ROOT/deploy/k8s/alpha/block-stack.yaml" >"$STACK_RENDERED"
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
if [[ "$RESTART_BLOCKVOLUME_BEFORE_READER" == "1" || "$RESTART_BLOCKVOLUME_BEFORE_READER" == "true" || "$COLLECT_OPS_STATUS" == "1" || "$COLLECT_OPS_STATUS" == "true" || -n "$DEMO_STOP_AFTER" ]]; then
  awk '/--launcher-durable-root=/{print; print "            - \"--launcher-status\""; next} {print}' "$STACK_RENDERED" >"$STACK_RENDERED.tmp"
  mv "$STACK_RENDERED.tmp" "$STACK_RENDERED"
  grep -q -- '--launcher-status' "$STACK_RENDERED" || { echo "failed to inject --launcher-status into $STACK_RENDERED" >&2; exit 1; }
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

log "artifact_dir=$ARTIFACT_DIR"
log "root=$ROOT"
log "namespace=$NAMESPACE"
log "node=$NODE_NAME"
log "image=$IMAGE"
log "csi_image=$CSI_IMAGE"
log "blockvolume_namespace=$BLOCKVOLUME_NAMESPACE"
log "launcher_pvc_owner_ref=$LAUNCHER_PVC_OWNER_REF"
log "launcher_state_hostpath=${LAUNCHER_STATE_HOSTPATH:-<emptyDir>}"
log "restart_csi_node_before_reader=$RESTART_CSI_NODE_BEFORE_READER"
log "restart_blockvolume_before_reader=$RESTART_BLOCKVOLUME_BEFORE_READER"
log "demo_stop_after=${DEMO_STOP_AFTER:-<none>}"
log "collect_ops_status=$COLLECT_OPS_STATUS"
log "keep_on_stop=$KEEP_ON_STOP"
log "after_blockvolume_ready_cmd=${AFTER_BLOCKVOLUME_READY_CMD:-<none>}"
log "break_after_blockvolume_ready=${BREAK_AFTER_BLOCKVOLUME_READY:-<none>}"
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

log "apply demo app PVC and writer pod"
kubectl apply -f "$DEMO_APP_MANIFEST" | tee "$ARTIFACT_DIR/apply-demo-app.log"

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

log "apply generated blockvolume workload"
kubectl apply -f "$ARTIFACT_DIR/generated-blockvolume.yaml" | tee "$ARTIFACT_DIR/apply-generated-blockvolume.log"
kubectl -n "$BLOCKVOLUME_NAMESPACE" wait --for=condition=available deploy -l app=sw-blockvolume --timeout=120s

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
if [[ "$RESTART_CSI_NODE_BEFORE_READER" == "1" || "$RESTART_CSI_NODE_BEFORE_READER" == "true" || "$RESTART_BLOCKVOLUME_BEFORE_READER" == "1" || "$RESTART_BLOCKVOLUME_BEFORE_READER" == "true" ]]; then
  wait_pod_log_contains sw-block-demo-writer "[app-writer] wrote and verified /data/demo.bin" "$WRITER_TIMEOUT"
else
  wait_pod_succeeded sw-block-demo-writer "$WRITER_TIMEOUT"
fi
kubectl -n "$NAMESPACE" logs sw-block-demo-writer | tee "$ARTIFACT_DIR/writer.log"
kubectl -n "$NAMESPACE" describe pod sw-block-demo-writer >"$ARTIFACT_DIR/writer.describe.before-delete.txt" 2>&1 || true

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
}

log "delete writer pod but keep PVC"
kubectl -n "$NAMESPACE" delete pod sw-block-demo-writer --wait=true --timeout=120s | tee "$ARTIFACT_DIR/delete-writer.log"

if [[ "$RESTART_BLOCKVOLUME_BEFORE_READER" == "1" || "$RESTART_BLOCKVOLUME_BEFORE_READER" == "true" ]]; then
  log "wait for writer unstage before blockvolume restart"
  wait_no_swblock_iscsi_sessions "$ARTIFACT_DIR/iscsi-sessions.before-blockvolume-restart.txt" 120
  restart_blockvolume_deployment
fi

log "start reader pod on the same PVC"
kubectl apply -f "$ROOT/deploy/k8s/alpha/demo-app-reader-pod.yaml" | tee "$ARTIFACT_DIR/apply-reader.log"
wait_pod_succeeded sw-block-demo-reader 240
kubectl -n "$NAMESPACE" logs sw-block-demo-reader | tee "$ARTIFACT_DIR/reader.log"
kubectl -n "$NAMESPACE" describe pod sw-block-demo-reader >"$ARTIFACT_DIR/reader.describe.before-delete.txt" 2>&1 || true
capture_iscsi_sessions_to "$ARTIFACT_DIR/iscsi-sessions.after-reader.txt"
kubectl -n kube-system logs -l app=sw-block-csi-node -c block-csi --tail=-1 >"$ARTIFACT_DIR/blockcsi-node.log" 2>&1 || true

log "reader verified data written by previous app pod"

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
