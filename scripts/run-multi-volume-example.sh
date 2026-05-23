#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-multi-volume-$(date -u +%Y%m%dT%H%M%SZ)}"
NAMESPACE="${SW_BLOCK_APP_NAMESPACE:-default}"
VOLUME_COUNT="${SW_BLOCK_MULTI_VOLUME_COUNT:-3}"
REPLICATION_FACTOR="${SW_BLOCK_MULTI_VOLUME_RF:-1}"
STORAGECLASS_NAME="${SW_BLOCK_MULTI_VOLUME_STORAGECLASS:-sw-block-multi}"
PVC_PREFIX="${SW_BLOCK_MULTI_VOLUME_PVC_PREFIX:-sw-block-multi-pvc}"
POD_PREFIX="${SW_BLOCK_MULTI_VOLUME_POD_PREFIX:-sw-block-multi}"
CHAP_SECRET_NAME="${SW_BLOCK_ISCSI_CHAP_SECRET_NAME:-sw-block-iscsi-chap}"
MASTER_PORT="${SW_BLOCK_MASTER_PORT_FORWARD_PORT:-}"
CLEANUP_REQUESTED="${SW_BLOCK_MULTI_VOLUME_CLEANUP:-1}"

MULTI_VOLUME_STATUS="ok"
FAILED_PHASE=""
CLEANUP_STATUS="external_to_script"
if [[ "$CLEANUP_REQUESTED" == "1" || "$CLEANUP_REQUESTED" == "true" ]]; then
  CLEANUP_STATUS="pending"
fi

mkdir -p "$ARTIFACT_DIR"/{manifests,logs,status}

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

log() {
  printf '[multi-volume] %s\n' "$*" | tee -a "$ARTIFACT_DIR/run.log"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

safe_capture() {
  local out="$1"
  shift
  "$@" >"$out" 2>&1 || true
}

sw_block_cmd() {
  if [[ -n "${SW_BLOCK_CLI:-}" ]]; then
    "$SW_BLOCK_CLI" "$@"
  elif command -v sw-block >/dev/null 2>&1; then
    sw-block "$@"
  else
    go run ./cmd/sw-block "$@"
  fi
}

find_free_port() {
  if command -v python3 >/dev/null 2>&1; then
    python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
    return
  fi
  for port in $(seq 29333 29433); do
    if ! (echo >"/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
      echo "$port"
      return
    fi
  done
  echo "9333"
}

wait_for_port() {
  local port="$1"
  for _ in $(seq 1 30); do
    if (echo >"/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

stop_port_forward() {
  if [[ -n "${PF_PID:-}" ]]; then
    kill "$PF_PID" >/dev/null 2>&1 || true
    wait "$PF_PID" >/dev/null 2>&1 || true
  fi
}

render_storageclass() {
  local out="$ARTIFACT_DIR/manifests/storageclass.yaml"
  {
    echo "apiVersion: storage.k8s.io/v1"
    echo "kind: StorageClass"
    echo "metadata:"
    echo "  name: $STORAGECLASS_NAME"
    echo "provisioner: block.csi.seaweedfs.com"
    echo "volumeBindingMode: Immediate"
    echo "allowVolumeExpansion: false"
    echo "parameters:"
    echo "  replicationFactor: \"$REPLICATION_FACTOR\""
    if kubectl -n "$NAMESPACE" get secret "$CHAP_SECRET_NAME" >/dev/null 2>&1; then
      echo "  csi.storage.k8s.io/node-stage-secret-name: \"$CHAP_SECRET_NAME\""
      echo "  csi.storage.k8s.io/node-stage-secret-namespace: \"$NAMESPACE\""
    fi
  } >"$out"
  echo "$out"
}

render_pvc() {
  local idx="$1"
  local out="$ARTIFACT_DIR/manifests/pvc-$idx.yaml"
  cat >"$out" <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${PVC_PREFIX}-${idx}
  namespace: ${NAMESPACE}
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: ${STORAGECLASS_NAME}
  resources:
    requests:
      storage: 1Mi
YAML
  echo "$out"
}

render_writer() {
  local idx="$1"
  local out="$ARTIFACT_DIR/manifests/writer-$idx.yaml"
  cat >"$out" <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${POD_PREFIX}-writer-${idx}
  namespace: ${NAMESPACE}
spec:
  restartPolicy: Never
  containers:
    - name: app
      image: busybox:1.36
      command:
        - /bin/sh
        - -c
        - |
          set -eu
          echo "[writer-$idx] writing through PVC mounted at /data"
          dd if=/dev/urandom of=/data/demo.bin bs=4096 count=1
          sha256sum /data/demo.bin > /data/demo.sha256
          sync
          sha256sum -c /data/demo.sha256
          echo "/data/demo.bin: OK"
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: ${PVC_PREFIX}-${idx}
YAML
  echo "$out"
}

render_reader() {
  local idx="$1"
  local out="$ARTIFACT_DIR/manifests/reader-$idx.yaml"
  cat >"$out" <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${POD_PREFIX}-reader-${idx}
  namespace: ${NAMESPACE}
spec:
  restartPolicy: Never
  containers:
    - name: app
      image: busybox:1.36
      command:
        - /bin/sh
        - -c
        - |
          set -eu
          echo "[reader-$idx] reading existing data from PVC mounted at /data"
          test -s /data/demo.bin
          test -s /data/demo.sha256
          sha256sum -c /data/demo.sha256
          echo "/data/demo.bin: OK"
      volumeMounts:
        - name: data
          mountPath: /data
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: ${PVC_PREFIX}-${idx}
YAML
  echo "$out"
}

collect_status_evidence() {
  local out="$ARTIFACT_DIR/status"
  local rc=0
  mkdir -p "$out"
  kubectl -n kube-system port-forward deploy/sw-blockmaster "${MASTER_PORT}:9333" >"$out/blockmaster-port-forward.log" 2>&1 &
  PF_PID=$!
  trap stop_port_forward RETURN
  if ! wait_for_port "$MASTER_PORT"; then
    echo "status evidence failed: blockmaster port-forward did not become ready" >"$out/status-evidence-error.txt"
    rc=1
  fi
  if [[ "$rc" -eq 0 ]] && ! sw_block_cmd ops cluster --master-api "127.0.0.1:${MASTER_PORT}" --timeout 30s -o json >"$out/cluster-evidence.json" 2>"$out/cluster-evidence.stderr.txt"; then
    rc=1
  fi
  if [[ "$rc" -eq 0 ]] && ! sw_block_cmd ops inventory --namespace "$NAMESPACE" --master "127.0.0.1:${MASTER_PORT}" --out "$out/inventory" --timeout 30s >"$out/inventory.stdout.txt" 2>"$out/inventory.stderr.txt"; then
    rc=1
  fi
  if [[ "$rc" -eq 0 ]] && ! sw_block_cmd ops report --from-bundle "$ARTIFACT_DIR" --out "$out/report" >"$out/report.stdout.txt" 2>"$out/report.stderr.txt"; then
    rc=1
  fi
  stop_port_forward
  unset PF_PID
  trap - RETURN
  return "$rc"
}

cleanup_multi_volume() {
  local rc=0
  for idx in $(seq 1 "$VOLUME_COUNT"); do
    kubectl -n "$NAMESPACE" delete pod "${POD_PREFIX}-reader-${idx}" "${POD_PREFIX}-writer-${idx}" --ignore-not-found=true --wait=true --timeout=120s || rc=1
  done
  for idx in $(seq 1 "$VOLUME_COUNT"); do
    kubectl -n "$NAMESPACE" delete pvc "${PVC_PREFIX}-${idx}" --ignore-not-found=true --wait=true --timeout=120s || rc=1
  done
  # RF>1 creates several generated blockvolume Deployments per PVC. The
  # launcher removes them asynchronously after PVC deletion; wait before
  # declaring helper-level cleanup success.
  for _ in $(seq 1 90); do
    local remaining
    remaining="$(kubectl -n "$NAMESPACE" get deploy -l app=sw-blockvolume -o name 2>/dev/null || true)"
    if [[ -z "$remaining" ]]; then
      break
    fi
    sleep 2
  done
  if kubectl -n "$NAMESPACE" get deploy -l app=sw-blockvolume -o name 2>/dev/null | grep -q .; then
    safe_capture "$ARTIFACT_DIR/logs/blockvolume-deployments.cleanup-timeout.txt" kubectl -n "$NAMESPACE" get deploy -l app=sw-blockvolume -o wide
    rc=1
  fi
  kubectl delete storageclass "$STORAGECLASS_NAME" --ignore-not-found=true --wait=true --timeout=120s || rc=1
  return "$rc"
}

capture_failure_diagnostics() {
  local phase="$1"
  local out="$ARTIFACT_DIR/diagnostics/$phase"
  mkdir -p "$out"
  safe_capture "$out/pods.txt" kubectl -n "$NAMESPACE" get pods -o wide
  safe_capture "$out/pvcs.txt" kubectl -n "$NAMESPACE" get pvc -o wide
  safe_capture "$out/blockvolume-deployments.txt" kubectl -n "$NAMESPACE" get deploy -l app=sw-blockvolume -o wide
  safe_capture "$out/blockvolume-deployments.yaml" kubectl -n "$NAMESPACE" get deploy -l app=sw-blockvolume -o yaml
  safe_capture "$out/blockvolume-pods.yaml" kubectl -n "$NAMESPACE" get pods -l app=sw-blockvolume -o yaml
  safe_capture "$out/volumeattachments.txt" kubectl get volumeattachments -o wide
  safe_capture "$out/volumeattachments.yaml" kubectl get volumeattachments -o yaml
  safe_capture "$out/events.txt" kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp
  safe_capture "$out/kube-system.txt" kubectl -n kube-system get pods,deploy,ds,svc -o wide
  safe_capture "$out/blockmaster.log" kubectl -n kube-system logs deploy/sw-blockmaster --tail=400
  safe_capture "$out/csi-controller.log" kubectl -n kube-system logs deploy/sw-block-csi-controller --all-containers --tail=400
  safe_capture "$out/csi-node.log" kubectl -n kube-system logs ds/sw-block-csi-node --all-containers --tail=400
  safe_capture "$out/blockvolume.log" kubectl -n "$NAMESPACE" logs -l sw-block.seaweedfs.com/volume --all-containers --tail=400
  safe_capture "$out/iscsi-sessions.txt" sudo -n iscsiadm -m session
  local diag_port diag_pf_pid
  diag_port="$(find_free_port)"
  kubectl -n kube-system port-forward deploy/sw-blockmaster "${diag_port}:9333" >"$out/blockmaster-port-forward.log" 2>&1 &
  diag_pf_pid=$!
  if wait_for_port "$diag_port"; then
    sw_block_cmd ops cluster --master-api "127.0.0.1:${diag_port}" --timeout 30s -o json >"$out/cluster-evidence.json" 2>"$out/cluster-evidence.stderr.txt" || true
    sw_block_cmd ops inventory --namespace "$NAMESPACE" --master "127.0.0.1:${diag_port}" --out "$out/inventory" --timeout 30s >"$out/inventory.stdout.txt" 2>"$out/inventory.stderr.txt" || true
  fi
  kill "$diag_pf_pid" >/dev/null 2>&1 || true
  wait "$diag_pf_pid" >/dev/null 2>&1 || true
}

write_summary() {
  local writer_count reader_count managed_count inventory_status
  writer_count="$(grep -R -l '/data/demo.bin: OK' "$ARTIFACT_DIR/logs"/writer-*.log 2>/dev/null | wc -l | tr -d ' ')"
  reader_count="$(grep -R -l '/data/demo.bin: OK' "$ARTIFACT_DIR/logs"/reader-*.log 2>/dev/null | wc -l | tr -d ' ')"
  managed_count="$(grep -c '^managed_volume=' "$ARTIFACT_DIR/status/report/summary.txt" 2>/dev/null || true)"
  inventory_status="$(sed -n 's/^inventory_status: //p' "$ARTIFACT_DIR/status/inventory/volume-inventory-summary.txt" 2>/dev/null | head -1)"
  {
    echo "multi_volume_status=$MULTI_VOLUME_STATUS"
    if [[ -n "$FAILED_PHASE" ]]; then
      echo "failed_phase=$FAILED_PHASE"
    fi
    echo "namespace=$NAMESPACE"
    echo "storageclass=$STORAGECLASS_NAME"
    echo "requested_volume_count=$VOLUME_COUNT"
    echo "replication_factor=$REPLICATION_FACTOR"
    echo "writer_verified_count=$writer_count"
    echo "reader_verified_count=$reader_count"
    echo "managed_volume_count=$managed_count"
    echo "inventory_status=${inventory_status:-unknown}"
    echo "status_report=status/report/index.html"
    echo "cleanup_status=$CLEANUP_STATUS"
  } >"$ARTIFACT_DIR/multi-volume-summary.txt"
}

require_cmd kubectl
if [[ "$NAMESPACE" != "default" ]]; then
  echo "SW_BLOCK_APP_NAMESPACE=$NAMESPACE is not supported by this helper; use default" >&2
  exit 2
fi
case "$VOLUME_COUNT" in
  ''|*[!0-9]*|0)
    echo "SW_BLOCK_MULTI_VOLUME_COUNT must be a positive integer; got: $VOLUME_COUNT" >&2
    exit 2
    ;;
esac
if [[ -z "$MASTER_PORT" ]]; then
  MASTER_PORT="$(find_free_port)"
fi

log "artifact_dir=$ARTIFACT_DIR"
log "namespace=$NAMESPACE"
log "volume_count=$VOLUME_COUNT"
log "master_port=$MASTER_PORT"

kubectl apply -f "$(render_storageclass)" | tee "$ARTIFACT_DIR/apply-storageclass.log"

for idx in $(seq 1 "$VOLUME_COUNT"); do
  log "create pvc $idx"
  kubectl apply -f "$(render_pvc "$idx")" | tee "$ARTIFACT_DIR/logs/apply-pvc-$idx.log"
  kubectl -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Bound "pvc/${PVC_PREFIX}-${idx}" --timeout=180s | tee "$ARTIFACT_DIR/logs/wait-pvc-$idx.log"
  safe_capture "$ARTIFACT_DIR/logs/pvc-$idx.after-bound.txt" kubectl -n "$NAMESPACE" get pvc "${PVC_PREFIX}-${idx}" -o wide
done

for idx in $(seq 1 "$VOLUME_COUNT"); do
  log "writer $idx"
  kubectl apply -f "$(render_writer "$idx")" | tee "$ARTIFACT_DIR/logs/apply-writer-$idx.log"
  if ! kubectl -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/${POD_PREFIX}-writer-${idx}" --timeout=240s | tee "$ARTIFACT_DIR/logs/wait-writer-$idx.log"; then
    safe_capture "$ARTIFACT_DIR/logs/writer-$idx.describe.txt" kubectl -n "$NAMESPACE" describe pod "${POD_PREFIX}-writer-${idx}"
    capture_failure_diagnostics "writer-$idx"
    exit 1
  fi
  kubectl -n "$NAMESPACE" logs "${POD_PREFIX}-writer-${idx}" >"$ARTIFACT_DIR/logs/writer-$idx.log"
  grep -q '/data/demo.bin: OK' "$ARTIFACT_DIR/logs/writer-$idx.log"
  kubectl -n "$NAMESPACE" delete pod "${POD_PREFIX}-writer-${idx}" --wait=true --timeout=120s | tee "$ARTIFACT_DIR/logs/delete-writer-$idx.log"
done

for idx in $(seq 1 "$VOLUME_COUNT"); do
  log "reader $idx"
  kubectl apply -f "$(render_reader "$idx")" | tee "$ARTIFACT_DIR/logs/apply-reader-$idx.log"
  if ! kubectl -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/${POD_PREFIX}-reader-${idx}" --timeout=240s | tee "$ARTIFACT_DIR/logs/wait-reader-$idx.log"; then
    safe_capture "$ARTIFACT_DIR/logs/reader-$idx.describe.txt" kubectl -n "$NAMESPACE" describe pod "${POD_PREFIX}-reader-${idx}"
    capture_failure_diagnostics "reader-$idx"
    exit 1
  fi
  kubectl -n "$NAMESPACE" logs "${POD_PREFIX}-reader-${idx}" >"$ARTIFACT_DIR/logs/reader-$idx.log"
  grep -q '/data/demo.bin: OK' "$ARTIFACT_DIR/logs/reader-$idx.log"
done

log "collect status evidence"
if ! collect_status_evidence; then
  MULTI_VOLUME_STATUS="failed"
  FAILED_PHASE="status_evidence"
fi

if [[ "$CLEANUP_REQUESTED" == "1" || "$CLEANUP_REQUESTED" == "true" ]]; then
  log "cleanup multi-volume resources"
  if cleanup_multi_volume; then
    CLEANUP_STATUS="ok"
  else
    CLEANUP_STATUS="failed"
    MULTI_VOLUME_STATUS="failed"
    if [[ -z "$FAILED_PHASE" ]]; then
      FAILED_PHASE="cleanup"
    fi
  fi
fi

write_summary
cat "$ARTIFACT_DIR/multi-volume-summary.txt"

if [[ "$MULTI_VOLUME_STATUS" != "ok" ]]; then
  log "FAIL: multi-volume loop incomplete"
  exit 1
fi
log "PASS: multi-volume loop complete"
