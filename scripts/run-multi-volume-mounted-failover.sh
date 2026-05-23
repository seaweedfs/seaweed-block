#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-multi-volume-mounted-failover-$(date -u +%Y%m%dT%H%M%SZ)}"
NAMESPACE="${SW_BLOCK_APP_NAMESPACE:-default}"
VOLUME_COUNT="${SW_BLOCK_MULTI_VOLUME_COUNT:-3}"
REPLICATION_FACTOR="${SW_BLOCK_MULTI_VOLUME_RF:-3}"
STORAGECLASS_NAME="${SW_BLOCK_MULTI_VOLUME_STORAGECLASS:-sw-block-multi-mounted}"
PVC_PREFIX="${SW_BLOCK_MULTI_VOLUME_PVC_PREFIX:-sw-block-multi-mounted-pvc}"
POD_PREFIX="${SW_BLOCK_MULTI_VOLUME_POD_PREFIX:-sw-block-multi-mounted}"
CHAP_SECRET_NAME="${SW_BLOCK_ISCSI_CHAP_SECRET_NAME:-sw-block-iscsi-chap}"
APP_NODE_SELECTOR="${SW_BLOCK_MULTI_VOLUME_APP_NODE:-${SW_BLOCK_BASIC_APP_NODE_SELECTOR:-}}"
MASTER_PORT="${SW_BLOCK_MASTER_PORT_FORWARD_PORT:-}"
MOUNTED_IO_TIMEOUT="${SW_BLOCK_MOUNTED_IO_TIMEOUT:-180s}"
STALE_IO_PROBE_TIMEOUT="${SW_BLOCK_STALE_IO_PROBE_TIMEOUT:-8s}"
FAILOVER_MODE="${SW_BLOCK_MULTI_VOLUME_FAILOVER_MODE:-sequential}"
TARGET_VOLUME_COUNT="${SW_BLOCK_MULTI_VOLUME_TARGET_COUNT:-$VOLUME_COUNT}"

STATUS="ok"
FAILED_PHASE=""

mkdir -p "$ARTIFACT_DIR"/{setup,failover,status,logs,manifests,host}

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

log() {
  printf '[multi-volume-mounted-failover] %s\n' "$*" | tee -a "$ARTIFACT_DIR/run.log"
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
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
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

start_master_port_forward() {
  if [[ -z "$MASTER_PORT" ]]; then
    MASTER_PORT="$(find_free_port)"
  fi
  kubectl -n kube-system port-forward deploy/sw-blockmaster "${MASTER_PORT}:9333" >"$ARTIFACT_DIR/status/blockmaster-port-forward.log" 2>&1 &
  PF_PID=$!
  wait_for_port "$MASTER_PORT"
}

stop_master_port_forward() {
  if [[ -n "${PF_PID:-}" ]]; then
    kill "$PF_PID" >/dev/null 2>&1 || true
    wait "$PF_PID" >/dev/null 2>&1 || true
  fi
}
trap 'stop_master_port_forward || true' EXIT

collect_cluster() {
  local out="$1"
  mkdir -p "$(dirname "$out")"
  sw_block_cmd ops cluster --master-api "127.0.0.1:${MASTER_PORT}" --timeout 30s -o json >"$out"
  test -s "$out"
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
    echo "  sw-block.seaweedfs.com/protocol: \"iscsi\""
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

render_writer_hold() {
  local idx="$1"
  local out="$ARTIFACT_DIR/manifests/writer-hold-$idx.yaml"
  cat >"$out" <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${POD_PREFIX}-writer-${idx}
  namespace: ${NAMESPACE}
  labels:
    sw-block-test: multi-volume-mounted-failover
    sw-block-test-volume-index: "${idx}"
spec:
  restartPolicy: Never
YAML
  if [[ -n "$APP_NODE_SELECTOR" ]]; then
    cat >>"$out" <<YAML
  nodeSelector:
    kubernetes.io/hostname: ${APP_NODE_SELECTOR}
YAML
  fi
  cat >>"$out" <<YAML
  securityContext:
    runAsNonRoot: true
    runAsUser: 1000
    runAsGroup: 1000
    fsGroup: 1000
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: app
      image: busybox:1.36
      securityContext:
        runAsNonRoot: true
        allowPrivilegeEscalation: false
        capabilities:
          drop:
            - ALL
        seccompProfile:
          type: RuntimeDefault
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
          echo "[writer-$idx] holding pod for mounted failover"
          sleep 3600
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

capture_host_path_evidence() {
  local label="$1"
  capture_iscsi_sessions_to "$ARTIFACT_DIR/host/iscsi-sessions.${label}.txt"
  if command -v sudo >/dev/null 2>&1; then
    sudo multipath -ll >"$ARTIFACT_DIR/host/multipath.${label}.txt" 2>&1 || true
  else
    multipath -ll >"$ARTIFACT_DIR/host/multipath.${label}.txt" 2>&1 || true
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
      else
        sg_rtpg "$raw" || true
      fi
    done
    if [ "$found" -eq 0 ]; then
      echo "no seaweed iSCSI by-path devices"
    fi
  } >"$ARTIFACT_DIR/host/sg-rtpg.${label}.txt" 2>&1
}

capture_iscsi_sessions_to() {
  local path="$1"
  if command -v sudo >/dev/null 2>&1; then
    sudo iscsiadm -m session >"$path" 2>&1 || true
  else
    iscsiadm -m session >"$path" 2>&1 || true
  fi
}

extract_plan() {
  local cluster_json="$1"
  local deploy_json="$2"
  local out="$3"
  python3 - "$cluster_json" "$deploy_json" "$out" <<'PY'
import json, sys
cluster = json.load(open(sys.argv[1]))
deploys = json.load(open(sys.argv[2])).get("items", [])
out = sys.argv[3]
deploy_by_slot = {}
for d in deploys:
    labels = d.get("metadata", {}).get("labels", {}) or {}
    key = (labels.get("sw-block.seaweedfs.com/volume"), labels.get("sw-block.seaweedfs.com/replica"))
    if key[0] and key[1]:
        deploy_by_slot[key] = d.get("metadata", {}).get("name", "")
vols = sorted(cluster.get("volumes", []), key=lambda v: v.get("pvc_name") or v.get("volume_id"))
with open(out, "w", encoding="utf-8") as f:
    for idx, v in enumerate(vols, 1):
        vid = v.get("volume_id", "")
        primary = v.get("primary_replica", "")
        deploy = deploy_by_slot.get((vid, primary), "")
        f.write("\t".join([
            str(idx),
            vid,
            v.get("pvc_name", ""),
            primary,
            v.get("primary_node", ""),
            v.get("publish_target", ""),
            deploy,
        ]) + "\n")
PY
}

volume_field() {
  local cluster_json="$1"
  local volume_id="$2"
  local field="$3"
  python3 - "$cluster_json" "$volume_id" "$field" <<'PY'
import json, sys
doc=json.load(open(sys.argv[1]))
vid=sys.argv[2]
field=sys.argv[3]
for v in doc.get("volumes", []):
    if v.get("volume_id") == vid:
        print(v.get(field, ""))
        break
PY
}

primary_count_for_volume() {
  local cluster_json="$1"
  local volume_id="$2"
  python3 - "$cluster_json" "$volume_id" <<'PY'
import json, sys
doc=json.load(open(sys.argv[1])); vid=sys.argv[2]
for v in doc.get("volumes", []):
    if v.get("volume_id")==vid:
        print(sum(1 for r in v.get("replicas", []) if r.get("role")=="primary"))
        break
PY
}

assert_non_target_stable() {
  local before="$1"
  local after="$2"
  local target_vid="$3"
  python3 - "$before" "$after" "$target_vid" <<'PY'
import json, sys
before=json.load(open(sys.argv[1]))
after=json.load(open(sys.argv[2]))
target=sys.argv[3]
after_by={v.get("volume_id"):v for v in after.get("volumes", [])}
bad=[]
for v in before.get("volumes", []):
    vid=v.get("volume_id")
    if not vid or vid == target:
        continue
    av=after_by.get(vid, {})
    for field in ("primary_replica","primary_node","publish_target"):
        if v.get(field) != av.get(field):
            bad.append(f"{vid}:{field}:{v.get(field)}->{av.get(field)}")
if bad:
    raise SystemExit("non_target_changed=" + ",".join(bad))
PY
}

assert_untouched_stable() {
  local before="$1"
  local after="$2"
  local target_csv="$3"
  python3 - "$before" "$after" "$target_csv" <<'PY'
import json, sys
before=json.load(open(sys.argv[1]))
after=json.load(open(sys.argv[2]))
targets=set(x for x in sys.argv[3].split(",") if x)
after_by={v.get("volume_id"):v for v in after.get("volumes", [])}
bad=[]
for v in before.get("volumes", []):
    vid=v.get("volume_id")
    if not vid or vid in targets:
        continue
    av=after_by.get(vid, {})
    for field in ("primary_replica","primary_node","publish_target"):
        if v.get(field) != av.get(field):
            bad.append(f"{vid}:{field}:{v.get(field)}->{av.get(field)}")
if bad:
    raise SystemExit("untouched_changed=" + ",".join(bad))
PY
}

writer_uid() {
  local idx="$1"
  kubectl -n "$NAMESPACE" get pod "${POD_PREFIX}-writer-${idx}" -o jsonpath='{.metadata.uid}'
}

exec_writer_check_after_failover() {
  local idx="$1"
  local out="$2"
  timeout "$MOUNTED_IO_TIMEOUT" kubectl -n "$NAMESPACE" exec "${POD_PREFIX}-writer-${idx}" -- /bin/sh -c "
set -eu
sha256sum -c /data/demo.sha256
echo '/data/demo.bin: OK'
dd if=/dev/urandom of=/data/demo-after-failover-${idx}.bin bs=4096 count=1
sha256sum /data/demo-after-failover-${idx}.bin > /data/demo-after-failover-${idx}.sha256
sync
sha256sum -c /data/demo-after-failover-${idx}.sha256
echo '/data/demo-after-failover-${idx}.bin: OK'
echo mounted_workload_checksum_passed
" >"$out" 2>&1
}

exec_writer_non_target_check() {
  local idx="$1"
  local round="$2"
  local out="$3"
  timeout "$MOUNTED_IO_TIMEOUT" kubectl -n "$NAMESPACE" exec "${POD_PREFIX}-writer-${idx}" -- /bin/sh -c "
set -eu
sha256sum -c /data/demo.sha256
echo '/data/demo.bin: OK'
dd if=/dev/urandom of=/data/non-target-${round}.bin bs=4096 count=1
sha256sum /data/non-target-${round}.bin > /data/non-target-${round}.sha256
sync
sha256sum -c /data/non-target-${round}.sha256
echo non_target_workload_ok
" >"$out" 2>&1
}

probe_stale_primary_path() {
  local volume_id="$1"
  local old_frontend="$2"
  local out="$3"
  local host="${old_frontend%:*}"
  local port="${old_frontend##*:}"
  local success=0
  local found=0
  local raw
  {
    echo "stale_primary_probe=direct_read"
    echo "volume_id=$volume_id"
    echo "old_frontend=$old_frontend"
    echo "timeout=$STALE_IO_PROBE_TIMEOUT"
    shopt -s nullglob
    for d in /dev/disk/by-path/*ip-"${host}:${port}"*io.seaweedfs:"${volume_id}"*; do
      found=1
      raw="$(readlink -f "$d")"
      echo "candidate_path=$d"
      echo "candidate_raw=$raw"
      if timeout "$STALE_IO_PROBE_TIMEOUT" sudo -n dd if="$raw" of=/dev/null bs=4096 count=1 iflag=direct status=none; then
        echo "candidate_result=unexpected_success"
        success=$((success + 1))
      else
        echo "candidate_result=expected_failure"
      fi
    done
    shopt -u nullglob
    if [[ "$found" -eq 0 ]]; then
      echo "candidate_result=no_stale_path"
    fi
    echo "old_primary_stale_io_success_count=$success"
  } >"$out" 2>&1
  echo "$success"
}

capture_volume_rtpg_states() {
  local volume_id="$1"
  local label="$2"
  local out="$3"
  local found=0
  local d raw frontend tmp result aas
  {
    echo "rtpg_state_capture=$label"
    echo "volume_id=$volume_id"
    shopt -s nullglob
    for d in /dev/disk/by-path/*io.seaweedfs:"${volume_id}"*; do
      found=1
      raw="$(readlink -f "$d")"
      frontend="$(printf '%s\n' "$d" | sed -n 's#.*ip-\([^/]*\)-iscsi.*#\1#p')"
      tmp="$(mktemp)"
      if sudo -n sg_rtpg "$raw" >"$tmp" 2>&1; then
        result=ok
      else
        result=failed
      fi
      aas="$(sed -n 's/.*asymmetric access state[[:space:]]*:[[:space:]]*\(0x[0-9a-fA-F]\+\).*/\1/p' "$tmp" | head -n1)"
      echo "rtpg_${label}_frontend=${frontend:-unknown} aas=${aas:-missing} result=$result raw=$raw source=$d"
      sed 's/^/  /' "$tmp"
      rm -f "$tmp"
    done
    shopt -u nullglob
    if [[ "$found" -eq 0 ]]; then
      echo "rtpg_${label}_result=no_paths"
    fi
  } >"$out" 2>&1
}

rtpg_aas_from_states() {
  local states="$1"
  local label="$2"
  local frontend="$3"
  awk -v key="rtpg_${label}_frontend=${frontend}" '
    $1 == key {
      for (i = 1; i <= NF; i++) {
        if ($i ~ /^aas=/) {
          sub(/^aas=/, "", $i)
          print $i
          exit
        }
      }
    }
  ' "$states"
}

verify_rtpg_transition() {
  local old_before="$1"
  local promoted_before="$2"
  local old_after="$3"
  local promoted_after="$4"
  if [[ "$old_before" != "0x00" ]]; then
    echo false
  elif [[ -z "$promoted_before" || "$promoted_before" == "missing" || "$promoted_before" == "0x00" ]]; then
    echo false
  elif [[ "$promoted_after" != "0x00" ]]; then
    echo false
  elif [[ "$old_after" == "0x00" ]]; then
    echo false
  else
    echo true
  fi
}

write_summary() {
  {
    echo "multi_volume_mounted_failover_status=$STATUS"
    if [[ "$FAILOVER_MODE" == "interleaved" ]]; then
      echo "multi_volume_interleaved_failover_status=$STATUS"
    fi
    if [[ -n "$FAILED_PHASE" ]]; then
      echo "failed_phase=$FAILED_PHASE"
    fi
    echo "requested_volume_count=$VOLUME_COUNT"
    echo "replication_factor=$REPLICATION_FACTOR"
    echo "failover_mode=$FAILOVER_MODE"
    echo "target_volume_count=$TARGET_VOLUME_COUNT"
    echo "recovered_volume_count=$(grep -h '^target_recovered=true$' "$ARTIFACT_DIR"/failover/volume-*/summary.txt 2>/dev/null | wc -l | tr -d '[:space:]')"
    echo "mounted_workload_checksum_passed_count=$(grep -R -h '^mounted_workload_checksum_passed$' "$ARTIFACT_DIR"/failover/volume-*/workload-after-failover.log 2>/dev/null | wc -l | tr -d '[:space:]')"
    echo "pod_recreate_used=$(grep -R '^pod_recreate_used=true$' "$ARTIFACT_DIR"/failover/volume-*/summary.txt >/dev/null 2>&1 && echo true || echo false)"
    echo "cross_interference_observed=$(grep -R '^cross_interference_observed=true$' "$ARTIFACT_DIR"/failover/volume-*/summary.txt >/dev/null 2>&1 && echo true || echo false)"
    if [[ -f "$ARTIFACT_DIR/failover/interleaved-summary.txt" ]]; then
      cat "$ARTIFACT_DIR/failover/interleaved-summary.txt"
    fi
    echo "transparent_failover_claimed=true"
    echo "cleanup_status=external_to_script"
  } >"$ARTIFACT_DIR/multi-volume-mounted-failover-summary.txt"
}

capture_failure_diagnostics() {
  local phase="$1"
  local out="$ARTIFACT_DIR/diagnostics/$phase"
  mkdir -p "$out"
  safe_capture "$out/pods.txt" kubectl -n "$NAMESPACE" get pods -o wide
  safe_capture "$out/pvcs.txt" kubectl -n "$NAMESPACE" get pvc -o wide
  safe_capture "$out/blockvolume-deployments.txt" kubectl -n "$NAMESPACE" get deploy -l app=sw-blockvolume -o wide
  safe_capture "$out/events.txt" kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp
  safe_capture "$out/kube-system.txt" kubectl -n kube-system get pods,deploy,ds,svc -o wide
  safe_capture "$out/blockmaster.log" kubectl -n kube-system logs deploy/sw-blockmaster --tail=600
  safe_capture "$out/csi-node.log" kubectl -n kube-system logs ds/sw-block-csi-node --all-containers --tail=600
  safe_capture "$out/blockvolume.log" kubectl -n "$NAMESPACE" logs -l sw-block.seaweedfs.com/volume --all-containers --tail=600
  capture_host_path_evidence "failure-${phase}"
}

if [[ "$NAMESPACE" != "default" ]]; then
  echo "only default namespace is supported by this helper" >&2
  exit 2
fi
if [[ "$REPLICATION_FACTOR" != "3" ]]; then
  echo "D3 expects SW_BLOCK_MULTI_VOLUME_RF=3; got $REPLICATION_FACTOR" >&2
  exit 2
fi

log "setup RF=$REPLICATION_FACTOR volumes=$VOLUME_COUNT app_node=${APP_NODE_SELECTOR:-<scheduler>}"
kubectl apply -f "$(render_storageclass)" | tee "$ARTIFACT_DIR/setup/apply-storageclass.log"

for idx in $(seq 1 "$VOLUME_COUNT"); do
  kubectl apply -f "$(render_pvc "$idx")" | tee "$ARTIFACT_DIR/setup/apply-pvc-$idx.log"
  kubectl -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Bound "pvc/${PVC_PREFIX}-${idx}" --timeout=180s | tee "$ARTIFACT_DIR/setup/wait-pvc-$idx.log"
done

for idx in $(seq 1 "$VOLUME_COUNT"); do
  kubectl apply -f "$(render_writer_hold "$idx")" | tee "$ARTIFACT_DIR/setup/apply-writer-$idx.log"
  if ! kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/${POD_PREFIX}-writer-${idx}" --timeout=300s | tee "$ARTIFACT_DIR/setup/wait-writer-$idx.log"; then
    safe_capture "$ARTIFACT_DIR/setup/writer-$idx.describe.txt" kubectl -n "$NAMESPACE" describe pod "${POD_PREFIX}-writer-${idx}"
    capture_failure_diagnostics "writer-$idx"
    STATUS="failed"
    FAILED_PHASE="writer_${idx}_ready"
    write_summary
    exit 1
  fi
  kubectl -n "$NAMESPACE" logs "${POD_PREFIX}-writer-${idx}" >"$ARTIFACT_DIR/logs/writer-$idx.log"
  grep -q '/data/demo.bin: OK' "$ARTIFACT_DIR/logs/writer-$idx.log"
  writer_uid "$idx" >"$ARTIFACT_DIR/setup/writer-$idx.uid.before"
done

capture_host_path_evidence "before"

start_master_port_forward
collect_cluster "$ARTIFACT_DIR/status/cluster-before.json"
kubectl -n "$NAMESPACE" get deploy -l app=sw-blockvolume -o json >"$ARTIFACT_DIR/status/deployments-before.json"
extract_plan "$ARTIFACT_DIR/status/cluster-before.json" "$ARTIFACT_DIR/status/deployments-before.json" "$ARTIFACT_DIR/status/failover-plan.tsv"

if [[ "$FAILOVER_MODE" == "interleaved" ]]; then
  mapfile -t plan_lines <"$ARTIFACT_DIR/status/failover-plan.tsv"
  if (( ${#plan_lines[@]} < TARGET_VOLUME_COUNT )); then
    STATUS="failed"
    FAILED_PHASE="interleaved_plan"
    write_summary
    exit 1
  fi
  collect_cluster "$ARTIFACT_DIR/failover/before-interleaved.json"
  target_ids=()
  target_indices=()
  fault_start_ns="$(date +%s%N)"
  for n in $(seq 0 $((TARGET_VOLUME_COUNT - 1))); do
    IFS=$'\t' read -r idx vid pvc primary node frontend deploy <<<"${plan_lines[$n]}"
    test -n "$deploy"
    target_ids+=("$vid")
    target_indices+=("$idx")
    dir="$ARTIFACT_DIR/failover/volume-${idx}"
    mkdir -p "$dir"
    uid_before="$(writer_uid "$idx")"
    {
      echo "target_index=$idx"
      echo "volume_id=$vid"
      echo "pvc=$pvc"
      echo "before_primary=$primary"
      echo "before_primary_node=$node"
      echo "before_publish_target=$frontend"
      echo "target_deployment=$deploy"
      echo "writer_pod=${POD_PREFIX}-writer-${idx}"
      echo "writer_pod_uid_before=$uid_before"
      echo "interleaved_fault=true"
    } >"$dir/summary.txt"
    capture_volume_rtpg_states "$vid" "before" "$dir/rtpg-before-states.txt"
    log "interleaved target volume $idx primary=$primary deploy=$deploy"
    kubectl -n "$NAMESPACE" scale "deploy/${deploy}" --replicas=0 | tee "$dir/scale-primary-zero.log"
  done
  fault_end_ns="$(date +%s%N)"
  python3 - "$fault_start_ns" "$fault_end_ns" >"$ARTIFACT_DIR/failover/interleaved-window.txt" <<'PY'
import sys
start=int(sys.argv[1])
end=int(sys.argv[2])
print(f"{(end-start)/1_000_000_000:.3f}")
PY
  fault_window="$(cat "$ARTIFACT_DIR/failover/interleaved-window.txt")"

  target_csv="$(IFS=,; echo "${target_ids[*]}")"
  for n in $(seq 0 $((TARGET_VOLUME_COUNT - 1))); do
    IFS=$'\t' read -r idx vid pvc primary node frontend deploy <<<"${plan_lines[$n]}"
    dir="$ARTIFACT_DIR/failover/volume-${idx}"
    for _ in $(seq 1 60); do
      ready="$(kubectl -n "$NAMESPACE" get "deploy/${deploy}" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
      [[ -z "$ready" || "$ready" == "0" ]] && break
      sleep 1
    done
    target_ready="$(kubectl -n "$NAMESPACE" get "deploy/${deploy}" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
    target_ready="${target_ready:-0}"
    promoted=""
    for _ in $(seq 1 120); do
      collect_cluster "$dir/after-poll.json" || true
      after_primary="$(volume_field "$dir/after-poll.json" "$vid" "primary_replica" || true)"
      if [[ -n "$after_primary" && "$after_primary" != "$primary" ]]; then
        promoted="$after_primary"
        break
      fi
      sleep 2
    done
    cp "$dir/after-poll.json" "$dir/after.json"
    if [[ -z "$promoted" ]]; then
      STATUS="failed"
      FAILED_PHASE="interleaved_promotion_volume_${idx}"
      echo "target_recovered=false" >>"$dir/summary.txt"
      echo "reason=promotion_not_observed" >>"$dir/summary.txt"
      capture_failure_diagnostics "interleaved-promotion-volume-${idx}"
      write_summary
      exit 1
    fi
    if ! exec_writer_check_after_failover "$idx" "$dir/workload-after-failover.log"; then
      STATUS="failed"
      FAILED_PHASE="interleaved_mounted_workload_volume_${idx}"
      echo "target_recovered=false" >>"$dir/summary.txt"
      echo "reason=mounted_workload_check_failed" >>"$dir/summary.txt"
      capture_failure_diagnostics "interleaved-mounted-workload-volume-${idx}"
      write_summary
      exit 1
    fi
    grep -q '^mounted_workload_checksum_passed$' "$dir/workload-after-failover.log"
    collect_cluster "$dir/after-workload.json"
    after_frontend="$(volume_field "$dir/after-workload.json" "$vid" "publish_target")"
    primary_count="$(primary_count_for_volume "$dir/after-workload.json" "$vid")"
    capture_volume_rtpg_states "$vid" "after" "$dir/rtpg-after-states.txt"
    rtpg_before_old="$(rtpg_aas_from_states "$dir/rtpg-before-states.txt" "before" "$frontend")"
    rtpg_before_promoted="$(rtpg_aas_from_states "$dir/rtpg-before-states.txt" "before" "$after_frontend")"
    rtpg_after_old="$(rtpg_aas_from_states "$dir/rtpg-after-states.txt" "after" "$frontend")"
    rtpg_after_promoted="$(rtpg_aas_from_states "$dir/rtpg-after-states.txt" "after" "$after_frontend")"
    rtpg_ok="$(verify_rtpg_transition "$rtpg_before_old" "$rtpg_before_promoted" "$rtpg_after_old" "$rtpg_after_promoted")"
    uid_after="$(writer_uid "$idx")"
    pod_recreate=false
    if [[ "$uid_after" != "$(sed -n 's/^writer_pod_uid_before=//p' "$dir/summary.txt")" ]]; then
      pod_recreate=true
    fi
    stale_success="$(probe_stale_primary_path "$vid" "$frontend" "$dir/stale-primary-probe.log")"
    {
      echo "failover_status=promoted"
      echo "promoted_replica=$promoted"
      echo "after_publish_target=$after_frontend"
      echo "post_failure_primary_count=$primary_count"
      echo "target_ready_replicas=$target_ready"
      echo "stale_primary_fence_evidence=target_ready_replicas=$target_ready,stale_path_direct_read_success_count=$stale_success"
      echo "stale_primary_probe=direct_read"
      echo "old_primary_stale_io_success_count=$stale_success"
      echo "rtpg_before_old_primary_aas=${rtpg_before_old:-missing}"
      echo "rtpg_before_promoted_aas=${rtpg_before_promoted:-missing}"
      echo "rtpg_after_old_primary_aas=${rtpg_after_old:-missing}"
      echo "rtpg_after_promoted_aas=${rtpg_after_promoted:-missing}"
      echo "rtpg_transition_verified=$rtpg_ok"
      echo "writer_pod_uid_after=$uid_after"
      echo "pod_recreate_used=$pod_recreate"
      echo "data_check_after_failover=mounted_workload_checksum_passed"
      echo "target_recovered=true"
      echo "cross_interference_observed=false"
      echo "transparent_failover_claimed=true"
    } >>"$dir/summary.txt"
    if [[ "$primary_count" != "1" || "$pod_recreate" != "false" || "$stale_success" != "0" || "$rtpg_ok" != "true" ]]; then
      STATUS="failed"
      FAILED_PHASE="interleaved_isolation_volume_${idx}"
      write_summary
      exit 1
    fi
  done

  collect_cluster "$ARTIFACT_DIR/failover/after-interleaved.json"
  assert_untouched_stable "$ARTIFACT_DIR/failover/before-interleaved.json" "$ARTIFACT_DIR/failover/after-interleaved.json" "$target_csv" && untouched_stable=true || untouched_stable=false
  non_target_failed=false
  for idx in $(seq 1 "$VOLUME_COUNT"); do
    skip=false
    for target_idx in "${target_indices[@]}"; do
      if [[ "$idx" == "$target_idx" ]]; then
        skip=true
      fi
    done
    if [[ "$skip" == "true" ]]; then
      continue
    fi
    if ! exec_writer_non_target_check "$idx" "interleaved" "$ARTIFACT_DIR/failover/untouched-volume-${idx}.log"; then
      non_target_failed=true
    fi
  done
  capture_host_path_evidence "after-interleaved"
  {
    echo "interleaved_fault_window_seconds=$fault_window"
    echo "interleaved_target_volume_count=$TARGET_VOLUME_COUNT"
    echo "untouched_volume_stable=$untouched_stable"
    echo "untouched_workload_ok=$([[ "$non_target_failed" == "false" ]] && echo true || echo false)"
  } >"$ARTIFACT_DIR/failover/interleaved-summary.txt"
  if [[ "$untouched_stable" != "true" || "$non_target_failed" != "false" ]]; then
    STATUS="failed"
    FAILED_PHASE="interleaved_untouched_volume"
    write_summary
    exit 1
  fi
  write_summary
  cat "$ARTIFACT_DIR/multi-volume-mounted-failover-summary.txt"
  if [[ "$STATUS" != "ok" ]]; then
    exit 1
  fi
  log "PASS: multi-volume interleaved mounted failover complete"
  exit 0
fi

while IFS=$'\t' read -r idx vid pvc primary node frontend deploy; do
  test -n "$vid"
  test -n "$primary"
  test -n "$deploy"
  dir="$ARTIFACT_DIR/failover/volume-${idx}"
  mkdir -p "$dir"
  collect_cluster "$dir/before.json"
  uid_before="$(writer_uid "$idx")"
  {
    echo "target_index=$idx"
    echo "volume_id=$vid"
    echo "pvc=$pvc"
    echo "before_primary=$primary"
    echo "before_primary_node=$node"
    echo "before_publish_target=$frontend"
    echo "target_deployment=$deploy"
    echo "writer_pod=${POD_PREFIX}-writer-${idx}"
    echo "writer_pod_uid_before=$uid_before"
  } >"$dir/summary.txt"
  capture_volume_rtpg_states "$vid" "before" "$dir/rtpg-before-states.txt"

  log "target volume $idx primary=$primary deploy=$deploy"
  kubectl -n "$NAMESPACE" scale "deploy/${deploy}" --replicas=0 | tee "$dir/scale-primary-zero.log"
  for _ in $(seq 1 60); do
    ready="$(kubectl -n "$NAMESPACE" get "deploy/${deploy}" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
    [[ -z "$ready" || "$ready" == "0" ]] && break
    sleep 1
  done
  target_ready="$(kubectl -n "$NAMESPACE" get "deploy/${deploy}" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
  target_ready="${target_ready:-0}"

  promoted=""
  for _ in $(seq 1 120); do
    collect_cluster "$dir/after-poll.json" || true
    after_primary="$(volume_field "$dir/after-poll.json" "$vid" "primary_replica" || true)"
    if [[ -n "$after_primary" && "$after_primary" != "$primary" ]]; then
      promoted="$after_primary"
      break
    fi
    sleep 2
  done
  cp "$dir/after-poll.json" "$dir/after.json"
  if [[ -z "$promoted" ]]; then
    STATUS="failed"
    FAILED_PHASE="promotion_volume_${idx}"
    echo "target_recovered=false" >>"$dir/summary.txt"
    echo "reason=promotion_not_observed" >>"$dir/summary.txt"
    capture_failure_diagnostics "promotion-volume-${idx}"
    write_summary
    exit 1
  fi

  if ! exec_writer_check_after_failover "$idx" "$dir/workload-after-failover.log"; then
    STATUS="failed"
    FAILED_PHASE="mounted_workload_volume_${idx}"
    echo "target_recovered=false" >>"$dir/summary.txt"
    echo "reason=mounted_workload_check_failed" >>"$dir/summary.txt"
    capture_failure_diagnostics "mounted-workload-volume-${idx}"
    write_summary
    exit 1
  fi
  grep -q '^mounted_workload_checksum_passed$' "$dir/workload-after-failover.log"

  non_target_failed=false
  for other in $(seq 1 "$VOLUME_COUNT"); do
    if [[ "$other" == "$idx" ]]; then
      continue
    fi
    if ! exec_writer_non_target_check "$other" "$idx" "$dir/non-target-${other}.log"; then
      non_target_failed=true
    fi
  done

  collect_cluster "$dir/after-workload.json"
  after_frontend="$(volume_field "$dir/after-workload.json" "$vid" "publish_target")"
  primary_count="$(primary_count_for_volume "$dir/after-workload.json" "$vid")"
  capture_volume_rtpg_states "$vid" "after" "$dir/rtpg-after-states.txt"
  rtpg_before_old="$(rtpg_aas_from_states "$dir/rtpg-before-states.txt" "before" "$frontend")"
  rtpg_before_promoted="$(rtpg_aas_from_states "$dir/rtpg-before-states.txt" "before" "$after_frontend")"
  rtpg_after_old="$(rtpg_aas_from_states "$dir/rtpg-after-states.txt" "after" "$frontend")"
  rtpg_after_promoted="$(rtpg_aas_from_states "$dir/rtpg-after-states.txt" "after" "$after_frontend")"
  rtpg_ok="$(verify_rtpg_transition "$rtpg_before_old" "$rtpg_before_promoted" "$rtpg_after_old" "$rtpg_after_promoted")"
  assert_non_target_stable "$dir/before.json" "$dir/after-workload.json" "$vid" && cross=false || cross=true
  if [[ "$non_target_failed" == "true" ]]; then
    cross=true
  fi
  uid_after="$(writer_uid "$idx")"
  pod_recreate=false
  if [[ "$uid_after" != "$uid_before" ]]; then
    pod_recreate=true
  fi
  stale_success="$(probe_stale_primary_path "$vid" "$frontend" "$dir/stale-primary-probe.log")"
  capture_host_path_evidence "after-volume-${idx}"

  {
    echo "failover_status=promoted"
    echo "promoted_replica=$promoted"
    echo "after_publish_target=$after_frontend"
    echo "post_failure_primary_count=$primary_count"
    echo "target_ready_replicas=$target_ready"
    echo "stale_primary_fence_evidence=target_ready_replicas=$target_ready,stale_path_direct_read_success_count=$stale_success"
    echo "stale_primary_probe=direct_read"
    echo "old_primary_stale_io_success_count=$stale_success"
    echo "rtpg_before_old_primary_aas=${rtpg_before_old:-missing}"
    echo "rtpg_before_promoted_aas=${rtpg_before_promoted:-missing}"
    echo "rtpg_after_old_primary_aas=${rtpg_after_old:-missing}"
    echo "rtpg_after_promoted_aas=${rtpg_after_promoted:-missing}"
    echo "rtpg_transition_verified=$rtpg_ok"
    echo "writer_pod_uid_after=$uid_after"
    echo "pod_recreate_used=$pod_recreate"
    echo "data_check_after_failover=mounted_workload_checksum_passed"
    echo "target_recovered=true"
    echo "cross_interference_observed=$cross"
    echo "transparent_failover_claimed=true"
  } >>"$dir/summary.txt"

  if [[ "$primary_count" != "1" || "$pod_recreate" != "false" || "$cross" != "false" || "$stale_success" != "0" || "$rtpg_ok" != "true" ]]; then
    STATUS="failed"
    FAILED_PHASE="isolation_volume_${idx}"
    write_summary
    exit 1
  fi
done <"$ARTIFACT_DIR/status/failover-plan.tsv"

write_summary
cat "$ARTIFACT_DIR/multi-volume-mounted-failover-summary.txt"
if [[ "$STATUS" != "ok" ]]; then
  exit 1
fi
log "PASS: multi-volume mounted failover complete"
