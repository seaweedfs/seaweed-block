#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-/tmp/sw-block-multi-volume-reattach-$(date -u +%Y%m%dT%H%M%SZ)}"
NAMESPACE="${SW_BLOCK_APP_NAMESPACE:-default}"
VOLUME_COUNT="${SW_BLOCK_MULTI_VOLUME_COUNT:-3}"
REPLICATION_FACTOR="${SW_BLOCK_MULTI_VOLUME_RF:-3}"
PVC_PREFIX="${SW_BLOCK_MULTI_VOLUME_PVC_PREFIX:-sw-block-multi-pvc}"
POD_PREFIX="${SW_BLOCK_MULTI_VOLUME_POD_PREFIX:-sw-block-multi}"
MASTER_PORT="${SW_BLOCK_MASTER_PORT_FORWARD_PORT:-}"

STATUS="ok"
FAILED_PHASE=""

mkdir -p "$ARTIFACT_DIR"/{setup,failover,status,logs,manifests}

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

log() {
  printf '[multi-volume-reattach] %s\n' "$*" | tee -a "$ARTIFACT_DIR/run.log"
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

collect_cluster() {
  local out="$1"
  mkdir -p "$(dirname "$out")"
  sw_block_cmd ops cluster --master-api "127.0.0.1:${MASTER_PORT}" --timeout 30s -o json >"$out"
  test -s "$out"
}

render_reader() {
  local idx="$1"
  local name="$2"
  local out="$ARTIFACT_DIR/manifests/${name}.yaml"
  cat >"$out" <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${name}
  namespace: ${NAMESPACE}
  labels:
    sw-block-test: multi-volume-reattach
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

extract_plan() {
  local cluster_json="$1"
  local deploy_json="$2"
  local out="$3"
  python3 - "$cluster_json" "$deploy_json" "$out" <<'PY'
import json, sys
cluster = json.load(open(sys.argv[1]))
deploys = json.load(open(sys.argv[2])).get("items", [])
out = sys.argv[3]
vols = sorted(cluster.get("volumes", []), key=lambda v: v.get("pvc_name") or v.get("volume_id"))
deploy_by_slot = {}
for d in deploys:
    labels = d.get("metadata", {}).get("labels", {}) or {}
    key = (labels.get("sw-block.seaweedfs.com/volume"), labels.get("sw-block.seaweedfs.com/replica"))
    if key[0] and key[1]:
        deploy_by_slot[key] = d.get("metadata", {}).get("name", "")
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

write_summary() {
  {
    echo "multi_volume_reattach_status=$STATUS"
    if [[ -n "$FAILED_PHASE" ]]; then
      echo "failed_phase=$FAILED_PHASE"
    fi
    echo "requested_volume_count=$VOLUME_COUNT"
    echo "replication_factor=$REPLICATION_FACTOR"
    echo "recovered_volume_count=$(grep -h '^target_recovered=true$' "$ARTIFACT_DIR"/failover/volume-*/summary.txt 2>/dev/null | wc -l | tr -d '[:space:]')"
    echo "cross_interference_observed=$(grep -R '^cross_interference_observed=true$' "$ARTIFACT_DIR"/failover/volume-*/summary.txt >/dev/null 2>&1 && echo true || echo false)"
    echo "cleanup_status=external_to_script"
  } >"$ARTIFACT_DIR/multi-volume-reattach-summary.txt"
}

trap 'stop_master_port_forward || true' EXIT

if [[ "$NAMESPACE" != "default" ]]; then
  echo "only default namespace is supported by this helper" >&2
  exit 2
fi
if [[ "$REPLICATION_FACTOR" != "3" ]]; then
  echo "D2 expects SW_BLOCK_MULTI_VOLUME_RF=3; got $REPLICATION_FACTOR" >&2
  exit 2
fi

log "setup RF=$REPLICATION_FACTOR volumes=$VOLUME_COUNT"
(
  cd "$ROOT"
  SW_BLOCK_ARTIFACT_DIR="$ARTIFACT_DIR/setup" \
  SW_BLOCK_MULTI_VOLUME_COUNT="$VOLUME_COUNT" \
  SW_BLOCK_MULTI_VOLUME_RF="$REPLICATION_FACTOR" \
  SW_BLOCK_MULTI_VOLUME_CLEANUP=0 \
  SW_BLOCK_CLI="${SW_BLOCK_CLI:-}" \
    bash scripts/run-multi-volume-example.sh "$ROOT"
)

for idx in $(seq 1 "$VOLUME_COUNT"); do
  kubectl -n "$NAMESPACE" delete pod "${POD_PREFIX}-reader-${idx}" --ignore-not-found=true --wait=true --timeout=120s
done

start_master_port_forward
collect_cluster "$ARTIFACT_DIR/status/cluster-before.json"
kubectl -n "$NAMESPACE" get deploy -l app=sw-blockvolume -o json >"$ARTIFACT_DIR/status/deployments-before.json"
extract_plan "$ARTIFACT_DIR/status/cluster-before.json" "$ARTIFACT_DIR/status/deployments-before.json" "$ARTIFACT_DIR/status/failover-plan.tsv"

while IFS=$'\t' read -r idx vid pvc primary node frontend deploy; do
  test -n "$vid"
  test -n "$primary"
  test -n "$deploy"
  dir="$ARTIFACT_DIR/failover/volume-${idx}"
  mkdir -p "$dir"
  collect_cluster "$dir/before.json"
  {
    echo "target_index=$idx"
    echo "volume_id=$vid"
    echo "pvc=$pvc"
    echo "before_primary=$primary"
    echo "before_primary_node=$node"
    echo "before_publish_target=$frontend"
    echo "target_deployment=$deploy"
  } >"$dir/summary.txt"

  log "target volume $idx primary=$primary deploy=$deploy"
  kubectl -n "$NAMESPACE" scale "deploy/${deploy}" --replicas=0 | tee "$dir/scale-primary-zero.log"
  for _ in $(seq 1 60); do
    ready="$(kubectl -n "$NAMESPACE" get "deploy/${deploy}" -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
    [[ -z "$ready" || "$ready" == "0" ]] && break
    sleep 1
  done

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
    write_summary
    exit 1
  fi
  after_frontend="$(volume_field "$dir/after.json" "$vid" "publish_target")"
  primary_count="$(python3 - "$dir/after.json" "$vid" <<'PY'
import json, sys
doc=json.load(open(sys.argv[1])); vid=sys.argv[2]
for v in doc.get("volumes", []):
    if v.get("volume_id")==vid:
        print(sum(1 for r in v.get("replicas", []) if r.get("role")=="primary"))
        break
PY
)"
  assert_non_target_stable "$dir/before.json" "$dir/after.json" "$vid" && cross=false || cross=true

  reader="${POD_PREFIX}-reattach-reader-${idx}"
  kubectl -n "$NAMESPACE" delete pod "$reader" --ignore-not-found=true --wait=true --timeout=120s
  kubectl apply -f "$(render_reader "$idx" "$reader")" | tee "$dir/apply-reader.log"
  kubectl -n "$NAMESPACE" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/${reader}" --timeout=240s | tee "$dir/wait-reader.log"
  kubectl -n "$NAMESPACE" logs "$reader" >"$dir/reader.log"
  grep -q '/data/demo.bin: OK' "$dir/reader.log"
  kubectl -n "$NAMESPACE" delete pod "$reader" --ignore-not-found=true --wait=true --timeout=120s

  {
    echo "failover_status=promoted"
    echo "promoted_replica=$promoted"
    echo "after_publish_target=$after_frontend"
    echo "post_failure_primary_count=$primary_count"
    echo "reader_verified=true"
    echo "pod_recreate_used=true"
    echo "target_recovered=true"
    echo "cross_interference_observed=$cross"
  } >>"$dir/summary.txt"
  if [[ "$primary_count" != "1" || "$cross" != "false" ]]; then
    STATUS="failed"
    FAILED_PHASE="isolation_volume_${idx}"
    write_summary
    exit 1
  fi
done <"$ARTIFACT_DIR/status/failover-plan.tsv"

write_summary
cat "$ARTIFACT_DIR/multi-volume-reattach-summary.txt"
if [[ "$STATUS" != "ok" ]]; then
  exit 1
fi
log "PASS: multi-volume reattach recovery complete"
