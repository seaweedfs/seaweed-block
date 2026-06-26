#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase95-failback-live-deployed-suite-gate}"
SUMMARY="${ARTIFACT_DIR}/phase95-failback-live-deployed-suite-summary.txt"
NAMESPACE="${SW_BLOCK_PHASE95_NAMESPACE:-kube-system}"
RELEASE="${SW_BLOCK_PHASE95_RELEASE:-sw-block}"
APP_NAMESPACE="${SW_BLOCK_PHASE95_APP_NAMESPACE:-default}"
NODE_NAME="${SW_BLOCK_PHASE95_NODE_NAME:-m02}"
IMAGE="${SW_BLOCK_PHASE95_IMAGE:-sw-block:phase95}"
CSI_IMAGE="${SW_BLOCK_PHASE95_CSI_IMAGE:-sw-block-csi:phase95}"
IMAGE_PULL_POLICY="${SW_BLOCK_PHASE95_IMAGE_PULL_POLICY:-IfNotPresent}"
KUBECONFIG_PATH="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"
REMOTE_IMPORT_NODES="${SW_BLOCK_PHASE95_REMOTE_IMPORT_NODES:-192.168.1.181}"

VALUES_DIR="${ARTIFACT_DIR}/values"
INSTALL_DIR="${ARTIFACT_DIR}/install"
APP_DIR="${ARTIFACT_DIR}/basic-app"
FAILBACK_DIR="${ARTIFACT_DIR}/failback"
CLEANUP_DIR="${ARTIFACT_DIR}/cleanup"

mkdir -p "${VALUES_DIR}" "${INSTALL_DIR}" "${APP_DIR}" "${FAILBACK_DIR}" "${CLEANUP_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

run_capture() {
  local name="$1"
  shift
  "$@" >"${ARTIFACT_DIR}/${name}.stdout.txt" 2>"${ARTIFACT_DIR}/${name}.stderr.txt"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    write_summary "phase95_failback_live_deployed_suite_status=blocked_missing_${1}"
    exit 2
  fi
}

wait_for_jsonpath() {
  local name="$1"
  local want="$2"
  local jsonpath="$3"
  shift 3
  for _ in $(seq 1 120); do
    local got
    got="$("$@" -o "jsonpath=${jsonpath}" 2>/dev/null || true)"
    if [ "${got}" = "${want}" ]; then
      write_summary "${name}=true"
      return 0
    fi
    sleep 2
  done
  write_summary "${name}=false"
  "$@" -o yaml >"${FAILBACK_DIR}/${name}.last.yaml" 2>&1 || true
  return 1
}

cleanup_example_pv_residue() {
  local pv pvs
  pvs="$(kubectl --kubeconfig "${KUBECONFIG_PATH}" get pv -o json 2>/dev/null | python3 -c '
import json, sys
doc = json.load(sys.stdin)
for item in doc.get("items", []):
    spec = item.get("spec") or {}
    claim = spec.get("claimRef") or {}
    name = item.get("metadata", {}).get("name", "")
    if claim.get("name") == "sw-block-example-pvc" or spec.get("storageClassName") == "sw-block-example":
        print(name)
' || true)"
  while read -r pv; do
    [ -n "${pv}" ] || continue
    kubectl --kubeconfig "${KUBECONFIG_PATH}" delete pv "${pv}" --ignore-not-found=true --wait=false >/dev/null 2>&1 || true
    sleep 1
    if kubectl --kubeconfig "${KUBECONFIG_PATH}" get pv "${pv}" >/dev/null 2>&1; then
      kubectl --kubeconfig "${KUBECONFIG_PATH}" patch pv "${pv}" --type=json -p='[{"op":"remove","path":"/metadata/finalizers"}]' >/dev/null 2>&1 || true
    fi
  done <<<"${pvs}"
}

cleanup() {
  set +e
  kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${NAMESPACE}" delete swblockreplicafailback --all --ignore-not-found=true >/dev/null 2>&1
  kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${NAMESPACE}" delete swblockvolume phase95-live-failback --ignore-not-found=true >/dev/null 2>&1
  kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${APP_NAMESPACE}" delete pod sw-block-example-reader sw-block-example-writer --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${APP_NAMESPACE}" delete pvc sw-block-example-pvc --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  kubectl --kubeconfig "${KUBECONFIG_PATH}" delete storageclass sw-block-example --ignore-not-found=true --wait=true --timeout=120s >/dev/null 2>&1
  helm --kubeconfig "${KUBECONFIG_PATH}" status "${RELEASE}" --namespace "${NAMESPACE}" >/dev/null 2>&1 &&
    helm --kubeconfig "${KUBECONFIG_PATH}" uninstall "${RELEASE}" --namespace "${NAMESPACE}" --wait --timeout 240s >/dev/null 2>&1
  cleanup_example_pv_residue
  sudo -n iscsiadm -m node 2>/dev/null | awk '/io.seaweedfs/ {print $1, $2}' | while read -r portal target; do
    sudo -n iscsiadm -m node -T "$target" -p "$portal" -o delete >/dev/null 2>&1 || true
  done
}

wait_for_blockmaster_grpc() {
  local port
  port="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
  kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${NAMESPACE}" port-forward deploy/sw-blockmaster "${port}:9333" >"${INSTALL_DIR}/blockmaster-port-forward.log" 2>&1 &
  local pf_pid=$!
  trap 'kill ${pf_pid} >/dev/null 2>&1 || true; wait ${pf_pid} >/dev/null 2>&1 || true; cleanup' EXIT
  for _ in $(seq 1 90); do
    if (echo >"/dev/tcp/127.0.0.1/${port}") >/dev/null 2>&1; then
      kill "${pf_pid}" >/dev/null 2>&1 || true
      wait "${pf_pid}" >/dev/null 2>&1 || true
      trap cleanup EXIT
      write_summary "blockmaster_grpc_ready=true"
      return 0
    fi
    sleep 1
  done
  kill "${pf_pid}" >/dev/null 2>&1 || true
  wait "${pf_pid}" >/dev/null 2>&1 || true
  trap cleanup EXIT
  write_summary "blockmaster_grpc_ready=false"
  kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${NAMESPACE}" get pods -o wide >"${INSTALL_DIR}/blockmaster-grpc-wait-pods.txt" 2>&1 || true
  kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${NAMESPACE}" logs deploy/sw-blockmaster --tail=200 >"${INSTALL_DIR}/blockmaster-grpc-wait.log" 2>&1 || true
  return 1
}

write_summary "phase95_failback_live_deployed_suite_status=running"
write_summary "phase95_scope=live_kubernetes_deployed_failback_target_owner_executor_grpc_smoke"
write_summary "live_kubernetes_install_claimed=true"
write_summary "frontend_publication_after_failback_claimed=false"
write_summary "storage_mutation_allowed=false"

require_cmd kubectl
require_cmd helm
require_cmd docker
require_cmd go

export KUBECONFIG="${KUBECONFIG_PATH}"

cleanup
set -e
trap cleanup EXIT

run_capture build-alpha-images env \
  SW_BLOCK_IMAGE="${IMAGE}" \
  SW_BLOCK_CSI_IMAGE="${CSI_IMAGE}" \
  SW_BLOCK_IMPORT_K3S=1 \
  SW_BLOCK_IMPORT_K3S_NODES="${REMOTE_IMPORT_NODES}" \
  SW_BLOCK_IMPORT_K3S_SSH_USER="${SW_BLOCK_PHASE95_IMPORT_SSH_USER:-testdev}" \
  SW_BLOCK_BUILD_ARTIFACT_DIR="${INSTALL_DIR}/image-build" \
  bash "${PRODUCT_ROOT}/scripts/build-alpha-images.sh" "${PRODUCT_ROOT}"
write_summary "local_images_built_and_imported=true"
write_summary "remote_import_nodes=${REMOTE_IMPORT_NODES:-none}"

(
  cd "${PRODUCT_ROOT}"
  go run ./cmd/sw-block ops generate-helm-values \
    --kubeconfig "${KUBECONFIG_PATH}" \
    --target-node "${NODE_NAME}" \
    --out "${VALUES_DIR}/values.day1.yaml" \
    --image "${IMAGE}" \
    --csi-image "${CSI_IMAGE}"
) >"${ARTIFACT_DIR}/generate-values.stdout.txt" 2>"${ARTIFACT_DIR}/generate-values.stderr.txt"

cat >"${VALUES_DIR}/values.phase95.yaml" <<YAML
image:
  pullPolicy: ${IMAGE_PULL_POLICY}
csiImage:
  pullPolicy: ${IMAGE_PULL_POLICY}
blockmaster:
  failbackRuntimeRPC: true
  nodeSelector:
    kubernetes.io/hostname: ${NODE_NAME}
failbackTargetOwner:
  create: true
  dryRun: false
  interval: 2s
  nodeSelector:
    kubernetes.io/hostname: ${NODE_NAME}
  activation:
    enabled: true
    policy: true
    runtimeEndpoint: blockmaster.${NAMESPACE}.svc:9333
failbackExecutor:
  create: true
  dryRun: false
  interval: 2s
  nodeSelector:
    kubernetes.io/hostname: ${NODE_NAME}
  execution:
    enabled: true
    policy: true
    failbackRuntimeGrpcAddr: blockmaster.${NAMESPACE}.svc:9333
YAML

run_capture helm-lint helm --kubeconfig "${KUBECONFIG_PATH}" lint "${PRODUCT_ROOT}/charts/seaweed-block" \
  -f "${VALUES_DIR}/values.day1.yaml" \
  -f "${VALUES_DIR}/values.phase95.yaml"
write_summary "helm_lint=pass"

helm --kubeconfig "${KUBECONFIG_PATH}" install "${RELEASE}" "${PRODUCT_ROOT}/charts/seaweed-block" \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  -f "${VALUES_DIR}/values.day1.yaml" \
  -f "${VALUES_DIR}/values.phase95.yaml" \
  --wait --timeout 10m >"${INSTALL_DIR}/helm-install.txt" 2>&1
write_summary "helm_install=pass"

kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${NAMESPACE}" rollout status deploy/sw-blockmaster --timeout=180s >"${INSTALL_DIR}/rollout-blockmaster.txt" 2>&1
kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${NAMESPACE}" rollout status deploy/sw-block-failback-target-owner --timeout=180s >"${INSTALL_DIR}/rollout-failback-target-owner.txt" 2>&1
kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${NAMESPACE}" rollout status deploy/sw-block-failback-executor --timeout=180s >"${INSTALL_DIR}/rollout-failback-executor.txt" 2>&1
write_summary "deployed_suite_pods_ready=true"
wait_for_blockmaster_grpc

if ! SW_BLOCK_ARTIFACT_DIR="${APP_DIR}" \
  SW_BLOCK_BASIC_APP_CLEANUP=0 \
  SW_BLOCK_INSTALL_MODE=helm \
  SW_BLOCK_HELM_RELEASE="${RELEASE}" \
  SW_BLOCK_HELM_NAMESPACE="${NAMESPACE}" \
  SW_BLOCK_HELM_VALUES_FILE="${VALUES_DIR}/values.day1.yaml" \
  SW_BLOCK_BASIC_APP_NODE_SELECTOR="${NODE_NAME}" \
    bash "${PRODUCT_ROOT}/scripts/run-basic-app-example.sh" "${PRODUCT_ROOT}" >"${APP_DIR}/run-basic-app.stdout.txt" 2>"${APP_DIR}/run-basic-app.stderr.txt"; then
  write_summary "first_volume_writer_reader=fail"
  kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${APP_NAMESPACE}" get pvc,pods -o wide >"${APP_DIR}/first-volume-failure-pvc-pods.txt" 2>&1 || true
  kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${APP_NAMESPACE}" describe pvc sw-block-example-pvc >"${APP_DIR}/first-volume-failure-pvc-describe.txt" 2>&1 || true
  kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${NAMESPACE}" get pods -o wide >"${APP_DIR}/first-volume-failure-system-pods.txt" 2>&1 || true
  kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${NAMESPACE}" logs deploy/sw-block-csi-controller -c csi-provisioner --tail=200 >"${APP_DIR}/first-volume-failure-provisioner.log" 2>&1 || true
  kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${NAMESPACE}" logs deploy/sw-block-csi-controller -c blockcsi --tail=200 >"${APP_DIR}/first-volume-failure-blockcsi.log" 2>&1 || true
  kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${NAMESPACE}" logs deploy/sw-blockmaster --tail=200 >"${APP_DIR}/first-volume-failure-blockmaster.log" 2>&1 || true
  kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${APP_NAMESPACE}" get events --sort-by=.lastTimestamp >"${APP_DIR}/first-volume-failure-events.txt" 2>&1 || true
  cat "${APP_DIR}/run-basic-app.stderr.txt" >&2 || true
  exit 1
fi

grep -q '^first_volume_status=ok$' "${APP_DIR}/first-volume-summary.txt"
grep -q '^writer_verified=true$' "${APP_DIR}/first-volume-summary.txt"
grep -q '^reader_verified=true$' "${APP_DIR}/first-volume-summary.txt"
write_summary "first_volume_writer_reader=pass"

authority_line="$(grep '^managed_volume_authority=' "${APP_DIR}/status/report/summary.txt" | head -1 || true)"
if [ -z "${authority_line}" ]; then
  echo "missing managed_volume_authority in report summary" >&2
  exit 1
fi
volume_id="$(sed -n 's/^volume_id=//p' "${APP_DIR}/first-volume-summary.txt" | head -1)"
pvc_name="$(sed -n 's/^pvc=//p' "${APP_DIR}/first-volume-summary.txt" | head -1)"
if [ -z "${pvc_name}" ]; then
  pvc_name="sw-block-example-pvc"
fi
primary_replica="$(printf '%s\n' "${authority_line}" | sed -n 's/.* primary=\([^ ]*\) .*/\1/p')"
authority_epoch="$(printf '%s\n' "${authority_line}" | sed -n 's/.* epoch=\([0-9][0-9]*\).*/\1/p')"

if [ -z "${volume_id}" ] || [ -z "${pvc_name}" ] || [ -z "${primary_replica}" ] || [ -z "${authority_epoch}" ]; then
  echo "failed to extract live authority facts volume_id=${volume_id:-} pvc=${pvc_name:-} primary=${primary_replica:-} epoch=${authority_epoch:-}" >&2
  exit 1
fi
target_replica="r-phase95-returned"
target_data_addr="127.0.0.1:39201"
target_ctrl_addr="127.0.0.1:39202"
write_summary "live_volume_id=${volume_id}"
write_summary "live_current_primary=${primary_replica}"
write_summary "live_current_epoch=${authority_epoch}"

cat >"${FAILBACK_DIR}/swblockvolume.yaml" <<YAML
apiVersion: block.seaweedfs.com/v1alpha1
kind: SwBlockVolume
metadata:
  name: phase95-live-failback
  namespace: ${NAMESPACE}
spec:
  pvcName: ${pvc_name}
YAML
kubectl --kubeconfig "${KUBECONFIG_PATH}" apply -f "${FAILBACK_DIR}/swblockvolume.yaml" >"${FAILBACK_DIR}/apply-volume.txt" 2>&1

cat >"${FAILBACK_DIR}/swblockvolume-status.json" <<JSON
{
  "status": {
    "volumeID": "${volume_id}",
    "pvcName": "${pvc_name}",
    "primaryReplicaID": "${primary_replica}",
    "authorityEpoch": ${authority_epoch},
    "status": "ready",
    "reasonCode": "first_volume_verified",
    "replicaReintegrations": [{
      "replicaID": "${target_replica}",
      "state": "fenced",
      "reasonCode": "returned_replica_frontend_fenced",
      "frontendFenced": true,
      "frontendPrimaryReady": false,
      "ackEligibilityKnown": true,
      "ackEligible": true,
      "durableFrontierKnown": true,
      "durableFrontierLsn": 52,
      "requiredFrontierKnown": true,
      "requiredFrontierLsn": 52,
      "targetDataAddr": "${target_data_addr}",
      "targetCtrlAddr": "${target_ctrl_addr}",
      "evidenceRefs": ["phase95-live-failback"]
    }],
    "executorContracts": [{
      "actionType": "authority.failback_returned_replica",
      "replicaID": "${target_replica}",
      "decision": "disabled",
      "reason": "executor_policy_disabled",
      "ownerExecutor": "authority_recovery_executor",
      "executionEnabled": false,
      "mutationAllowed": false,
      "preflightDecision": "ready",
      "preflightReason": "preconditions_satisfied",
      "allowedMutationClass": ["failback"],
      "forbiddenMutationClass": ["frontend_publication", "rebuild_traffic", "ack_eligibility"],
      "terminalEvidenceRequired": [
        "ack_eligible_true",
        "frontend_fenced_before_failback",
        "failback_authority_owner",
        "authority_epoch_advanced",
        "single_primary_after_failback",
        "publish_target_swapped_after_failback",
        "no_cross_volume_identity_change"
      ],
      "evidenceRefs": ["phase95-live-failback"]
    }]
  }
}
JSON
kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${NAMESPACE}" patch swblockvolume phase95-live-failback \
  --subresource=status --type=merge --patch-file "${FAILBACK_DIR}/swblockvolume-status.json" \
  >"${FAILBACK_DIR}/patch-volume-status.txt" 2>&1
write_summary "swblockvolume_failback_contract_patched=true"

target_name="phase95-live-failback-${target_replica}-failback"
wait_for_jsonpath "failback_target_created" "${target_replica}" "{.spec.replicaID}" \
  kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${NAMESPACE}" get swblockreplicafailback "${target_name}"
kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${NAMESPACE}" get swblockreplicafailback "${target_name}" -o yaml >"${FAILBACK_DIR}/target.created.yaml"

wait_for_jsonpath "failback_executor_completed" "failed_back" "{.status.state}" \
  kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${NAMESPACE}" get swblockreplicafailback "${target_name}"
kubectl --kubeconfig "${KUBECONFIG_PATH}" -n "${NAMESPACE}" get swblockreplicafailback "${target_name}" -o json >"${FAILBACK_DIR}/target.final.json"

python3 - "${FAILBACK_DIR}/target.final.json" <<'PY'
import json, sys
doc = json.load(open(sys.argv[1]))
status = doc.get("status") or {}
required = {
    "reasonCode": "failback_completed",
    "failbackStarted": True,
    "authorityEpochAdvanced": True,
    "singlePrimaryAfterFailback": True,
    "publishTargetSwappedAfterFailback": True,
    "noCrossVolumeIdentityChange": True,
}
for key, want in required.items():
    if status.get(key) != want:
        raise SystemExit(f"{key}={status.get(key)!r}, want {want!r}")
if status.get("failbackMutationAllowed") is not False:
    raise SystemExit("failbackMutationAllowed must remain false in status")
PY
write_summary "executor_status_failed_back=true"
write_summary "master_publisher_epoch_advanced=true"
write_summary "publish_target_swapped_after_failback=true"
write_summary "failback_status_mutation_allowed=false"

can_i() {
  local key="$1"
  local want="$2"
  shift 2
  local got
  got="$(kubectl --kubeconfig "${KUBECONFIG_PATH}" auth can-i "$@" --as "system:serviceaccount:${NAMESPACE}:sw-block-seaweed-block-failback-executor" -n "${NAMESPACE}" || true)"
  echo "${key}=${got}" >>"${FAILBACK_DIR}/rbac.txt"
  if [ "${got}" != "${want}" ]; then
    echo "RBAC ${key}: got ${got}, want ${want}" >&2
    return 1
  fi
  write_summary "${key}=${got}"
}

can_i "executor_patch_failback_status_allowed" "yes" patch swblockreplicafailbacks --subresource=status
can_i "executor_patch_swblockvolumes_denied" "no" patch swblockvolumes
can_i "executor_patch_pvc_denied" "no" patch pvc
can_i "executor_create_pods_denied" "no" create pods

cleanup
set -e
trap - EXIT
SW_BLOCK_ARTIFACT_DIR="${CLEANUP_DIR}/verify" \
SW_BLOCK_HELM_RELEASE="${RELEASE}" \
SW_BLOCK_HELM_NAMESPACE="${NAMESPACE}" \
  bash "${PRODUCT_ROOT}/scripts/verify-helm-cleanup.sh" >"${CLEANUP_DIR}/verify.stdout.txt" 2>"${CLEANUP_DIR}/verify.stderr.txt"
grep -q '^cleanup_status=ok$' "${CLEANUP_DIR}/verify/cleanup-summary.txt"
write_summary "cleanup_status=ok"
write_summary "phase95_failback_live_deployed_suite_status=ok"
