#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase59-rebuild-planning-close-gate}"
KUBECTL="${KUBECTL:-kubectl}"
NAMESPACE="${SW_BLOCK_PHASE59_NAMESPACE:-sw-block-phase59-rebuild-planning}"
NODE_NAME="${SW_BLOCK_PHASE59_NODE_NAME:-m02}"
RUNNER_IMAGE="${SW_BLOCK_PHASE59_RUNNER_IMAGE:-busybox:1.36}"
IMAGE_PULL_POLICY="${SW_BLOCK_PHASE59_IMAGE_PULL_POLICY:-IfNotPresent}"
TARGET_OWNER_SA="sw-block-rebuild-target-owner"
EXEC_SA="sw-block-authority-executor-exec"
SUMMARY="${ARTIFACT_DIR}/phase59-rebuild-planning-close-summary.txt"
BIN_DIR="${ARTIFACT_DIR}/bin"
VOLUME_NAME="phase59-volume"
REBUILD_TARGET_NAME="phase59-volume-r2-rebuild"
VOLUME_CRD="swblockvolumes.block.seaweedfs.com"
REBUILD_CRD="swblockreplicarebuilds.block.seaweedfs.com"

mkdir -p "${ARTIFACT_DIR}" "${BIN_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

run_capture() {
  local name="$1"
  shift
  "$@" >"${ARTIFACT_DIR}/${name}.stdout.txt" 2>"${ARTIFACT_DIR}/${name}.stderr.txt"
}

run_expect_success() {
  local name="$1"
  shift
  if run_capture "$name" "$@"; then
    return 0
  fi
  echo "expected success for ${name}" >&2
  cat "${ARTIFACT_DIR}/${name}.stderr.txt" >&2 || true
  return 1
}

assert_eq() {
  local name="$1"
  local actual="$2"
  local expected="$3"
  write_summary "${name}=${actual}"
  if [ "${actual}" != "${expected}" ]; then
    echo "${name}: got ${actual}, expected ${expected}" >&2
    return 1
  fi
}

crd_preexisting() {
  local crd="$1"
  if "${KUBECTL}" get crd "${crd}" >/dev/null 2>&1; then
    echo "true"
  else
    echo "false"
  fi
}

PREEXISTING_VOLUME_CRD="$(crd_preexisting "${VOLUME_CRD}")"
PREEXISTING_REBUILD_CRD="$(crd_preexisting "${REBUILD_CRD}")"

cleanup() {
  set +e
  "${KUBECTL}" delete job -n "${NAMESPACE}" -l block.seaweedfs.com/phase59=true --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrolebinding sw-block-phase59-rebuild-target-owner sw-block-phase59-authority-executor --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrole sw-block-phase59-rebuild-target-owner sw-block-phase59-authority-executor --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete namespace "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1
  for _ in $(seq 1 30); do
    if ! "${KUBECTL}" get namespace "${NAMESPACE}" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  if [ "${PREEXISTING_REBUILD_CRD}" != "true" ]; then
    "${KUBECTL}" delete crd "${REBUILD_CRD}" --ignore-not-found >/dev/null 2>&1
  fi
  if [ "${PREEXISTING_VOLUME_CRD}" != "true" ]; then
    "${KUBECTL}" delete crd "${VOLUME_CRD}" --ignore-not-found >/dev/null 2>&1
  fi
}
trap cleanup EXIT

write_summary "phase59_rebuild_planning_close_status=running"
write_summary "phase59_node=${NODE_NAME}"
write_summary "preexisting_volume_crd=${PREEXISTING_VOLUME_CRD}"
write_summary "preexisting_rebuild_crd=${PREEXISTING_REBUILD_CRD}"

run_expect_success kubectl-version "${KUBECTL}" version --client=true
run_expect_success build-sw-block go build -o "${BIN_DIR}/sw-block" "${PRODUCT_ROOT}/cmd/sw-block"
run_expect_success sw-block-version "${BIN_DIR}/sw-block" --version

run_expect_success apply-volume-crd "${KUBECTL}" apply -f "${PRODUCT_ROOT}/charts/seaweed-block/crds/swblockvolumes.block.seaweedfs.com.yaml"
run_expect_success apply-rebuild-crd "${KUBECTL}" apply -f "${PRODUCT_ROOT}/charts/seaweed-block/crds/swblockreplicarebuilds.block.seaweedfs.com.yaml"
run_expect_success wait-volume-crd-established "${KUBECTL}" wait --for=condition=Established "crd/${VOLUME_CRD}" --timeout=60s
run_expect_success wait-rebuild-crd-established "${KUBECTL}" wait --for=condition=Established "crd/${REBUILD_CRD}" --timeout=60s

cat >"${ARTIFACT_DIR}/namespace.yaml" <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: ${NAMESPACE}
  labels:
    block.seaweedfs.com/phase59-gate: "true"
YAML
run_expect_success apply-namespace "${KUBECTL}" apply -f "${ARTIFACT_DIR}/namespace.yaml"

wait_crd_storage_ready() {
  local name="$1"
  local resource="$2"
  for attempt in $(seq 1 60); do
    if run_capture "storage-ready-${name}-${attempt}" "${KUBECTL}" get "${resource}" -n "${NAMESPACE}"; then
      write_summary "${name}_storage_ready_attempt=${attempt}"
      return 0
    fi
    sleep 1
  done
  echo "${name} storage did not become ready" >&2
  cat "${ARTIFACT_DIR}/storage-ready-${name}-60.stderr.txt" >&2 || true
  return 1
}

wait_crd_storage_ready swblockvolumes swblockvolumes
wait_crd_storage_ready swblockreplicarebuilds swblockreplicarebuilds

cat >"${ARTIFACT_DIR}/rbac.yaml" <<YAML
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${TARGET_OWNER_SA}
  namespace: ${NAMESPACE}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${EXEC_SA}
  namespace: ${NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: sw-block-phase59-rebuild-target-owner
rules:
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockvolumes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockreplicarebuilds"]
    verbs: ["get", "list", "watch", "create"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: sw-block-phase59-authority-executor
rules:
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockvolumes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockreplicarebuilds"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockreplicarebuilds/status"]
    verbs: ["get", "update", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sw-block-phase59-rebuild-target-owner
subjects:
  - kind: ServiceAccount
    name: ${TARGET_OWNER_SA}
    namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sw-block-phase59-rebuild-target-owner
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sw-block-phase59-authority-executor
subjects:
  - kind: ServiceAccount
    name: ${EXEC_SA}
    namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sw-block-phase59-authority-executor
YAML
run_expect_success apply-rbac "${KUBECTL}" apply -f "${ARTIFACT_DIR}/rbac.yaml"

cat >"${ARTIFACT_DIR}/volume.yaml" <<YAML
apiVersion: block.seaweedfs.com/v1alpha1
kind: SwBlockVolume
metadata:
  name: ${VOLUME_NAME}
  namespace: ${NAMESPACE}
spec:
  pvcName: phase59-pvc
YAML
run_expect_success apply-volume "${KUBECTL}" apply -f "${ARTIFACT_DIR}/volume.yaml"

cat >"${ARTIFACT_DIR}/volume-status.json" <<JSON
{
  "status": {
    "volumeID": "pvc-phase59",
    "pvcName": "phase59-pvc",
    "status": "unknown",
    "reasonCode": "candidate_frontier_behind",
    "replicaReintegrations": [{
      "replicaID": "r2",
      "state": "recovering",
      "reasonCode": "candidate_frontier_behind",
      "frontendFenced": true,
      "frontendPrimaryReady": false,
      "ackEligibilityKnown": true,
      "ackEligible": false,
      "durableFrontierKnown": true,
      "durableFrontierLsn": 51,
      "requiredFrontierKnown": true,
      "requiredFrontierLsn": 52,
      "evidenceRefs": ["phase59-returned-replica-evidence.txt"]
    }],
    "executorContracts": [{
      "actionType": "authority.rebuild_returned_replica",
      "replicaID": "r2",
      "decision": "disabled",
      "reason": "executor_policy_disabled",
      "ownerExecutor": "authority_recovery_executor",
      "executionEnabled": false,
      "mutationAllowed": false,
      "preflightDecision": "ready",
      "preflightReason": "preconditions_satisfied",
      "allowedMutationClass": ["rebuild_traffic"],
      "forbiddenMutationClass": ["ack_eligibility", "frontend_publication", "failback"],
      "terminalEvidenceRequired": [
        "frontend_fenced_before_rebuild",
        "primary_unchanged",
        "durable_frontier_caught_up",
        "no_frontend_publication",
        "no_cross_volume_identity_change"
      ],
      "evidenceRefs": ["phase59-rebuild-contract-evidence.txt"]
    }]
  }
}
JSON
run_expect_success patch-volume-status "${KUBECTL}" patch swblockvolume "${VOLUME_NAME}" -n "${NAMESPACE}" --subresource=status --type=merge --patch-file "${ARTIFACT_DIR}/volume-status.json"

job_manifest() {
  local name="$1"
  local service_account="$2"
  shift 2
  local args_yaml=""
  for arg in "$@"; do
    args_yaml="${args_yaml}            - \"${arg}\"\n"
  done
  cat >"${ARTIFACT_DIR}/${name}.yaml" <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: ${name}
  namespace: ${NAMESPACE}
  labels:
    block.seaweedfs.com/phase59: "true"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        block.seaweedfs.com/phase59: "true"
    spec:
      nodeName: ${NODE_NAME}
      serviceAccountName: ${service_account}
      restartPolicy: Never
      containers:
        - name: sw-block
          image: ${RUNNER_IMAGE}
          imagePullPolicy: ${IMAGE_PULL_POLICY}
          command: ["/sw-block-bin/sw-block"]
          args:
$(printf "%b" "${args_yaml}")
          volumeMounts:
            - name: sw-block-bin
              mountPath: /sw-block-bin
              readOnly: true
      volumes:
        - name: sw-block-bin
          hostPath:
            path: ${BIN_DIR}
            type: Directory
YAML
}

run_job() {
  local name="$1"
  run_expect_success "apply-job-${name}" "${KUBECTL}" apply -f "${ARTIFACT_DIR}/${name}.yaml"
  if run_capture "wait-job-${name}" "${KUBECTL}" wait -n "${NAMESPACE}" --for=condition=complete "job/${name}" --timeout=90s; then
    run_capture "logs-job-${name}" "${KUBECTL}" logs -n "${NAMESPACE}" "job/${name}" || true
    return 0
  fi
  "${KUBECTL}" describe job "${name}" -n "${NAMESPACE}" >"${ARTIFACT_DIR}/describe-job-${name}.txt" 2>&1 || true
  "${KUBECTL}" logs -n "${NAMESPACE}" "job/${name}" >"${ARTIFACT_DIR}/logs-job-${name}.stdout.txt" 2>"${ARTIFACT_DIR}/logs-job-${name}.stderr.txt" || true
  echo "job ${name} did not complete" >&2
  cat "${ARTIFACT_DIR}/wait-job-${name}.stderr.txt" >&2 || true
  cat "${ARTIFACT_DIR}/logs-job-${name}.stdout.txt" >&2 || true
  return 1
}

job_manifest phase59-target-owner-1 "${TARGET_OWNER_SA}" ops rebuild-target-owner "--namespace=${NAMESPACE}"
run_job phase59-target-owner-1
cat "${ARTIFACT_DIR}/logs-job-phase59-target-owner-1.stdout.txt" >>"${SUMMARY}"

target_count="$("${KUBECTL}" get swblockreplicarebuilds -n "${NAMESPACE}" --no-headers 2>/dev/null | wc -l | tr -d ' ')"
assert_eq "rebuild_target_count_after_owner" "${target_count}" "1"

target_volume="$("${KUBECTL}" get swblockreplicarebuilds "${REBUILD_TARGET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.spec.volumeName}')"
target_replica="$("${KUBECTL}" get swblockreplicarebuilds "${REBUILD_TARGET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.spec.replicaID}')"
target_status="$("${KUBECTL}" get swblockreplicarebuilds "${REBUILD_TARGET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.status.state}' 2>/dev/null || true)"
assert_eq "rebuild_target_volume_name" "${target_volume}" "${VOLUME_NAME}"
assert_eq "rebuild_target_replica_id" "${target_replica}" "r2"
assert_eq "rebuild_target_status_before_executor" "${target_status}" ""

job_manifest phase59-target-owner-2 "${TARGET_OWNER_SA}" ops rebuild-target-owner "--namespace=${NAMESPACE}"
run_job phase59-target-owner-2
cat "${ARTIFACT_DIR}/logs-job-phase59-target-owner-2.stdout.txt" >>"${SUMMARY}"

target_count="$("${KUBECTL}" get swblockreplicarebuilds -n "${NAMESPACE}" --no-headers 2>/dev/null | wc -l | tr -d ' ')"
assert_eq "rebuild_target_count_after_idempotent_owner" "${target_count}" "1"

job_manifest phase59-authority-executor "${EXEC_SA}" ops authority-executor "--namespace=${NAMESPACE}" "--allowed-mutation-class=rebuild_traffic" "--enable-execution" "--execution-policy"
run_job phase59-authority-executor
cat "${ARTIFACT_DIR}/logs-job-phase59-authority-executor.stdout.txt" >>"${SUMMARY}"

planned_state="$("${KUBECTL}" get swblockreplicarebuilds "${REBUILD_TARGET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.status.state}')"
planned_reason="$("${KUBECTL}" get swblockreplicarebuilds "${REBUILD_TARGET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.status.reasonCode}')"
traffic_started="$("${KUBECTL}" get swblockreplicarebuilds "${REBUILD_TARGET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.status.rebuildTrafficStarted}')"
no_frontend="$("${KUBECTL}" get swblockreplicarebuilds "${REBUILD_TARGET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.status.noFrontendPublication}')"
no_cross_volume="$("${KUBECTL}" get swblockreplicarebuilds "${REBUILD_TARGET_NAME}" -n "${NAMESPACE}" -o jsonpath='{.status.noCrossVolumeIdentityChange}')"
assert_eq "rebuild_status_state_after_executor" "${planned_state}" "planned"
assert_eq "rebuild_status_reason_after_executor" "${planned_reason}" "rebuild_progress_planned"
assert_eq "rebuild_traffic_started_after_executor" "${traffic_started}" "false"
assert_eq "no_frontend_publication_after_executor" "${no_frontend}" "true"
assert_eq "no_cross_volume_identity_change_after_executor" "${no_cross_volume}" "true"

volume_status="$("${KUBECTL}" get swblockvolume "${VOLUME_NAME}" -n "${NAMESPACE}" -o jsonpath='{.status.reasonCode}')"
volume_finalizers="$("${KUBECTL}" get swblockvolume "${VOLUME_NAME}" -n "${NAMESPACE}" -o jsonpath='{.metadata.finalizers}' 2>/dev/null || true)"
assert_eq "swblockvolume_reason_unchanged" "${volume_status}" "candidate_frontier_behind"
assert_eq "swblockvolume_finalizers_unchanged" "${volume_finalizers}" ""

write_summary "phase59_rebuild_planning_close_status=ok"
