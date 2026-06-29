#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase54-authority-executor-callsite-gate}"
KUBECTL="${KUBECTL:-kubectl}"
NAMESPACE="${SW_BLOCK_PHASE54_NAMESPACE:-sw-block-phase54-authority-executor-callsite}"
EXEC_SA="sw-block-authority-executor-exec"
EXEC_USER="system:serviceaccount:${NAMESPACE}:${EXEC_SA}"
IMAGE="${SW_BLOCK_PHASE54_IMAGE:-sw-block:local}"
IMAGE_PULL_POLICY="${SW_BLOCK_PHASE54_IMAGE_PULL_POLICY:-IfNotPresent}"
NODE_NAME="${SW_BLOCK_PHASE54_NODE_NAME:-m02}"
SUMMARY="${ARTIFACT_DIR}/phase54-authority-executor-callsite-summary.txt"

mkdir -p "${ARTIFACT_DIR}"
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

jsonpath() {
  local resource="$1"
  local path="$2"
  "${KUBECTL}" get "${resource}" -n "${NAMESPACE}" -o "jsonpath=${path}" 2>/dev/null || true
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

can_i_as() {
  local name="$1"
  local expected="$2"
  shift 2
  run_capture "can-i-${name}" "${KUBECTL}" auth can-i "$@" --as "${EXEC_USER}" -n "${NAMESPACE}" || true
  local actual
  actual="$(tr -d '\r\n' <"${ARTIFACT_DIR}/can-i-${name}.stdout.txt")"
  assert_eq "${name}" "${actual}" "${expected}"
}

cleanup_crds_if_empty() {
  local volume_count eligibility_count
  volume_count="$("${KUBECTL}" get swblockvolumes -A --no-headers 2>/dev/null | wc -l | tr -d ' ')"
  eligibility_count="$("${KUBECTL}" get swblockreplicaeligibilities -A --no-headers 2>/dev/null | wc -l | tr -d ' ')"
  if [ "${volume_count}" = "0" ] && [ "${eligibility_count}" = "0" ]; then
    "${KUBECTL}" delete crd swblockvolumes.block.seaweedfs.com swblockreplicaeligibilities.block.seaweedfs.com --ignore-not-found >/dev/null 2>&1 || true
  fi
}

cleanup() {
  set +e
  "${KUBECTL}" delete job -n "${NAMESPACE}" -l block.seaweedfs.com/phase54-callsite=true --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrolebinding sw-block-phase54-authority-executor-callsite --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrole sw-block-phase54-authority-executor-callsite --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete namespace "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1
  for _ in $(seq 1 30); do
    if ! "${KUBECTL}" get namespace "${NAMESPACE}" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  cleanup_crds_if_empty
}
trap cleanup EXIT

write_summary "phase54_authority_executor_callsite_status=running"
write_summary "phase54_authority_executor_image=${IMAGE}"
write_summary "phase54_authority_executor_node=${NODE_NAME}"

run_expect_success kubectl-version "${KUBECTL}" version --client=true
run_expect_success apply-volume-crd "${KUBECTL}" apply -f "${PRODUCT_ROOT}/charts/seaweed-block/crds/swblockvolumes.block.seaweedfs.com.yaml"
run_expect_success apply-eligibility-crd "${KUBECTL}" apply -f "${PRODUCT_ROOT}/charts/seaweed-block/crds/swblockreplicaeligibilities.block.seaweedfs.com.yaml"

cat >"${ARTIFACT_DIR}/namespace.yaml" <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: ${NAMESPACE}
  labels:
    block.seaweedfs.com/phase54-gate: "true"
YAML
run_expect_success apply-namespace "${KUBECTL}" apply -f "${ARTIFACT_DIR}/namespace.yaml"

cat >"${ARTIFACT_DIR}/rbac.yaml" <<YAML
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${EXEC_SA}
  namespace: ${NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: sw-block-phase54-authority-executor-callsite
rules:
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockvolumes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockreplicaeligibilities"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockreplicaeligibilities/status"]
    verbs: ["get", "update", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sw-block-phase54-authority-executor-callsite
subjects:
  - kind: ServiceAccount
    name: ${EXEC_SA}
    namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sw-block-phase54-authority-executor-callsite
YAML
run_expect_success apply-rbac "${KUBECTL}" apply -f "${ARTIFACT_DIR}/rbac.yaml"

can_i_as "exec_patch_swblockreplicaeligibilities_status_allowed" "yes" patch swblockreplicaeligibilities --subresource=status
can_i_as "exec_patch_swblockreplicaeligibilities_main_denied" "no" patch swblockreplicaeligibilities
can_i_as "exec_patch_swblockvolumes_status_denied" "no" patch swblockvolumes --subresource=status
can_i_as "exec_patch_swblockvolumes_finalizers_denied" "no" patch swblockvolumes --subresource=finalizers
can_i_as "exec_create_events_denied" "no" create events
can_i_as "exec_create_pods_denied" "no" create pods
can_i_as "exec_patch_pvc_denied" "no" patch pvc
can_i_as "exec_update_storageclass_denied" "no" update storageclasses

create_volume() {
  local name="$1"
  local volume_id="$2"
  local pvc_name="$3"
  local replica_id="$4"
  local frontend_fenced="$5"
  local frontend_primary_ready="$6"
  local durable_lsn="$7"
  local required_lsn="$8"

  cat >"${ARTIFACT_DIR}/${name}.yaml" <<YAML
apiVersion: block.seaweedfs.com/v1alpha1
kind: SwBlockVolume
metadata:
  name: ${name}
  namespace: ${NAMESPACE}
spec:
  pvcName: ${pvc_name}
YAML
  run_expect_success "apply-volume-${name}" "${KUBECTL}" apply -f "${ARTIFACT_DIR}/${name}.yaml"
  cat >"${ARTIFACT_DIR}/${name}-status.json" <<JSON
{
  "status": {
    "volumeID": "${volume_id}",
    "pvcName": "${pvc_name}",
    "status": "unknown",
    "reasonCode": "returned_replica_frontend_fenced",
    "replicaReintegrations": [{
      "replicaID": "${replica_id}",
      "state": "fenced",
      "reasonCode": "returned_replica_frontend_fenced",
      "frontendFenced": ${frontend_fenced},
      "frontendPrimaryReady": ${frontend_primary_ready},
      "ackEligibilityKnown": true,
      "ackEligible": false,
      "durableFrontierKnown": true,
      "durableFrontierLsn": ${durable_lsn},
      "requiredFrontierKnown": true,
      "requiredFrontierLsn": ${required_lsn},
      "evidenceRefs": ["${name}-returned-evidence.txt"]
    }],
    "executorContracts": [{
      "actionType": "authority.reintegrate_returned_replica",
      "replicaID": "${replica_id}",
      "decision": "disabled",
      "reason": "executor_policy_disabled",
      "ownerExecutor": "authority_recovery_executor",
      "executionEnabled": false,
      "mutationAllowed": false,
      "preflightDecision": "ready",
      "preflightReason": "preconditions_satisfied",
      "allowedMutationClass": ["ack_eligibility"],
      "forbiddenMutationClass": ["frontend_publication", "rebuild_traffic", "failback"],
      "terminalEvidenceRequired": [
        "ack_eligibility_known",
        "ack_eligible_true",
        "frontend_fenced_after_execution",
        "primary_unchanged",
        "durable_frontier_covered",
        "no_cross_volume_identity_change"
      ],
      "evidenceRefs": ["${name}-contract-evidence.txt"]
    }]
  }
}
JSON
  run_expect_success "patch-volume-status-${name}" "${KUBECTL}" patch swblockvolume "${name}" -n "${NAMESPACE}" --subresource=status --type=merge --patch-file "${ARTIFACT_DIR}/${name}-status.json"
}

create_target() {
  local name="$1"
  local volume_name="$2"
  local volume_id="$3"
  local pvc_name="$4"
  local replica_id="$5"
  cat >"${ARTIFACT_DIR}/${name}.yaml" <<YAML
apiVersion: block.seaweedfs.com/v1alpha1
kind: SwBlockReplicaEligibility
metadata:
  name: ${name}
  namespace: ${NAMESPACE}
spec:
  volumeName: ${volume_name}
  volumeID: ${volume_id}
  pvcName: ${pvc_name}
  replicaID: ${replica_id}
YAML
  run_expect_success "apply-target-${name}" "${KUBECTL}" apply -f "${ARTIFACT_DIR}/${name}.yaml"
}

clear_objects() {
  run_capture "delete-synthetic-objects" "${KUBECTL}" delete swblockvolumes,swblockreplicaeligibilities --all -n "${NAMESPACE}" --ignore-not-found || true
}

run_executor_job() {
  local case_name="$1"
  local job_name="phase54-executor-${case_name}"
  "${KUBECTL}" delete job "${job_name}" -n "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
  cat >"${ARTIFACT_DIR}/${job_name}.yaml" <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
  namespace: ${NAMESPACE}
  labels:
    block.seaweedfs.com/phase54-callsite: "true"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        block.seaweedfs.com/phase54-callsite: "true"
    spec:
      nodeSelector:
        kubernetes.io/hostname: ${NODE_NAME}
      serviceAccountName: ${EXEC_SA}
      restartPolicy: Never
      containers:
        - name: authority-executor
          image: ${IMAGE}
          imagePullPolicy: ${IMAGE_PULL_POLICY}
          command: ["/usr/local/bin/sw-block"]
          args:
            - "ops"
            - "authority-executor"
            - "--namespace=${NAMESPACE}"
            - "--allowed-mutation-class=ack_eligibility"
            - "--execution-policy"
            - "--enable-execution"
YAML
  run_expect_success "apply-job-${case_name}" "${KUBECTL}" apply -f "${ARTIFACT_DIR}/${job_name}.yaml"
  if ! run_capture "wait-job-${case_name}" "${KUBECTL}" wait --for=condition=complete "job/${job_name}" -n "${NAMESPACE}" --timeout=120s; then
    "${KUBECTL}" describe job "${job_name}" -n "${NAMESPACE}" >"${ARTIFACT_DIR}/${case_name}-job-describe.txt" 2>&1 || true
    "${KUBECTL}" logs "job/${job_name}" -n "${NAMESPACE}" >"${ARTIFACT_DIR}/${case_name}-executor.log" 2>&1 || true
    cat "${ARTIFACT_DIR}/${case_name}-executor.log" >&2 || true
    return 1
  fi
  run_expect_success "logs-job-${case_name}" "${KUBECTL}" logs "job/${job_name}" -n "${NAMESPACE}"
  cp "${ARTIFACT_DIR}/logs-job-${case_name}.stdout.txt" "${ARTIFACT_DIR}/${case_name}-executor.log"
}

clear_objects
create_volume "target-missing" "pvc-target-missing" "pvc-target-missing" "r1" "true" "false" "52" "52"
run_executor_job "target-missing"
target_missing_log="$(tr -d '\r' <"${ARTIFACT_DIR}/target-missing-executor.log")"
if ! grep -q "authority_executor=blocked" <<<"${target_missing_log}"; then
  echo "target-missing did not block" >&2
  exit 1
fi
if ! grep -q "ack_eligibility_target_missing=1" <<<"${target_missing_log}"; then
  echo "target-missing did not report missing target" >&2
  exit 1
fi
assert_eq "target_missing_mutation_attempts" "$(grep -o 'mutation_attempts=[0-9]*' "${ARTIFACT_DIR}/target-missing-executor.log" | head -1 | cut -d= -f2)" "0"
target_count="$("${KUBECTL}" get swblockreplicaeligibilities -n "${NAMESPACE}" --no-headers 2>/dev/null | wc -l | tr -d ' ')"
assert_eq "target_missing_created_targets" "${target_count}" "0"

clear_objects
create_volume "terminal-missing" "pvc-terminal-missing" "pvc-terminal-missing" "r1" "false" "false" "52" "52"
create_target "terminal-missing-r1" "terminal-missing" "pvc-terminal-missing" "pvc-terminal-missing" "r1"
run_executor_job "terminal-missing"
terminal_missing_log="$(tr -d '\r' <"${ARTIFACT_DIR}/terminal-missing-executor.log")"
if ! grep -q "authority_executor=blocked" <<<"${terminal_missing_log}"; then
  echo "terminal-missing did not block" >&2
  exit 1
fi
if ! grep -q "terminal_evidence_missing=1" <<<"${terminal_missing_log}"; then
  echo "terminal-missing did not report terminal evidence missing" >&2
  exit 1
fi
assert_eq "terminal_missing_mutation_attempts" "$(grep -o 'mutation_attempts=[0-9]*' "${ARTIFACT_DIR}/terminal-missing-executor.log" | head -1 | cut -d= -f2)" "0"
assert_eq "terminal_missing_target_reason_absent" "$(jsonpath swblockreplicaeligibility/terminal-missing-r1 '{.status.reasonCode}')" ""

clear_objects
create_volume "complete" "pvc-complete" "pvc-complete" "r1" "true" "false" "52" "52"
create_target "complete-r1" "complete" "pvc-complete" "pvc-complete" "r1"
run_executor_job "complete"
complete_log="$(tr -d '\r' <"${ARTIFACT_DIR}/complete-executor.log")"
if ! grep -q "authority_executor=executed" <<<"${complete_log}"; then
  echo "complete case did not execute" >&2
  exit 1
fi
assert_eq "complete_mutation_attempts" "$(grep -o 'mutation_attempts=[0-9]*' "${ARTIFACT_DIR}/complete-executor.log" | head -1 | cut -d= -f2)" "1"
assert_eq "complete_ack_eligibility_mutation_attempts" "$(grep -o 'ack_eligibility_mutation_attempts=[0-9]*' "${ARTIFACT_DIR}/complete-executor.log" | head -1 | cut -d= -f2)" "1"
assert_eq "complete_target_reason" "$(jsonpath swblockreplicaeligibility/complete-r1 '{.status.reasonCode}')" "ack_eligibility_recorded"
assert_eq "complete_target_ack_known" "$(jsonpath swblockreplicaeligibility/complete-r1 '{.status.ackEligibilityKnown}')" "true"
assert_eq "complete_target_ack_eligible" "$(jsonpath swblockreplicaeligibility/complete-r1 '{.status.ackEligible}')" "true"
assert_eq "complete_target_frontend_fenced" "$(jsonpath swblockreplicaeligibility/complete-r1 '{.status.frontendFencedAfterExecution}')" "true"
assert_eq "complete_target_primary_unchanged" "$(jsonpath swblockreplicaeligibility/complete-r1 '{.status.primaryUnchanged}')" "true"
assert_eq "complete_target_frontier_covered" "$(jsonpath swblockreplicaeligibility/complete-r1 '{.status.durableFrontierCovered}')" "true"
assert_eq "complete_target_no_cross_volume" "$(jsonpath swblockreplicaeligibility/complete-r1 '{.status.noCrossVolumeIdentityChange}')" "true"
assert_eq "complete_target_ready_condition" "$(jsonpath swblockreplicaeligibility/complete-r1 '{.status.conditions[0].reason}')" "ack_eligibility_recorded"
assert_eq "complete_swblockvolume_ack_still_false" "$(jsonpath swblockvolume/complete '{.status.replicaReintegrations[0].ackEligible}')" "false"
run_expect_success "complete-target-json" "${KUBECTL}" get swblockreplicaeligibility complete-r1 -n "${NAMESPACE}" -o json
if ! grep -q '"no_frontend_publication"' "${ARTIFACT_DIR}/complete-target-json.stdout.txt" ||
   ! grep -q '"no_rebuild_traffic"' "${ARTIFACT_DIR}/complete-target-json.stdout.txt" ||
   ! grep -q '"no_failback"' "${ARTIFACT_DIR}/complete-target-json.stdout.txt" ||
   ! grep -q '"no_primary_authority_change"' "${ARTIFACT_DIR}/complete-target-json.stdout.txt"; then
  echo "complete target missing non-claims" >&2
  exit 1
fi
write_summary "complete_target_nonclaims_ok=true"

clear_objects
write_summary "phase54_authority_executor_callsite_status=ok"
