#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase54-authority-executor-negative-gate}"
KUBECTL="${KUBECTL:-kubectl}"
NAMESPACE="${SW_BLOCK_PHASE54_NAMESPACE:-sw-block-phase54-authority-executor-negative}"
EXEC_SA="sw-block-authority-executor-exec"
EXEC_USER="system:serviceaccount:${NAMESPACE}:${EXEC_SA}"
IMAGE="${SW_BLOCK_PHASE54_IMAGE:-sw-block:local}"
IMAGE_PULL_POLICY="${SW_BLOCK_PHASE54_IMAGE_PULL_POLICY:-IfNotPresent}"
NODE_NAME="${SW_BLOCK_PHASE54_NODE_NAME:-m02}"
SUMMARY="${ARTIFACT_DIR}/phase54-authority-executor-negative-summary.txt"

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
  "${KUBECTL}" delete job -n "${NAMESPACE}" -l block.seaweedfs.com/phase54-negative=true --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrolebinding sw-block-phase54-authority-executor-negative --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrole sw-block-phase54-authority-executor-negative --ignore-not-found >/dev/null 2>&1
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

write_summary "phase54_authority_executor_negative_status=running"
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
  name: sw-block-phase54-authority-executor-negative
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
  name: sw-block-phase54-authority-executor-negative
subjects:
  - kind: ServiceAccount
    name: ${EXEC_SA}
    namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sw-block-phase54-authority-executor-negative
YAML
run_expect_success apply-rbac "${KUBECTL}" apply -f "${ARTIFACT_DIR}/rbac.yaml"

can_i_as "exec_patch_swblockreplicaeligibilities_status_allowed" "yes" patch swblockreplicaeligibilities --subresource=status
can_i_as "exec_patch_swblockreplicaeligibilities_main_denied" "no" patch swblockreplicaeligibilities
can_i_as "exec_patch_swblockvolumes_status_denied" "no" patch swblockvolumes --subresource=status
can_i_as "exec_create_events_denied" "no" create events
can_i_as "exec_create_pods_denied" "no" create pods
can_i_as "exec_patch_pvc_denied" "no" patch pvc
can_i_as "exec_update_storageclass_denied" "no" update storageclasses

clear_objects() {
  run_capture "delete-synthetic-objects" "${KUBECTL}" delete swblockvolumes,swblockreplicaeligibilities --all -n "${NAMESPACE}" --ignore-not-found || true
}

apply_volume_status() {
  local name="$1"
  local volume_id="$2"
  local pvc_name="$3"
  local status_json="$4"
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
  printf '%s\n' "${status_json}" >"${ARTIFACT_DIR}/${name}-status.json"
  run_expect_success "patch-volume-status-${name}" "${KUBECTL}" patch swblockvolume "${name}" -n "${NAMESPACE}" --subresource=status --type=merge --patch-file "${ARTIFACT_DIR}/${name}-status.json"
}

volume_status_json() {
  local volume_id="$1"
  local pvc_name="$2"
  local replica_id="$3"
  local contract_decision="$4"
  local contract_reason="$5"
  local preflight_decision="$6"
  local preflight_reason="$7"
  local frontend_fenced="$8"
  local frontend_primary_ready="$9"
  local ack_known="${10}"
  local ack_eligible="${11}"
  local durable_known="${12}"
  local durable_lsn="${13}"
  local required_known="${14}"
  local required_lsn="${15}"
  cat <<JSON
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
      "ackEligibilityKnown": ${ack_known},
      "ackEligible": ${ack_eligible},
      "durableFrontierKnown": ${durable_known},
      "durableFrontierLsn": ${durable_lsn},
      "requiredFrontierKnown": ${required_known},
      "requiredFrontierLsn": ${required_lsn},
      "evidenceRefs": ["${pvc_name}-returned-evidence.txt"]
    }],
    "executorContracts": [{
      "actionType": "authority.reintegrate_returned_replica",
      "replicaID": "${replica_id}",
      "decision": "${contract_decision}",
      "reason": "${contract_reason}",
      "ownerExecutor": "authority_recovery_executor",
      "executionEnabled": false,
      "mutationAllowed": false,
      "preflightDecision": "${preflight_decision}",
      "preflightReason": "${preflight_reason}",
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
      "evidenceRefs": ["${pvc_name}-contract-evidence.txt"]
    }]
  }
}
JSON
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

run_executor_job() {
  local case_name="$1"
  local job_name="phase54-negative-${case_name}"
  "${KUBECTL}" delete job "${job_name}" -n "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
  cat >"${ARTIFACT_DIR}/${job_name}.yaml" <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
  namespace: ${NAMESPACE}
  labels:
    block.seaweedfs.com/phase54-negative: "true"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        block.seaweedfs.com/phase54-negative: "true"
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

field_from_log() {
  local field="$1"
  local log="$2"
  grep -o "${field}=[^ ]*" "${log}" | head -1 | cut -d= -f2
}

run_hold_case() {
  local case_name="$1"
  local expected_reason="$2"
  local status_json="$3"
  clear_objects
  apply_volume_status "${case_name}" "pvc-${case_name}" "pvc-${case_name}" "${status_json}"
  create_target "${case_name}-r1" "${case_name}" "pvc-${case_name}" "pvc-${case_name}" "r1"
  run_executor_job "${case_name}"
  local log="${ARTIFACT_DIR}/${case_name}-executor.log"
  if ! grep -q "authority_executor=blocked" "${log}"; then
    echo "${case_name} did not block" >&2
    exit 1
  fi
  assert_eq "${case_name}_blocked_reason" "$(field_from_log terminal_evidence_missing "${log}")" "1"
  assert_eq "${case_name}_mutation_attempts" "$(field_from_log mutation_attempts "${log}")" "0"
  assert_eq "${case_name}_target_reason_absent" "$(jsonpath "swblockreplicaeligibility/${case_name}-r1" '{.status.reasonCode}')" ""
  write_summary "${case_name}_expected_hold=${expected_reason}"
}

run_hold_case "blocked-preflight" "preflight_not_ready" "$(volume_status_json "pvc-blocked-preflight" "pvc-blocked-preflight" "r1" "blocked" "preflight_not_ready" "hold" "returned_replica_ack_eligibility_unknown" "true" "false" "false" "false" "true" "52" "true" "52")"
run_hold_case "stale-frontier" "durable_frontier_behind" "$(volume_status_json "pvc-stale-frontier" "pvc-stale-frontier" "r1" "disabled" "executor_policy_disabled" "ready" "preconditions_satisfied" "true" "false" "true" "false" "true" "51" "true" "52")"
run_hold_case "unsafe-frontend" "frontend_no_longer_fenced" "$(volume_status_json "pvc-unsafe-frontend" "pvc-unsafe-frontend" "r1" "disabled" "executor_policy_disabled" "ready" "preconditions_satisfied" "false" "true" "true" "false" "true" "52" "true" "52")"

clear_objects
apply_volume_status "ambiguous-target" "pvc-ambiguous-target" "pvc-ambiguous-target" "$(volume_status_json "pvc-ambiguous-target" "pvc-ambiguous-target" "r1" "disabled" "executor_policy_disabled" "ready" "preconditions_satisfied" "true" "false" "true" "false" "true" "52" "true" "52")"
create_target "ambiguous-target-r1-a" "ambiguous-target" "pvc-ambiguous-target" "pvc-ambiguous-target" "r1"
create_target "ambiguous-target-r1-b" "ambiguous-target" "pvc-ambiguous-target" "pvc-ambiguous-target" "r1"
run_executor_job "ambiguous-target"
assert_eq "ambiguous_target_missing_count" "$(field_from_log ack_eligibility_target_missing "${ARTIFACT_DIR}/ambiguous-target-executor.log")" "1"
assert_eq "ambiguous_target_mutation_attempts" "$(field_from_log mutation_attempts "${ARTIFACT_DIR}/ambiguous-target-executor.log")" "0"
assert_eq "ambiguous_target_a_reason_absent" "$(jsonpath swblockreplicaeligibility/ambiguous-target-r1-a '{.status.reasonCode}')" ""
assert_eq "ambiguous_target_b_reason_absent" "$(jsonpath swblockreplicaeligibility/ambiguous-target-r1-b '{.status.reasonCode}')" ""

clear_objects
apply_volume_status "identity-mismatch" "pvc-identity-mismatch" "pvc-identity-mismatch" "$(volume_status_json "pvc-identity-mismatch" "pvc-identity-mismatch" "r1" "disabled" "executor_policy_disabled" "ready" "preconditions_satisfied" "true" "false" "true" "false" "true" "52" "true" "52")"
create_target "identity-mismatch-r1" "other-volume" "pvc-other" "pvc-other" "r1"
run_executor_job "identity-mismatch"
assert_eq "identity_mismatch_target_missing_count" "$(field_from_log ack_eligibility_target_missing "${ARTIFACT_DIR}/identity-mismatch-executor.log")" "1"
assert_eq "identity_mismatch_mutation_attempts" "$(field_from_log mutation_attempts "${ARTIFACT_DIR}/identity-mismatch-executor.log")" "0"
assert_eq "identity_mismatch_target_reason_absent" "$(jsonpath swblockreplicaeligibility/identity-mismatch-r1 '{.status.reasonCode}')" ""

clear_objects
apply_volume_status "partial-a" "pvc-partial-a" "pvc-partial-a" "$(volume_status_json "pvc-partial-a" "pvc-partial-a" "r1" "disabled" "executor_policy_disabled" "ready" "preconditions_satisfied" "true" "false" "true" "false" "true" "52" "true" "52")"
create_target "partial-a-r1" "partial-a" "pvc-partial-a" "pvc-partial-a" "r1"
apply_volume_status "partial-b" "pvc-partial-b" "pvc-partial-b" "$(volume_status_json "pvc-partial-b" "pvc-partial-b" "r1" "blocked" "preflight_not_ready" "hold" "returned_replica_ack_eligibility_unknown" "true" "false" "false" "false" "true" "52" "true" "52")"
create_target "partial-b-r1" "partial-b" "pvc-partial-b" "pvc-partial-b" "r1"
apply_volume_status "partial-c" "pvc-partial-c" "pvc-partial-c" '{"status":{"volumeID":"pvc-partial-c","pvcName":"pvc-partial-c","status":"ready","reasonCode":"first_volume_verified","executorContracts":[]}}'
run_executor_job "partial"
if ! grep -q "authority_executor=partial" "${ARTIFACT_DIR}/partial-executor.log"; then
  echo "partial case did not report partial" >&2
  exit 1
fi
assert_eq "partial_contracts" "$(field_from_log contracts "${ARTIFACT_DIR}/partial-executor.log")" "2"
assert_eq "partial_mutation_attempts" "$(field_from_log mutation_attempts "${ARTIFACT_DIR}/partial-executor.log")" "1"
assert_eq "partial_terminal_missing" "$(field_from_log terminal_evidence_missing "${ARTIFACT_DIR}/partial-executor.log")" "1"
assert_eq "partial_a_reason" "$(jsonpath swblockreplicaeligibility/partial-a-r1 '{.status.reasonCode}')" "ack_eligibility_recorded"
assert_eq "partial_b_reason_absent" "$(jsonpath swblockreplicaeligibility/partial-b-r1 '{.status.reasonCode}')" ""
assert_eq "partial_c_no_target" "$("${KUBECTL}" get swblockreplicaeligibilities -n "${NAMESPACE}" --no-headers 2>/dev/null | grep -c 'partial-c' || true)" "0"

clear_objects
write_summary "phase54_authority_executor_negative_status=ok"
