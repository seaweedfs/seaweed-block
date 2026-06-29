#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase54-authority-executor-multivolume-gate}"
KUBECTL="${KUBECTL:-kubectl}"
NAMESPACE="${SW_BLOCK_PHASE54_NAMESPACE:-sw-block-phase54-authority-executor-multivolume}"
EXEC_SA="sw-block-authority-executor-exec"
EXEC_USER="system:serviceaccount:${NAMESPACE}:${EXEC_SA}"
IMAGE="${SW_BLOCK_PHASE54_IMAGE:-sw-block:local}"
IMAGE_PULL_POLICY="${SW_BLOCK_PHASE54_IMAGE_PULL_POLICY:-IfNotPresent}"
NODE_NAME="${SW_BLOCK_PHASE54_NODE_NAME:-m02}"
SUMMARY="${ARTIFACT_DIR}/phase54-authority-executor-multivolume-summary.txt"

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
  "${KUBECTL}" delete job -n "${NAMESPACE}" -l block.seaweedfs.com/phase54-multivolume=true --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrolebinding sw-block-phase54-authority-executor-multivolume --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrole sw-block-phase54-authority-executor-multivolume --ignore-not-found >/dev/null 2>&1
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

write_summary "phase54_authority_executor_multivolume_status=running"
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
  name: sw-block-phase54-authority-executor-multivolume
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
  name: sw-block-phase54-authority-executor-multivolume
subjects:
  - kind: ServiceAccount
    name: ${EXEC_SA}
    namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sw-block-phase54-authority-executor-multivolume
YAML
run_expect_success apply-rbac "${KUBECTL}" apply -f "${ARTIFACT_DIR}/rbac.yaml"

can_i_as "exec_patch_swblockreplicaeligibilities_status_allowed" "yes" patch swblockreplicaeligibilities --subresource=status
can_i_as "exec_patch_swblockreplicaeligibilities_main_denied" "no" patch swblockreplicaeligibilities
can_i_as "exec_patch_swblockvolumes_status_denied" "no" patch swblockvolumes --subresource=status
can_i_as "exec_create_events_denied" "no" create events

apply_volume_status() {
  local name="$1"
  local pvc_name="$2"
  local status_json="$3"
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

no_contract_status_json() {
  local volume_id="$1"
  local pvc_name="$2"
  cat <<JSON
{"status":{"volumeID":"${volume_id}","pvcName":"${pvc_name}","status":"ready","reasonCode":"first_volume_verified","executorContracts":[]}}
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
  local job_name="phase54-multivolume"
  "${KUBECTL}" delete job "${job_name}" -n "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
  cat >"${ARTIFACT_DIR}/${job_name}.yaml" <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
  namespace: ${NAMESPACE}
  labels:
    block.seaweedfs.com/phase54-multivolume: "true"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        block.seaweedfs.com/phase54-multivolume: "true"
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
  run_expect_success "apply-job" "${KUBECTL}" apply -f "${ARTIFACT_DIR}/${job_name}.yaml"
  if ! run_capture "wait-job" "${KUBECTL}" wait --for=condition=complete "job/${job_name}" -n "${NAMESPACE}" --timeout=120s; then
    "${KUBECTL}" describe job "${job_name}" -n "${NAMESPACE}" >"${ARTIFACT_DIR}/job-describe.txt" 2>&1 || true
    "${KUBECTL}" logs "job/${job_name}" -n "${NAMESPACE}" >"${ARTIFACT_DIR}/executor.log" 2>&1 || true
    cat "${ARTIFACT_DIR}/executor.log" >&2 || true
    return 1
  fi
  run_expect_success "logs-job" "${KUBECTL}" logs "job/${job_name}" -n "${NAMESPACE}"
  cp "${ARTIFACT_DIR}/logs-job.stdout.txt" "${ARTIFACT_DIR}/executor.log"
}

field_from_log() {
  local field="$1"
  local log="$2"
  grep -o "${field}=[^ ]*" "${log}" | head -1 | cut -d= -f2
}

apply_volume_status "eligible-a" "pvc-eligible-a" "$(volume_status_json "pvc-eligible-a" "pvc-eligible-a" "r1" "disabled" "executor_policy_disabled" "ready" "preconditions_satisfied" "true" "false" "true" "false" "true" "52" "true" "52")"
create_target "eligible-a-r1" "eligible-a" "pvc-eligible-a" "pvc-eligible-a" "r1"

apply_volume_status "eligible-b" "pvc-eligible-b" "$(volume_status_json "pvc-eligible-b" "pvc-eligible-b" "r2" "disabled" "executor_policy_disabled" "ready" "preconditions_satisfied" "true" "false" "true" "false" "true" "77" "true" "77")"
create_target "eligible-b-r2" "eligible-b" "pvc-eligible-b" "pvc-eligible-b" "r2"

apply_volume_status "blocked-c" "pvc-blocked-c" "$(volume_status_json "pvc-blocked-c" "pvc-blocked-c" "r3" "blocked" "preflight_not_ready" "hold" "returned_replica_ack_eligibility_unknown" "true" "false" "false" "false" "true" "52" "true" "52")"
create_target "blocked-c-r3" "blocked-c" "pvc-blocked-c" "pvc-blocked-c" "r3"

apply_volume_status "no-contract-d" "pvc-no-contract-d" "$(no_contract_status_json "pvc-no-contract-d" "pvc-no-contract-d")"

apply_volume_status "mismatch-e" "pvc-mismatch-e" "$(volume_status_json "pvc-mismatch-e" "pvc-mismatch-e" "r4" "disabled" "executor_policy_disabled" "ready" "preconditions_satisfied" "true" "false" "true" "false" "true" "99" "true" "99")"
create_target "mismatch-e-r4" "other-volume" "pvc-other" "pvc-other" "r4"

run_executor_job
if ! grep -q "authority_executor=partial" "${ARTIFACT_DIR}/executor.log"; then
  echo "multivolume case did not report partial" >&2
  exit 1
fi
assert_eq "multivolume_contracts" "$(field_from_log contracts "${ARTIFACT_DIR}/executor.log")" "4"
assert_eq "multivolume_mutation_attempts" "$(field_from_log mutation_attempts "${ARTIFACT_DIR}/executor.log")" "2"
assert_eq "multivolume_ack_mutation_attempts" "$(field_from_log ack_eligibility_mutation_attempts "${ARTIFACT_DIR}/executor.log")" "2"
assert_eq "multivolume_terminal_missing" "$(field_from_log terminal_evidence_missing "${ARTIFACT_DIR}/executor.log")" "1"
assert_eq "multivolume_target_missing" "$(field_from_log ack_eligibility_target_missing "${ARTIFACT_DIR}/executor.log")" "1"

assert_eq "eligible_a_reason" "$(jsonpath swblockreplicaeligibility/eligible-a-r1 '{.status.reasonCode}')" "ack_eligibility_recorded"
assert_eq "eligible_a_ack" "$(jsonpath swblockreplicaeligibility/eligible-a-r1 '{.status.ackEligible}')" "true"
assert_eq "eligible_a_no_cross_volume" "$(jsonpath swblockreplicaeligibility/eligible-a-r1 '{.status.noCrossVolumeIdentityChange}')" "true"
assert_eq "eligible_a_source_ack_still_false" "$(jsonpath swblockvolume/eligible-a '{.status.replicaReintegrations[0].ackEligible}')" "false"

assert_eq "eligible_b_reason" "$(jsonpath swblockreplicaeligibility/eligible-b-r2 '{.status.reasonCode}')" "ack_eligibility_recorded"
assert_eq "eligible_b_ack" "$(jsonpath swblockreplicaeligibility/eligible-b-r2 '{.status.ackEligible}')" "true"
assert_eq "eligible_b_no_cross_volume" "$(jsonpath swblockreplicaeligibility/eligible-b-r2 '{.status.noCrossVolumeIdentityChange}')" "true"
assert_eq "eligible_b_source_ack_still_false" "$(jsonpath swblockvolume/eligible-b '{.status.replicaReintegrations[0].ackEligible}')" "false"

assert_eq "blocked_c_reason_absent" "$(jsonpath swblockreplicaeligibility/blocked-c-r3 '{.status.reasonCode}')" ""
assert_eq "mismatch_e_reason_absent" "$(jsonpath swblockreplicaeligibility/mismatch-e-r4 '{.status.reasonCode}')" ""
assert_eq "no_contract_d_target_count" "$("${KUBECTL}" get swblockreplicaeligibilities -n "${NAMESPACE}" --no-headers 2>/dev/null | grep -c 'no-contract-d' || true)" "0"

run_expect_success "targets-json" "${KUBECTL}" get swblockreplicaeligibilities -n "${NAMESPACE}" -o json
eligible_written_count="$("${KUBECTL}" get swblockreplicaeligibilities -n "${NAMESPACE}" -o jsonpath='{range .items[*]}{.status.reasonCode}{"\n"}{end}' | grep -c '^ack_eligibility_recorded$' || true)"
assert_eq "eligible_written_count" "${eligible_written_count}" "2"

cross_contamination_count="$("${KUBECTL}" get swblockreplicaeligibilities -n "${NAMESPACE}" -o jsonpath='{range .items[*]}{.metadata.name}{"="}{.status.reasonCode}{"\n"}{end}' | grep -E 'blocked-c|mismatch-e|no-contract-d' | grep -c 'ack_eligibility_recorded' || true)"
assert_eq "cross_contamination_count" "${cross_contamination_count}" "0"

run_capture "delete-synthetic-objects" "${KUBECTL}" delete swblockvolumes,swblockreplicaeligibilities --all -n "${NAMESPACE}" --ignore-not-found || true
write_summary "phase54_authority_executor_multivolume_status=ok"
