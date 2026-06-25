#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase71-frontend-publication-live-api-boundary-gate}"
KUBECTL="${KUBECTL:-kubectl}"
NAMESPACE="${SW_BLOCK_PHASE71_NAMESPACE:-sw-block-phase71-gate}"
SUMMARY="${ARTIFACT_DIR}/phase71-frontend-publication-live-api-boundary-summary.txt"
EXECUTOR_AS="system:serviceaccount:${NAMESPACE}:sw-block-frontend-publication-executor"

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

run_expect_failure() {
  local name="$1"
  shift
  if run_capture "$name" "$@"; then
    echo "expected failure for ${name}" >&2
    cat "${ARTIFACT_DIR}/${name}.stdout.txt" >&2 || true
    return 1
  fi
  return 0
}

can_i_as() {
  local user="$1"
  local key="$2"
  local expected="$3"
  shift 3
  local allowed="false"
  if "${KUBECTL}" auth can-i "$@" --as "${user}" -n "${NAMESPACE}" >/dev/null 2>&1; then
    allowed="true"
  fi
  write_summary "${key}=${allowed}"
  if [ "${allowed}" != "${expected}" ]; then
    echo "unexpected ${key}: got ${allowed}, expected ${expected}" >&2
    return 1
  fi
}

jsonpath_equals() {
  local name="$1"
  local path="$2"
  local expected="$3"
  run_expect_success "${name}" "${KUBECTL}" get swblockfrontendpublication phase71-target -n "${NAMESPACE}" -o "jsonpath=${path}"
  local actual
  actual="$(cat "${ARTIFACT_DIR}/${name}.stdout.txt")"
  if [ "${actual}" != "${expected}" ]; then
    echo "unexpected ${name}: got '${actual}', expected '${expected}'" >&2
    return 1
  fi
}

cleanup() {
  set +e
  "${KUBECTL}" delete clusterrolebinding sw-block-phase71-frontend-publication-executor --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrole sw-block-phase71-frontend-publication-executor --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete namespace "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1
}
trap cleanup EXIT

write_summary "phase71_frontend_publication_live_api_boundary_status=running"
write_summary "harness=live_kubernetes_crd_rbac"
write_summary "frontend_publication_attempts=0"
write_summary "failback_attempts=0"
write_summary "storage_mutation_allowed=false"

run_expect_success apply-crd "${KUBECTL}" apply -f "${PRODUCT_ROOT}/charts/seaweed-block/crds/swblockfrontendpublications.block.seaweedfs.com.yaml"

cat >"${ARTIFACT_DIR}/namespace.yaml" <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: ${NAMESPACE}
YAML
run_expect_success apply-namespace "${KUBECTL}" apply -f "${ARTIFACT_DIR}/namespace.yaml"

cat >"${ARTIFACT_DIR}/rbac.yaml" <<YAML
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sw-block-frontend-publication-executor
  namespace: ${NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: sw-block-phase71-frontend-publication-executor
rules:
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockfrontendpublications"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockfrontendpublications/status"]
    verbs: ["get", "update", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sw-block-phase71-frontend-publication-executor
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sw-block-phase71-frontend-publication-executor
subjects:
  - kind: ServiceAccount
    name: sw-block-frontend-publication-executor
    namespace: ${NAMESPACE}
YAML
run_expect_success apply-rbac "${KUBECTL}" apply -f "${ARTIFACT_DIR}/rbac.yaml"

cat >"${ARTIFACT_DIR}/target.yaml" <<YAML
apiVersion: block.seaweedfs.com/v1alpha1
kind: SwBlockFrontendPublication
metadata:
  name: phase71-target
  namespace: ${NAMESPACE}
  labels:
    keep: "true"
  annotations:
    keep: "true"
spec:
  volumeName: phase71-volume
  volumeID: pvc-phase71
  pvcName: phase71-pvc
  replicaID: r2
  sourceEligibilityName: phase71-eligibility
  ackEligibilityKnown: true
  ackEligible: true
  frontendFencedAfterExecution: true
  primaryUnchanged: true
  durableFrontierCovered: true
  noCrossVolumeIdentityChange: true
  frontendPublicationDecision: disabled
  frontendPublicationReason: frontend_publication_policy_disabled
  frontendPublicationMutationAllowed: false
YAML
run_expect_success apply-target "${KUBECTL}" apply -f "${ARTIFACT_DIR}/target.yaml"

can_i_as "${EXECUTOR_AS}" "executor_get_targets_allowed" "true" get swblockfrontendpublications
can_i_as "${EXECUTOR_AS}" "executor_list_targets_allowed" "true" list swblockfrontendpublications
can_i_as "${EXECUTOR_AS}" "executor_watch_targets_allowed" "true" watch swblockfrontendpublications
can_i_as "${EXECUTOR_AS}" "executor_patch_target_status_allowed" "true" patch swblockfrontendpublications --subresource=status
can_i_as "${EXECUTOR_AS}" "executor_update_target_status_allowed" "true" update swblockfrontendpublications --subresource=status

for verb in create update patch delete; do
  can_i_as "${EXECUTOR_AS}" "executor_${verb}_targets_allowed" "false" "${verb}" swblockfrontendpublications
done
can_i_as "${EXECUTOR_AS}" "executor_patch_target_finalizers_allowed" "false" patch swblockfrontendpublications --subresource=finalizers
can_i_as "${EXECUTOR_AS}" "executor_create_events_allowed" "false" create events
for resource in swblockvolumes swblockreplicaeligibilities pods deployments persistentvolumeclaims persistentvolumes storageclasses secrets nodes csidrivers csinodes; do
  for verb in create update patch delete; do
    can_i_as "${EXECUTOR_AS}" "executor_${resource}_${verb}_allowed" "false" "${verb}" "${resource}"
  done
done

STATUS_PATCH='{"status":{"observedAt":"2026-06-25T00:00:00Z","executor":"frontend-publication-executor","state":"blocked","reasonCode":"frontend_publication_policy_disabled","publicationMutationAllowed":false,"frontendPublished":false,"failbackStarted":false,"noStorageMutation":true,"noCrossVolumeIdentityChange":true,"conditions":[{"type":"Blocked","status":"True","reason":"frontend_publication_policy_disabled","severity":"warning","message":"frontend publication executor remains disabled by policy"}],"nonClaims":["no_frontend_publication","no_failback","no_storage_mutation"]}}'
run_expect_success executor-status-patch "${KUBECTL}" patch swblockfrontendpublication phase71-target -n "${NAMESPACE}" --as "${EXECUTOR_AS}" --subresource=status --type=merge -p "${STATUS_PATCH}"
write_summary "executor_status_patch_succeeded=true"

jsonpath_equals status-state "{.status.state}" "blocked"
jsonpath_equals status-reason "{.status.reasonCode}" "frontend_publication_policy_disabled"
jsonpath_equals status-publication-allowed "{.status.publicationMutationAllowed}" "false"
jsonpath_equals status-frontend-published "{.status.frontendPublished}" "false"
jsonpath_equals status-failback-started "{.status.failbackStarted}" "false"
jsonpath_equals status-no-storage "{.status.noStorageMutation}" "true"
write_summary "frontend_publication_executor_status_writes=true"
write_summary "frontend_publication_executor_status=blocked"
write_summary "frontend_publication_executor_reason=frontend_publication_policy_disabled"
write_summary "frontend_publication_executor_status_mutation_allowed=true"
write_summary "frontend_publication_mutation_allowed=false"
write_summary "frontend_published=false"
write_summary "failback_started=false"

run_expect_failure executor-spec-patch "${KUBECTL}" patch swblockfrontendpublication phase71-target -n "${NAMESPACE}" --as "${EXECUTOR_AS}" --type=merge -p '{"spec":{"replicaID":"r3"}}'
write_summary "executor_spec_patch_allowed=false"
run_expect_failure executor-label-patch "${KUBECTL}" patch swblockfrontendpublication phase71-target -n "${NAMESPACE}" --as "${EXECUTOR_AS}" --type=merge -p '{"metadata":{"labels":{"changed":"true"}}}'
write_summary "executor_label_patch_allowed=false"
run_expect_failure executor-finalizers-patch "${KUBECTL}" patch swblockfrontendpublication phase71-target -n "${NAMESPACE}" --as "${EXECUTOR_AS}" --subresource=finalizers --type=merge -p '{"metadata":{"finalizers":["example.com/foreign"]}}'
write_summary "executor_finalizers_endpoint_allowed=false"

jsonpath_equals final-spec-replica "{.spec.replicaID}" "r2"
jsonpath_equals final-label-keep "{.metadata.labels.keep}" "true"
jsonpath_equals final-annotation-keep "{.metadata.annotations.keep}" "true"
write_summary "target_object_integrity_preserved=true"
write_summary "frontend_publication_executor_rbac_status_only=true"
write_summary "phase71_frontend_publication_live_api_boundary_status=ok"
