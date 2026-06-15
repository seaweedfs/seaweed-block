#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase42-lifecycle-owner-admission-gate}"
KUBECTL="${KUBECTL:-kubectl}"
NAMESPACE="${SW_BLOCK_PHASE42_NAMESPACE:-sw-block-phase42-gate}"
FINALIZER="block.seaweedfs.com/swblockvolume-protection"
SUMMARY="${ARTIFACT_DIR}/phase42-lifecycle-owner-admission-gate-summary.txt"

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

assert_jsonpath_equals() {
  local name="$1"
  local path="$2"
  local expected="$3"
  run_expect_success "${name}" "${KUBECTL}" get swblockvolume phase42-a -n "${NAMESPACE}" -o "jsonpath=${path}"
  local actual
  actual="$(cat "${ARTIFACT_DIR}/${name}.stdout.txt")"
  if [ "${actual}" != "${expected}" ]; then
    echo "unexpected ${name}: got '${actual}', expected '${expected}'" >&2
    return 1
  fi
}

assert_jsonpath_empty() {
  local name="$1"
  local path="$2"
  run_expect_success "${name}" "${KUBECTL}" get swblockvolume phase42-a -n "${NAMESPACE}" -o "jsonpath=${path}"
  local actual
  actual="$(cat "${ARTIFACT_DIR}/${name}.stdout.txt")"
  if [ -n "${actual}" ]; then
    echo "unexpected ${name}: got '${actual}', expected empty" >&2
    return 1
  fi
}

cleanup() {
  set +e
  "${KUBECTL}" delete validatingadmissionpolicybinding sw-block-phase42-finalizer-only --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete validatingadmissionpolicy sw-block-phase42-finalizer-only --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrolebinding sw-block-phase42-operator-status sw-block-phase42-lifecycle-owner --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrole sw-block-phase42-operator-status sw-block-phase42-lifecycle-owner --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete namespace "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1
}
trap cleanup EXIT

write_summary "phase42_lifecycle_owner_admission_status=running"
write_summary "harness=live_kubernetes_validating_admission_policy"

if ! run_capture kubectl-api-versions "${KUBECTL}" api-versions; then
  write_summary "phase42_lifecycle_owner_admission_status=blocked"
  write_summary "blocked_reason=kubernetes_api_unreachable"
  exit 2
fi

if ! "${KUBECTL}" api-resources --api-group=admissionregistration.k8s.io 2>/dev/null | grep -q '^validatingadmissionpolicies'; then
  write_summary "phase42_lifecycle_owner_admission_status=blocked"
  write_summary "blocked_reason=validating_admission_policy_unavailable"
  exit 2
fi

run_expect_success apply-crd "${KUBECTL}" apply -f "${PRODUCT_ROOT}/charts/seaweed-block/crds/swblockvolumes.block.seaweedfs.com.yaml"

cat >"${ARTIFACT_DIR}/namespace.yaml" <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: ${NAMESPACE}
  labels:
    block.seaweedfs.com/phase42-gate: "true"
YAML
run_expect_success apply-namespace "${KUBECTL}" apply -f "${ARTIFACT_DIR}/namespace.yaml"

cat >"${ARTIFACT_DIR}/rbac.yaml" <<YAML
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sw-block-operator-status
  namespace: ${NAMESPACE}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: sw-block-lifecycle-owner
  namespace: ${NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: sw-block-phase42-operator-status
rules:
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockvolumes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockvolumes/status"]
    verbs: ["get", "update", "patch"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: sw-block-phase42-lifecycle-owner
rules:
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockvolumes"]
    verbs: ["get", "list", "watch", "patch"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sw-block-phase42-operator-status
subjects:
  - kind: ServiceAccount
    name: sw-block-operator-status
    namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sw-block-phase42-operator-status
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sw-block-phase42-lifecycle-owner
subjects:
  - kind: ServiceAccount
    name: sw-block-lifecycle-owner
    namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sw-block-phase42-lifecycle-owner
YAML
run_expect_success apply-rbac "${KUBECTL}" apply -f "${ARTIFACT_DIR}/rbac.yaml"

cat >"${ARTIFACT_DIR}/admission.yaml" <<YAML
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: sw-block-phase42-finalizer-only
spec:
  failurePolicy: Fail
  matchConstraints:
    resourceRules:
      - apiGroups: ["block.seaweedfs.com"]
        apiVersions: ["v1alpha1"]
        operations: ["UPDATE"]
        resources: ["swblockvolumes"]
  validations:
    - expression: >-
        request.userInfo.username != 'system:serviceaccount:${NAMESPACE}:sw-block-lifecycle-owner' ||
        (
          object.spec == oldObject.spec &&
          (has(object.status) == has(oldObject.status)) &&
          (!has(object.status) || object.status == oldObject.status) &&
          (has(object.metadata.labels) == has(oldObject.metadata.labels)) &&
          (!has(object.metadata.labels) || object.metadata.labels == oldObject.metadata.labels) &&
          (has(object.metadata.annotations) == has(oldObject.metadata.annotations)) &&
          (!has(object.metadata.annotations) || object.metadata.annotations == oldObject.metadata.annotations) &&
          (has(object.metadata.ownerReferences) == has(oldObject.metadata.ownerReferences)) &&
          (!has(object.metadata.ownerReferences) || object.metadata.ownerReferences == oldObject.metadata.ownerReferences) &&
          (!has(object.metadata.finalizers) ||
            (size(object.metadata.finalizers) <= 1 &&
             object.metadata.finalizers.all(f, f == '${FINALIZER}'))) &&
          (!has(oldObject.metadata.finalizers) ||
            (size(oldObject.metadata.finalizers) <= 1 &&
             oldObject.metadata.finalizers.all(f, f == '${FINALIZER}')))
        )
      message: lifecycle-owner may patch only the Seaweed Block finalizer
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: sw-block-phase42-finalizer-only
spec:
  policyName: sw-block-phase42-finalizer-only
  validationActions: [Deny]
  matchResources:
    namespaceSelector:
      matchLabels:
        block.seaweedfs.com/phase42-gate: "true"
YAML
run_expect_success apply-admission "${KUBECTL}" apply -f "${ARTIFACT_DIR}/admission.yaml"

OWNER_AS="system:serviceaccount:${NAMESPACE}:sw-block-lifecycle-owner"

cat >"${ARTIFACT_DIR}/volume.yaml" <<YAML
apiVersion: block.seaweedfs.com/v1alpha1
kind: SwBlockVolume
metadata:
  name: phase42-a
  namespace: ${NAMESPACE}
  labels:
    keep: "true"
  annotations:
    keep: "true"
spec:
  pvcName: phase42-a
  storageClass: sw-block
YAML
run_expect_success apply-volume "${KUBECTL}" apply -f "${ARTIFACT_DIR}/volume.yaml"

cat >"${ARTIFACT_DIR}/admission-probe.yaml" <<YAML
apiVersion: block.seaweedfs.com/v1alpha1
kind: SwBlockVolume
metadata:
  name: phase42-admission-probe
  namespace: ${NAMESPACE}
  labels:
    probe: "initial"
spec:
  pvcName: phase42-admission-probe
  storageClass: sw-block
YAML
run_expect_success apply-admission-probe "${KUBECTL}" apply -f "${ARTIFACT_DIR}/admission-probe.yaml"

wait_for_admission_policy() {
  local attempt
  for attempt in $(seq 1 30); do
    if run_capture "admission-policy-propagation-${attempt}" "${KUBECTL}" patch swblockvolume phase42-admission-probe -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p "{\"metadata\":{\"labels\":{\"probe\":\"denied-${attempt}\"}}}"; then
      sleep 1
      continue
    fi
    if grep -q "lifecycle-owner may patch only" "${ARTIFACT_DIR}/admission-policy-propagation-${attempt}.stderr.txt"; then
      write_summary "admission_policy_propagated=true"
      return 0
    fi
    sleep 1
  done
  write_summary "admission_policy_propagated=false"
  echo "validating admission policy did not deny a known-bad lifecycle-owner patch in time" >&2
  return 1
}

wait_for_admission_policy

OPERATOR_AS="system:serviceaccount:${NAMESPACE}:sw-block-operator-status"

run_expect_failure operator-status-main-patch "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OPERATOR_AS}" --type=merge -p "{\"metadata\":{\"finalizers\":[\"${FINALIZER}\"]}}"
write_summary "operator_status_main_patch_allowed=false"

run_expect_success lifecycle-owner-add-finalizer "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p "{\"metadata\":{\"finalizers\":[\"${FINALIZER}\"]}}"
write_summary "lifecycle_owner_finalizer_add_allowed=true"

run_expect_success lifecycle-owner-add-finalizer-idempotent "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p "{\"metadata\":{\"finalizers\":[\"${FINALIZER}\"]}}"
write_summary "lifecycle_owner_finalizer_add_idempotent=true"

run_expect_success lifecycle-owner-remove-finalizer "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p '{"metadata":{"finalizers":[]}}'
write_summary "lifecycle_owner_finalizer_remove_allowed=true"

run_expect_success lifecycle-owner-remove-finalizer-idempotent "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p '{"metadata":{"finalizers":[]}}'
write_summary "lifecycle_owner_finalizer_remove_idempotent=true"

run_expect_failure lifecycle-owner-spec-patch "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p '{"spec":{"pvcName":"changed"}}'
write_summary "lifecycle_owner_spec_patch_allowed=false"

run_expect_failure lifecycle-owner-label-patch "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p '{"metadata":{"labels":{"changed":"true"}}}'
write_summary "lifecycle_owner_label_patch_allowed=false"

run_expect_failure lifecycle-owner-annotation-patch "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p '{"metadata":{"annotations":{"changed":"true"}}}'
write_summary "lifecycle_owner_annotation_patch_allowed=false"

run_expect_failure lifecycle-owner-ownerreferences-patch "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p '{"metadata":{"ownerReferences":[{"apiVersion":"v1","kind":"ConfigMap","name":"foreign-owner","uid":"00000000-0000-0000-0000-000000000000"}]}}'
write_summary "lifecycle_owner_ownerreferences_patch_allowed=false"

run_expect_failure lifecycle-owner-deletiontimestamp-patch "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p '{"metadata":{"deletionTimestamp":"2026-01-01T00:00:00Z"}}'
write_summary "lifecycle_owner_deletiontimestamp_patch_allowed=false"

run_expect_failure lifecycle-owner-foreign-finalizer "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p '{"metadata":{"finalizers":["example.com/foreign"]}}'
write_summary "lifecycle_owner_foreign_finalizer_allowed=false"

run_expect_failure lifecycle-owner-mixed-patch "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p "{\"metadata\":{\"finalizers\":[\"${FINALIZER}\"]},\"spec\":{\"pvcName\":\"changed\"}}"
write_summary "lifecycle_owner_mixed_patch_allowed=false"

run_expect_failure lifecycle-owner-mixed-metadata-patch "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p "{\"metadata\":{\"finalizers\":[\"${FINALIZER}\"],\"annotations\":{\"changed\":\"true\"}}}"
write_summary "lifecycle_owner_mixed_metadata_patch_allowed=false"

if run_capture lifecycle-owner-main-status-patch "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p '{"status":{"status":"ready","reasonCode":"forbidden"}}'; then
  write_summary "lifecycle_owner_main_status_patch_request_denied=false"
else
  write_summary "lifecycle_owner_main_status_patch_request_denied=true"
fi
assert_jsonpath_empty final-status-empty "{.status.status}"
write_summary "lifecycle_owner_main_status_mutated=false"

run_expect_failure lifecycle-owner-status-subresource-patch "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --subresource=status --type=merge -p '{"status":{"status":"ready","reasonCode":"forbidden"}}'
write_summary "lifecycle_owner_status_subresource_patch_allowed=false"

if "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --subresource=finalizers --type=merge -p "{\"metadata\":{\"finalizers\":[\"${FINALIZER}\"]}}" >"${ARTIFACT_DIR}/finalizers-endpoint.stdout.txt" 2>"${ARTIFACT_DIR}/finalizers-endpoint.stderr.txt"; then
  write_summary "finalizers_endpoint_allowed=true"
  echo "unexpected /finalizers subresource success" >&2
  exit 1
fi
write_summary "finalizers_endpoint_allowed=false"

for resource in pods deployments persistentvolumeclaims persistentvolumes storageclasses secrets nodes csidrivers csinodes; do
  for verb in create update patch delete; do
    if "${KUBECTL}" auth can-i "${verb}" "${resource}" --as "${OWNER_AS}" -n "${NAMESPACE}" >/dev/null 2>&1; then
      write_summary "lifecycle_owner_${resource}_${verb}_allowed=true"
      echo "unexpected ${verb} permission for ${resource}" >&2
      exit 1
    fi
    write_summary "lifecycle_owner_${resource}_${verb}_allowed=false"
  done
done

assert_jsonpath_equals final-spec-pvc "{.spec.pvcName}" "phase42-a"
assert_jsonpath_equals final-label-keep "{.metadata.labels.keep}" "true"
assert_jsonpath_equals final-annotation-keep "{.metadata.annotations.keep}" "true"
assert_jsonpath_empty final-finalizers-empty "{range .metadata.finalizers[*]}{.}{'\n'}{end}"
assert_jsonpath_empty final-ownerreferences-empty "{range .metadata.ownerReferences[*]}{.name}{'\n'}{end}"
write_summary "object_integrity_preserved=true"

run_expect_success final-object "${KUBECTL}" get swblockvolume phase42-a -n "${NAMESPACE}" -o yaml

write_summary "phase42_lifecycle_owner_admission_status=ok"
