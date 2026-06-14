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
          object.status == oldObject.status &&
          object.metadata.labels == oldObject.metadata.labels &&
          object.metadata.annotations == oldObject.metadata.annotations &&
          object.metadata.ownerReferences == oldObject.metadata.ownerReferences &&
          has(object.metadata.finalizers) &&
          size(object.metadata.finalizers) <= 1 &&
          object.metadata.finalizers.all(f, f == '${FINALIZER}') &&
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

OPERATOR_AS="system:serviceaccount:${NAMESPACE}:sw-block-operator-status"
OWNER_AS="system:serviceaccount:${NAMESPACE}:sw-block-lifecycle-owner"

run_expect_failure operator-status-main-patch "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OPERATOR_AS}" --type=merge -p "{\"metadata\":{\"finalizers\":[\"${FINALIZER}\"]}}"
write_summary "operator_status_main_patch_allowed=false"

run_expect_success lifecycle-owner-add-finalizer "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p "{\"metadata\":{\"finalizers\":[\"${FINALIZER}\"]}}"
write_summary "lifecycle_owner_finalizer_add_allowed=true"

run_expect_success lifecycle-owner-remove-finalizer "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p '{"metadata":{"finalizers":[]}}'
write_summary "lifecycle_owner_finalizer_remove_allowed=true"

run_expect_failure lifecycle-owner-spec-patch "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p '{"spec":{"pvcName":"changed"}}'
write_summary "lifecycle_owner_spec_patch_allowed=false"

run_expect_failure lifecycle-owner-label-patch "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p '{"metadata":{"labels":{"changed":"true"}}}'
write_summary "lifecycle_owner_label_patch_allowed=false"

run_expect_failure lifecycle-owner-foreign-finalizer "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p '{"metadata":{"finalizers":["example.com/foreign"]}}'
write_summary "lifecycle_owner_foreign_finalizer_allowed=false"

run_expect_failure lifecycle-owner-mixed-patch "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --type=merge -p "{\"metadata\":{\"finalizers\":[\"${FINALIZER}\"]},\"spec\":{\"pvcName\":\"changed\"}}"
write_summary "lifecycle_owner_mixed_patch_allowed=false"

if "${KUBECTL}" patch swblockvolume phase42-a -n "${NAMESPACE}" --as "${OWNER_AS}" --subresource=finalizers --type=merge -p "{\"metadata\":{\"finalizers\":[\"${FINALIZER}\"]}}" >"${ARTIFACT_DIR}/finalizers-endpoint.stdout.txt" 2>"${ARTIFACT_DIR}/finalizers-endpoint.stderr.txt"; then
  write_summary "finalizers_endpoint_allowed=true"
  echo "unexpected /finalizers subresource success" >&2
  exit 1
fi
write_summary "finalizers_endpoint_allowed=false"

for resource in pods deployments persistentvolumeclaims persistentvolumes storageclasses secrets nodes csidrivers csinodes; do
  if "${KUBECTL}" auth can-i patch "${resource}" --as "${OWNER_AS}" -n "${NAMESPACE}" >/dev/null 2>&1; then
    write_summary "lifecycle_owner_${resource}_patch_allowed=true"
    echo "unexpected patch permission for ${resource}" >&2
    exit 1
  fi
  write_summary "lifecycle_owner_${resource}_patch_allowed=false"
done

run_expect_success final-object "${KUBECTL}" get swblockvolume phase42-a -n "${NAMESPACE}" -o yaml

write_summary "phase42_lifecycle_owner_admission_status=ok"
