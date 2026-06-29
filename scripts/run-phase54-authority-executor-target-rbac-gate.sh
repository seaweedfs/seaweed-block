#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase54-authority-executor-target-rbac-gate}"
KUBECTL="${KUBECTL:-kubectl}"
NAMESPACE="${SW_BLOCK_PHASE54_NAMESPACE:-sw-block-phase54-authority-executor}"
DEFAULT_SA="sw-block-authority-executor-default"
EXEC_SA="sw-block-authority-executor-exec"
DEFAULT_USER="system:serviceaccount:${NAMESPACE}:${DEFAULT_SA}"
EXEC_USER="system:serviceaccount:${NAMESPACE}:${EXEC_SA}"
SUMMARY="${ARTIFACT_DIR}/phase54-authority-executor-target-rbac-summary.txt"

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

can_i_as() {
  local user="$1"
  local name="$2"
  local expected="$3"
  shift 3
  run_capture "can-i-${name}" "${KUBECTL}" auth can-i "$@" --as "${user}" -n "${NAMESPACE}" || true
  local actual
  actual="$(tr -d '\r\n' <"${ARTIFACT_DIR}/can-i-${name}.stdout.txt")"
  write_summary "${name}=${actual}"
  if [ "${actual}" != "${expected}" ]; then
    echo "unexpected can-i ${name}: got ${actual}, expected ${expected}" >&2
    return 1
  fi
}

cleanup() {
  set +e
  "${KUBECTL}" delete clusterrolebinding sw-block-phase54-authority-executor-default --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrolebinding sw-block-phase54-authority-executor-exec --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrole sw-block-phase54-authority-executor-default --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrole sw-block-phase54-authority-executor-exec --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete namespace "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1
}
trap cleanup EXIT

write_summary "phase54_authority_executor_target_rbac_status=running"

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
  name: ${DEFAULT_SA}
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
  name: sw-block-phase54-authority-executor-default
rules:
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockvolumes"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: sw-block-phase54-authority-executor-exec
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
  name: sw-block-phase54-authority-executor-default
subjects:
  - kind: ServiceAccount
    name: ${DEFAULT_SA}
    namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sw-block-phase54-authority-executor-default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sw-block-phase54-authority-executor-exec
subjects:
  - kind: ServiceAccount
    name: ${EXEC_SA}
    namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sw-block-phase54-authority-executor-exec
YAML
run_expect_success apply-rbac "${KUBECTL}" apply -f "${ARTIFACT_DIR}/rbac.yaml"

can_i_as "${DEFAULT_USER}" "default_get_swblockvolumes_allowed" "yes" get swblockvolumes
can_i_as "${DEFAULT_USER}" "default_patch_swblockvolumes_denied" "no" patch swblockvolumes
can_i_as "${DEFAULT_USER}" "default_patch_swblockvolumes_status_denied" "no" patch swblockvolumes --subresource=status
can_i_as "${DEFAULT_USER}" "default_patch_swblockreplicaeligibilities_status_denied" "no" patch swblockreplicaeligibilities --subresource=status
can_i_as "${DEFAULT_USER}" "default_create_events_denied" "no" create events

can_i_as "${EXEC_USER}" "exec_get_swblockvolumes_allowed" "yes" get swblockvolumes
can_i_as "${EXEC_USER}" "exec_get_swblockreplicaeligibilities_allowed" "yes" get swblockreplicaeligibilities
can_i_as "${EXEC_USER}" "exec_patch_swblockreplicaeligibilities_status_allowed" "yes" patch swblockreplicaeligibilities --subresource=status
can_i_as "${EXEC_USER}" "exec_update_swblockreplicaeligibilities_status_allowed" "yes" update swblockreplicaeligibilities --subresource=status

can_i_as "${EXEC_USER}" "exec_patch_swblockreplicaeligibilities_main_denied" "no" patch swblockreplicaeligibilities
can_i_as "${EXEC_USER}" "exec_patch_swblockvolumes_denied" "no" patch swblockvolumes
can_i_as "${EXEC_USER}" "exec_patch_swblockvolumes_status_denied" "no" patch swblockvolumes --subresource=status
can_i_as "${EXEC_USER}" "exec_patch_swblockvolumes_finalizers_denied" "no" patch swblockvolumes --subresource=finalizers
can_i_as "${EXEC_USER}" "exec_create_events_denied" "no" create events
can_i_as "${EXEC_USER}" "exec_create_pods_denied" "no" create pods
can_i_as "${EXEC_USER}" "exec_patch_pvc_denied" "no" patch pvc
can_i_as "${EXEC_USER}" "exec_update_storageclass_denied" "no" update storageclasses
can_i_as "${EXEC_USER}" "exec_delete_swblockreplicaeligibilities_denied" "no" delete swblockreplicaeligibilities

write_summary "phase54_authority_executor_target_rbac_status=ok"
