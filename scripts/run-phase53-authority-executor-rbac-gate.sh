#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase53-authority-executor-rbac-gate}"
KUBECTL="${KUBECTL:-kubectl}"
NAMESPACE="${SW_BLOCK_PHASE53_NAMESPACE:-sw-block-phase53-authority-executor}"
SA_NAME="sw-block-authority-executor"
SA_USER="system:serviceaccount:${NAMESPACE}:${SA_NAME}"
SUMMARY="${ARTIFACT_DIR}/phase53-authority-executor-rbac-summary.txt"

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

can_i() {
  local name="$1"
  local expected="$2"
  shift 2
  run_capture "can-i-${name}" "${KUBECTL}" auth can-i "$@" --as "${SA_USER}" -n "${NAMESPACE}" || true
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
  "${KUBECTL}" delete clusterrolebinding sw-block-phase53-authority-executor --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrole sw-block-phase53-authority-executor --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete namespace "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1
}
trap cleanup EXIT

write_summary "phase53_authority_executor_rbac_status=running"

run_expect_success kubectl-version "${KUBECTL}" version --client=true
run_expect_success kubectl-api-versions "${KUBECTL}" api-versions
run_expect_success apply-crd "${KUBECTL}" apply -f "${PRODUCT_ROOT}/charts/seaweed-block/crds/swblockvolumes.block.seaweedfs.com.yaml"

cat >"${ARTIFACT_DIR}/namespace.yaml" <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: ${NAMESPACE}
  labels:
    block.seaweedfs.com/phase53-gate: "true"
YAML
run_expect_success apply-namespace "${KUBECTL}" apply -f "${ARTIFACT_DIR}/namespace.yaml"

cat >"${ARTIFACT_DIR}/rbac.yaml" <<YAML
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${SA_NAME}
  namespace: ${NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: sw-block-phase53-authority-executor
rules:
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockvolumes"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sw-block-phase53-authority-executor
subjects:
  - kind: ServiceAccount
    name: ${SA_NAME}
    namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sw-block-phase53-authority-executor
YAML
run_expect_success apply-rbac "${KUBECTL}" apply -f "${ARTIFACT_DIR}/rbac.yaml"

can_i "authority_executor_get_swblockvolumes_allowed" "yes" get swblockvolumes
can_i "authority_executor_list_swblockvolumes_allowed" "yes" list swblockvolumes
can_i "authority_executor_watch_swblockvolumes_allowed" "yes" watch swblockvolumes
can_i "authority_executor_patch_swblockvolumes_denied" "no" patch swblockvolumes
can_i "authority_executor_update_swblockvolumes_denied" "no" update swblockvolumes
can_i "authority_executor_delete_swblockvolumes_denied" "no" delete swblockvolumes
can_i "authority_executor_patch_status_denied" "no" patch swblockvolumes --subresource=status
can_i "authority_executor_patch_finalizers_denied" "no" patch swblockvolumes --subresource=finalizers
can_i "authority_executor_create_events_denied" "no" create events
can_i "authority_executor_patch_pods_denied" "no" patch pods
can_i "authority_executor_patch_pvc_denied" "no" patch pvc
can_i "authority_executor_update_storageclass_denied" "no" update storageclasses

write_summary "phase53_authority_executor_rbac_status=ok"
