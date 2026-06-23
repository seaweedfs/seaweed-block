#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase58-rebuild-target-owner-rbac-gate}"
KUBECTL="${KUBECTL:-kubectl}"
NAMESPACE="${SW_BLOCK_PHASE58_NAMESPACE:-sw-block-phase58-rebuild-target-owner}"
DEFAULT_SA="sw-block-rebuild-target-owner-default"
OWNER_SA="sw-block-rebuild-target-owner"
DEFAULT_USER="system:serviceaccount:${NAMESPACE}:${DEFAULT_SA}"
OWNER_USER="system:serviceaccount:${NAMESPACE}:${OWNER_SA}"
SUMMARY="${ARTIFACT_DIR}/phase58-rebuild-target-owner-rbac-summary.txt"
VOLUME_CRD="swblockvolumes.block.seaweedfs.com"
REBUILD_CRD="swblockreplicarebuilds.block.seaweedfs.com"

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
  "${KUBECTL}" delete clusterrolebinding sw-block-phase58-rebuild-target-owner-default sw-block-phase58-rebuild-target-owner --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrole sw-block-phase58-rebuild-target-owner-default sw-block-phase58-rebuild-target-owner --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete namespace "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1
  if [ "${PREEXISTING_REBUILD_CRD}" != "true" ]; then
    "${KUBECTL}" delete crd "${REBUILD_CRD}" --ignore-not-found >/dev/null 2>&1
  fi
  if [ "${PREEXISTING_VOLUME_CRD}" != "true" ]; then
    "${KUBECTL}" delete crd "${VOLUME_CRD}" --ignore-not-found >/dev/null 2>&1
  fi
}
trap cleanup EXIT

write_summary "phase58_rebuild_target_owner_rbac_status=running"
write_summary "preexisting_volume_crd=${PREEXISTING_VOLUME_CRD}"
write_summary "preexisting_rebuild_crd=${PREEXISTING_REBUILD_CRD}"

run_expect_success kubectl-version "${KUBECTL}" version --client=true
run_expect_success apply-volume-crd "${KUBECTL}" apply -f "${PRODUCT_ROOT}/charts/seaweed-block/crds/swblockvolumes.block.seaweedfs.com.yaml"
run_expect_success apply-rebuild-crd "${KUBECTL}" apply -f "${PRODUCT_ROOT}/charts/seaweed-block/crds/swblockreplicarebuilds.block.seaweedfs.com.yaml"

cat >"${ARTIFACT_DIR}/namespace.yaml" <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: ${NAMESPACE}
  labels:
    block.seaweedfs.com/phase58-gate: "true"
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
  name: ${OWNER_SA}
  namespace: ${NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: sw-block-phase58-rebuild-target-owner-default
rules:
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockvolumes"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: sw-block-phase58-rebuild-target-owner
rules:
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockvolumes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockreplicarebuilds"]
    verbs: ["get", "list", "watch", "create"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sw-block-phase58-rebuild-target-owner-default
subjects:
  - kind: ServiceAccount
    name: ${DEFAULT_SA}
    namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sw-block-phase58-rebuild-target-owner-default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sw-block-phase58-rebuild-target-owner
subjects:
  - kind: ServiceAccount
    name: ${OWNER_SA}
    namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sw-block-phase58-rebuild-target-owner
YAML
run_expect_success apply-rbac "${KUBECTL}" apply -f "${ARTIFACT_DIR}/rbac.yaml"

cat >"${ARTIFACT_DIR}/rebuild-target.yaml" <<YAML
apiVersion: block.seaweedfs.com/v1alpha1
kind: SwBlockReplicaRebuild
metadata:
  name: phase58-rebuild-r2
  namespace: ${NAMESPACE}
spec:
  volumeName: phase58-volume
  volumeID: pvc-phase58
  pvcName: phase58-pvc
  replicaID: r2
YAML

can_i_as "${DEFAULT_USER}" "default_get_swblockvolumes_allowed" "yes" get swblockvolumes
can_i_as "${DEFAULT_USER}" "default_create_swblockreplicarebuilds_denied" "no" create swblockreplicarebuilds
can_i_as "${DEFAULT_USER}" "default_patch_swblockreplicarebuilds_status_denied" "no" patch swblockreplicarebuilds --subresource=status

can_i_as "${OWNER_USER}" "owner_get_swblockvolumes_allowed" "yes" get swblockvolumes
can_i_as "${OWNER_USER}" "owner_get_swblockreplicarebuilds_allowed" "yes" get swblockreplicarebuilds
can_i_as "${OWNER_USER}" "owner_create_swblockreplicarebuilds_allowed" "yes" create swblockreplicarebuilds

can_i_as "${OWNER_USER}" "owner_patch_swblockreplicarebuilds_main_denied" "no" patch swblockreplicarebuilds
can_i_as "${OWNER_USER}" "owner_update_swblockreplicarebuilds_denied" "no" update swblockreplicarebuilds
can_i_as "${OWNER_USER}" "owner_delete_swblockreplicarebuilds_denied" "no" delete swblockreplicarebuilds
can_i_as "${OWNER_USER}" "owner_patch_swblockreplicarebuilds_status_denied" "no" patch swblockreplicarebuilds --subresource=status
can_i_as "${OWNER_USER}" "owner_patch_swblockvolumes_denied" "no" patch swblockvolumes
can_i_as "${OWNER_USER}" "owner_patch_swblockvolumes_status_denied" "no" patch swblockvolumes --subresource=status
can_i_as "${OWNER_USER}" "owner_patch_swblockvolumes_finalizers_denied" "no" patch swblockvolumes --subresource=finalizers
can_i_as "${OWNER_USER}" "owner_create_events_denied" "no" create events
can_i_as "${OWNER_USER}" "owner_create_pods_denied" "no" create pods
can_i_as "${OWNER_USER}" "owner_patch_pvc_denied" "no" patch pvc
can_i_as "${OWNER_USER}" "owner_update_storageclass_denied" "no" update storageclasses

if run_capture "default-create-rebuild-target" "${KUBECTL}" create -f "${ARTIFACT_DIR}/rebuild-target.yaml" --as "${DEFAULT_USER}"; then
  write_summary "default_create_rebuild_target_runtime_denied=false"
  echo "default identity unexpectedly created rebuild target" >&2
  exit 1
else
  write_summary "default_create_rebuild_target_runtime_denied=true"
fi

run_expect_success owner-create-rebuild-target "${KUBECTL}" create -f "${ARTIFACT_DIR}/rebuild-target.yaml" --as "${OWNER_USER}"
write_summary "owner_create_rebuild_target_runtime_allowed=true"

if run_capture "owner-patch-rebuild-target" "${KUBECTL}" patch swblockreplicarebuilds phase58-rebuild-r2 -n "${NAMESPACE}" --type=merge --as "${OWNER_USER}" -p '{"spec":{"replicaID":"changed"}}'; then
  write_summary "owner_patch_rebuild_target_runtime_denied=false"
  echo "owner identity unexpectedly patched rebuild target" >&2
  exit 1
else
  write_summary "owner_patch_rebuild_target_runtime_denied=true"
fi

run_expect_success get-rebuild-target "${KUBECTL}" get swblockreplicarebuilds phase58-rebuild-r2 -n "${NAMESPACE}" -o jsonpath='{.spec.volumeName}{"\n"}{.spec.volumeID}{"\n"}{.spec.pvcName}{"\n"}{.spec.replicaID}{"\n"}{.status.state}{"\n"}'
mapfile -t target_lines <"${ARTIFACT_DIR}/get-rebuild-target.stdout.txt"
write_summary "runtime_rebuild_target_volume_name=${target_lines[0]:-}"
write_summary "runtime_rebuild_target_volume_id=${target_lines[1]:-}"
write_summary "runtime_rebuild_target_pvc_name=${target_lines[2]:-}"
write_summary "runtime_rebuild_target_replica_id=${target_lines[3]:-}"
write_summary "runtime_rebuild_target_status_state=${target_lines[4]:-}"

if [ "${target_lines[0]:-}" != "phase58-volume" ] ||
  [ "${target_lines[1]:-}" != "pvc-phase58" ] ||
  [ "${target_lines[2]:-}" != "phase58-pvc" ] ||
  [ "${target_lines[3]:-}" != "r2" ] ||
  [ "${target_lines[4]:-}" != "" ]; then
  echo "unexpected rebuild target payload" >&2
  cat "${ARTIFACT_DIR}/get-rebuild-target.stdout.txt" >&2 || true
  exit 1
fi

write_summary "phase58_rebuild_target_owner_rbac_status=ok"
