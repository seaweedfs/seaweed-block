#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase57-authority-executor-rebuild-target-rbac-gate}"
KUBECTL="${KUBECTL:-kubectl}"
NAMESPACE="${SW_BLOCK_PHASE57_NAMESPACE:-sw-block-phase57-authority-executor}"
DEFAULT_SA="sw-block-authority-executor-default"
EXEC_SA="sw-block-authority-executor-exec"
DEFAULT_USER="system:serviceaccount:${NAMESPACE}:${DEFAULT_SA}"
EXEC_USER="system:serviceaccount:${NAMESPACE}:${EXEC_SA}"
SUMMARY="${ARTIFACT_DIR}/phase57-authority-executor-rebuild-target-rbac-summary.txt"
VOLUME_CRD="swblockvolumes.block.seaweedfs.com"
ELIGIBILITY_CRD="swblockreplicaeligibilities.block.seaweedfs.com"
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
PREEXISTING_ELIGIBILITY_CRD="$(crd_preexisting "${ELIGIBILITY_CRD}")"
PREEXISTING_REBUILD_CRD="$(crd_preexisting "${REBUILD_CRD}")"

cleanup() {
  set +e
  "${KUBECTL}" delete clusterrolebinding sw-block-phase57-authority-executor-default --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrolebinding sw-block-phase57-authority-executor-exec --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrole sw-block-phase57-authority-executor-default --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrole sw-block-phase57-authority-executor-exec --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete namespace "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1
  if [ "${PREEXISTING_REBUILD_CRD}" != "true" ]; then
    "${KUBECTL}" delete crd "${REBUILD_CRD}" --ignore-not-found >/dev/null 2>&1
  fi
  if [ "${PREEXISTING_ELIGIBILITY_CRD}" != "true" ]; then
    "${KUBECTL}" delete crd "${ELIGIBILITY_CRD}" --ignore-not-found >/dev/null 2>&1
  fi
  if [ "${PREEXISTING_VOLUME_CRD}" != "true" ]; then
    "${KUBECTL}" delete crd "${VOLUME_CRD}" --ignore-not-found >/dev/null 2>&1
  fi
}
trap cleanup EXIT

write_summary "phase57_authority_executor_rebuild_target_rbac_status=running"
write_summary "preexisting_volume_crd=${PREEXISTING_VOLUME_CRD}"
write_summary "preexisting_eligibility_crd=${PREEXISTING_ELIGIBILITY_CRD}"
write_summary "preexisting_rebuild_crd=${PREEXISTING_REBUILD_CRD}"

run_expect_success kubectl-version "${KUBECTL}" version --client=true
run_expect_success apply-volume-crd "${KUBECTL}" apply -f "${PRODUCT_ROOT}/charts/seaweed-block/crds/swblockvolumes.block.seaweedfs.com.yaml"
run_expect_success apply-eligibility-crd "${KUBECTL}" apply -f "${PRODUCT_ROOT}/charts/seaweed-block/crds/swblockreplicaeligibilities.block.seaweedfs.com.yaml"
run_expect_success apply-rebuild-crd "${KUBECTL}" apply -f "${PRODUCT_ROOT}/charts/seaweed-block/crds/swblockreplicarebuilds.block.seaweedfs.com.yaml"

cat >"${ARTIFACT_DIR}/namespace.yaml" <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: ${NAMESPACE}
  labels:
    block.seaweedfs.com/phase57-gate: "true"
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
  name: sw-block-phase57-authority-executor-default
rules:
  - apiGroups: ["block.seaweedfs.com"]
    resources: ["swblockvolumes"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: sw-block-phase57-authority-executor-exec
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
  name: sw-block-phase57-authority-executor-default
subjects:
  - kind: ServiceAccount
    name: ${DEFAULT_SA}
    namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sw-block-phase57-authority-executor-default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: sw-block-phase57-authority-executor-exec
subjects:
  - kind: ServiceAccount
    name: ${EXEC_SA}
    namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sw-block-phase57-authority-executor-exec
YAML
run_expect_success apply-rbac "${KUBECTL}" apply -f "${ARTIFACT_DIR}/rbac.yaml"

cat >"${ARTIFACT_DIR}/rebuild-target.yaml" <<YAML
apiVersion: block.seaweedfs.com/v1alpha1
kind: SwBlockReplicaRebuild
metadata:
  name: phase57-rebuild-r1
  namespace: ${NAMESPACE}
spec:
  volumeName: phase57-volume
  volumeID: pvc-phase57
  pvcName: phase57-pvc
  replicaID: r1
YAML
run_expect_success apply-rebuild-target "${KUBECTL}" apply -f "${ARTIFACT_DIR}/rebuild-target.yaml"

can_i_as "${DEFAULT_USER}" "default_get_swblockvolumes_allowed" "yes" get swblockvolumes
can_i_as "${DEFAULT_USER}" "default_patch_swblockvolumes_denied" "no" patch swblockvolumes
can_i_as "${DEFAULT_USER}" "default_patch_swblockvolumes_status_denied" "no" patch swblockvolumes --subresource=status
can_i_as "${DEFAULT_USER}" "default_patch_swblockreplicarebuilds_status_denied" "no" patch swblockreplicarebuilds --subresource=status
can_i_as "${DEFAULT_USER}" "default_create_events_denied" "no" create events

can_i_as "${EXEC_USER}" "exec_get_swblockvolumes_allowed" "yes" get swblockvolumes
can_i_as "${EXEC_USER}" "exec_get_swblockreplicarebuilds_allowed" "yes" get swblockreplicarebuilds
can_i_as "${EXEC_USER}" "exec_patch_swblockreplicarebuilds_status_allowed" "yes" patch swblockreplicarebuilds --subresource=status
can_i_as "${EXEC_USER}" "exec_update_swblockreplicarebuilds_status_allowed" "yes" update swblockreplicarebuilds --subresource=status

can_i_as "${EXEC_USER}" "exec_patch_swblockreplicarebuilds_main_denied" "no" patch swblockreplicarebuilds
can_i_as "${EXEC_USER}" "exec_delete_swblockreplicarebuilds_denied" "no" delete swblockreplicarebuilds
can_i_as "${EXEC_USER}" "exec_patch_swblockvolumes_denied" "no" patch swblockvolumes
can_i_as "${EXEC_USER}" "exec_patch_swblockvolumes_status_denied" "no" patch swblockvolumes --subresource=status
can_i_as "${EXEC_USER}" "exec_patch_swblockvolumes_finalizers_denied" "no" patch swblockvolumes --subresource=finalizers
can_i_as "${EXEC_USER}" "exec_patch_swblockreplicaeligibilities_status_denied" "no" patch swblockreplicaeligibilities --subresource=status
can_i_as "${EXEC_USER}" "exec_create_events_denied" "no" create events
can_i_as "${EXEC_USER}" "exec_create_pods_denied" "no" create pods
can_i_as "${EXEC_USER}" "exec_patch_pvc_denied" "no" patch pvc
can_i_as "${EXEC_USER}" "exec_update_storageclass_denied" "no" update storageclasses

if run_capture "default-rebuild-status-patch" "${KUBECTL}" patch swblockreplicarebuilds phase57-rebuild-r1 -n "${NAMESPACE}" --subresource=status --type=merge --as "${DEFAULT_USER}" -p '{"status":{"state":"planned"}}'; then
  write_summary "default_rebuild_status_patch_runtime_denied=false"
  echo "default identity unexpectedly patched rebuild status" >&2
  exit 1
else
  write_summary "default_rebuild_status_patch_runtime_denied=true"
fi

run_expect_success exec-rebuild-status-patch "${KUBECTL}" patch swblockreplicarebuilds phase57-rebuild-r1 -n "${NAMESPACE}" --subresource=status --type=merge --as "${EXEC_USER}" -p '{"status":{"executor":"authority_recovery_executor","state":"planned","reasonCode":"rebuild_progress_planned","frontendFencedBeforeRebuild":true,"primaryUnchanged":true,"durableFrontierKnown":true,"durableFrontierLsn":51,"requiredFrontierKnown":true,"requiredFrontierLsn":52,"durableFrontierCaughtUp":false,"rebuildTrafficStarted":false,"noFrontendPublication":true,"noCrossVolumeIdentityChange":true,"evidenceGeneration":"phase57-live-gate","conditions":[{"type":"Recovering","status":"True","reason":"rebuild_progress_planned","severity":"info","message":"planned only; no rebuild traffic started"}],"evidenceRefs":["phase57"],"nonClaims":["no_rebuild_data_movement","no_frontend_publication","no_failback","no_primary_authority_change","no_cross_volume_mutation"]}}'

run_expect_success get-rebuild-status "${KUBECTL}" get swblockreplicarebuilds phase57-rebuild-r1 -n "${NAMESPACE}" -o jsonpath='{.status.state}{"\n"}{.status.reasonCode}{"\n"}{.status.rebuildTrafficStarted}{"\n"}{.status.noFrontendPublication}{"\n"}{.status.noCrossVolumeIdentityChange}{"\n"}'
mapfile -t status_lines <"${ARTIFACT_DIR}/get-rebuild-status.stdout.txt"
write_summary "runtime_rebuild_status_state=${status_lines[0]:-}"
write_summary "runtime_rebuild_status_reason=${status_lines[1]:-}"
write_summary "runtime_rebuild_traffic_started=${status_lines[2]:-}"
write_summary "runtime_no_frontend_publication=${status_lines[3]:-}"
write_summary "runtime_no_cross_volume_identity_change=${status_lines[4]:-}"

if [ "${status_lines[0]:-}" != "planned" ] ||
  [ "${status_lines[1]:-}" != "rebuild_progress_planned" ] ||
  [ "${status_lines[2]:-}" != "false" ] ||
  [ "${status_lines[3]:-}" != "true" ] ||
  [ "${status_lines[4]:-}" != "true" ]; then
  echo "unexpected rebuild status payload" >&2
  cat "${ARTIFACT_DIR}/get-rebuild-status.stdout.txt" >&2 || true
  exit 1
fi

write_summary "phase57_authority_executor_rebuild_target_rbac_status=ok"
