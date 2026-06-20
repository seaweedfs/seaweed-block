#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase47-returned-replica-status-schema-rbac-gate}"
KUBECTL="${KUBECTL:-kubectl}"
NAMESPACE="${SW_BLOCK_PHASE47_NAMESPACE:-sw-block-phase47-status-gate}"
SA_NAME="sw-block-operator-status"
SA_USER="system:serviceaccount:${NAMESPACE}:${SA_NAME}"
SUMMARY="${ARTIFACT_DIR}/phase47-returned-replica-status-schema-rbac-summary.txt"

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
  "${KUBECTL}" delete clusterrolebinding sw-block-phase47-operator-status --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrole sw-block-phase47-operator-status --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete namespace "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1
}
trap cleanup EXIT

write_summary "phase47_returned_replica_status_schema_rbac_status=running"
write_summary "harness=live_kubernetes_status_subresource_server_dry_run"

if ! run_capture kubectl-version "${KUBECTL}" version --client=true; then
  write_summary "phase47_returned_replica_status_schema_rbac_status=blocked"
  write_summary "blocked_reason=kubectl_unavailable"
  exit 2
fi

if ! run_capture kubectl-api-versions "${KUBECTL}" api-versions; then
  write_summary "phase47_returned_replica_status_schema_rbac_status=blocked"
  write_summary "blocked_reason=kubernetes_api_unreachable"
  exit 2
fi

run_expect_success apply-crd "${KUBECTL}" apply -f "${PRODUCT_ROOT}/charts/seaweed-block/crds/swblockvolumes.block.seaweedfs.com.yaml"

cat >"${ARTIFACT_DIR}/namespace.yaml" <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: ${NAMESPACE}
  labels:
    block.seaweedfs.com/phase47-gate: "true"
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
  name: sw-block-phase47-operator-status
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
kind: ClusterRoleBinding
metadata:
  name: sw-block-phase47-operator-status
subjects:
  - kind: ServiceAccount
    name: ${SA_NAME}
    namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sw-block-phase47-operator-status
YAML
run_expect_success apply-rbac "${KUBECTL}" apply -f "${ARTIFACT_DIR}/rbac.yaml"

cat >"${ARTIFACT_DIR}/swblockvolume.yaml" <<YAML
apiVersion: block.seaweedfs.com/v1alpha1
kind: SwBlockVolume
metadata:
  name: phase47-returned
  namespace: ${NAMESPACE}
spec:
  pvcName: phase47-returned
  storageClass: seaweed-block
YAML
run_expect_success apply-volume "${KUBECTL}" apply -f "${ARTIFACT_DIR}/swblockvolume.yaml"

can_i "operator_status_patch_status_allowed" "yes" patch swblockvolumes --subresource=status
can_i "operator_status_update_status_allowed" "yes" update swblockvolumes --subresource=status
can_i "operator_status_create_events_allowed" "yes" create events
can_i "operator_status_main_patch_allowed" "no" patch swblockvolumes
can_i "operator_status_finalizers_patch_allowed" "no" patch swblockvolumes --subresource=finalizers
can_i "operator_status_pods_patch_allowed" "no" patch pods
can_i "operator_status_pvc_patch_allowed" "no" patch pvc
can_i "operator_status_storageclass_update_allowed" "no" update storageclasses

cat >"${ARTIFACT_DIR}/valid-status-patch.json" <<'JSON'
{
  "status": {
    "volumeID": "pvc-returned",
    "pvcName": "phase47-returned",
    "status": "recovering",
    "reasonCode": "returned_replica_frontend_fenced",
    "conditions": [
      {
        "type": "Ready",
        "status": "Unknown",
        "reason": "returned_replica_frontend_fenced",
        "severity": "info",
        "message": "returned replica is fenced pending reintegration evidence"
      }
    ],
    "replicaReintegrations": [
      {
        "replicaID": "r1",
        "state": "fenced",
        "reasonCode": "returned_replica_frontend_fenced",
        "frontendFenced": true,
        "frontendPrimaryReady": false,
        "ackEligible": false,
        "durableFrontierKnown": true,
        "durableFrontierLsn": 52,
        "requiredFrontierKnown": true,
        "requiredFrontierLsn": 52,
        "evidenceRefs": ["returned-replica-summary.txt"]
      }
    ],
    "allowedActions": [
      {
        "type": "authority.reintegrate_returned_replica",
        "mode": "dry_run",
        "sideEffectClass": "authority_mutating",
        "ownerExecutor": "authority_recovery_executor",
        "decision": "allowed",
        "mutationAllowed": false,
        "preconditions": ["returned_replica_frontend_fenced", "durable_frontier_evidence"],
        "invariantRefs": ["INV-RETURNED-REPLICA-FENCING-001", "INV-RETURNED-REPLICA-FRONTIER-001"],
        "evidenceRequired": "returned_replica_reintegration_evidence",
        "evidenceRefs": ["returned-replica-summary.txt"]
      }
    ],
    "evidenceRefs": ["returned-replica-summary.txt"]
  }
}
JSON

cat >"${ARTIFACT_DIR}/snake-status-patch.json" <<'JSON'
{
  "status": {
    "status": "recovering",
    "allowedActions": [
      {
        "type": "authority.reintegrate_returned_replica",
        "mode": "dry_run",
        "mutation_allowed": false
      }
    ]
  }
}
JSON

cat >"${ARTIFACT_DIR}/bad-mode-status-patch.json" <<'JSON'
{
  "status": {
    "status": "recovering",
    "allowedActions": [
      {
        "type": "authority.reintegrate_returned_replica",
        "mode": "execute",
        "mutationAllowed": false
      }
    ]
  }
}
JSON

run_expect_success valid-status-server-dry-run \
  "${KUBECTL}" patch swblockvolume phase47-returned -n "${NAMESPACE}" \
    --subresource=status --type=merge --patch-file "${ARTIFACT_DIR}/valid-status-patch.json" \
    --dry-run=server -o yaml --as "${SA_USER}"
write_summary "valid_returned_replica_status_server_dry_run=true"

run_expect_failure snake-status-server-dry-run \
  "${KUBECTL}" patch swblockvolume phase47-returned -n "${NAMESPACE}" \
    --subresource=status --type=merge --patch-file "${ARTIFACT_DIR}/snake-status-patch.json" \
    --dry-run=server -o yaml --as "${SA_USER}"
write_summary "snake_case_action_rejected=true"

run_expect_failure bad-mode-status-server-dry-run \
  "${KUBECTL}" patch swblockvolume phase47-returned -n "${NAMESPACE}" \
    --subresource=status --type=merge --patch-file "${ARTIFACT_DIR}/bad-mode-status-patch.json" \
    --dry-run=server -o yaml --as "${SA_USER}"
write_summary "unsupported_action_mode_rejected=true"

run_expect_failure main-patch-denied \
  "${KUBECTL}" patch swblockvolume phase47-returned -n "${NAMESPACE}" \
    --type=merge -p '{"metadata":{"labels":{"bad":"true"}}}' \
    --dry-run=server -o yaml --as "${SA_USER}"
write_summary "main_object_patch_rejected=true"

run_expect_success status-not-mutated "${KUBECTL}" get swblockvolume phase47-returned -n "${NAMESPACE}" -o jsonpath='{.status.status}'
status_value="$(cat "${ARTIFACT_DIR}/status-not-mutated.stdout.txt")"
if [ -n "${status_value}" ]; then
  echo "server-side dry-run unexpectedly mutated status: ${status_value}" >&2
  exit 1
fi
write_summary "server_dry_run_status_mutated=false"

write_summary "phase47_returned_replica_status_schema_rbac_status=ok"
