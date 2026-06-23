#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase54-authority-executor-live-close-gate}"
KUBECTL="${KUBECTL:-kubectl}"
NAMESPACE="${SW_BLOCK_PHASE54_NAMESPACE:-sw-block-phase54-authority-executor-live-close}"
EXEC_SA="sw-block-authority-executor-exec"
EXEC_USER="system:serviceaccount:${NAMESPACE}:${EXEC_SA}"
IMAGE="${SW_BLOCK_PHASE54_IMAGE:-sw-block:local}"
IMAGE_PULL_POLICY="${SW_BLOCK_PHASE54_IMAGE_PULL_POLICY:-IfNotPresent}"
NODE_NAME="${SW_BLOCK_PHASE54_NODE_NAME:-m02}"
WORK_DIR="${SW_BLOCK_ISCSI_FAILOVER_WORK_DIR:-/tmp/sw-block-phase54-live-close}"
BIN_DIR="${ARTIFACT_DIR}/bin"
FAILOVER_DIR="${ARTIFACT_DIR}/returned_failover"
SUMMARY="${ARTIFACT_DIR}/phase54-authority-executor-live-close-summary.txt"

mkdir -p "${ARTIFACT_DIR}" "${BIN_DIR}" "${FAILOVER_DIR}"
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

assert_file_contains() {
  local name="$1"
  local file="$2"
  local pattern="$3"
  local count
  count="$(grep -Ec "${pattern}" "${file}" || true)"
  write_summary "${name}=${count}"
  if [ "${count}" = "0" ]; then
    echo "${name}: pattern ${pattern} not found in ${file}" >&2
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
  "${KUBECTL}" delete job -n "${NAMESPACE}" -l block.seaweedfs.com/phase54-live-close=true --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrolebinding sw-block-phase54-authority-executor-live-close --ignore-not-found >/dev/null 2>&1
  "${KUBECTL}" delete clusterrole sw-block-phase54-authority-executor-live-close --ignore-not-found >/dev/null 2>&1
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

write_summary "phase54_authority_executor_live_close_status=running"
write_summary "phase54_authority_executor_image=${IMAGE}"
write_summary "phase54_authority_executor_node=${NODE_NAME}"

if git -C "${PRODUCT_ROOT}" rev-parse HEAD >"${ARTIFACT_DIR}/git.sha" 2>"${ARTIFACT_DIR}/git.stderr.txt"; then
  git -C "${PRODUCT_ROOT}" status --short >"${ARTIFACT_DIR}/git.status" 2>>"${ARTIFACT_DIR}/git.stderr.txt" || true
else
  echo "unknown" >"${ARTIFACT_DIR}/git.sha"
  echo "product root is not a git worktree: ${PRODUCT_ROOT}" >>"${ARTIFACT_DIR}/git.stderr.txt"
fi

run_expect_success "build-blockmaster" go build -o "${BIN_DIR}/blockmaster" "${PRODUCT_ROOT}/cmd/blockmaster"
run_expect_success "build-blockvolume" go build -o "${BIN_DIR}/blockvolume" "${PRODUCT_ROOT}/cmd/blockvolume"
run_expect_success "build-sw-block" go build -o "${BIN_DIR}/sw-block" "${PRODUCT_ROOT}/cmd/sw-block"

run_expect_success "blockmaster-version" "${BIN_DIR}/blockmaster" --version
run_expect_success "blockvolume-version" "${BIN_DIR}/blockvolume" --version
run_expect_success "sw-block-version" "${BIN_DIR}/sw-block" --version

SW_BLOCK_ARTIFACT_DIR="${FAILOVER_DIR}" \
SW_BLOCK_BIN_DIR="${BIN_DIR}" \
SW_BLOCK_ISCSI_FAILOVER_WORK_DIR="${WORK_DIR}" \
SW_BLOCK_RETURN_R1_AFTER_FAILOVER=1 \
SW_BLOCK_DEGRADED_PROBE_INTERVAL=1s \
SW_BLOCK_DEGRADED_PROBE_COOLDOWN_BASE=1s \
SW_BLOCK_DEGRADED_PROBE_COOLDOWN_CAP=3s \
  bash "${PRODUCT_ROOT}/scripts/run-iscsi-alua-mounted-failover-smoke.sh" "${PRODUCT_ROOT}" \
  >"${ARTIFACT_DIR}/returned-failover.stdout.txt" 2>"${ARTIFACT_DIR}/returned-failover.stderr.txt"

assert_file_contains "failover_pass_count" "${FAILOVER_DIR}/run.log" "\\[iscsi-failover\\] PASS:"
assert_file_contains "r1_return_phase_count" "${FAILOVER_DIR}/run.log" "restart returned r1 after r2 promotion"
assert_file_contains "pre_checksum_count" "${FAILOVER_DIR}/pre-check-after-failover.log" "/pre.bin: OK"
assert_file_contains "post_checksum_count" "${FAILOVER_DIR}/post-check.log" "/post.bin: OK"
assert_file_contains "r1_returned_supporting_log_count" "${FAILOVER_DIR}/blockvolume-r1-returned.log" "admitted as SUPPORTING replica r1"
assert_file_contains "r1_returned_recovery_log_count" "${FAILOVER_DIR}/blockvolume-r1-returned.log" "durable recovered: recovered LSN="

python3 - "${FAILOVER_DIR}" <<'PY'
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
r1 = json.loads((root / "status-r1-returned.json").read_text())
r2_before = json.loads((root / "status-r2-healthy.json").read_text())
r2_after = json.loads((root / "status-r2-after-r1-return.json").read_text())
dr1 = json.loads((root / "status-durable-r1-returned.json").read_text())["Volumes"][0]
dr2 = json.loads((root / "status-durable-r2-primary-after-r1-return.json").read_text())["Volumes"][0]

required = int(dr2.get("DurableLSN") or 0)
r1_durable = int(dr1.get("DurableLSN") or 0)
r2_durable = int(dr2.get("DurableLSN") or 0)
r1_non_primary = str(r1.get("AuthorityRole") != "primary").lower()
r1_frontend_fenced = str(r1.get("FrontendPrimaryReady") is False).lower()
r2_primary_before = str(r2_before.get("AuthorityRole") == "primary").lower()
r2_primary_after = str(r2_after.get("AuthorityRole") == "primary").lower()
r2_primary_unchanged = str(r2_primary_before == "true" and r2_primary_after == "true").lower()
frontier_covered = str(r1_durable >= required and required > 0).lower()

(root / "phase54-live-derived-summary.txt").write_text("\n".join([
    "phase54_live_evidence_status=ok",
    f"previous_primary_non_primary={r1_non_primary}",
    f"previous_primary_frontend_fenced={r1_frontend_fenced}",
    f"current_primary_before={r2_primary_before}",
    f"current_primary_after={r2_primary_after}",
    f"current_primary_unchanged={r2_primary_unchanged}",
    f"required_frontier_lsn={required}",
    f"r1_durable_lsn={r1_durable}",
    f"r2_durable_lsn={r2_durable}",
    f"durable_frontier_covered={frontier_covered}",
]) + "\n")

if not (r1_non_primary == r1_frontend_fenced == r2_primary_before == r2_primary_after == r2_primary_unchanged == frontier_covered == "true"):
    raise SystemExit("live returned-replica evidence did not satisfy D7 preconditions")
PY

while IFS= read -r line; do
  write_summary "${line}"
done <"${FAILOVER_DIR}/phase54-live-derived-summary.txt"

mkdir -p "${FAILOVER_DIR}/product-observation" "${FAILOVER_DIR}/report"
python3 - "${FAILOVER_DIR}" <<'PY'
import datetime
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
r1 = json.loads((root / "status-r1-returned.json").read_text())
r2 = json.loads((root / "status-r2-after-r1-return.json").read_text())
dr1 = json.loads((root / "status-durable-r1-returned.json").read_text())["Volumes"][0]
dr2 = json.loads((root / "status-durable-r2-primary-after-r1-return.json").read_text())["Volumes"][0]
required = int(dr2.get("DurableLSN") or 0)
r1_durable = int(dr1.get("DurableLSN") or 0)
cluster = {
    "schema_version": "1.0",
    "captured_at": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
    "product_revision": "phase54-live-close",
    "status": "recovering",
    "volumes": [{
        "volume_id": "v1",
        "namespace": "testops",
        "pvc_name": "iscsi-returned-replica-chain",
        "replication_factor": 2,
        "ack_profile": "sync-quorum",
        "status": "recovering",
        "reason": "returned_replica_frontend_fenced",
        "primary_replica": "r2",
        "primary_node": "m02",
        "publish_target": "127.0.0.1:3281",
        "epoch": int(r2.get("Epoch") or 0),
        "endpoint_version": int(r2.get("EndpointVersion") or 0),
        "required_frontier_known": required > 0,
        "required_frontier_lsn": required,
        "replicas": [{
            "replica_id": "r1",
            "server_id": "s1",
            "kubernetes_node": "m02",
            "observed": True,
            "role": "returned",
            "replication_role": str(r1.get("ReplicationRole") or "replica_ready"),
            "durable_latched": bool(dr1.get("Latched")),
            "durable_frontier_known": bool(dr1.get("FrontierKnown")),
            "durable_frontier_lsn": r1_durable,
            "healthy": bool(r1.get("Healthy")),
            "frontend_primary_ready": bool(r1.get("FrontendPrimaryReady")),
            "frontend_protocol": "iscsi",
            "frontend_addr": "127.0.0.1:3280",
            "status_addr": "127.0.0.1:19603",
            "stale_primary_fenced": True,
            "support_bundle_path": "status-r1-returned.json"
        }, {
            "replica_id": "r2",
            "server_id": "s2",
            "kubernetes_node": "m02",
            "observed": True,
            "role": "primary",
            "replication_role": str(r2.get("ReplicationRole") or "none"),
            "durable_latched": bool(dr2.get("Latched")),
            "durable_frontier_known": bool(dr2.get("FrontierKnown")),
            "durable_frontier_lsn": required,
            "healthy": bool(r2.get("Healthy")),
            "frontend_primary_ready": bool(r2.get("FrontendPrimaryReady")),
            "frontend_protocol": "iscsi",
            "frontend_addr": "127.0.0.1:3281",
            "status_addr": "127.0.0.1:19613"
        }]
    }],
    "non_claims": ["no automatic returned-replica failback or rebuild mutation executed"]
}
(root / "product-observation" / "cluster-evidence.json").write_text(json.dumps(cluster, indent=2) + "\n")
PY

run_expect_success "ops-report" "${BIN_DIR}/sw-block" ops report --from-bundle "${FAILOVER_DIR}" --out "${FAILOVER_DIR}/report"
run_expect_success "ops-explain" "${BIN_DIR}/sw-block" ops explain volume --from-bundle "${FAILOVER_DIR}" v1
cp "${ARTIFACT_DIR}/ops-explain.stdout.txt" "${FAILOVER_DIR}/explain.txt"

dashboard_port=19488
"${BIN_DIR}/sw-block" ops dashboard --from-bundle "${FAILOVER_DIR}" --listen "127.0.0.1:${dashboard_port}" --serve-duration 8s \
  >"${FAILOVER_DIR}/dashboard.stdout.txt" 2>"${FAILOVER_DIR}/dashboard.stderr.txt" &
dashboard_pid=$!
for _ in $(seq 1 80); do
  if curl -fsS "http://127.0.0.1:${dashboard_port}/operator-snapshot.json" >"${FAILOVER_DIR}/dashboard-operator-snapshot.json" 2>"${FAILOVER_DIR}/dashboard-curl.stderr.txt"; then
    break
  fi
  sleep 0.1
done
test -s "${FAILOVER_DIR}/dashboard-operator-snapshot.json"
wait "${dashboard_pid}" || true

assert_file_contains "report_returned_replica_projection_count" "${FAILOVER_DIR}/report/summary.txt" "managed_volume_returned_replica=v1 replica=r1 state=fenced reason=returned_replica_frontend_fenced"
assert_file_contains "report_action_allowed_count" "${FAILOVER_DIR}/report/summary.txt" "managed_volume_action=authority.reintegrate_returned_replica mode=dry_run side_effect=authority_mutating executor=authority_recovery_executor decision=allowed"
assert_file_contains "explain_action_count" "${FAILOVER_DIR}/explain.txt" "managed_volume_action authority.reintegrate_returned_replica mode=dry_run"
assert_file_contains "dashboard_action_count" "${FAILOVER_DIR}/dashboard-operator-snapshot.json" "\"type\": \"authority.reintegrate_returned_replica\""

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
  name: sw-block-phase54-authority-executor-live-close
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
  name: sw-block-phase54-authority-executor-live-close
subjects:
  - kind: ServiceAccount
    name: ${EXEC_SA}
    namespace: ${NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: sw-block-phase54-authority-executor-live-close
YAML
run_expect_success apply-rbac "${KUBECTL}" apply -f "${ARTIFACT_DIR}/rbac.yaml"

can_i_as "exec_patch_swblockreplicaeligibilities_status_allowed" "yes" patch swblockreplicaeligibilities --subresource=status
can_i_as "exec_patch_swblockreplicaeligibilities_main_denied" "no" patch swblockreplicaeligibilities
can_i_as "exec_patch_swblockvolumes_status_denied" "no" patch swblockvolumes --subresource=status
can_i_as "exec_create_events_denied" "no" create events
can_i_as "exec_create_pods_denied" "no" create pods
can_i_as "exec_patch_pvc_denied" "no" patch pvc

read_required="$(grep '^required_frontier_lsn=' "${FAILOVER_DIR}/phase54-live-derived-summary.txt" | cut -d= -f2)"
read_r1_durable="$(grep '^r1_durable_lsn=' "${FAILOVER_DIR}/phase54-live-derived-summary.txt" | cut -d= -f2)"

cat >"${ARTIFACT_DIR}/live-returned.yaml" <<YAML
apiVersion: block.seaweedfs.com/v1alpha1
kind: SwBlockVolume
metadata:
  name: live-returned
  namespace: ${NAMESPACE}
spec:
  pvcName: iscsi-returned-replica-chain
YAML
run_expect_success apply-live-volume "${KUBECTL}" apply -f "${ARTIFACT_DIR}/live-returned.yaml"

cat >"${ARTIFACT_DIR}/live-returned-status.json" <<JSON
{
  "status": {
    "volumeID": "v1",
    "pvcName": "iscsi-returned-replica-chain",
    "status": "unknown",
    "reasonCode": "returned_replica_frontend_fenced",
    "replicaReintegrations": [{
      "replicaID": "r1",
      "state": "fenced",
      "reasonCode": "returned_replica_frontend_fenced",
      "frontendFenced": true,
      "frontendPrimaryReady": false,
      "ackEligibilityKnown": true,
      "ackEligible": false,
      "durableFrontierKnown": true,
      "durableFrontierLsn": ${read_r1_durable},
      "requiredFrontierKnown": true,
      "requiredFrontierLsn": ${read_required},
      "evidenceRefs": ["returned_failover/phase54-live-derived-summary.txt", "returned_failover/status-r1-returned.json"]
    }],
    "executorContracts": [{
      "actionType": "authority.reintegrate_returned_replica",
      "replicaID": "r1",
      "decision": "disabled",
      "reason": "executor_policy_disabled",
      "ownerExecutor": "authority_recovery_executor",
      "executionEnabled": false,
      "mutationAllowed": false,
      "preflightDecision": "ready",
      "preflightReason": "preconditions_satisfied",
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
      "evidenceRefs": ["returned_failover/phase54-live-derived-summary.txt"]
    }]
  }
}
JSON
run_expect_success patch-live-volume-status "${KUBECTL}" patch swblockvolume live-returned -n "${NAMESPACE}" --subresource=status --type=merge --patch-file "${ARTIFACT_DIR}/live-returned-status.json"

cat >"${ARTIFACT_DIR}/live-r1-target.yaml" <<YAML
apiVersion: block.seaweedfs.com/v1alpha1
kind: SwBlockReplicaEligibility
metadata:
  name: live-r1
  namespace: ${NAMESPACE}
spec:
  volumeName: live-returned
  volumeID: v1
  pvcName: iscsi-returned-replica-chain
  replicaID: r1
YAML
run_expect_success apply-live-target "${KUBECTL}" apply -f "${ARTIFACT_DIR}/live-r1-target.yaml"

job_name="phase54-live-close-executor"
"${KUBECTL}" delete job "${job_name}" -n "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
cat >"${ARTIFACT_DIR}/${job_name}.yaml" <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
  namespace: ${NAMESPACE}
  labels:
    block.seaweedfs.com/phase54-live-close: "true"
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        block.seaweedfs.com/phase54-live-close: "true"
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
run_expect_success apply-executor-job "${KUBECTL}" apply -f "${ARTIFACT_DIR}/${job_name}.yaml"
if ! run_capture wait-executor-job "${KUBECTL}" wait --for=condition=complete "job/${job_name}" -n "${NAMESPACE}" --timeout=120s; then
  "${KUBECTL}" describe job "${job_name}" -n "${NAMESPACE}" >"${ARTIFACT_DIR}/executor-job-describe.txt" 2>&1 || true
  "${KUBECTL}" logs "job/${job_name}" -n "${NAMESPACE}" >"${ARTIFACT_DIR}/executor.log" 2>&1 || true
  cat "${ARTIFACT_DIR}/executor.log" >&2 || true
  exit 1
fi
run_expect_success logs-executor-job "${KUBECTL}" logs "job/${job_name}" -n "${NAMESPACE}"
cp "${ARTIFACT_DIR}/logs-executor-job.stdout.txt" "${ARTIFACT_DIR}/executor.log"

if ! grep -q "authority_executor=executed" "${ARTIFACT_DIR}/executor.log"; then
  echo "executor did not report executed" >&2
  cat "${ARTIFACT_DIR}/executor.log" >&2
  exit 1
fi

assert_eq "executor_contracts" "$(grep -o 'contracts=[0-9]*' "${ARTIFACT_DIR}/executor.log" | head -1 | cut -d= -f2)" "1"
assert_eq "executor_mutation_attempts" "$(grep -o 'mutation_attempts=[0-9]*' "${ARTIFACT_DIR}/executor.log" | head -1 | cut -d= -f2)" "1"
assert_eq "executor_ack_mutation_attempts" "$(grep -o 'ack_eligibility_mutation_attempts=[0-9]*' "${ARTIFACT_DIR}/executor.log" | head -1 | cut -d= -f2)" "1"
assert_eq "executor_terminal_missing" "$(grep -o 'terminal_evidence_missing=[0-9]*' "${ARTIFACT_DIR}/executor.log" | head -1 | cut -d= -f2)" "0"
assert_eq "executor_target_missing" "$(grep -o 'ack_eligibility_target_missing=[0-9]*' "${ARTIFACT_DIR}/executor.log" | head -1 | cut -d= -f2)" "0"

assert_eq "target_reason" "$(jsonpath swblockreplicaeligibility/live-r1 '{.status.reasonCode}')" "ack_eligibility_recorded"
assert_eq "target_ack_known" "$(jsonpath swblockreplicaeligibility/live-r1 '{.status.ackEligibilityKnown}')" "true"
assert_eq "target_ack_eligible" "$(jsonpath swblockreplicaeligibility/live-r1 '{.status.ackEligible}')" "true"
assert_eq "target_frontend_fenced" "$(jsonpath swblockreplicaeligibility/live-r1 '{.status.frontendFencedAfterExecution}')" "true"
assert_eq "target_primary_unchanged" "$(jsonpath swblockreplicaeligibility/live-r1 '{.status.primaryUnchanged}')" "true"
assert_eq "target_frontier_covered" "$(jsonpath swblockreplicaeligibility/live-r1 '{.status.durableFrontierCovered}')" "true"
assert_eq "target_no_cross_volume" "$(jsonpath swblockreplicaeligibility/live-r1 '{.status.noCrossVolumeIdentityChange}')" "true"
assert_eq "source_ack_still_false" "$(jsonpath swblockvolume/live-returned '{.status.replicaReintegrations[0].ackEligible}')" "false"

run_expect_success target-json "${KUBECTL}" get swblockreplicaeligibility live-r1 -n "${NAMESPACE}" -o json
if grep -Eq 'frontend publication|rebuild traffic|failback' "${ARTIFACT_DIR}/target-json.stdout.txt"; then
  write_summary "target_nonclaims_ok=true"
else
  echo "target non-claims missing frontend/rebuild/failback entries" >&2
  exit 1
fi

run_capture delete-synthetic-objects "${KUBECTL}" delete swblockvolumes,swblockreplicaeligibilities --all -n "${NAMESPACE}" --ignore-not-found || true
write_summary "phase54_authority_executor_live_close_status=ok"
