#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase56-returned-replica-rebuild-contract-gate}"
SUMMARY="${ARTIFACT_DIR}/phase56-returned-replica-rebuild-contract-summary.txt"
BIN_DIR="${ARTIFACT_DIR}/bin"
BUNDLE_DIR="${ARTIFACT_DIR}/bundle"
REPORT_DIR="${ARTIFACT_DIR}/report"

mkdir -p "${ARTIFACT_DIR}" "${BIN_DIR}" "${BUNDLE_DIR}" "${REPORT_DIR}"
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

assert_file_contains() {
  local name="$1"
  local file="$2"
  local pattern="$3"
  local count
  count="$(grep -E -c "${pattern}" "${file}" || true)"
  write_summary "${name}=${count}"
  if [ "${count}" = "0" ]; then
    echo "${name}: missing pattern ${pattern} in ${file}" >&2
    return 1
  fi
}

write_summary "phase56_returned_replica_rebuild_contract_status=running"

run_expect_success build-sw-block go build -o "${BIN_DIR}/sw-block" ./cmd/sw-block

cat >"${ARTIFACT_DIR}/make-bundle.go" <<'GO'
package main

import (
	"os"
	"path/filepath"
	"time"

	ops "github.com/seaweedfs/seaweed-block/core/ops"
)

func main() {
	if len(os.Args) != 2 {
		panic("usage: make-bundle <bundle-dir>")
	}
	productDir := filepath.Join(os.Args[1], "demo", "product-observation")
	if err := os.MkdirAll(productDir, 0o755); err != nil {
		panic(err)
	}
	cluster := ops.NewClusterEvidence(time.Date(2026, 6, 23, 18, 0, 0, 0, time.UTC))
	cluster.ProductRevision = "phase56-test"
	cluster.Status = ops.ObservationStatusRecovering
	cluster.Volumes = []ops.VolumeEvidence{{
		VolumeID:              "pvc-rebuild",
		Namespace:             "default",
		PVCName:               "rebuild-pvc",
		ReplicationFactor:     3,
		Status:                ops.ObservationStatusRecovering,
		PrimaryReplica:        "r2",
		PrimaryNode:           "m02",
		PublishTarget:         "192.168.1.184:3260",
		Epoch:                 3,
		EndpointVersion:       7,
		RequiredFrontierKnown: true,
		RequiredFrontierLSN:   4241,
		Replicas: []ops.ReplicaEvidence{{
			ReplicaID:            "r1",
			KubernetesNode:       "m01",
			Observed:             true,
			Role:                 "previous_primary",
			ReplicationRole:      "replica_behind",
			Healthy:              false,
			FrontendPrimaryReady: false,
			AckEligibilityKnown:  true,
			AckEligible:          false,
			DurableFrontierKnown: true,
			DurableFrontierLSN:   4240,
			StalePrimaryFenced:   true,
			SupportBundlePath:    "returned-replica-rebuild-summary.txt",
		}, {
			ReplicaID:            "r2",
			KubernetesNode:       "m02",
			Observed:             true,
			Role:                 "primary",
			Healthy:              true,
			FrontendPrimaryReady: true,
			ReplicationRole:      "primary",
			FrontendAddr:         "192.168.1.184:3260",
			DurableFrontierKnown: true,
			DurableFrontierLSN:   4241,
		}},
	}}
	raw, err := ops.MarshalObservationJSON(cluster)
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(productDir, ops.ClusterEvidenceArtifact), raw, 0o644); err != nil {
		panic(err)
	}
}
GO

run_expect_success make-bundle go run "${ARTIFACT_DIR}/make-bundle.go" "${BUNDLE_DIR}"
run_expect_success ops-report "${BIN_DIR}/sw-block" ops report --from-bundle "${BUNDLE_DIR}" --out "${REPORT_DIR}"
run_expect_success ops-explain "${BIN_DIR}/sw-block" ops explain volume --from-bundle "${BUNDLE_DIR}" pvc-rebuild
cp "${ARTIFACT_DIR}/ops-explain.stdout.txt" "${ARTIFACT_DIR}/explain.txt"

SUMMARY_TXT="${REPORT_DIR}/summary.txt"
SNAPSHOT_JSON="${REPORT_DIR}/operator-snapshot.json"

assert_file_contains "summary_rebuild_preflight_ready" "${SUMMARY_TXT}" '^managed_volume_executor_preflight=authority\.rebuild_returned_replica target=r1 decision=ready reason=preconditions_satisfied mode=dry_run executor=authority_recovery_executor mutation_allowed=false .*required_lsn=4241 durable_lsn=4240$'
assert_file_contains "summary_rebuild_contract_disabled" "${SUMMARY_TXT}" '^managed_volume_executor_contract=authority\.rebuild_returned_replica target=r1 decision=disabled reason=executor_policy_disabled executor=authority_recovery_executor execution_enabled=false mutation_allowed=false allowed_mutation=rebuild_traffic terminal_evidence=frontend_fenced_before_rebuild,primary_unchanged,durable_frontier_caught_up,no_frontend_publication,no_cross_volume_identity_change$'
assert_file_contains "summary_rebuild_action_disabled" "${SUMMARY_TXT}" '^managed_volume_action=authority\.rebuild_returned_replica mode=dry_run side_effect=authority_mutating executor=authority_recovery_executor decision=rejected reason=policy_disabled$'
assert_file_contains "explain_rebuild_contract_disabled" "${ARTIFACT_DIR}/explain.txt" '^managed_volume_executor_contract authority\.rebuild_returned_replica target=r1 decision=disabled reason=executor_policy_disabled executor=authority_recovery_executor execution_enabled=false mutation_allowed=false allowed_mutation=rebuild_traffic terminal_evidence=frontend_fenced_before_rebuild,primary_unchanged,durable_frontier_caught_up,no_frontend_publication,no_cross_volume_identity_change$'

python3 - "${SNAPSHOT_JSON}" <<'PY'
import json, sys
doc = json.load(open(sys.argv[1], encoding="utf-8"))
volumes = doc.get("volumes") or []
assert len(volumes) == 1, volumes
status = volumes[0].get("status") or {}
preflights = status.get("executor_preflights") or []
contracts = status.get("executor_contracts") or []
actions = volumes[0].get("allowed_actions") or []
assert len(preflights) == 1, preflights
assert preflights[0]["action_type"] == "authority.rebuild_returned_replica", preflights
assert preflights[0]["decision"] == "ready", preflights
assert preflights[0]["durable_frontier_lsn"] == 4240, preflights
assert preflights[0]["required_frontier_lsn"] == 4241, preflights
assert len(contracts) == 1, contracts
assert contracts[0]["action_type"] == "authority.rebuild_returned_replica", contracts
assert contracts[0]["decision"] == "disabled", contracts
assert contracts[0]["execution_enabled"] is False, contracts
assert contracts[0]["mutation_allowed"] is False, contracts
assert contracts[0]["allowed_mutation_class"] == ["rebuild_traffic"], contracts
assert "no_frontend_publication" in contracts[0]["terminal_evidence_required"], contracts
rebuild_actions = [a for a in actions if a.get("type") == "authority.rebuild_returned_replica"]
assert len(rebuild_actions) == 1, actions
assert rebuild_actions[0]["decision"] == "rejected", rebuild_actions
assert rebuild_actions[0]["decision_reason"] == "policy_disabled", rebuild_actions
assert rebuild_actions[0]["mutation_allowed"] is False, rebuild_actions
PY
write_summary "operator_snapshot_rebuild_contract=ok"

dashboard_port="$(python3 - <<'PY'
import socket
s=socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
"${BIN_DIR}/sw-block" ops dashboard --from-bundle "${BUNDLE_DIR}" --listen "127.0.0.1:${dashboard_port}" --serve-duration 8s \
  >"${ARTIFACT_DIR}/dashboard.stdout.txt" 2>"${ARTIFACT_DIR}/dashboard.stderr.txt" &
dashboard_pid=$!
trap 'kill ${dashboard_pid} >/dev/null 2>&1 || true; wait ${dashboard_pid} >/dev/null 2>&1 || true' EXIT
for _ in $(seq 1 40); do
  if python3 - "http://127.0.0.1:${dashboard_port}/operator-snapshot.json" >"${ARTIFACT_DIR}/dashboard-snapshot.json" <<'PY'
import sys, urllib.request
with urllib.request.urlopen(sys.argv[1], timeout=1) as r:
	print(r.read().decode())
PY
  then
    break
  fi
  sleep 0.2
done
python3 - "${ARTIFACT_DIR}/dashboard-snapshot.json" <<'PY'
import json, sys
doc = json.load(open(sys.argv[1], encoding="utf-8"))
contracts = doc["volumes"][0]["status"]["executor_contracts"]
assert contracts[0]["action_type"] == "authority.rebuild_returned_replica", contracts
assert contracts[0]["decision"] == "disabled", contracts
assert contracts[0]["allowed_mutation_class"] == ["rebuild_traffic"], contracts
PY
write_summary "dashboard_rebuild_contract=ok"

write_summary "phase56_returned_replica_rebuild_contract_status=ok"
