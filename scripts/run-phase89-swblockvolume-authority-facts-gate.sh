#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase89-swblockvolume-authority-facts-gate}"
SUMMARY="${ARTIFACT_DIR}/phase89-swblockvolume-authority-facts-summary.txt"
TEST_LOG="${ARTIFACT_DIR}/go-test-core-ops.log"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

require_text() {
  local name="$1"
  local pattern="$2"
  local file="$3"
  local found="false"
  if grep -Fq -- "${pattern}" "${file}"; then
    found="true"
  fi
  write_summary "${name}=${found}"
  if [ "${found}" != "true" ]; then
    echo "missing ${name}: ${pattern}" >&2
    return 1
  fi
}

require_absent() {
  local name="$1"
  local pattern="$2"
  local file="$3"
  local absent="true"
  if grep -Fq -- "${pattern}" "${file}"; then
    absent="false"
  fi
  write_summary "${name}=${absent}"
  if [ "${absent}" != "true" ]; then
    echo "unexpected ${name}: ${pattern}" >&2
    return 1
  fi
}

write_summary "phase89_swblockvolume_authority_facts_status=running"
write_summary "phase89_scope=swblockvolume_status_authority_fact_projection"
write_summary "failback_activation_attempted=false"
write_summary "failback_target_created=false"
write_summary "storage_mutation_allowed=false"

TEST_RE='TestObservationReportSummary_IncludesManagedVolumeStatus|TestManagedVolumeProjection_HealthyFirstVolumeReady|TestManagedVolumeOperatorContract_ReadinessConditionAndEvents|TestOperatorStatusReconcilerWritesStatusOnlyProjection|TestPhase89SwBlockVolumeAuthorityFactsSchema'
if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${TEST_RE}" -count=1 -v) >"${TEST_LOG}" 2>&1; then
  write_summary "go_test_core_ops_authority_projection=pass"
else
  write_summary "go_test_core_ops_authority_projection=fail"
  cat "${TEST_LOG}" >&2 || true
  exit 1
fi

CRD="${PRODUCT_ROOT}/charts/seaweed-block/crds/swblockvolumes.block.seaweedfs.com.yaml"
require_text "crd_status_primary_replica_id" "primaryReplicaID:" "${CRD}"
require_text "crd_status_publish_target" "publishTarget:" "${CRD}"
require_text "crd_status_authority_epoch" "authorityEpoch:" "${CRD}"
require_text "crd_status_authority_endpoint_version" "authorityEndpointVersion:" "${CRD}"
require_absent "crd_status_omits_snake_primary_replica_id" "primary_replica_id:" "${CRD}"
require_absent "crd_status_omits_snake_authority_epoch" "authority_epoch:" "${CRD}"

require_text "projection_preserves_primary_replica" "projection.PrimaryReplicaID != \"r1\"" "${PRODUCT_ROOT}/core/ops/managed_volume_model_test.go"
require_text "operator_snapshot_snake_authority" "\"primary_replica_id\":\"r1\"" "${PRODUCT_ROOT}/core/ops/managed_volume_operator_contract_test.go"
require_text "crd_status_camel_authority" "\"primaryReplicaID\":\"r1\"" "${PRODUCT_ROOT}/core/ops/operator_status_controller_test.go"
require_text "report_summary_authority_line" "managed_volume_authority=pvc-healthy primary=r1 publish_target=192.168.1.181:3260 epoch=1 endpoint_version=1" "${PRODUCT_ROOT}/core/ops/observation_report_test.go"

write_summary "failback_activation_inputs_visible=true"
write_summary "expected_current_replica_source=swblockvolume.status.primaryReplicaID"
write_summary "expected_current_epoch_source=swblockvolume.status.authorityEpoch"
write_summary "automatic_failback_claimed=false"
write_summary "frontend_publication_after_failback_claimed=false"
write_summary "phase89_swblockvolume_authority_facts_status=ok"
