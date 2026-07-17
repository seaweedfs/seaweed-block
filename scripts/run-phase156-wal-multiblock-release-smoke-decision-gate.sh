#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase156-wal-multiblock-release-smoke-decision-gate}"
SUMMARY="${ARTIFACT_DIR}/phase156-wal-multiblock-release-smoke-decision-summary.txt"

mkdir -p "${ARTIFACT_DIR}/helm"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

require_grep() {
  local file="$1"
  local needle="$2"
  local label="$3"
  if ! grep -Fq -- "${needle}" "${file}"; then
    echo "missing ${label}: ${needle} in ${file}" >&2
    exit 1
  fi
  write_summary "${label}=true"
}

require_not_grep() {
  local file="$1"
  local needle="$2"
  local label="$3"
  if grep -Fq -- "${needle}" "${file}"; then
    echo "unexpected ${label}: ${needle} in ${file}" >&2
    exit 1
  fi
  write_summary "${label}=true"
}

write_summary "phase156_wal_multiblock_release_smoke_decision_status=running"
write_summary "runtime_opt_in_name=durable-wal-multiblock-records"
write_summary "default_wal_format_unchanged=true"
write_summary "source_gated_status=kept"
write_summary "published_image_smoke_required=true"
write_summary "published_image_smoke_scope=explicit_opt_in_recovery_status"
write_summary "recovery_test_disable_flusher_user_claim=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "release_image_claim_allowed=false"

cd "${ROOT}"

require_grep "charts/seaweed-block/values.yaml" "durableWALMultiBlockRecords: false" "values_default_multiblock_false"
require_grep "charts/seaweed-block/values.yaml" "durableWALRecoveryTestDisableFlusher: false" "values_default_recovery_test_disable_flusher_false"
require_grep "charts/seaweed-block/values.schema.json" '"durableWALMultiBlockRecords": { "type": "boolean" }' "schema_documents_multiblock_opt_in"
require_grep "charts/seaweed-block/values.schema.json" '"durableWALRecoveryTestDisableFlusher": { "type": "boolean" }' "schema_documents_recovery_test_hook"

helm template sw-block charts/seaweed-block --namespace kube-system \
  >"${ARTIFACT_DIR}/helm/default-template.yaml" \
  2>"${ARTIFACT_DIR}/helm/default-template.stderr.txt"
require_not_grep "${ARTIFACT_DIR}/helm/default-template.yaml" "--launcher-durable-wal-multiblock-records" "helm_default_omits_multiblock_opt_in"
require_not_grep "${ARTIFACT_DIR}/helm/default-template.yaml" "--launcher-durable-wal-recovery-test-disable-flusher" "helm_default_omits_recovery_test_disable_flusher"

helm template sw-block charts/seaweed-block --namespace kube-system \
  --set blockmaster.durableWALMultiBlockRecords=true \
  --set blockmaster.durableWALRecoveryTestDisableFlusher=true \
  >"${ARTIFACT_DIR}/helm/explicit-opt-in-template.yaml" \
  2>"${ARTIFACT_DIR}/helm/explicit-opt-in-template.stderr.txt"
require_grep "${ARTIFACT_DIR}/helm/explicit-opt-in-template.yaml" "--launcher-durable-wal-multiblock-records" "helm_explicit_renders_multiblock_opt_in"
require_grep "${ARTIFACT_DIR}/helm/explicit-opt-in-template.yaml" "--launcher-durable-wal-recovery-test-disable-flusher" "helm_explicit_renders_recovery_test_hook"

require_grep "docs/releases/wal-multiblock-opt-in.md" "Status: source-gated opt-in. This is **not** a default format change." "release_doc_status_source_gated"
require_grep "docs/releases/wal-multiblock-opt-in.md" "Phase 156 release-smoke decision: keep this opt-in source-gated." "phase156_decision_documented"
require_grep "docs/releases/wal-multiblock-opt-in.md" "A future public or published-image claim requires a matching-image smoke" "published_image_smoke_requirement_documented"
require_grep "docs/releases/wal-multiblock-opt-in.md" "explicit opt-in enabled" "published_image_smoke_scope_opt_in_documented"
require_grep "docs/releases/wal-multiblock-opt-in.md" 'durable-status `DurableLSN == HeadLSN == recovered LSN`' "published_image_smoke_scope_head_lsn_documented"
require_grep "docs/releases/wal-multiblock-opt-in.md" "phase155_mounted_durable_status_head_lsn_confirmation_status=ok" "phase155_mounted_confirmation_cited"
require_grep "docs/releases/wal-multiblock-opt-in.md" "No performance, throughput, latency, RTO, RPO, or SLO claim." "performance_non_claim_documented"
require_grep "docs/releases/wal-multiblock-opt-in.md" "No RoCE, NVMe/RDMA, GPU Direct, cuFile/cuObject, or NIXL claim." "rdma_non_claim_documented"
require_grep "docs/releases/wal-multiblock-opt-in.md" "Keep the recovery-test flusher-disable hook out of user release guidance." "recovery_test_hook_not_user_guidance"

require_grep "docs/releases/README.md" "wal-multiblock-opt-in.md" "release_index_links_boundary_doc"
require_grep "docs/releases/README.md" "future published-image smoke" "release_index_future_smoke_boundary_documented"
require_grep "docs/releases/README.md" "public release-image claim" "release_index_public_claim_boundary_documented"
require_grep "README.md" "WAL multi-block record opt-in" "root_readme_feature_status_documented"
require_grep "README.md" "requires a future matching-image release smoke" "root_readme_future_smoke_boundary_documented"
require_grep "README.md" "public image claim" "root_readme_public_claim_boundary_documented"
require_grep "charts/seaweed-block/README.md" "This is a lab-only optimization boundary backed by Phase 151/152/155 gates." "chart_readme_phase155_boundary_documented"
require_grep "charts/seaweed-block/README.md" "not a release-image claim until a matching-image smoke" "chart_readme_future_smoke_boundary_documented"
require_grep "charts/seaweed-block/README.md" "recovery/status path" "chart_readme_smoke_scope_documented"
require_grep "docs/releases/nvme-tcp-supported-lab.md" "156 | WAL multi-block release-smoke decision keeps opt-in source-gated | PASS" "nvme_lab_phase156_row_documented"

write_summary "matching_image_smoke_scope_documented=true"
write_summary "phase155_mounted_confirmation_cited=true"
write_summary "release_note_non_claims_documented=true"
write_summary "phase156_decision=keep_source_gated_until_matching_image_smoke"
write_summary "next_recommendation=phase157_nvme_rdma_capability_boundary"
write_summary "cleanup_status=ok"
write_summary "phase156_wal_multiblock_release_smoke_decision_status=ok"
