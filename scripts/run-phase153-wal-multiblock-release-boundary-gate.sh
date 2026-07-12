#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase153-wal-multiblock-release-boundary-gate}"
SUMMARY="${ARTIFACT_DIR}/phase153-wal-multiblock-release-boundary-summary.txt"

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

write_summary "phase153_wal_multiblock_release_boundary_status=running"
write_summary "frontend_transport=tcp"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "default_wal_format_unchanged=true"
write_summary "feature_gate_default=false"
write_summary "runtime_opt_in_name=durable-wal-multiblock-records"

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
  >"${ARTIFACT_DIR}/helm/explicit-multiblock-template.yaml" \
  2>"${ARTIFACT_DIR}/helm/explicit-multiblock-template.stderr.txt"
require_grep "${ARTIFACT_DIR}/helm/explicit-multiblock-template.yaml" "--launcher-durable-wal-multiblock-records" "helm_explicit_renders_multiblock_opt_in"
require_not_grep "${ARTIFACT_DIR}/helm/explicit-multiblock-template.yaml" "--launcher-durable-wal-recovery-test-disable-flusher" "helm_multiblock_opt_in_does_not_enable_recovery_test_hook"

helm template sw-block charts/seaweed-block --namespace kube-system \
  --set blockmaster.durableWALRecoveryTestDisableFlusher=true \
  >"${ARTIFACT_DIR}/helm/explicit-recovery-test-template.yaml" \
  2>"${ARTIFACT_DIR}/helm/explicit-recovery-test-template.stderr.txt"
require_grep "${ARTIFACT_DIR}/helm/explicit-recovery-test-template.yaml" "--launcher-durable-wal-recovery-test-disable-flusher" "helm_explicit_renders_recovery_test_hook"

require_grep "docs/releases/wal-multiblock-opt-in.md" "WAL Multi-Block Record Opt-In Boundary" "release_boundary_doc_exists"
require_grep "docs/releases/wal-multiblock-opt-in.md" "durableWALMultiBlockRecords: true" "runtime_opt_in_documented"
require_grep "docs/releases/wal-multiblock-opt-in.md" "--durable-wal-multiblock-records" "blockvolume_opt_in_flag_documented"
require_grep "docs/releases/wal-multiblock-opt-in.md" "durableWALRecoveryTestDisableFlusher" "recovery_test_hook_documented"
require_grep "docs/releases/wal-multiblock-opt-in.md" "not a production or user-facing tuning knob" "recovery_test_hook_not_user_feature"
require_grep "docs/releases/wal-multiblock-opt-in.md" "phase151_wal_multiblock_mounted_nvme_profile_status=ok" "mounted_profile_gate_passed"
require_grep "docs/releases/wal-multiblock-opt-in.md" "phase152_wal_multiblock_recovery_compatibility_status=ok" "mounted_recovery_gate_passed"
require_grep "docs/releases/wal-multiblock-opt-in.md" "recovered_lsn_after_restart=14545" "recovery_lsn_evidence_documented"
require_grep "docs/releases/wal-multiblock-opt-in.md" "No performance, throughput, latency, RTO, RPO, or SLO claim." "performance_non_claim_documented"
require_grep "docs/releases/wal-multiblock-opt-in.md" "No RoCE, NVMe/RDMA, GPU Direct, cuFile/cuObject, or NIXL claim." "rdma_non_claim_documented"
require_grep "docs/releases/wal-multiblock-opt-in.md" "HeadLSN" "remaining_head_lsn_followup_listed"

require_grep "docs/releases/README.md" "wal-multiblock-opt-in.md" "release_index_links_boundary_doc"
require_grep "docs/releases/README.md" "not a RoCE, NVMe/RDMA, or performance claim" "release_index_non_claims_documented"
require_grep "docs/releases/nvme-tcp-supported-lab.md" "153 | WAL multi-block release-boundary documentation | PASS" "nvme_lab_release_boundary_row_documented"
require_grep "charts/seaweed-block/README.md" "durableWALMultiBlockRecords: true" "chart_readme_documents_opt_in"
require_grep "charts/seaweed-block/README.md" "Do not set" "chart_readme_warns_recovery_test_hook"
require_grep "README.md" "WAL multi-block record opt-in" "root_readme_feature_status_documented"
require_grep "README.md" "default-off" "root_readme_default_off_documented"

require_grep "internal/docs/qa-assignments/phase151-wal-multiblock-mounted-nvme-profile-qa-signoff.md" "phase151_wal_multiblock_mounted_nvme_profile_status=ok" "phase151_signoff_cited"
require_grep "internal/docs/qa-assignments/phase152-wal-multiblock-recovery-compatibility-qa-signoff.md" "phase152_wal_multiblock_recovery_compatibility_status=ok" "phase152_signoff_cited"
require_grep "internal/docs/qa-assignments/phase152-wal-multiblock-recovery-compatibility-qa-signoff.md" "HeadLSN" "phase152_followup_cited"

write_summary "release_note_non_claims_documented=true"
write_summary "remaining_followups_listed=true"
write_summary "phase153_decision=document_opt_in"
write_summary "next_recommendation=phase154_durable_status_head_lsn_cleanup"
write_summary "cleanup_status=ok"
write_summary "phase153_wal_multiblock_release_boundary_status=ok"
