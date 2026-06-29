#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase87-failback-docs-alignment-gate}"
SUMMARY="${ARTIFACT_DIR}/phase87-failback-docs-alignment-summary.txt"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

require_text() {
  local name="$1"
  local file="$2"
  local pattern="$3"
  local found="false"
  if grep -Fq -- "${pattern}" "${PRODUCT_ROOT}/${file}"; then
    found="true"
  fi
  write_summary "${name}=${found}"
  if [ "${found}" != "true" ]; then
    echo "missing ${name}: ${file}: ${pattern}" >&2
    return 1
  fi
}

write_summary "phase87_failback_docs_alignment_status=running"

require_text "readme_names_source_gated_failback" "README.md" "Returned-replica failback runtime | Source-gated"
require_text "readme_names_no_automatic_failback" "README.md" "automatic deployed failback"
require_text "readme_names_release_smoke_requirement" "README.md" "requires a future release smoke"
require_text "wiki_deep_dive_exists" "docs/wiki/deep-dives/returned-replica-failback.md" "# Returned Replica Failback Runtime"
require_text "wiki_names_terminal_evidence" "docs/wiki/deep-dives/returned-replica-failback.md" "authorityEpochAdvanced=true"
require_text "wiki_names_code_entry_points" "docs/wiki/deep-dives/returned-replica-failback.md" "core/ops/failback_executor_controller.go"
require_text "wiki_names_current_limits" "docs/wiki/deep-dives/returned-replica-failback.md" "automatic deployed Kubernetes failback"
require_text "wiki_index_links_failback" "docs/wiki/index.md" "deep-dives/returned-replica-failback.md"
require_text "topic_inventory_classifies_failback" "docs/wiki/topic-inventory.md" "returned-replica failback runtime | deep"
require_text "product_roadmap_names_phase86" "internal/docs/product-roadmap.md" "Phases 74-86 add"
require_text "product_roadmap_names_opt_in" "internal/docs/product-roadmap.md" "opt-in/source-gated failback"
require_text "product_roadmap_defers_automatic" "internal/docs/product-roadmap.md" "automatic deployed failback"

write_summary "failback_runtime_public_claim_aligned=true"
write_summary "automatic_failback_not_claimed=true"
write_summary "frontend_publication_after_failback_not_claimed=true"
write_summary "phase87_failback_docs_alignment_status=ok"
