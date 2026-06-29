#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase98-failback-frontend-workload-close-gate}"
SUMMARY="${ARTIFACT_DIR}/phase98-failback-frontend-workload-close-summary.txt"
PHASE95_DIR="${ARTIFACT_DIR}/phase95"
PHASE95_SUMMARY="${PHASE95_DIR}/phase95-failback-live-deployed-suite-summary.txt"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

require_line() {
  local pattern="$1"
  if ! grep -q "$pattern" "${PHASE95_SUMMARY}"; then
    echo "missing required evidence ${pattern} in ${PHASE95_SUMMARY}" >&2
    write_summary "phase98_failback_frontend_workload_close_status=failed"
    exit 1
  fi
}

write_summary "phase98_failback_frontend_workload_close_status=running"
write_summary "phase98_scope=deployed_failback_frontend_publication_workload_io_cleanup"

SW_BLOCK_ARTIFACT_DIR="${PHASE95_DIR}" \
SW_BLOCK_PHASE95_FRONTEND_PUBLICATION_CLOSE=true \
  bash "${PRODUCT_ROOT}/scripts/run-phase95-failback-live-deployed-suite-gate.sh" "${PRODUCT_ROOT}"

require_line '^phase95_failback_live_deployed_suite_status=ok$'
require_line '^executor_status_failed_back=true$'
require_line '^master_publisher_epoch_advanced=true$'
require_line '^publish_target_swapped_after_failback=true$'
require_line '^frontend_publication_target_published=true$'
require_line '^frontend_published=true$'
require_line '^frontend_publication_failback_started=false$'
require_line '^frontend_publication_storage_mutation_allowed=false$'
require_line '^post_failback_publication_writer_verified=true$'
require_line '^post_failback_publication_reader_verified=true$'
require_line '^cleanup_status=ok$'

cat "${PHASE95_SUMMARY}" >>"${SUMMARY}"
write_summary "phase98_failback_frontend_workload_close_status=ok"
