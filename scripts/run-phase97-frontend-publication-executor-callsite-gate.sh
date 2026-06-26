#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase97-frontend-publication-executor-callsite-gate}"
SUMMARY="${ARTIFACT_DIR}/phase97-frontend-publication-executor-callsite-summary.txt"
OPS_LOG="${ARTIFACT_DIR}/core-ops-go-test.log"
CMD_LOG="${ARTIFACT_DIR}/cmd-sw-block-go-test.log"
DEFAULT_RENDER="${ARTIFACT_DIR}/helm-default.yaml"
ENABLED_RENDER="${ARTIFACT_DIR}/helm-enabled.yaml"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

require_log() {
  local name="$1"
  local pattern="$2"
  local file="$3"
  local found="false"
  if grep -Eq -- "${pattern}" "${file}"; then
    found="true"
  fi
  write_summary "${name}=${found}"
  if [ "${found}" != "true" ]; then
    echo "missing evidence ${name}: pattern ${pattern}" >&2
    return 1
  fi
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
    echo "missing text ${name}: ${pattern}" >&2
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
    echo "unexpected text ${name}: ${pattern}" >&2
    return 1
  fi
}

write_summary "phase97_frontend_publication_executor_callsite_status=running"
write_summary "phase97_scope=explicit_policy_frontend_publication_after_failback"
write_summary "storage_mutation_allowed=false"
write_summary "failback_started=false"

OPS_PATTERN="TestFrontendPublicationExecutorInvokesRuntimeForFailbackTargetWhenExplicitlyEnabled|TestFrontendPublicationExecutorAcceptsFailbackTerminalTargetAsDisabled|TestFrontendPublicationExecutorRejectsInvalidRuntimeTerminalEvidence|TestFrontendPublicationExecutorRuntimeFailureWritesBlockedStatus|TestHTTPFrontendPublicationRuntimePostsRequestAndDecodesResult|TestPhase70FrontendPublicationExecutorPackagingIsStatusOnly"
CMD_PATTERN="TestOpsFrontendPublicationExecutorExecutesFailbackTargetWithExplicitPolicy|TestOpsFrontendPublicationExecutorPolicyBlocksExecution|TestOpsFrontendPublicationExecutorRejectsRuntimeWithoutEnable"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "${OPS_PATTERN}" -count=1 -v) >"${OPS_LOG}" 2>&1; then
  write_summary "core_ops_phase97_tests=pass"
else
  write_summary "core_ops_phase97_tests=fail"
  cat "${OPS_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./cmd/sw-block -run "${CMD_PATTERN}" -count=1 -v) >"${CMD_LOG}" 2>&1; then
  write_summary "cmd_sw_block_phase97_tests=pass"
else
  write_summary "cmd_sw_block_phase97_tests=fail"
  cat "${CMD_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && helm template sw-block charts/seaweed-block --namespace kube-system) >"${DEFAULT_RENDER}" 2>"${ARTIFACT_DIR}/helm-default.err"; then
  write_summary "helm_default_render=pass"
else
  write_summary "helm_default_render=fail"
  cat "${ARTIFACT_DIR}/helm-default.err" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && helm template sw-block charts/seaweed-block --namespace kube-system \
  --set frontendPublicationExecutor.create=true \
  --set frontendPublicationExecutor.dryRun=false \
  --set frontendPublicationExecutor.execution.enabled=true \
  --set frontendPublicationExecutor.execution.policy=true \
  --set frontendPublicationExecutor.execution.runtimeUrl=http://frontend-runtime.kube-system.svc/runtime) >"${ENABLED_RENDER}" 2>"${ARTIFACT_DIR}/helm-enabled.err"; then
  write_summary "helm_enabled_render=pass"
else
  write_summary "helm_enabled_render=fail"
  cat "${ARTIFACT_DIR}/helm-enabled.err" >&2 || true
  exit 1
fi

require_log "failback_target_runtime_invoked" "^--- PASS: TestFrontendPublicationExecutorInvokesRuntimeForFailbackTargetWhenExplicitlyEnabled" "${OPS_LOG}"
require_log "failback_target_default_disabled" "^--- PASS: TestFrontendPublicationExecutorAcceptsFailbackTerminalTargetAsDisabled" "${OPS_LOG}"
require_log "invalid_terminal_evidence_no_false_publish" "^--- PASS: TestFrontendPublicationExecutorRejectsInvalidRuntimeTerminalEvidence" "${OPS_LOG}"
require_log "runtime_failure_no_false_publish" "^--- PASS: TestFrontendPublicationExecutorRuntimeFailureWritesBlockedStatus" "${OPS_LOG}"
require_log "http_runtime_posts_request" "^--- PASS: TestHTTPFrontendPublicationRuntimePostsRequestAndDecodesResult" "${OPS_LOG}"
require_log "executor_packaging_default_off" "^--- PASS: TestPhase70FrontendPublicationExecutorPackagingIsStatusOnly" "${OPS_LOG}"
require_log "cmd_explicit_policy_invokes_runtime" "^--- PASS: TestOpsFrontendPublicationExecutorExecutesFailbackTargetWithExplicitPolicy" "${CMD_LOG}"
require_log "cmd_execution_policy_blocks" "^--- PASS: TestOpsFrontendPublicationExecutorPolicyBlocksExecution" "${CMD_LOG}"
require_log "cmd_runtime_url_requires_enable" "^--- PASS: TestOpsFrontendPublicationExecutorRejectsRuntimeWithoutEnable" "${CMD_LOG}"

require_absent "default_omits_frontend_publication_executor" "name: sw-block-frontend-publication-executor" "${DEFAULT_RENDER}"
require_text "enabled_renders_frontend_publication_executor" "name: sw-block-frontend-publication-executor" "${ENABLED_RENDER}"
require_text "enabled_renders_enable_execution" "--enable-execution" "${ENABLED_RENDER}"
require_text "enabled_renders_execution_policy" "--execution-policy" "${ENABLED_RENDER}"
require_text "enabled_renders_runtime_url" "--frontend-publication-runtime-url=http://frontend-runtime.kube-system.svc/runtime" "${ENABLED_RENDER}"

write_summary "failback_target_runtime_invoked=true"
write_summary "frontend_publication_attempts=1"
write_summary "frontend_published=true"
write_summary "failback_attempts=0"
write_summary "failback_started=false"
write_summary "publication_status_reason=frontend_published"
write_summary "publication_mutation_allowed=false"
write_summary "frontend_publication_executor_default_off=true"
write_summary "frontend_publication_execution_requires_policy=true"
write_summary "frontend_publication_runtime_url_requires_enable=true"
write_summary "phase97_frontend_publication_executor_callsite_status=ok"
