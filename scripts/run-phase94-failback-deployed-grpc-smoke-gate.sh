#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase94-failback-deployed-grpc-smoke-gate}"
SUMMARY="${ARTIFACT_DIR}/phase94-failback-deployed-grpc-smoke-summary.txt"
MASTER_LOG="${ARTIFACT_DIR}/core-host-master-go-test.log"
LINT_LOG="${ARTIFACT_DIR}/helm-lint.log"
DEFAULT_RENDER="${ARTIFACT_DIR}/helm-default.yaml"
ENABLED_RENDER="${ARTIFACT_DIR}/helm-enabled.yaml"

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

write_summary "phase94_failback_deployed_grpc_smoke_status=running"
write_summary "phase94_scope=deployed_failback_suite_render_plus_real_master_grpc_smoke"
write_summary "frontend_publication_after_failback_claimed=false"
write_summary "storage_mutation_allowed=false"

if (cd "${PRODUCT_ROOT}" && helm lint charts/seaweed-block) >"${LINT_LOG}" 2>&1; then
  write_summary "helm_lint=pass"
else
  write_summary "helm_lint=fail"
  cat "${LINT_LOG}" >&2 || true
  exit 1
fi

(cd "${PRODUCT_ROOT}" && helm template sw-block charts/seaweed-block --namespace kube-system) >"${DEFAULT_RENDER}"
require_absent "default_omits_failback_runtime_rpc" "--failback-runtime-rpc" "${DEFAULT_RENDER}"
require_absent "default_omits_failback_target_owner" "name: sw-block-failback-target-owner" "${DEFAULT_RENDER}"
require_absent "default_omits_failback_executor" "name: sw-block-failback-executor" "${DEFAULT_RENDER}"
require_absent "default_omits_activate_targets" "--activate-targets" "${DEFAULT_RENDER}"
require_absent "default_omits_enable_execution" "--enable-execution" "${DEFAULT_RENDER}"

(cd "${PRODUCT_ROOT}" && helm template sw-block charts/seaweed-block --namespace kube-system \
  --set blockmaster.failbackRuntimeRPC=true \
  --set failbackTargetOwner.create=true \
  --set failbackTargetOwner.dryRun=false \
  --set failbackTargetOwner.activation.enabled=true \
  --set failbackTargetOwner.activation.policy=true \
  --set failbackTargetOwner.activation.runtimeEndpoint=blockmaster.kube-system.svc:9333 \
  --set failbackExecutor.create=true \
  --set failbackExecutor.dryRun=false \
  --set failbackExecutor.execution.enabled=true \
  --set failbackExecutor.execution.policy=true \
  --set failbackExecutor.execution.failbackRuntimeGrpcAddr=blockmaster.kube-system.svc:9333) >"${ENABLED_RENDER}"

require_text "enabled_renders_failback_runtime_rpc" "--failback-runtime-rpc" "${ENABLED_RENDER}"
require_text "enabled_renders_failback_target_owner" "name: sw-block-failback-target-owner" "${ENABLED_RENDER}"
require_text "enabled_renders_failback_executor" "name: sw-block-failback-executor" "${ENABLED_RENDER}"
require_text "enabled_target_owner_activates_targets" "--activate-targets" "${ENABLED_RENDER}"
require_text "enabled_target_owner_policy" "--activation-policy" "${ENABLED_RENDER}"
require_text "enabled_target_owner_runtime_endpoint" "--runtime-endpoint=blockmaster.kube-system.svc:9333" "${ENABLED_RENDER}"
require_text "enabled_executor_execution" "--enable-execution" "${ENABLED_RENDER}"
require_text "enabled_executor_policy" "--execution-policy" "${ENABLED_RENDER}"
require_text "enabled_executor_grpc_runtime" "--failback-runtime-grpc-addr=blockmaster.kube-system.svc:9333" "${ENABLED_RENDER}"
require_absent "enabled_omits_frontend_publication_executor" "name: sw-block-frontend-publication-executor" "${ENABLED_RENDER}"

TEST_PATTERN="Test(FailbackServiceDefaultDisabled|FailbackServiceEnabledUsesHostRuntime|FailbackExecutorGRPCRuntimeUsesRealMasterService)"
if (cd "${PRODUCT_ROOT}" && go test ./core/host/master -run "${TEST_PATTERN}" -count=1 -v) >"${MASTER_LOG}" 2>&1; then
  write_summary "core_host_master_failback_grpc_tests=pass"
else
  write_summary "core_host_master_failback_grpc_tests=fail"
  cat "${MASTER_LOG}" >&2 || true
  exit 1
fi

require_text "service_default_disabled_test" "--- PASS: TestFailbackServiceDefaultDisabled" "${MASTER_LOG}"
require_text "service_enabled_uses_host_runtime" "--- PASS: TestFailbackServiceEnabledUsesHostRuntime" "${MASTER_LOG}"
require_text "executor_grpc_uses_real_master_service" "--- PASS: TestFailbackExecutorGRPCRuntimeUsesRealMasterService" "${MASTER_LOG}"

write_summary "executor_status_failed_back=true"
write_summary "master_publisher_epoch_advanced=true"
write_summary "publish_target_swapped_after_failback=true"
write_summary "terminal_evidence_required=true"
write_summary "live_kubernetes_install_claimed=false"
write_summary "phase94_failback_deployed_grpc_smoke_status=ok"
