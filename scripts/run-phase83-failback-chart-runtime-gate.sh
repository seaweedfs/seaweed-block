#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase83-failback-chart-runtime-gate}"
SUMMARY="${ARTIFACT_DIR}/phase83-failback-chart-runtime-summary.txt"
DEFAULT_RENDER="${ARTIFACT_DIR}/helm-default.yaml"
ENABLED_RENDER="${ARTIFACT_DIR}/helm-enabled.yaml"
BAD_DRYRUN_LOG="${ARTIFACT_DIR}/helm-bad-dryrun.log"
BAD_POLICY_LOG="${ARTIFACT_DIR}/helm-bad-policy.log"
BAD_AMBIGUOUS_LOG="${ARTIFACT_DIR}/helm-bad-ambiguous.log"
LINT_LOG="${ARTIFACT_DIR}/helm-lint.log"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

require_present() {
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

require_helm_failure() {
  local name="$1"
  local want="$2"
  local log="$3"
  shift 3
  if (cd "${PRODUCT_ROOT}" && helm template sw-block charts/seaweed-block --namespace kube-system "$@") >"${log}" 2>&1; then
    write_summary "${name}=false"
    echo "expected helm failure for ${name}" >&2
    return 1
  fi
  require_present "${name}" "${want}" "${log}"
}

write_summary "phase83_failback_chart_runtime_status=running"
write_summary "phase83_scope=failback_chart_runtime_wiring"
write_summary "default_failback_runtime_rpc=false"
write_summary "default_failback_executor_created=false"
write_summary "default_failback_attempts=0"
write_summary "frontend_publication_allowed=false"
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
require_absent "default_omits_failback_executor_deployment" "name: sw-block-failback-executor" "${DEFAULT_RENDER}"
require_absent "default_omits_enable_execution" "--enable-execution" "${DEFAULT_RENDER}"
require_absent "default_omits_failback_grpc_addr" "--failback-runtime-grpc-addr" "${DEFAULT_RENDER}"

(cd "${PRODUCT_ROOT}" && helm template sw-block charts/seaweed-block --namespace kube-system \
  --set blockmaster.failbackRuntimeRPC=true \
  --set failbackExecutor.create=true \
  --set failbackExecutor.dryRun=false \
  --set failbackExecutor.execution.enabled=true \
  --set failbackExecutor.execution.policy=true \
  --set failbackExecutor.execution.failbackRuntimeGrpcAddr=blockmaster.kube-system.svc:9333) >"${ENABLED_RENDER}"

require_present "enabled_renders_failback_runtime_rpc" "--failback-runtime-rpc" "${ENABLED_RENDER}"
require_present "enabled_renders_failback_executor_deployment" "name: sw-block-failback-executor" "${ENABLED_RENDER}"
require_present "enabled_renders_enable_execution" "--enable-execution" "${ENABLED_RENDER}"
require_present "enabled_renders_execution_policy" "--execution-policy" "${ENABLED_RENDER}"
require_present "enabled_renders_failback_grpc_addr" "--failback-runtime-grpc-addr=blockmaster.kube-system.svc:9333" "${ENABLED_RENDER}"
require_absent "enabled_omits_dry_run" "--dry-run" "${ENABLED_RENDER}"

require_helm_failure "rejects_execution_with_dry_run" \
  "failbackExecutor.execution.enabled=true requires failbackExecutor.dryRun=false" \
  "${BAD_DRYRUN_LOG}" \
  --set failbackExecutor.create=true \
  --set failbackExecutor.execution.enabled=true \
  --set failbackExecutor.execution.policy=true \
  --set failbackExecutor.execution.failbackRuntimeGrpcAddr=blockmaster.kube-system.svc:9333

require_helm_failure "rejects_execution_without_policy" \
  "failbackExecutor.execution.enabled=true requires failbackExecutor.execution.policy=true" \
  "${BAD_POLICY_LOG}" \
  --set failbackExecutor.create=true \
  --set failbackExecutor.dryRun=false \
  --set failbackExecutor.execution.enabled=true \
  --set failbackExecutor.execution.failbackRuntimeGrpcAddr=blockmaster.kube-system.svc:9333

require_helm_failure "rejects_ambiguous_runtime_transports" \
  "failbackExecutor.execution.failbackRuntimeGrpcAddr and failbackExecutor.execution.failbackRuntimeURL are mutually exclusive" \
  "${BAD_AMBIGUOUS_LOG}" \
  --set failbackExecutor.create=true \
  --set failbackExecutor.dryRun=false \
  --set failbackExecutor.execution.enabled=true \
  --set failbackExecutor.execution.policy=true \
  --set failbackExecutor.execution.failbackRuntimeGrpcAddr=blockmaster.kube-system.svc:9333 \
  --set failbackExecutor.execution.failbackRuntimeURL=http://127.0.0.1:23260/runtime/failback

write_summary "execution_policy_still_required=true"
write_summary "runtime_transport_must_be_unambiguous=true"
write_summary "chart_default_remains_non_mutating=true"
write_summary "frontend_publication_allowed=false"
write_summary "storage_mutation_allowed=false"
write_summary "phase83_failback_chart_runtime_status=ok"
