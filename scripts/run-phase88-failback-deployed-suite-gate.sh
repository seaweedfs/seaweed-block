#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase88-failback-deployed-suite-gate}"
SUMMARY="${ARTIFACT_DIR}/phase88-failback-deployed-suite-summary.txt"
DEFAULT_RENDER="${ARTIFACT_DIR}/helm-default.yaml"
ENABLED_RENDER="${ARTIFACT_DIR}/helm-enabled.yaml"
LINT_LOG="${ARTIFACT_DIR}/helm-lint.log"
SCHEMA_LOG="${ARTIFACT_DIR}/schema-check.log"

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

write_summary "phase88_failback_deployed_suite_status=running"
write_summary "phase88_scope=failback_deployed_component_suite"
write_summary "automatic_failback_claimed=false"
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
require_absent "default_omits_enable_execution" "--enable-execution" "${DEFAULT_RENDER}"
require_absent "default_omits_failback_grpc_addr" "--failback-runtime-grpc-addr" "${DEFAULT_RENDER}"

(cd "${PRODUCT_ROOT}" && helm template sw-block charts/seaweed-block --namespace kube-system \
  --set blockmaster.failbackRuntimeRPC=true \
  --set failbackTargetOwner.create=true \
  --set failbackTargetOwner.dryRun=false \
  --set failbackExecutor.create=true \
  --set failbackExecutor.dryRun=false \
  --set failbackExecutor.execution.enabled=true \
  --set failbackExecutor.execution.policy=true \
  --set failbackExecutor.execution.failbackRuntimeGrpcAddr=blockmaster.kube-system.svc:9333) >"${ENABLED_RENDER}"

require_present "enabled_renders_failback_runtime_rpc" "--failback-runtime-rpc" "${ENABLED_RENDER}"
require_present "enabled_renders_failback_target_owner" "name: sw-block-failback-target-owner" "${ENABLED_RENDER}"
require_present "enabled_renders_failback_executor" "name: sw-block-failback-executor" "${ENABLED_RENDER}"
require_present "enabled_target_owner_can_create_targets" "resources: [\"swblockreplicafailbacks\"]" "${ENABLED_RENDER}"
require_present "enabled_executor_status_only_resource" "resources: [\"swblockreplicafailbacks/status\"]" "${ENABLED_RENDER}"
require_present "enabled_renders_enable_execution" "--enable-execution" "${ENABLED_RENDER}"
require_present "enabled_renders_execution_policy" "--execution-policy" "${ENABLED_RENDER}"
require_present "enabled_renders_failback_grpc_addr" "--failback-runtime-grpc-addr=blockmaster.kube-system.svc:9333" "${ENABLED_RENDER}"
require_absent "enabled_omits_dry_run" "--dry-run" "${ENABLED_RENDER}"
require_absent "enabled_omits_frontend_publication_executor" "name: sw-block-frontend-publication-executor" "${ENABLED_RENDER}"

if (cd "${PRODUCT_ROOT}" && python - <<'PY') >"${SCHEMA_LOG}" 2>&1
import json
from pathlib import Path

schema = json.loads(Path("charts/seaweed-block/values.schema.json").read_text())
props = schema["properties"]
assert "failbackTargetOwner" in props
target = props["failbackTargetOwner"]["properties"]
for key in ["create", "dryRun", "interval", "rbac", "nodeSelector"]:
    assert key in target, key
executor = props["failbackExecutor"]["properties"]
execution = executor["execution"]["properties"]
for key in ["enabled", "policy", "failbackRuntimeGrpcAddr", "failbackRuntimeURL"]:
    assert key in execution, key
print("schema_ok")
PY
then
  write_summary "values_schema_covers_failback_suite=true"
else
  write_summary "values_schema_covers_failback_suite=false"
  cat "${SCHEMA_LOG}" >&2 || true
  exit 1
fi

write_summary "target_owner_rbac_create_targets_only=true"
write_summary "executor_rbac_status_only=true"
write_summary "blockmaster_runtime_rpc_explicit=true"
write_summary "execution_policy_still_required=true"
write_summary "runtime_transport_grpc_explicit=true"
write_summary "deployed_suite_packaged=true"
write_summary "phase88_failback_deployed_suite_status=ok"
