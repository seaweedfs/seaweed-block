#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase91-failback-target-activation-policy-gate}"
SUMMARY="${ARTIFACT_DIR}/phase91-failback-target-activation-policy-summary.txt"
CORE_TEST_LOG="${ARTIFACT_DIR}/go-test-core-ops.log"
CMD_TEST_LOG="${ARTIFACT_DIR}/go-test-cmd-sw-block.log"
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

write_summary "phase91_failback_target_activation_policy_status=running"
write_summary "phase91_scope=explicit_failback_target_activation_policy"
write_summary "failback_runtime_call_attempted=false"
write_summary "frontend_publication_after_failback_claimed=false"
write_summary "storage_mutation_allowed=false"

if (cd "${PRODUCT_ROOT}" && go test ./core/ops -run "TestFailbackTargetOwner|TestPhase75FailbackTargetOwnerPackagingIsNarrow" -count=1 -v) >"${CORE_TEST_LOG}" 2>&1; then
  write_summary "go_test_core_ops_failback_target_activation=pass"
else
  write_summary "go_test_core_ops_failback_target_activation=fail"
  cat "${CORE_TEST_LOG}" >&2 || true
  exit 1
fi

if (cd "${PRODUCT_ROOT}" && go test ./cmd/sw-block -run "TestOpsFailbackTargetOwner" -count=1 -v) >"${CMD_TEST_LOG}" 2>&1; then
  write_summary "go_test_cmd_failback_target_activation=pass"
else
  write_summary "go_test_cmd_failback_target_activation=fail"
  cat "${CMD_TEST_LOG}" >&2 || true
  exit 1
fi

(cd "${PRODUCT_ROOT}" && helm template sw-block charts/seaweed-block --namespace kube-system \
  --set failbackTargetOwner.create=true) >"${DEFAULT_RENDER}"
require_absent "default_omits_activate_targets" "--activate-targets" "${DEFAULT_RENDER}"
require_absent "default_omits_activation_policy" "--activation-policy" "${DEFAULT_RENDER}"
require_absent "default_omits_runtime_endpoint" "--runtime-endpoint" "${DEFAULT_RENDER}"

(cd "${PRODUCT_ROOT}" && helm template sw-block charts/seaweed-block --namespace kube-system \
  --set failbackTargetOwner.create=true \
  --set failbackTargetOwner.dryRun=false \
  --set failbackTargetOwner.activation.enabled=true \
  --set failbackTargetOwner.activation.policy=true \
  --set failbackTargetOwner.activation.runtimeEndpoint=blockmaster.kube-system.svc:9333) >"${ENABLED_RENDER}"
require_text "enabled_renders_activate_targets" "--activate-targets" "${ENABLED_RENDER}"
require_text "enabled_renders_activation_policy" "--activation-policy" "${ENABLED_RENDER}"
require_text "enabled_renders_runtime_endpoint" "--runtime-endpoint=blockmaster.kube-system.svc:9333" "${ENABLED_RENDER}"

require_text "activation_requires_policy" "failback target activation is disabled by product policy" "${PRODUCT_ROOT}/core/ops/failback_target_owner_controller.go"
require_text "activation_requires_runtime_endpoint" "failback target activation requires runtime endpoint" "${PRODUCT_ROOT}/core/ops/failback_target_owner_controller.go"
require_text "activated_target_decision_enabled" "created.Spec.FailbackDecision != AuthorityExecutorFailbackDecisionEnabled" "${PRODUCT_ROOT}/core/ops/failback_target_owner_controller_test.go"
require_text "activated_target_runtime_endpoint" "created.Spec.RuntimeEndpoint != \"blockmaster.kube-system.svc:9333\"" "${PRODUCT_ROOT}/core/ops/failback_target_owner_controller_test.go"

write_summary "activation_default_off=true"
write_summary "activation_policy_required=true"
write_summary "activation_runtime_endpoint_required=true"
write_summary "activated_target_failback_decision=enabled"
write_summary "activated_target_failback_mutation_allowed=true"
write_summary "phase91_failback_target_activation_policy_status=ok"
