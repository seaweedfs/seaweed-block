# Phase 91 QA Sign-off: Failback Target Activation Policy

Status: pending QA.

## Scope

Validate that failback target activation is default-off, policy-gated, and
runtime-endpoint-gated. Validate that activation only changes target spec and
does not call the runtime.

## Gate

Run:

```text
bash scripts/run-phase91-failback-target-activation-policy-gate.sh .
swblock validate testops/scenarios/failback-target-activation-policy-chain.yaml
```

## Required Evidence

The summary must contain:

```text
phase91_failback_target_activation_policy_status=ok
go_test_core_ops_failback_target_activation=pass
go_test_cmd_failback_target_activation=pass
default_omits_activate_targets=true
default_omits_activation_policy=true
default_omits_runtime_endpoint=true
enabled_renders_activate_targets=true
enabled_renders_activation_policy=true
enabled_renders_runtime_endpoint=true
activation_policy_required=true
activation_runtime_endpoint_required=true
activated_target_failback_decision=enabled
activated_target_failback_mutation_allowed=true
failback_runtime_call_attempted=false
frontend_publication_after_failback_claimed=false
storage_mutation_allowed=false
```

## Pass Criteria

- Default chart render omits every activation flag.
- Explicit chart values render all activation flags.
- Reconciler rejects activation without policy.
- Reconciler rejects activation without runtime endpoint.
- Activated target keeps expected-current authority fields from Phase 90.
- Target owner still does not call the failback runtime.

## Non-Claims

Phase 91 does not prove executor/runtime failback. It only prepares an explicit
enabled target for a later executor handoff gate.
