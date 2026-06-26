# Current Plan: Phase 91 Failback Target Activation Policy

Status: complete.

## Goal

Phase 91 adds an explicit activation policy to the failback target-owner path.
By default, target-owner behavior remains unchanged: it creates disabled,
non-mutating `SwBlockReplicaFailback` targets only.

When all activation knobs are explicitly supplied, the owner may create an
enabled target:

```text
--activate-targets
--activation-policy
--runtime-endpoint <addr>
```

This phase still does not call the failback runtime. It only stamps the target
so the separate failback executor can decide whether to execute it.

## Deliverables

### D1: Target Owner Policy

`FailbackTargetOwnerReconciler` now has:

```text
ActivateTargets
ActivationPolicyEnabled
RuntimeEndpoint
```

Activation fails closed unless policy is enabled and a runtime endpoint is
present.

### D2: CLI And Helm Wiring

`sw-block ops failback-target-owner` accepts:

```text
--activate-targets
--activation-policy
--runtime-endpoint <addr>
```

Helm exposes these under:

```text
failbackTargetOwner.activation.enabled
failbackTargetOwner.activation.policy
failbackTargetOwner.activation.runtimeEndpoint
```

Defaults keep activation off and omit all activation flags.

### D3: Gate

Added:

```text
scripts/run-phase91-failback-target-activation-policy-gate.sh
testops/scenarios/failback-target-activation-policy-chain.yaml
```

The gate proves:

```text
default Helm render omits activation flags
explicit values render activation flags
policy is required
runtime endpoint is required
enabled targets retain expected-current authority facts
no failback runtime call is attempted
frontend publication remains unclaimed
```

## Verification

```text
bash scripts/run-phase91-failback-target-activation-policy-gate.sh .
swblock validate testops/scenarios/failback-target-activation-policy-chain.yaml
```

Expected terminal evidence:

```text
phase91_failback_target_activation_policy_status=ok
activation_default_off=true
activation_policy_required=true
activation_runtime_endpoint_required=true
activated_target_failback_decision=enabled
activated_target_failback_mutation_allowed=true
failback_runtime_call_attempted=false
```

## Next

Phase 92 should run the first integrated target-owner -> executor handoff smoke:

```text
target owner creates an enabled target with expected-current facts
executor consumes that target
runtime remains fake/local unless a live service gate is explicitly selected
terminal evidence decides status
frontend publication remains separate
```
