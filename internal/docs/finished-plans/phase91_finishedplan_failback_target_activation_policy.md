# Phase 91 Finished Plan: Failback Target Activation Policy

Status: complete.

Date: 2026-06-26.

## What Changed

Failback target creation can now be explicitly activated under a policy gate.
Defaults still create disabled, non-mutating targets only.

Activation requires all three inputs:

```text
activateTargets=true
activationPolicyEnabled=true
runtimeEndpoint=<addr>
```

The CLI and Helm chart expose these as explicit opt-in knobs.

## Boundary

This phase does not execute failback. It only controls the target spec produced
by the target owner.

Non-claims:

```text
no runtime call
no frontend publication
no storage mutation
no automatic failback
```

## Verification

```text
scripts/run-phase91-failback-target-activation-policy-gate.sh .
swblock validate testops/scenarios/failback-target-activation-policy-chain.yaml
```

Expected result:

```text
phase91_failback_target_activation_policy_status=ok
```
