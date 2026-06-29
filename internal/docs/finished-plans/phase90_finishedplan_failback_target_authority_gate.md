# Phase 90 Finished Plan: Failback Target Authority Gate

Status: complete.

Date: 2026-06-26.

## What Changed

The failback target owner now requires current authority facts from
`SwBlockVolume.status` before it creates a `SwBlockReplicaFailback` target.

Created targets include:

```text
spec.expectedCurrentReplicaID
spec.expectedCurrentEpoch
```

Those values come directly from:

```text
SwBlockVolume.status.primaryReplicaID
SwBlockVolume.status.authorityEpoch
```

## Why

Failback is an authority mutation. The runtime must know which replica/epoch it
expects to be current before it can safely move authority back to a returned
replica. Without this gate, the target owner could create a target that the
executor later rejects as missing execution facts.

## Boundary

Phase 90 still creates disabled targets only:

```text
failbackDecision=disabled
failbackMutationAllowed=false
```

It does not call the runtime, execute failback, publish a frontend, or mutate
storage.

## Verification

```text
scripts/run-phase90-failback-target-authority-gate.sh .
swblock validate testops/scenarios/failback-target-authority-chain.yaml
```

Expected result:

```text
phase90_failback_target_authority_status=ok
```
