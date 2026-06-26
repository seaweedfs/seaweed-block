# Current Plan: Phase 93 Failback Handoff Isolation

Status: complete.

## Goal

Phase 93 proves multi-volume isolation for the local failback target-owner ->
executor handoff.

The key risk is cross-volume authority mix-up:

```text
volume A expectedCurrentReplicaID/epoch copied to volume B
volume A target data/control address copied to volume B
executor runtime request loses per-volume identity
```

## Deliverables

### D1: Multi-Volume Handoff Test

Added `TestFailbackTargetOwnerExecutorHandoffIsolatesMultipleVolumes`.

The test creates two independent volumes:

```text
pvc-a: returned=r1 current=r2 epoch=7  target=data-a-r1/ctrl-a-r1
pvc-b: returned=r3 current=r4 epoch=11 target=data-b-r3/ctrl-b-r3
```

It proves:

```text
target owner creates two enabled targets
executor makes two runtime requests
each request keeps the correct volumeID
each request keeps the correct expected-current replica and epoch
each request keeps the correct target data/control address
frontend publication remains false
storage mutation remains false
```

### D2: Gate

Added:

```text
scripts/run-phase93-failback-handoff-isolation-gate.sh
testops/scenarios/failback-handoff-isolation-chain.yaml
```

## Verification

```text
bash scripts/run-phase93-failback-handoff-isolation-gate.sh .
swblock validate testops/scenarios/failback-handoff-isolation-chain.yaml
```

Expected terminal evidence:

```text
phase93_failback_handoff_isolation_status=ok
multi_volume_target_create_count=2
multi_volume_runtime_request_count=2
cross_volume_expected_current_mixup=false
cross_volume_target_addr_mixup=false
```

## Next

Phase 94 can move to a live deployed blockmaster gRPC failback smoke, assuming
the lab is ready and the team wants to pay the runtime-test cost now. Keep
frontend publication as a separate later gate.
