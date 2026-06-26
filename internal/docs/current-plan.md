# Current Plan: Phase 92 Failback Target -> Executor Handoff

Status: complete.

## Goal

Phase 92 proves the first integrated handoff between the failback target owner
and failback executor:

```text
SwBlockVolume.status authority facts
-> target owner creates enabled SwBlockReplicaFailback target
-> executor consumes the target
-> runtime request receives expected-current replica and epoch
-> executor writes terminal failed_back status from runtime evidence
```

This is still a local/fake-runtime gate. It does not claim a live Kubernetes
blockmaster runtime smoke or frontend publication.

## Deliverables

### D1: Integrated Handoff Test

Added `TestFailbackTargetOwnerExecutorHandoffUsesExpectedCurrentAuthority`.

It proves:

```text
target owner creates one enabled target
expectedCurrentReplicaID=r2 reaches the runtime request
expectedCurrentEpoch=7 reaches the runtime request
executor writes failed_back only after valid terminal runtime evidence
frontend publication remains false
storage mutation remains false
```

### D2: Gate

Added:

```text
scripts/run-phase92-failback-target-executor-handoff-gate.sh
testops/scenarios/failback-target-executor-handoff-chain.yaml
```

## Verification

```text
bash scripts/run-phase92-failback-target-executor-handoff-gate.sh .
swblock validate testops/scenarios/failback-target-executor-handoff-chain.yaml
```

Expected terminal evidence:

```text
phase92_failback_target_executor_handoff_status=ok
target_owner_created_enabled_target=true
executor_consumed_target=true
runtime_request_expected_current_replica=r2
runtime_request_expected_current_epoch=7
executor_terminal_state=failed_back
frontend_publication_after_failback_claimed=false
storage_mutation_allowed=false
```

## Next

Phase 93 should choose the next proof boundary:

```text
live blockmaster gRPC runtime smoke using the deployed suite, or
multi-volume failback handoff isolation before live runtime
```

Do not combine frontend publication into that step.
