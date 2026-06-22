# Phase 53 Finished Plan: Returned-Replica Authority Executor Skeleton

Status: complete.

Branch: `phase53-returned-replica-executor-skeleton`

## Summary

Phase 53 adds the first returned-replica authority executor process boundary
without enabling returned-replica execution.

The new executor consumes `SwBlockVolume.status.executorContracts[]`, reports
disabled/blocked contract counters, rejects `--enable-execution`, and fails
closed if any contract claims execution or mutation is enabled. It does not
patch status, finalizers, specs, workloads, Events, or storage state.

## What Changed

- Added `AuthorityExecutorReconciler`.
- Added `sw-block ops authority-executor`.
- Added disabled-by-default Helm packaging under `authorityExecutor`.
- Added read-only authority-executor RBAC:
  - `get/list/watch` on `swblockvolumes`.
- Added local tests for:
  - disabled contract observation,
  - fail-closed unsafe contract handling,
  - CLI output,
  - `--enable-execution` rejection,
  - Helm default-disabled packaging and read-only RBAC.
- Added a live TestRunner RBAC gate:
  - `testops/scenarios/authority-executor-rbac-chain.yaml`.

## Closed Acceptance

```text
authority executor command exists
default Helm value is authorityExecutor.create=false
enabled Helm template renders Deployment and read-only RBAC
RBAC allows get/list/watch swblockvolumes only
RBAC denies main patch/update/delete
RBAC denies status/finalizer patch
RBAC denies Events and workload/storage mutation
--enable-execution is rejected
reconciler reports mutation_attempts=0
reconciler fails closed on execution-enabled contracts
```

## Validation

```text
go test -count=1 ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
helm template sw-block charts/seaweed-block --namespace kube-system --set authorityExecutor.create=true
bash -n scripts/run-phase53-authority-executor-rbac-gate.sh
swblock validate testops/scenarios/authority-executor-rbac-chain.yaml
swblock run testops/scenarios/authority-executor-rbac-chain.yaml
```

All passed. Live run:

```text
run: 20260622-084926-2c2d
actions: 12/12 PASS
```

## Non-Claims

- No ACK eligibility mutation.
- No frontend publication.
- No rebuild traffic.
- No failback.
- No Events.
- No status/finalizer/spec/workload/storage mutation.
- No release-image claim.

## Next Step

Phase 54 may design the first bounded ACK-eligibility mutation. It must reuse
the Phase 52 executor contract and Phase 53 executor process/RBAC boundary, and
it must add terminal evidence and multi-volume isolation before execution is
enabled.
