# Current Plan: Phase 53 Returned-Replica Executor Skeleton

Status: complete. Finished record:
`internal/docs/finished-plans/phase53_finishedplan_returned_replica_executor_skeleton.md`.

Branch: `phase53-returned-replica-executor-skeleton`

## Goal

Add the first product-owned returned-replica executor shape without enabling any
returned-replica mutation.

Phase 52 published the non-mutating executor contract in
`SwBlockVolume.status.executorContracts[]`. Phase 53 adds the future executor
process boundary that consumes that contract, proves its RBAC is read-only, and
fails closed if any contract claims execution is already enabled.

## Scope

In scope:

- Add `sw-block ops authority-executor`.
- Add a small `AuthorityExecutorReconciler`.
- Package the executor behind `authorityExecutor.create=false`.
- Grant only `get/list/watch` on `swblockvolumes`.
- Observe returned-replica executor contracts and report counters.
- Reject `--enable-execution`.
- Fail closed if a contract has `executionEnabled=true` or
  `mutationAllowed=true`.

Out of scope:

- No ACK eligibility mutation.
- No frontend publication.
- No rebuild traffic.
- No failback.
- No Events.
- No status patches.
- No finalizer/spec/storage/workload mutation.

## Success Criteria

1. CLI:
   - `sw-block ops authority-executor --namespace <ns>` exits 0 in disabled
     mode.
   - output includes `authority_executor=disabled`.
   - output includes `mutation_allowed=false`.
   - output includes zero mutation attempts.
   - `--enable-execution` is rejected.

2. Reconciler:
   - counts disabled returned-replica executor contracts.
   - counts blocked returned-replica executor contracts.
   - counts terminal-evidence requirements.
   - returns an error on execution-enabled or mutating contracts.

3. Helm/RBAC:
   - default `authorityExecutor.create=false`.
   - optional Deployment uses `sw-block ops authority-executor`.
   - RBAC has only `get/list/watch` on `swblockvolumes`.
   - RBAC has no patch/update/create/delete/status/finalizer/Event verbs.

4. Non-claims stay true:
   - no executor mutation,
   - no returned-replica rebuild,
   - no failback,
   - no storage traffic.

## Validation

Run before close:

```text
go test -count=1 ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
helm template sw-block charts/seaweed-block --namespace kube-system --set authorityExecutor.create=true
bash -n scripts/run-phase53-authority-executor-rbac-gate.sh
swblock validate testops/scenarios/authority-executor-rbac-chain.yaml
swblock run testops/scenarios/authority-executor-rbac-chain.yaml
```

Optional QA follow-up:

```text
kubectl auth can-i get/list/watch swblockvolumes --as <authority-executor-sa>
kubectl auth can-i patch swblockvolumes --as <authority-executor-sa> # no
kubectl auth can-i patch swblockvolumes/status --as <authority-executor-sa> # no
kubectl auth can-i create events --as <authority-executor-sa> # no
```

## Expected Next Phase

Phase 54 may design the first bounded ACK-eligibility mutation only if Phase 53
proves the executor boundary is present, disabled by default, and read-only.
