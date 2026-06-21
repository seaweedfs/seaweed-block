# Current Plan: Phase 50 Returned-Replica Executor Preflight Status Schema

Status: complete; local validation PASS; live gate wrapper updated.

Working branch: `phase50-test-validation-hygiene`

Decision note: Phase 50 keeps returned-replica reintegration non-mutating. It
publishes the Phase 49 executor preflight into machine-readable
operator-snapshot and SwBlockVolume `.status` surfaces so a future executor can
consume a typed contract instead of parsing report text.

Previous product phase: Phase 49 is closed in
`internal/docs/finished-plans/phase49_finishedplan_returned_replica_executor_preflight.md`.

Finished plan:
`internal/docs/finished-plans/phase50_finishedplan_returned_replica_preflight_status_schema.md`.

## Product Goal

Make returned-replica executor preflight visible in the same machine-readable
surfaces used by the rest of the operation layer:

```text
ManagedVolume projection
-> executor_preflights[] in operator-snapshot.json
-> executorPreflights[] in SwBlockVolume.status
-> CRD OpenAPI schema validates the camelCase status payload
-> status writer tests catch casing/schema drift before live QA
```

## Why This Is Next

Phase 49 made the preflight explicit, but only report and explain text carried
it. That is useful for humans but insufficient for a future in-cluster executor.
Before any mutation is proposed, the preflight must be a typed status field with
schema and writer coverage.

## Scope Contract

| In | Out |
|---|---|
| operator-snapshot `status.executor_preflights[]` | ACK eligibility mutation |
| CRD `status.executorPreflights[]` | frontend publication mutation |
| OpenAPI schema and camelCase tests | rebuild traffic |
| status writer/reconciler tests | automatic failback |
| returned-replica bundle surface regression | lifecycle-owner RBAC expansion |

## D1: Operator-Snapshot Status

Goal: carry `ReturnedReplicaExecutorPreflight` in the snapshot contract.

Acceptance:

```text
[x] ManagedVolumeOperatorStatus includes executor_preflights[]
[x] returned-replica projection produces one ready preflight
[x] snapshot JSON remains snake_case
[x] mutation_allowed=false
```

## D2: CRD Status Schema

Goal: publish the same preflight to SwBlockVolume `.status` with Kubernetes
camelCase fields.

Acceptance:

```text
[x] SwBlockVolumeCRDStatus includes executorPreflights[]
[x] CRD OpenAPI schema includes all fields
[x] decision enum includes ready/hold
[x] mode enum is dry_run
[x] snake_case is rejected by schema tests
```

## D3: Writer / Reconciler Coverage

Goal: shift live-API class bugs left into local tests.

Acceptance:

```text
[x] KubernetesStatusClient emits camelCase executorPreflights
[x] status conformance gate includes executorPreflights
[x] OperatorStatusReconciler writes the preflight into volume status
[x] no spec/finalizer/storage mutation is added
```

## D4: Live Gate Wrapper

Goal: extend the existing returned-replica status/RBAC live gate so QA can
server-side dry-run `executorPreflights[]` against a real CRD schema and the
operator-status RBAC boundary.

Acceptance:

```text
[x] valid status patch includes executorPreflights[]
[x] server-side dry-run output contains executorPreflights
[x] forbiddenMutationClass is projected
[x] snake_case executor preflight payload is rejected
[x] unsupported executor preflight mode is rejected
```

## D5: Validation

Validation completed:

```text
[x] go test -count=1 ./cmd/sw-block
[x] go test -count=1 ./core/ops
[x] swblock validate testops/scenarios/returned-replica-status-schema-rbac-chain.yaml
```

## Next Phase Candidate

The next phase can either:

```text
1. run a live CRD/status schema gate for executorPreflights, or
2. start a still-bounded executor design gate that defines exact mutations,
   admission/RBAC, terminal evidence, and multi-volume isolation.
```

Do not enable returned-replica mutation until that executor phase exists and is
QA-validated.
