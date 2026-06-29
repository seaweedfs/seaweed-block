# Phase 50 Finished Plan: Returned-Replica Preflight Status Schema

Status: complete.

Branch: `phase50-test-validation-hygiene`

## Summary

Phase 50 publishes the Phase 49 returned-replica executor preflight into
machine-readable status surfaces. The preflight is now available in
operator-snapshot JSON and SwBlockVolume `.status`, with schema and writer tests
covering the camelCase CRD payload.

This phase remains non-mutating. It does not execute reintegration, rebuild,
ACK eligibility, frontend publication, or failback.

## What Changed

- Added `executor_preflights[]` to `ManagedVolumeOperatorStatus`.
- Added `executorPreflights[]` to `SwBlockVolumeCRDStatus`.
- Added `SwBlockVolumeCRDExecutorPreflight` as the camelCase CRD DTO.
- Added OpenAPI schema for `status.executorPreflights[]`.
- Extended status writer, reconciler, conformance, and command tests.
- Extended the returned-replica live status/RBAC gate so server-side dry-run
  validates `executorPreflights[]` against the real CRD schema.
- Fixed a time-sensitive delete-safety test fixture so package tests remain
  deterministic.

## Closed Acceptance

```text
operator-snapshot carries executor_preflights[]
SwBlockVolume.status carries executorPreflights[]
CRD schema includes actionType/decision/reason/mode/executor/frontier fields
CRD schema rejects snake_case field drift
writer emits camelCase status payload
reconciler publishes preflight from ManagedVolume projection
live gate valid patch includes executorPreflights
live gate rejects snake_case/mode drift for executorPreflights
mutation_allowed remains false
```

## Validation

```text
go test -count=1 ./cmd/sw-block
go test -count=1 ./core/ops
swblock validate testops/scenarios/returned-replica-status-schema-rbac-chain.yaml
swblock run testops/scenarios/returned-replica-status-schema-rbac-chain.yaml
```

All passed. Live run `20260620-224255-2225` completed 16/16 actions.

Key live summary:

```text
operator_status_patch_status_allowed=yes
operator_status_main_patch_allowed=no
operator_status_finalizers_patch_allowed=no
valid_returned_replica_status_server_dry_run=true
valid_executor_preflight_status_server_dry_run=true
executor_preflight_forbidden_mutation_class_projected=true
snake_case_action_rejected=true
unsupported_action_mode_rejected=true
server_dry_run_status_mutated=false
phase47_returned_replica_status_schema_rbac_status=ok
```

## Non-Claims

- No ACK eligibility mutation.
- No frontend publication.
- No rebuild traffic.
- No automatic failback.
- No lifecycle-owner or operator-status RBAC expansion.
- No release-image claim.

## Next Step

A future phase may add a live CRD/status schema gate for
`executorPreflights[]`, then separately design the actual mutating executor with
admission/RBAC, exact mutation set, terminal evidence, failure handling, and
multi-volume isolation.
