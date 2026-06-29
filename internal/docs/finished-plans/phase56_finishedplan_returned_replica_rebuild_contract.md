# Phase 56 Finished Plan: Returned Replica Rebuild/Catch-up Contract

Status: complete.

Branch: `phase54-returned-replica-reintegration-executor`

## Goal

Move beyond Phase 54's ACK-eligibility executor by defining the next returned
replica boundary: a fenced returned replica whose durable frontier is behind the
required frontier must surface as a rebuild/catch-up candidate, without
executing rebuild traffic yet.

## What Changed

Phase 56 split the returned-replica executor model into two paths:

- `authority.reintegrate_returned_replica`: the existing Phase 54 ACK
  eligibility path, used only when the returned replica is fenced and its
  durable frontier covers the required frontier.
- `authority.rebuild_returned_replica`: the new rebuild/catch-up contract path,
  used when the returned replica is fenced but its durable frontier is behind.

The rebuild contract is explicit but disabled:

```text
execution_enabled=false
mutation_allowed=false
allowedMutationClass=["rebuild_traffic"]
forbiddenMutationClass=["ack_eligibility","frontend_publication","failback"]
```

Terminal evidence required for a future rebuild executor:

```text
frontend_fenced_before_rebuild
primary_unchanged
durable_frontier_caught_up
no_frontend_publication
no_cross_volume_identity_change
```

## Safety Boundary

No data movement was implemented.

No frontend publication, failback, ACK mutation, or authority change was added.
The authority executor ignores disabled rebuild contracts and fails closed if
any unsupported/non-ACK contract is incorrectly marked executable or mutating.

## Verification

Local:

```text
go test ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
swblock validate testops/scenarios/returned-replica-rebuild-contract-chain.yaml
```

QA:

```text
returned-replica-rebuild-contract-chain
20260623-144531-00ee
14/14 PASS
```

Terminal evidence:

```text
phase56_returned_replica_rebuild_contract_status=ok
summary_rebuild_preflight_ready=1
summary_rebuild_contract_disabled=1
summary_rebuild_action_disabled=1
explain_rebuild_contract_disabled=1
operator_snapshot_rebuild_contract=ok
dashboard_rebuild_contract=ok
```

Sign-off:

```text
internal/docs/qa-assignments/phase56-returned-replica-rebuild-contract-qa-signoff.md
```

## Next

Phase 57 should decide whether to start the actual rebuild executor skeleton or
first add a stronger rebuild data-plane design gate. The minimum next feature
must include real evidence for catch-up progress and terminal completion before
any frontend publication or failback can be considered.
