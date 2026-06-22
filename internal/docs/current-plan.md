# Current Plan: Phase 52 Returned-Replica Executor Contract

Status: complete. Finished record:
`internal/docs/finished-plans/phase52_finishedplan_returned_replica_executor_contract.md`.

Branch: `phase52-returned-replica-executor-contract`

Decision note: Phase 52 still does not execute returned-replica reintegration,
rebuild, failback, frontend publication, or storage traffic. It adds the
machine-readable contract for the future executor boundary so the product can
say exactly what would be allowed, what remains forbidden, and what terminal
evidence must exist before any later mutating executor is enabled.

## Why This Phase Exists

Phases 46-51 made returned-replica state visible and progressively safer:

- Phase 46 exposed returned-replica facts and dry-run actions.
- Phase 47 admitted the dry-run reintegration action only with fenced/frontier
  evidence.
- Phase 48 connected the live iSCSI returned-replica scenario to the same
  managed-volume evidence path.
- Phase 49 added a typed executor preflight.
- Phase 50 published that preflight into SwBlockVolume status.
- Phase 51 required explicit ACK eligibility evidence rather than treating a
  default false as proof.

The remaining gap before a real executor is not code that mutates state. The
gap is a contract: which mutation class would a future executor own, which
mutation classes are still forbidden, and which terminal evidence proves it did
not accidentally publish an old frontend, change primary authority, or mix
volume identity.

## Scope

In scope:

- Add a returned-replica executor contract derived from the existing preflight.
- Publish the contract in:
  - report summary
  - explain text
  - operator-snapshot JSON
  - dashboard JSON
  - SwBlockVolume `.status.executorContracts[]`
- Keep `executionEnabled=false` and `mutationAllowed=false`.
- Name the only future allowed mutation class as `ack_eligibility`.
- Keep these mutation classes explicitly forbidden:
  - `frontend_publication`
  - `rebuild_traffic`
  - `failback`
- Require terminal evidence:
  - `ack_eligibility_known`
  - `ack_eligible_true`
  - `frontend_fenced_after_execution`
  - `primary_unchanged`
  - `durable_frontier_covered`
  - `no_cross_volume_identity_change`
- Extend the live returned-replica CRD/RBAC gate so the contract validates
  against the real Kubernetes status subresource schema.

Out of scope:

- No returned-replica rebuild execution.
- No automatic reintegration or failback.
- No frontend publication.
- No authority mutation.
- No storage write or rebuild traffic.
- No RBAC expansion beyond existing status/events.

## Success Criteria

1. A ready preflight creates exactly one executor contract with:
   - `decision=disabled`
   - `reason=executor_policy_disabled`
   - `execution_enabled=false`
   - `mutation_allowed=false`
   - `allowed_mutation=ack_eligibility`
   - terminal evidence listed above

2. A held preflight creates a blocked executor contract with:
   - `decision=blocked`
   - `reason=preflight_not_ready`
   - copied preflight reason
   - no allowed mutation class
   - copied forbidden mutation class

3. SwBlockVolume CRD status uses camelCase:
   - `executorContracts`
   - `executionEnabled`
   - `allowedMutationClass`
   - `terminalEvidenceRequired`

4. The live server-dry-run gate accepts the valid `executorContracts[]`
   payload and rejects snake-case drift.

5. Existing non-claims stay true:
   - no executor command
   - no lifecycle-owner involvement
   - no finalizer/spec/storage/workload mutation
   - no broad rebuild/failback claim

## Validation

Run before close:

```text
go test -count=1 ./core/ops ./cmd/sw-block
bash -n scripts/run-phase47-returned-replica-status-schema-rbac-gate.sh
swblock validate testops/scenarios/returned-replica-status-schema-rbac-chain.yaml
swblock run testops/scenarios/returned-replica-status-schema-rbac-chain.yaml
```

The live run must show:

```text
valid_executor_contract_status_server_dry_run=true
executor_contract_execution_disabled_projected=true
executor_contract_terminal_evidence_projected=true
```

## Expected Close

Close Phase 52 only if the executor contract is visible and schema-valid while
execution remains disabled. The next phase may decide whether to start an
executor implementation, but it must reuse this contract rather than inventing
a separate mutation path.
