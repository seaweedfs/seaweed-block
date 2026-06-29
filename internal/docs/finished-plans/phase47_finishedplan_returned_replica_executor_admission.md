# Phase 47 Finished Plan: Returned-Replica Executor Admission

Status: complete; QA PASS.

Branch: `phase47-returned-replica-executor-gate`

## Goal

Move returned-replica reintegration from a blanket rejected action to an
evidence-gated dry-run admission decision.

Phase 47 does **not** execute reintegration. It only answers whether the product
has enough evidence to show:

```text
authority.reintegrate_returned_replica decision=allowed mode=dry_run
```

## What Landed

- `authority.reintegrate_returned_replica` changed from
  `policy_disabled` to `dry_run` policy.
- The action remains:
  - `mode=dry_run`,
  - `mutation_allowed=false`,
  - `side_effect=authority_mutating`,
  - `owner_executor=authority_recovery_executor`.
- Required facts are now returned-replica-specific:
  - `authority.primary_replica`,
  - `returned_replica.frontend_fenced`,
  - `returned_replica.required_frontier_covered`.
- `VolumeEvidence` now carries:
  - `required_frontier_known`,
  - `required_frontier_lsn`.
- Product surfaces now show `decision=allowed` only when required frontier
  evidence is present and covered.
- Missing/behind frontier and frontend-ready returned replicas still reject.
- Schema/RBAC conformance includes the new returned-replica action payload.

## QA Evidence

| Gate | Evidence | Result |
|---|---|---|
| D1 dry-run admission | `go test -count=1 ./core/ops` | PASS |
| D2 schema/RBAC conformance | `TestPhase40D1KubernetesStatusClientConformsToCRDSchemaAndRBAC` | PASS |
| D2 live API status dry-run | `20260620-101008-dbca` | PASS, 12/12 |
| D3 product surfaces | `TestOpsReturnedReplicaFromBundleSurfacesAcrossReportExplainDashboard` | PASS |
| D4 component live gate | `20260619-155251-ac1e` | PASS, 16/16 |
| D4 iSCSI live gate | `20260619-155300-ba6d` | PASS, 57/57 |

## Product Claim After Phase 47

Seaweed Block can claim:

```text
When a returned replica is observed as non-primary/frontend-fenced and its
durable frontier covers the required frontier, product surfaces may show
authority.reintegrate_returned_replica as an allowed dry-run action.
```

## Non-Claims

Seaweed Block must still not claim:

- automatic failback,
- automatic rebuild,
- data-plane catch-up execution from the operator/action model,
- ACK eligibility mutation,
- frontend promotion,
- production HA/SLO,
- release-image validation.

## Known Boundary

The live iSCSI returned-replica scenario proves the storage safety path but does
not yet emit a managed-volume report bundle with required-frontier evidence.
The allowed action decision is therefore validated through the product-surface
bundle test, not directly from the live iSCSI chain.

Before any future mutating executor phase, add a live scenario artifact that
emits the required-frontier managed-volume evidence from the same run. The D2
live API gate already proves the returned-replica status/action payload validates
against the real Kubernetes status subresource and that operator-status cannot
patch the main object.

## Next Possible Phase

A future executor phase may consider a bounded returned-replica reintegration
executor only if it separately proves:

- exact action owner,
- real API/RBAC/admission boundary,
- evidence freshness,
- no frontend/ACK eligibility until post-executor terminal evidence,
- multi-volume isolation,
- cleanup hygiene.
