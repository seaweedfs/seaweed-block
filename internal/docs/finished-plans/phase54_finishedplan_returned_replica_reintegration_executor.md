# Phase 54 Finished Plan: Returned-Replica Reintegration Executor Milestone

Status: complete.

Branch: `phase54-returned-replica-reintegration-executor`

## Summary

Phase 54 turns returned-replica reintegration from a non-mutating status model
into the first bounded executor capability.

The only productized mutation is narrow:

```text
record returned-replica ACK eligibility on SwBlockReplicaEligibility.status
```

The executor still does not publish a frontend, start rebuild/catch-up traffic,
change primary authority, perform failback, mutate broad `SwBlockVolume.status`,
or touch another volume.

## What Changed

- Added the `ack_eligibility` execution policy gate to `sw-block ops
  authority-executor`.
- Added `SwBlockReplicaEligibility` as the narrow ACK eligibility target CRD.
- Added Kubernetes status writer support for
  `SwBlockReplicaEligibility.status`.
- Added execution RBAC that can patch only
  `swblockreplicaeligibilities/status`.
- Connected the executor call-site to terminal returned-replica evidence:
  - ACK eligibility known,
  - ACK ineligible before execution,
  - frontend still fenced,
  - primary unchanged,
  - durable frontier covered,
  - no cross-volume identity change.
- Added negative/hold handling for unsafe, stale, missing, ambiguous, and
  mismatched evidence.
- Added dedicated multi-volume isolation and live close gates.

## Closed Acceptance

```text
default executor remains disabled
unsupported mutation class fails closed
missing target holds with zero mutation
terminal evidence missing holds with zero mutation
unsafe frontend/frontier/preflight states hold with zero mutation
ambiguous or mismatched target holds with zero mutation
complete terminal evidence writes exactly one target status
multi-volume reconcile does not contaminate other volumes
live returned-replica path writes ACK eligibility only after real evidence
executor cannot patch SwBlockVolume status/finalizers/main object
executor cannot create Events or mutate pods/PVCs/storageclasses
cleanup remains zero-residue
```

## Validation

Local checks:

```text
go test ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
swblock validate testops/scenarios/authority-executor-target-rbac-chain.yaml
swblock validate testops/scenarios/authority-executor-callsite-chain.yaml
swblock validate testops/scenarios/authority-executor-negative-chain.yaml
swblock validate testops/scenarios/authority-executor-multivolume-chain.yaml
swblock validate testops/scenarios/authority-executor-live-close-chain.yaml
```

Live QA:

```text
D3 target RBAC: PASS, 14/14
D4 call-site terminal evidence: PASS, 36/36, run 20260623-110832-6b9c
D5 negative/hold matrix: PASS, 26/26, run 20260623-112339-a395
D6 multi-volume isolation: PASS, 32/32, run 20260623-113753-d07f
D7 live close gate: PASS, 34/34, run 20260623-114709-aa80
```

## Product Outcome

The returned-replica operation layer now has a full fact -> judgment -> action
-> evidence loop for the first bounded authority executor write.

The write is deliberately not a rebuild or failback. It records that a
returned replica is eligible to participate in ACK decisions after live
terminal evidence proves the old primary is fenced, the current primary is
unchanged, and the durable frontier is covered.

## Remaining Non-Claims

- No frontend publication.
- No rebuild/catch-up traffic.
- No automatic failback.
- No broad returned-replica rebuild product claim.
- No production HA/SLO claim.
- No release-image claim until the release smoke runs on published images.

## Next Step

The next milestone can choose one of two directions:

- release hardening if this executor capability is intended for a v0.5/v0.6
  beta cut,
- or the next operation owner that can reuse the same evidence/action boundary,
  such as returned-replica rebuild execution, node cleanup execution, or NVMe
  ANA parity.
