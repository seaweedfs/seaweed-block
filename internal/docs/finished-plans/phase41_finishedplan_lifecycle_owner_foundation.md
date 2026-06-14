# Phase 41 Finished Plan: Lifecycle Owner Foundation

Status: closed on 2026-06-14.

Branch: `phase41-lifecycle-owner-foundation`

## Outcome

Phase 41 establishes the control-plane boundary that future storage-lifecycle
features must reuse, without breaking the released status/events-only
`operator-status` boundary and without shipping any Kubernetes lifecycle
mutation. The product now has:

```text
an explicit lifecycle-owner contract (observer / lifecycle-owner / executor),
delete-safety converted into executable allowed/rejected/unknown preconditions,
a dry-run lifecycle-owner finalizer-release action surface (never executed),
and a schema-aware API/RBAC boundary gate for the future mutating component.
```

The Phase 39 finding drove the phase: changing `metadata.finalizers` on a CRD
requires main-object `patch swblockvolumes`, which is too broad for the
status-only observer. Rather than weaken that boundary with an RBAC tweak, Phase
41 defines a separate lifecycle-owner role and **defers** finalizer mutation until
a real Kubernetes API/admission proof exists.

## Delivered

- Lifecycle-owner control contract
  (`internal/docs/ref/lifecycle-owner-control-contract.md`): three roles with
  separate, non-implicit RBAC; allowed/forbidden permissions per role; the action
  contract fields; delete-safety precondition table; required gates before any
  mutation ships.
- Finalizer strategy decision
  (`internal/docs/ref/lifecycle-owner-finalizer-strategy.md`): **Path B (defer)**.
  Phase 41 does not ship finalizer add/remove; the release non-claim
  ("delete-safety is status-only guidance, not Kubernetes deletion protection")
  is kept.
- Delete-safety preconditions: clean → `releasable/allowed`, residue →
  `blocked/rejected`, missing → `requested/unknown`, stale →
  `requested/unknown reason=cleanup_evidence_stale`; data-plane `Ready` stays
  independent of lifecycle release.
- Dry-run lifecycle-owner action surface across report, operator-snapshot, and
  CRD `allowedActions`: `safe_k8s.release_swblockvolume_finalizer`, `mode=dry_run`,
  `ownerExecutor=lifecycle_owner`, `mutationAllowed=false`, decision mirrors
  delete-safety; no finalizer mutation Events.
- Schema-aware API boundary gate
  (`scripts/run-phase41-lifecycle-owner-api-boundary.{ps1,sh}`,
  `testops/scenarios/lifecycle-owner-api-boundary-chain.yaml`,
  `TestPhase41D2LifecycleOwnerFinalizerBoundary`): observer cannot patch the main
  object; lifecycle-owner identity may issue only a finalizer-shaped patch;
  spec/unrelated-metadata/fake-`/finalizers` patches are rejected.
- Cleanup freshness: `verify-helm-cleanup.sh` now emits
  `cleanup_observed_at=<RFC3339 UTC>`; `cleanup_status` + residue counters + exit
  behavior unchanged.
- Multi-volume isolation regression (four volumes: blocked / ready / releasable /
  stale) proving per-volume `deleteSafety`, per-volume dry-run action, and no
  cross-volume contamination.

## QA Result

All gates pass — see
`internal/docs/qa-assignments/phase41-lifecycle-owner-foundation-qa-signoff.md`:

```text
G1 contract review                 PASS
G2 API boundary gate               PASS (schema-aware/equivalent-RBAC)
G3 delete-safety decision semantics PASS
G4 cleanup freshness source        PASS (cleanup_observed_at RFC3339 UTC)
G5 dry-run lifecycle-owner action  PASS
G6 multi-volume isolation          PASS
G7 live operator-status boundary   PASS (status/events-only; RBAC unchanged)
```

## Required Carry-Forward (Phase 42 entry point)

```text
A full live-apiserver/envtest lifecycle-owner RBAC/admission gate is still
required before any finalizer mutation (main-object patch swblockvolumes) can
ship. Phase 41's boundary gate is schema-aware/equivalent-RBAC, not a live
apiserver/admission proof.
```

## Non-Claims (unchanged)

Phase 41 does not implement a mutating operator, finalizer add/remove, automatic
cleanup, repair/rebuild/failback/backup/restore/promotion/fencing, NVMe ANA
parity, or production SLOs. It defines the boundary those features must use.

## Next

Phase 42: real lifecycle-owner API/admission gate — an envtest/live-apiserver
proof that a lifecycle-owner identity can perform only a constrained
`metadata.finalizers` patch (spec/unrelated-metadata/storage/workload mutation
fails against a real API server) before any actual finalizer add/remove is
enabled. NVMe / rebuild / backup remain deferred until that proof exists.
