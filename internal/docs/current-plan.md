# Current Plan: Phase 41 - Lifecycle Owner Foundation

Status: dev-complete, 90% complete, awaiting QA/review. Started on
2026-06-14.

Branch: `phase41-lifecycle-owner-foundation`

Previous phase: Phase 40 is closed in
`internal/docs/finished-plans/phase40_finishedplan_operator_production_hardening.md`.

## Product Goal

Turn the v0.4 status/events-only operator foundation into a safe lifecycle
owner foundation without breaking the read-only operator-status boundary.

Phase 40 made the control plane useful as an observation surface: CRD status,
Events, node evidence, cleanup visibility, delete-safety status, action
decisions, install drift, and API conformance. Phase 39 proved why the next
step cannot be a local patch: CRD finalizer mutation requires main-object
`patch swblockvolumes`, so it must belong to a clearly scoped lifecycle owner,
not the status-only observer.

The hard exit statement:

```text
Seaweed Block has an explicit lifecycle-owner contract and a gated first
mutation path. Status-only observation remains status/events-only. Any lifecycle
mutation is owned by a separate component, guarded by real Kubernetes API/RBAC
tests, and explained through CRD status, Events, reports, and QA evidence.
```

## Why This Comes Before NVMe / Rebuild / Backup

NVMe ANA parity, returned-replica rebuild/failback, and backup/restore all add
state transitions. If the product cannot clearly answer who owns a lifecycle
mutation, what facts authorize it, how it is audited, and how it is blocked,
those features will add more status debt.

Phase 41 is not another unbounded ops-fix loop. It has one purpose: define and
prove the mutation boundary that later features can reuse.

## Scope Contract

| In | Out |
|---|---|
| lifecycle-owner responsibility contract | NVMe ANA parity |
| real CRD/RBAC/envtest gate before mutation | rebuild/failback implementation |
| finalizer strategy and ownership decision | backup/snapshot/restore |
| first bounded lifecycle mutation behind an explicit gate | automatic cleanup of host/storage residue |
| delete-safety preconditions and user-visible block reasons | promotion/fencing executor |
| multi-volume isolation for lifecycle status/mutation | broad production lifecycle claims |

Allowed implementation rule:

```text
Phase 41 may introduce a separate lifecycle-owner component or mode only after
the contract and API/RBAC gates prove the boundary.

Phase 41 must not broaden operator-status into a mutating controller.
Phase 41 must not execute cleanup, rebuild, failback, backup, restore, or
promotion. If finalizer mutation lands, it is the only allowed lifecycle
mutation and must be release-gated.
```

## D1: Lifecycle Owner Contract

Goal: make ownership explicit before code changes.

Status: dev-complete, awaiting review.

Acceptance:

```text
[x] document the three roles: observer/status writer, lifecycle owner, executor
[x] define which component may patch CR metadata/finalizers
[x] define which component may patch CR status
[x] define which component may create Events
[x] define which component may execute storage/workload/host mutation
[x] define evidence, preconditions, and audit fields for each allowed action
[x] explain why operator-status remains status/events-only
```

Verification:

```text
docs review: current-plan, roadmap, lifecycle-owner control contract
no code changes that broaden RBAC before this contract is accepted
```

## D2: Real Kubernetes API / RBAC Harness

Goal: stop live-only schema/RBAC bugs before QA.

Status: dev-complete for the non-mutating Phase 41 path. The schema-aware
boundary gate is in place; a real live-apiserver/envtest path remains a required
carry-forward before any finalizer mutation ships.

Acceptance:

```text
[x] harness loads real SwBlockCluster/SwBlockVolume CRD status schemas
[x] harness uses equivalent ServiceAccount/RBAC tokens for observer and
      lifecycle owner
[x] status-only observer can patch status and create Events only
[x] lifecycle owner can perform only the explicitly approved finalizer-shaped
      main-object patch
[x] wrong status casing, enum drift, endpoint drift, RBAC broadening, spec
      patch, unrelated metadata patch, and fake `/finalizers` endpoint fail
[x] record that a live-apiserver/envtest version is still required before any
      finalizer mutation ships
```

Verification:

```text
go test ./core/ops ./cmd/sw-block
scripts/run-phase41-lifecycle-owner-api-boundary.ps1
scripts/run-phase41-lifecycle-owner-api-boundary.sh
testops/scenarios/lifecycle-owner-api-boundary-chain.yaml
future envtest/live-apiserver target with real CRD schemas and RBAC
negative tests for forbidden spec/storage/workload mutation
```

## D3: Delete-Safety To Lifecycle Preconditions

Goal: convert Phase 39 delete-safety status into executable preconditions
without executing cleanup.

Status: dev-complete, awaiting QA/review.

Acceptance:

```text
[x] blocked delete evidence rejects lifecycle release with stable reason
[x] clean delete evidence permits release intent
[x] missing evidence produces decision=unknown and release_allowed=false
[x] stale evidence produces decision=unknown from an explicit freshness fact
[x] cleanup-required evidence suggests scripted verification only
[x] CRD/report/dashboard/operator-snapshot agree on decision and evidence
[x] multi-volume evidence stays isolated per volume
```

Verification:

```text
from-bundle scenarios: clean, blocked, stale, missing, multi-volume
operator-status remains status/events-only
```

## D4: Finalizer Strategy Gate

Goal: decide whether Phase 41 lands a finalizer mutation or stops at a
documented design block.

Status: dev-complete, awaiting review. Phase 41 defers finalizer mutation and
continues with a dry-run/status lifecycle-owner path.

Acceptance:

```text
[x] explicitly choose one strategy:
    - lifecycle-owner owns finalizer mutation with main-object patch RBAC, or
    - finalizer mutation is deferred to a future CSI/lifecycle controller
[x] if lifecycle-owner owns it, add admission/code/RBAC proof that spec and
      storage mutation stay forbidden
[x] if deferred, document the exact future owner and release non-claim
[x] user-facing impact is documented for PVC/CR deletion
```

Verification:

```text
design review
RBAC can-i matrix
envtest/live-apiserver proof for the chosen strategy
```

## D5: First Bounded Mutation Prototype

Goal: if D4 approves mutation, implement only finalizer add/remove. If D4
defers mutation, implement the dry-run lifecycle-owner status path instead.

Status: dev-complete, awaiting QA/review. D4 deferred mutation, so D5 shipped
the dry-run lifecycle-owner status path.

Acceptance:

```text
[x] disabled by default or explicitly beta-gated
[x] no storage/workload/host cleanup is executed
[x] clean volume: finalizer release is allowed and audited
[x] blocked volume: finalizer release is rejected and audited
[x] repeated reconciles are idempotent
[x] Events are bounded and stable
[x] operator-status RBAC remains unchanged
```

Verification:

```text
live lab or envtest finalizer lifecycle gate
can-i matrix
no pods/PVC/PV/StorageClass/host mutation
cleanup verifier remains zero-residue after test
```

## D6: Multi-Volume And Failure Isolation Gate

Goal: prove the lifecycle-owner path does not mix volume identities or poison
other volumes.

Status: dev-complete for the status/dry-run path. A live finalizer mutation gate
is not applicable because D4 deferred mutation.

Acceptance:

```text
[x] one blocked volume does not block status or lifecycle decisions for others
[x] one releasable volume does not release another volume
[x] stale evidence on one volume stays Unknown only for that volume
[x] Events, allowedActions, deleteSafety, and finalizer state are per-volume
[x] cleanup verification remains clean
```

Verification:

```text
multi-volume from-bundle gate
live lab gate if finalizer mutation is enabled
CRD/report/dashboard/operator-snapshot agreement
```

## D7: Close Gate

Goal: close Phase 41 only if the lifecycle-owner boundary is real enough to
build future features on.

Acceptance:

```text
[x] D1-D6 pass or a documented D4 design block is accepted
[x] operator-status remains status/events-only
[x] lifecycle-owner permissions are minimal and tested for the non-mutating
      status/dry-run path
[x] first mutation path is either gated and QA-proven or explicitly deferred
[x] user-facing docs state what deletion/finalizer behavior is and is not
[x] roadmap is updated for Phase 42
[ ] QA sign-off is received
```

Verification:

```text
finished plan
QA sign-off
roadmap update
git diff --check
```

## Current Progress

- 0%: Phase 41 opened from the v0.4 beta operator-status release baseline. The
  phase is intentionally larger than a single finalizer fix: it must establish
  a lifecycle-owner boundary, a real Kubernetes API/RBAC test harness, and the
  first bounded mutation decision before adding storage lifecycle features.
- 14%: D1 dev-complete. Added
  `internal/docs/ref/lifecycle-owner-control-contract.md`, defining the
  observer/status writer, lifecycle owner, and executor roles; preserving the
  released operator-status status/events-only boundary; and making finalizer
  mutation an explicit lifecycle-owner strategy decision rather than an RBAC
  patch to the observer.
- 24%: D2 schema-aware boundary gate dev-complete. Added
  `TestPhase41D2LifecycleOwnerFinalizerBoundary`, Phase 41 D2 wrapper scripts,
  and a TestOps scenario. The gate proves the observer cannot patch the main
  `SwBlockVolume`, the future lifecycle-owner identity can only make a
  finalizer-shaped main-object patch, and spec/unrelated metadata/fake
  `/finalizers` endpoint mutation is rejected. A true live-apiserver/envtest
  execution path remains open before D2 can be called fully closed.
- 42%: D3 dev-complete. Delete-safety now uses
  `decision=unknown` for missing cleanup evidence instead of treating missing
  evidence as confirmed blocked residue. From-bundle tests prove blocked
  residue rejects release, clean evidence allows release, missing evidence keeps
  data-plane readiness separate from lifecycle release, and the surfaces carry
  the same delete-safety decision. Cleanup summaries now carry
  `cleanup_observed_at`, and stale cleanup evidence produces
  `decision=unknown reason=cleanup_evidence_stale` instead of allowing
  finalizer release.
- 56%: D4 strategy dev-complete. Added
  `internal/docs/ref/lifecycle-owner-finalizer-strategy.md`. Phase 41 will not
  ship finalizer add/remove because the future lifecycle owner still lacks a
  real API/admission proof for main-object patch confinement. The release
  non-claim remains: delete-safety is status-only guidance, not Kubernetes
  deletion protection.
- 70%: D5 dev-complete. Delete-safety decisions now project a dry-run
  `safe_k8s.release_swblockvolume_finalizer` lifecycle-owner action across
  report, operator-snapshot, and CRD allowedActions. Clean evidence produces
  `decision=allowed`, residue produces `decision=rejected`, and missing/stale
  evidence produces `decision=unknown`; all carry `mutationAllowed=false`.
- 84%: D6 dev-complete for the non-mutating Phase 41 path. The multi-volume
  operator-status regression now covers blocked, ready, releasable, and stale
  delete-safety decisions in one reconcile. Each volume keeps its own
  `deleteSafety` and dry-run lifecycle-owner action decision, with no finalizer
  mutation Events and no cross-volume contamination.
- 90%: D7 local close criteria are satisfied for the non-mutating slice. Phase
  41 intentionally closes as lifecycle-owner boundary + dry-run/status decision
  work, not as finalizer execution. The live-apiserver/envtest admission/RBAC
  proof remains a required carry-forward before any future lifecycle-owner
  component can receive main-object `patch swblockvolumes`.

## Prerequisites / Risks

- `tp01` was reported `NotReady`/unreachable during recent QA. Restore before
  any multi-node live gate.
- Do not broaden operator-status RBAC. It is the released status/events-only
  observer and must stay that way.
- A finalizer controller for CRDs needs main-object patch permission. Treat this
  as a lifecycle-owner design decision, not a small RBAC tweak.
- Mock-only tests are insufficient for shipping future CRD/RBAC writers. Phase
  41 uses schema-aware equivalent RBAC for the non-mutating boundary; a real
  live-apiserver/envtest proof is required before finalizer mutation.
- If D4 cannot prove a safe mutation boundary, Phase 41 should close with a
  documented defer decision rather than shipping a weak mutating controller.

## Next Step

Send Phase 41 to QA using
`internal/docs/qa-assignments/phase41-lifecycle-owner-foundation-qa.md`. If QA
passes, write the finished plan. If QA finds a live boundary gap, fix that
before closing.
