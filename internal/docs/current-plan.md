# Current Plan: Phase 41 - Lifecycle Owner Foundation

Status: open, 0% complete. Started on 2026-06-14.

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

Acceptance:

```text
[ ] harness installs real SwBlockCluster/SwBlockVolume CRDs
[ ] harness uses real or equivalent ServiceAccounts and ClusterRoles
[ ] status-only observer can patch status and create Events only
[ ] lifecycle owner can perform only the explicitly approved lifecycle mutation
[ ] wrong status casing, enum drift, endpoint drift, and RBAC broadening fail
[ ] finalizer/main-object patch behavior is tested against the real API
```

Verification:

```text
go test ./core/ops ./cmd/sw-block
new envtest/live-apiserver target with real CRD schemas and RBAC
negative tests for forbidden spec/storage/workload mutation
```

## D3: Delete-Safety To Lifecycle Preconditions

Goal: convert Phase 39 delete-safety status into executable preconditions
without executing cleanup.

Acceptance:

```text
[ ] blocked delete evidence rejects lifecycle release with stable reason
[ ] clean delete evidence permits release intent
[ ] missing or stale evidence produces Unknown/EvidenceStale, not allowed
[ ] cleanup-required evidence suggests scripted verification only
[ ] CRD/report/dashboard/operator-snapshot agree on decision and evidence
[ ] multi-volume evidence stays isolated per volume
```

Verification:

```text
from-bundle scenarios: clean, blocked, stale, missing, multi-volume
operator-status remains status/events-only
```

## D4: Finalizer Strategy Gate

Goal: decide whether Phase 41 lands a finalizer mutation or stops at a
documented design block.

Acceptance:

```text
[ ] explicitly choose one strategy:
    - lifecycle-owner owns finalizer mutation with main-object patch RBAC, or
    - finalizer mutation is deferred to a future CSI/lifecycle controller
[ ] if lifecycle-owner owns it, add admission/code/RBAC proof that spec and
      storage mutation stay forbidden
[ ] if deferred, document the exact future owner and release non-claim
[ ] user-facing impact is documented for PVC/CR deletion
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

Acceptance:

```text
[ ] disabled by default or explicitly beta-gated
[ ] no storage/workload/host cleanup is executed
[ ] clean volume: finalizer release is allowed and audited
[ ] blocked volume: finalizer release is rejected and audited
[ ] repeated reconciles are idempotent
[ ] Events are bounded and stable
[ ] operator-status RBAC remains unchanged
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

Acceptance:

```text
[ ] one blocked volume does not block status or lifecycle decisions for others
[ ] one releasable volume does not release another volume
[ ] stale evidence on one volume stays Unknown only for that volume
[ ] Events, allowedActions, deleteSafety, and finalizer state are per-volume
[ ] cleanup verification remains clean
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
[ ] D1-D6 pass or a documented D4 design block is accepted
[ ] operator-status remains status/events-only
[ ] lifecycle-owner permissions are minimal and tested against real API/RBAC
[ ] first mutation path is either gated and QA-proven or explicitly deferred
[ ] user-facing docs state what deletion/finalizer behavior is and is not
[ ] roadmap is updated for Phase 42
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

## Prerequisites / Risks

- `tp01` was reported `NotReady`/unreachable during recent QA. Restore before
  any multi-node live gate.
- Do not broaden operator-status RBAC. It is the released status/events-only
  observer and must stay that way.
- A finalizer controller for CRDs needs main-object patch permission. Treat this
  as a lifecycle-owner design decision, not a small RBAC tweak.
- Mock-only tests are insufficient for CRD/RBAC writers. D2 must use real CRD
  schemas and real or equivalent RBAC.
- If D4 cannot prove a safe mutation boundary, Phase 41 should close with a
  documented defer decision rather than shipping a weak mutating controller.

## Next Step

Start D1 by writing the lifecycle-owner contract and aligning it with the
existing standard model: fact owners publish evidence, the observer writes
status, the lifecycle owner owns CR lifecycle mutation, and executors remain
separate until explicitly introduced.
