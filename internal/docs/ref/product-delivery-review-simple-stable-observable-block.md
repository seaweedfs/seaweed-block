# Product Delivery Review: Simple, Stable, Observable Kubernetes Block

Date: 2026-05-22

Status: internal review. This is an analysis artifact, not a new phase plan and
not an implementation ticket.

## Executive Summary

The product direction should stay narrow:

```text
simple Kubernetes PVC-backed block storage
-> stable first-volume and recovery loops
-> observable by default
-> honest non-claims
```

The recent work moved Seaweed Block from "storage mechanisms are proven in lab
gates" toward "a user can install, create a PVC, verify data, and inspect what
happened." That is the right direction. The next risk is not lack of features;
it is release-shape drift: Helm, dashboard, ManagedVolume model, release notes,
and README must describe one coherent product loop.

Recommended next milestone:

```text
v0.3-alpha release hardening:
Helm install -> first PVC -> read-only ops dashboard/report -> cleanup
```

Executable plan:

- `internal/docs/current-plan.md`

Do not start operator/CRD implementation yet. Keep CRD/operator work as a
post-Helm lifecycle track. The model and operations work is useful now, but it
should serve the user-facing install/observe loop before becoming another
internal abstraction project.

## What We Have

### Storage And Recovery Capability

Closed lab-backed capabilities include:

- dynamic PVC provisioning through CSI,
- product-owned generated `blockvolume` lifecycle,
- first-volume writer/reader verification,
- RF=3 `sync-quorum` recovery through CSI/pod recreate,
- iSCSI ALUA + Linux dm-multipath transparent mounted failover,
- Kubernetes node-loss recovery through CSI/pod recreate,
- stale-primary fencing evidence,
- product-owned control-plane event stream.

These are strong technical proofs. They should be presented as gated alpha
claims, not broad production guarantees.

### Day-1 User Loop

v0.2 alpha closed the script-based user path:

```text
activate stack
-> create PVC
-> writer verifies data
-> reader verifies persisted data
-> report evidence
-> cleanup
```

This is the first usable product loop. It should remain the reference path for
any README/tutorial language until Helm becomes equally polished.

### Helm Packaging

Helm chart structure exists:

- `charts/seaweed-block/Chart.yaml`
- chart README,
- values and schema,
- blockmaster template,
- CSI controller/node templates,
- RBAC,
- CSIDriver,
- StorageClass,
- CHAP Secret,
- cluster-spec ConfigMap.

Helm gates exist:

- `testops/scenarios/helm-first-volume-chain.yaml`
- `testops/scenarios/helm-single-node-first-volume-chain.yaml`

The chart has crossed the "basic structure" line. It still needs release
hardening before it should become the default README path.

### Operations / Observation

The operations surface has materially improved:

- `sw-block ops cluster`
- `sw-block ops volumes`
- `sw-block ops describe`
- `sw-block ops timeline`
- `sw-block ops explain`
- `sw-block ops report`
- `sw-block ops dashboard`

Evidence outputs now include:

- human-readable text,
- `cluster-evidence.json`,
- `timeline.jsonl`,
- local HTML report/dashboard,
- support-bundle replay.

This is now enough for "observable alpha" if the docs are corrected.

### ManagedVolume Model

The ManagedVolume read model now composes:

- Kubernetes PVC/PV/Pod/Node facts,
- CSI stage/publish facts,
- authority/primary/epoch facts,
- replica/durable frontier facts,
- host-path/multipath facts,
- workload checksum evidence,
- evidence refs and reason codes.

This is the right product model. It should be treated as the read-side semantic
core for operations, report, dashboard, and future operator status.

## What Is Still Incomplete

### 1. Release Shape Drift

Current docs are not fully synchronized with current capability.

Examples:

- README still says "hosted dashboard/UI" is missing, but `sw-block ops
  dashboard` now exists as a local hosted read-only dashboard.
- Roadmap still contains stale language saying Phase 22 is "next" and Phase 24
  is "active", even though Phase 22/23/24 are closed for their current scopes.
- Release notes stop at v0.2 alpha; Helm and hosted read-only dashboard are not
  captured as a coherent v0.3 boundary.

This is not cosmetic. If docs drift, PM/QA/users cannot tell what is safe to
try.

### 2. Helm Is Usable But Not Yet Release-Polished

The chart exists and gates exist, but v0.3 should not close until these are
boring:

- immutable image path documented,
- `:alpha` drift warning retained,
- single-node and multi-node expectations documented,
- `generate-helm-values` input/output explained,
- uninstall and host cleanup clearly described,
- first-volume summary points to report/dashboard,
- release note captures evidence and non-claims.

### 3. Operations Are Useful But Need One Coherent Tutorial

The operations surface now has enough commands. The missing piece is not more
commands; it is a user tutorial that says:

```text
install
create first volume
open/read report
open/read dashboard
inspect timeline
cleanup
```

Every command should point to the same evidence story. Avoid making users choose
between `inventory`, `cluster`, `report`, `dashboard`, and raw Kubernetes logs
without context.

### 4. Model Hardening Started But Needs Reconciliation

The ManagedVolume model is useful and tested. But the roadmap/protocol docs
need a reconciliation pass:

- which invariant rows are release blockers,
- which are future operator blockers,
- which are already closed,
- which facts are still TestOps-shaped rather than product-owned.

This should be a review pass, not a large refactor.

### 5. Operator/CRD Is Premature For The Immediate Product Goal

CRD/operator work is valuable, but it should come after Helm release hardening.

Reason:

- The product promise today is "simple stable observable block."
- Helm + dashboard directly improves that promise.
- CRD/operator introduces new lifecycle ownership and RBAC/audit questions.
- Mutating actions are still explicitly out of scope.

The right operator preparation now is documentation and status-shape review,
not controller implementation.

## Recommended Sequencing

### Immediate: v0.3 Alpha Release Reconciliation

Goal:

```text
Helm install -> first PVC -> report/dashboard -> cleanup
```

Work items:

1. Rewrite roadmap to mark Phase 22/23/24 closed and make Helm release
   hardening the active milestone.
2. Add `docs/releases/v0.3-alpha.md`.
3. Update README:
   - script path = alpha/dev fallback,
   - Helm path = next recommended Kubernetes install path once v0.3 gate is
     green,
   - dashboard = local read-only dashboard, not production hosted UI,
   - explicit non-claims retained.
4. Update `docs/quickstart-kubernetes.md` around:
   - `sw-block ops generate-helm-values`,
   - value/template explanation,
   - single-node vs multi-node behavior,
   - first-volume and dashboard artifacts.
5. Run/record Helm single-node and multi-node gates again if code changed.

This is the highest-value next step because it turns existing work into a
usable product release.

### Next: Operations Tutorial And Dashboard Polish

Goal:

```text
user can diagnose first-volume success/failure from one local surface
```

Work items:

- dashboard link in first-volume summary,
- bundle-backed dashboard tutorial,
- event folding/deduplication,
- blocked-state examples,
- screenshot/browser smoke if useful,
- clear `ops report` vs `ops dashboard` distinction.

### Then: Model / Protocol Reconciliation

Goal:

```text
ensure operations and future operator depend on stable facts, not ad-hoc glue
```

Work items:

- review `internal/docs/protocol/invariant-ledger.md`,
- mark stale "next" language as closed/deferred,
- add release-blocker matrix:
  - Helm blocker,
  - operations blocker,
  - operator blocker,
  - future HA blocker.
- identify facts that still require TestOps artifacts before they can become
  product-owned.

### Later: Operator / CRD

Goal:

```text
Kubernetes-native status and lifecycle after Helm contract is stable
```

Start only after:

- Helm install/uninstall is stable,
- read-only dashboard/report is documented,
- ManagedVolume facts are reconciled,
- mutating action policy/RBAC/audit boundaries are written.

## Recommended Roadmap State

Current useful release ladder should be:

| Release | Product Meaning | Status |
|---|---|---|
| `v0.1-alpha` | core K8s block/recovery/observation foundation | documented |
| `v0.2-alpha` | script-based Day-1 first-volume loop | documented |
| `v0.3-alpha` | Helm install + first-volume + read-only dashboard/report | next release to polish |
| `v0.4-beta-candidate` | operator lifecycle / CRD status / day-2 controller | later |

Do not make operator the next milestone unless Helm v0.3 is intentionally
deferred.

## Product Definition Of Done For The Next Release

The next release should close only when a cold user can do this:

```text
helm values generated from current cluster
helm install succeeds
PVC created through Kubernetes
writer/reader checksum passes
sw-block ops dashboard or report explains status
helm uninstall + host cleanup verified
README and release note match the exact claim
```

And a failure case should do this:

```text
PVC/writer blocks
support bundle/report/dashboard names the reason
safe next step is visible
no false success is claimed
```

## Decision Recommendation

Pause new Phase 25 implementation.

Open a short release-reconciliation phase instead:

```text
Phase 25: v0.3 Helm + Observable First-Volume Release Reconciliation
```

This phase should be mostly docs, gates, and claim alignment, with only small
code fixes if the Helm/dashboard user path exposes a real gap.

This best serves the stated goal:

```text
deliver a simple, stable, observable Kubernetes block product,
then keep closing loops.
```
