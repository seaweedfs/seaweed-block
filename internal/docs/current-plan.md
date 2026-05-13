# Current Plan: Product-Owned Blockvolume Lifecycle MVP

Status: active, D1-D7 implemented and live runner-native gate passed, 90%
implementation. Opened after closing
`finished-plans/phase11_finishedplan_cluster_ops_inventory_lifecycle_visibility_mvp.md`.

QA needed now: yes, for D8 formal close validation against
`qa-assignments/product-owned-blockvolume-lifecycle-mvp-close-hard-gate.md`.

Current dev slice: D8 QA close validation and any close-report fixes.

## Product Question

Can an early Kubernetes user create and delete Seaweed Block PVCs without
running a separate manifest-apply script, while still getting the same
observable inventory and cleanup evidence?

The last two plans proved:

```text
first volume works -> read-only cluster inventory is useful
```

This plan moves the next user-visible gap:

```text
PVC appears -> product creates/updates blockvolume workload -> inventory shows it
PVC deleted -> product removes blockvolume workload -> inventory shows cleanup
```

The narrow claim after this plan should be:

```text
On the supported alpha Kubernetes path, generated blockvolume workloads are
product-owned by a small reconciler/operator path instead of a user-run apply
script. Users still use normal Kubernetes PVCs, and `sw-block ops inventory`
shows the lifecycle state and evidence.
```

This is still not a full production operator. It does not claim multi-node
scheduling, upgrade safety, repair, rebuild, live RF=2/RF=3 Kubernetes
operation, metrics, or UI.

## Why This Is Next

The current user experience still has one awkward manual step:

```text
bash scripts/apply-k8s-alpha-blockvolumes.sh
```

That is acceptable for TestOps but not for a normal product loop. A light-use
user expects the product to own generated workloads once the install is done.
The inventory plan made this visible: it can now detect missing generated
Deployments, orphan Deployments, unplaced processes, stale status endpoints,
and per-volume support bundles. The next step is to reduce how often users hit
those states by making lifecycle reconciliation product-owned.

## Current Honest State

What already works:

- Dynamic PVC provisioning can create PV/PVC identity and blockmaster manifest
  output.
- Generated blockvolume manifests are multi-doc safe and can support two
  concurrent PVCs on one alpha node with distinct ports.
- `sw-block ops inventory` can observe multiple PVCs, generated Deployments,
  status endpoints, nested status bundles, and stale/orphan residue.
- The quickstart and TestOps scenarios can still use scripts as guardrails.
- The live lifecycle gate
  `cluster-ops-inventory-chain` passed on m02 as run
  `20260512-202811-1aa7`: two PVCs reconciled through the product-owned path,
  inventory reported PVC owner references, and deleting one PVC left the other
  visible and untouched.

What is still weak:

- QA has not yet independently cold-run the quickstart and operations manual
  for this product-owned lifecycle wording.
- Delete/uninstall ownership is split between product objects, scripts, and
  TestOps guardrails.
- We need to keep the first version conservative: no broad cluster sweeps, no
  admin repair commands, no hidden cleanup of unrelated resources.

## Scope

In scope:

- Define the lifecycle ownership contract for generated blockvolume workloads.
- Add or wire a minimal reconciler path that applies blockmaster-generated
  blockvolume manifests for the supported alpha namespace/labels.
- Make deletion scoped and attributable: PVC/PV disappearance should remove or
  mark only the matching generated workload.
- Keep `sw-block ops inventory` as the observation gate for every lifecycle
  state.
- Update quickstart so the happy path no longer requires a separate
  `apply-k8s-alpha-blockvolumes.sh` step.
- Add a v1 user operations manual for install, first volume, inventory,
  delete, failure bundle collection, cleanup, and known limits.
- Add fast tests first, then one runner-native lifecycle gate.

Out of scope:

- Full Kubernetes operator with CRDs and leader election.
- Multi-node placement policy.
- Repair/promote/rebuild/admin actions.
- Upgrade/uninstall safety beyond the alpha path.
- RF=2/RF=3 live Kubernetes lifecycle.
- UI/metrics.

## Top Blocking Issues

### P0: Manual Apply Script Blocks Product Feel

Users should not have to know that blockmaster writes files under `/manifests`
and then run a separate script.

Close requirement: the documented happy path creates a PVC and reaches a ready
blockvolume workload without asking the user to run
`apply-k8s-alpha-blockvolumes.sh`.

### P0: Lifecycle Ownership Must Be Scoped

A reconciler must only touch Seaweed Block resources it owns. Broad namespace
or cluster cleanup is not acceptable as product behavior.

Close requirement: generated workloads carry stable labels/owner identity and
delete/update logic targets only the matching volume/PVC identity.

### P0: Inventory Must Prove The Lifecycle

The previous plan made inventory the operational truth surface. This plan
should not add hidden behavior that only works if logs are inspected.

Close requirement: every lifecycle gate asserts inventory rows for created,
updated, deleted, partial, and orphan states.

### P1: Keep Reconciler Small And Testable

The first product-owned lifecycle loop should be mostly component-tested. The
long k3s gate should prove the user boundary, not every branch.

Close requirement: manifest selection, apply/delete scoping, idempotency, and
orphan handling are covered by fast tests.

## Deliverables

### D1: Lifecycle Ownership Contract

Document the exact ownership model:

- which labels identify generated blockvolume workloads,
- which PVC/PV fields map to volume id and replica id,
- which component owns create/update/delete,
- which states remain explicit non-claims.

Reference:
`ref/blockvolume-lifecycle-ownership-contract.md`.

### D2: Minimal Reconciler Implementation

Implement the smallest product-owned path that can:

- discover blockmaster-generated manifests or equivalent desired state,
- apply/update generated Deployments for supported alpha PVCs,
- avoid touching unrelated Deployments,
- delete or mark stale owned Deployments when the corresponding PVC/PV is gone,
- emit enough logs/events for TestOps and operators.

The implementation can start as a controller-side loop or a dedicated command,
but the user-facing runbook should not require manual manifest apply.

### D3: Inventory Integration

Ensure `sw-block ops inventory` clearly reports:

- reconciled workload present,
- workload missing,
- workload stale/orphaned,
- delete pending or cleanup complete,
- non-claims for states still handled by TestOps guardrails.

### D4: Quickstart Update

Update `docs/quickstart-kubernetes.md` so the primary happy path is:

```text
preflight -> install/launch -> create PVC/app -> inventory -> delete -> inventory
```

The old apply script can remain as an internal fallback, but it must not be the
main user path.

### D5: V1 User Operations Manual

Add a concise user-facing operations manual that answers:

- how to install or launch the alpha path,
- how to create the first PVC and confirm the backing `blockvolume` exists,
- how to run `sw-block ops inventory` and read the output,
- how to delete one PVC and confirm scoped cleanup,
- how to collect a support bundle when the volume is unhealthy,
- how to retry safely after an interrupted run,
- what is explicitly not claimed: upgrade safety, broad uninstall safety,
  multi-node scheduling, live RF=2/RF=3 Kubernetes operation, repair, metrics,
  and UI.

This manual can link to the quickstart, but it must stand on its own as the
operator-facing v1 path for a light user.

The manual must be based on
`ref/light-use-block-storage-ux-research.md`, especially the common
operational ladder:

```text
preflight/prereqs -> install -> wait/verify components -> create StorageClass ->
create PVC + app -> verify bound/running/I/O -> inspect status -> teardown ->
collect support data on failure
```

Minimum manual sections:

- prerequisites and runnable preflight,
- one default path first: iSCSI + `walstore` + supported alpha Kubernetes,
- install and component readiness checks,
- first PVC and app I/O verification,
- cluster inventory and per-volume support bundle collection,
- delete/teardown with scoped cleanup checks,
- retry after interrupted or partial runs,
- support bundle instructions for failure after volume identity exists,
- clear `ops-status-unavailable` guidance when no volume identity was reached,
- retained-state and non-claim section matching the research: no production HA,
  no broad distro matrix, no upgrade/uninstall safety, no live RF=2/RF=3
  Kubernetes claim, no performance SLO, no UI/operator-grade reconciliation.

### D6: Fast Tests

Add component tests for:

- manifest ownership labels,
- idempotent apply/update,
- scoped delete,
- orphan detection,
- two PVCs on one node,
- no mutation of unrelated workloads.

### D7: Runner-Native Lifecycle Gate

Add or update a TestOps scenario that:

```text
pre_clean
install/launch alpha product
create PVC without manual apply script
wait for generated blockvolume workload
run writer/reader or attach smoke
run inventory and assert lifecycle fields
delete PVC
run inventory and assert scoped cleanup or documented pending state
collect_and_cleanup(always)
```

### D8: QA Close Gate

Ask QA to validate as a new user:

- follow the quickstart without manual apply,
- follow the v1 operations manual without implementation knowledge,
- create two PVCs and confirm both reconcile,
- delete one PVC and confirm the other remains untouched,
- inspect inventory before and after delete,
- inject one orphan/stale generated workload and confirm it is named or cleaned
  only if owned,
- report any over-claim or confusing lifecycle state.

## Gates To Close

This plan closes only when:

1. The ownership contract is documented.
2. The primary quickstart no longer requires manual blockvolume manifest apply.
3. A v1 user operations manual exists and matches the implemented path.
4. Fast tests cover idempotent create/update/delete and no unrelated mutation.
5. A live runner-native gate creates a usable PVC through the product-owned
   lifecycle path.
6. Two PVCs can coexist without port or identity collision.
7. Deleting one PVC does not delete or corrupt the other.
8. Inventory proves create/delete/stale states without log spelunking.
9. QA validates the user experience independently and reports no blocking
   usability issue.

## Success Statement

After this plan, Seaweed Block can make a stronger light-use claim:

```text
On the supported alpha Kubernetes path, users create and delete PVCs through
normal Kubernetes objects while Seaweed Block owns the generated blockvolume
workload lifecycle. Operators can verify the result with read-only inventory.
```
