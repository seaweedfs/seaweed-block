# Finished Plan: Durable Volume Restart And Reattach MVP

Status: closed. QA close report passed 8/8 hard-gate clauses in
`qa-assignments/durable-volume-restart-reattach-mvp-close-report.md`.

Opened after closing
`finished-plans/phase12_finishedplan_product_owned_blockvolume_lifecycle_mvp.md`.

Closed after QA-owned runs:

- restart gate `20260512-221315-a784`: PASS, 5/5 phases, 59/59 actions,
- failure gate `20260512-221946-a81a`: PASS, 5/5 phases, 26/26 actions.

Non-blocking follow-ups carried forward:

- Restart-gate cleanup assertion should include `blockcsi` if future gates want
  standalone zero-process hygiene instead of suite-level cleanup.
- Bad-hostPath inventory is a distinct failure subclass: Deployment exists,
  durable init/status endpoint fails, inventory reports `replica_degraded` and
  `collection_error: ops_status`.

## Product Question

Can an early Kubernetes user keep data after the generated `blockvolume`
workload restarts, using the same product-owned lifecycle path and inventory
evidence established by the previous plan?

The last three plans proved:

```text
first volume works -> inventory explains cluster state -> product owns
generated blockvolume Deployment lifecycle
```

This plan moves the next visible user gap:

```text
PVC writes data -> blockvolume restarts -> same PVC reattaches -> data is still
readable -> inventory/support bundle explains durable state
```

The narrow claim after this plan should be:

```text
On the supported single-node alpha Kubernetes path, a generated RF=1 iSCSI
blockvolume can restart and reattach to the same PVC without losing data,
when configured with the documented durable host path.
```

This is still not production HA. It does not claim node loss, multi-node
scheduling, live RF=2/RF=3 Kubernetes operation, upgrade safety, rebuild,
failover, performance, or UI.

## Why This Is Next

The product now feels closer to a normal Kubernetes storage product: users
create PVCs and the product materializes backing `blockvolume` workloads. But
several docs still honestly say the default alpha path uses throwaway pod-local
state in some paths. That blocks a stronger light-use product claim.

A storage product must survive at least its own workload restart before users
can trust it for light use. We already have partial restart machinery and
operations evidence:

- `scripts/run-k8s-blockvolume-restart.sh` exists as a restart-oriented wrapper.
- `scripts/run-alpha-app-demo.sh` has `SW_BLOCK_RESTART_BLOCKVOLUME_BEFORE_READER`
  and `SW_BLOCK_LAUNCHER_STATE_HOSTPATH` hooks.
- `sw-block ops status` reports durable entries, latched state, epoch, and
  endpoint version.
- `sw-block ops inventory` now maps PVCs to generated workloads and nested
  status bundles.

This plan turns those pieces into a product-facing durable restart path.

## Current Honest State

What already works:

- Product-owned generated workload reconciliation is live-gated.
- Two PVCs can coexist on one alpha node with distinct ports.
- Inventory exposes PVC owner references and per-replica support bundles.
- The demo can run a writer then replacement reader through the same PVC.
- Restart hooks exist in scripts, and the prior hardening work validated
  durable status fields in smaller slices.

What is still weak:

- The default quickstart still frames generated blockvolume storage as
  non-durable pod-local state.
- Durable root selection is not yet a simple, documented user path.
- Restart evidence is scattered across scripts and prior QA reports rather
  than one current product gate.
- Inventory must prove durable readiness in a way a user can understand
  without reading blockvolume logs.
- Cleanup must not erase retained durable data unless the user explicitly asks
  for a clean lab.

## Scope

In scope:

- Define the durable root layout and ownership for the supported alpha restart
  path.
- Make the operations manual show how to enable durable host-path storage for
  the alpha path.
- Ensure generated `blockvolume` Deployments carry the durable-root and
  lifecycle arguments needed for restart.
- Add or tighten fast tests around durable-root rendering, hostpath injection,
  and inventory/status fields.
- Add a runner-native gate that writes data, restarts the generated
  `blockvolume`, reattaches/replaces the app pod, and verifies checksum.
- Use `sw-block ops inventory` and nested `sw-block ops status` bundles to
  prove durable state after restart.
- Keep cleanup scoped and explicit about retained `/var/lib/sw-block` state.

Out of scope:

- Node loss.
- Multi-node scheduling.
- Live RF=2/RF=3 Kubernetes lifecycle.
- Rebuild or returned-replica reintegration.
- Upgrade/uninstall safety.
- Performance SLO.
- UI/metrics.

## Top Blocking Issues

### P0: Durable Storage Must Be User-Selectable And Visible

Users need one documented way to say "keep the blockvolume state here" and one
way to verify it is actually used.

Close requirement: `docs/operations-v1.md` and the live gate both use an
explicit durable host path, and generated manifests show the durable-root
mapping.

### P0: Restart Must Preserve Data Through The Normal PVC Path

The proof must go through Kubernetes PVC attach/read, not just a direct file or
status endpoint check.

Close requirement: a writer pod writes and verifies data, the generated
`blockvolume` Deployment restarts, a replacement reader pod mounts the same PVC
and verifies the same checksum.

### P0: Inventory Must Explain Durable State

A passing restart is not enough if operators cannot tell what persisted.

Close requirement: inventory/support bundles show durable entry evidence
including operational/latched state, epoch/endpoint evidence when available,
and no contradictory health wording.

### P1: Cleanup Must Preserve The Right Boundary

Normal demo cleanup should remove Kubernetes resources and active sessions.
Durable data retention must be explicit, not accidental.

Close requirement: cleanup artifacts state whether the durable host path was
retained or removed, and broad deletion remains a TestOps guardrail only.

## Deliverables

### D1: Durable Root Contract Refresh

Review and update `ref/durable-root-layout-contract.md` for the current
product-owned lifecycle path:

- generated Deployment hostPath/volumeMount layout,
- mapping from PVC/volume ID to durable replica path,
- ownership of retained data,
- cleanup and non-claims.

### D2: Fast Rendering And Status Tests

Add or tighten fast tests for:

- `--launcher-state-hostpath` rendering,
- generated Deployment hostPath and durable-root args,
- PVC owner references preserved with durable host path enabled,
- inventory/status summary fields that prove durable entry state.

### D3: Operations Manual Update

Update `docs/operations-v1.md` with a "durable restart path" section:

```text
set SW_BLOCK_LAUNCHER_STATE_HOSTPATH -> run demo restart wrapper -> inspect
inventory/status durable entry -> cleanup/retention notes
```

Keep the default quickstart honest: the durable restart path is supported when
configured, not an implicit production durability claim.

Status: complete. The operations manual now documents a run-scoped durable
hostPath, restart wrapper, durable status evidence, inventory bundle collection,
and cleanup/retention semantics. The Kubernetes quickstart links to that path
and keeps the default first-volume claim separate from restart durability.

### D4: Runner-Native Restart Gate

Add or tighten a TestOps scenario that:

```text
pre_clean
build/import alpha images
install/launch with product-owned lifecycle and durable host path
create PVC/app writer
wait for blockvolume ready
restart generated blockvolume Deployment
wait for durable status readiness
delete writer and start replacement reader
verify checksum
run inventory and assert durable support bundle evidence
delete PVC and cleanup
collect_and_cleanup(always)
```

Status: dev gate passed. Run `20260512-211604-1339` on m02 passed 5/5 phases
and 59/59 actions at commit `e90ce49`. The restart wrapper collects
`ops-inventory-after-restart` during the live post-restart window, and the
runner-native chain asserts inventory, PVC owner reference, nested
`sw-block ops status` bundle, and durable entry evidence.

### D5: Failure/Partial-State Evidence

Add at least one focused failure fixture or assertion for:

- durable host path missing/unwritable, or
- status endpoint unavailable after restart, or
- durable status not latched/operational after restart timeout.

The goal is a useful bundle, not a broad chaos matrix.

Status: dev gate passed. Run `20260512-214412-860a` on m02 passed 5/5 phases
and 26/26 actions at commit `aae2e53`. The new
`csi-rf1-durable-restart-failure-chain` uses an unwritable durable hostPath,
expects the user workflow to fail, collects `ops-inventory-on-failure`, and
asserts an unhealthy inventory with the PVC row, actionable issue, inventory
bundle, and `collection_error: ops_status` evidence.

### D6: QA Close Gate

Ask QA to validate as a user:

- follow the durable restart section in the operations manual,
- run the runner-native restart gate,
- confirm writer/reader checksum across blockvolume restart,
- confirm inventory/support bundle durable evidence,
- confirm cleanup and retained-state wording are honest,
- report any over-claim or confusing durable-state wording.

Status: assignment complete. The hard gate lives at
`qa-assignments/durable-volume-restart-reattach-mvp-close-hard-gate.md` and
covers runbook discoverability, durable manifest evidence, PVC data survival
across restart, durable status, inventory/support bundle, cleanup boundary,
bad-hostPath failure bundle, and non-claims.

## Gates To Close

This plan closes only when:

1. Durable host-path layout and non-claims are documented.
2. Fast tests cover durable-root rendering and status/inventory evidence.
3. The operations manual shows the durable restart path.
4. A live runner-native gate proves write -> blockvolume restart -> reattach ->
   read checksum.
5. Inventory/support bundles prove durable entry state after restart.
6. Cleanup artifacts distinguish Kubernetes cleanup from durable data
   retention/removal.
7. QA validates independently and reports no blocking usability issue.

## Success Statement

After this plan, Seaweed Block can make a stronger light-use claim:

```text
On the supported single-node alpha Kubernetes path, users can configure a
durable host path and verify that a generated RF=1 iSCSI blockvolume survives
its own workload restart with data still readable through the PVC.
```
