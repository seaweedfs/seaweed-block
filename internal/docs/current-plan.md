# Current Plan: Multi-Node Attach And Placement MVP

Status: active, opened after closing
`finished-plans/phase13_finishedplan_durable_volume_restart_reattach_mvp.md`,
5% implementation.

QA needed now: no. First QA checkpoint is after a dev-owned substrate audit and
one runner-native multi-node attach gate design are ready.

Current dev slice: D1 multi-node attach reality audit.

## Product Question

Can an early Kubernetes user run Seaweed Block on a small multi-node cluster,
create a PVC, run an app pod on a selected node, and understand where the
backing `blockvolume` landed and how the app attached to it?

The last plans proved the single-node alpha loop:

```text
install -> product-owned blockvolume lifecycle -> inventory -> durable
blockvolume restart -> same PVC reattaches and reads data
```

This plan moves the next visible user gap:

```text
two-node Kubernetes lab -> PVC -> placement is explicit -> app pod attaches
through the normal CSI path -> inventory explains node/endpoint ownership
```

The narrow claim after this plan should be:

```text
On a supported two-node alpha Kubernetes lab, a generated RF=1 iSCSI
blockvolume can be placed on a known node, an app pod can attach through the
normal CSI path, write/read data, and `sw-block ops inventory` can explain the
PVC, blockvolume node, frontend endpoint, and support bundle.
```

This is not HA. It does not claim node-loss survival, automatic rescheduling,
RF=2/RF=3 live Kubernetes operation, cross-node failover, rebuild, performance,
upgrade safety, or UI.

## Why This Is Next

A Kubernetes block product cannot remain credible if every proof is
single-node-only. After durable restart, the next user-facing question is not
"can RF=3 rebuild?" yet; it is simpler:

```text
When my cluster has more than one node, can I see and control placement well
enough to use the block safely?
```

This also de-risks later availability work. Basic failover and returned-replica
rebuild depend on a correct placement and endpoint model:

- which Kubernetes node owns the `blockvolume` process,
- which frontend address the CSI node plugin should attach to,
- whether loopback-only endpoints are still acceptable,
- how inventory reports desired vs observed placement,
- what happens when an app pod and blockvolume are not on the same node.

If this layer is fuzzy, RF=2/RF=3 work will produce ambiguous failures.

## Current Honest State

What already works:

- Single-node dynamic PVC create/write/read/delete is gated.
- Two PVCs can coexist on one alpha node with distinct ports.
- Generated `blockvolume` Deployments are product-owned and PVC-owned.
- Durable hostPath restart works for RF=1 on the single-node alpha path.
- Inventory reports Kubernetes node, server, frontend, status address,
  lifecycle owner, and support bundle.

What is still weak or unknown:

- The alpha path has historically used loopback frontend addresses such as
  `127.0.0.1:3260`; that is probably not valid when the app pod lands on a
  different node from the `blockvolume`.
- The product may need explicit placement constraints before multi-node attach
  can be claimed honestly.
- The current quickstart does not tell users how to reason about node
  placement.
- Inventory can report node/server fields, but the multi-node correctness of
  those fields is not yet release-gated.
- Cleanup and residue checks must cover both nodes, not only m02.

## Scope

In scope:

- Audit current multi-node attach behavior before changing code.
- Define the supported alpha placement model for RF=1 multi-node Kubernetes:
  same-node attach only, remote-node attach, or both.
- Make frontend/status endpoint selection explicit for multi-node labs.
- Add fast tests for generated Deployment placement fields and endpoint
  rendering.
- Add a runner-native two-node gate that creates a PVC, runs writer/reader on
  the supported node placement, and asserts inventory explains placement.
- Add at least one negative/partial-state fixture where placement or endpoint
  reachability is wrong and the bundle explains it.
- Update `docs/operations-v1.md` with a small multi-node alpha section and
  non-claims.

Out of scope:

- Node failure or reboot survival.
- Automatic blockvolume rescheduling.
- RF=2/RF=3 live Kubernetes lifecycle.
- Cross-node failover while mounted.
- Rebuild/returned-replica reintegration.
- Performance SLOs.
- Production operator UX.
- UI/metrics.

## Top Blocking Issues

### P0: Placement Model Must Be Honest

Users need to know whether the app pod must run on the same Kubernetes node as
the generated `blockvolume`, or whether remote iSCSI attach is supported.

Close requirement: the plan documents and gates one supported model. Any other
model is an explicit non-claim.

### P0: Frontend Endpoint Must Be Reachable For The Claimed Model

Loopback endpoints are fine for single-node local attach, but they are not a
general multi-node endpoint.

Close requirement: the generated manifest and inventory expose endpoint
addresses that are reachable for the claimed placement model, or the gate
proves same-node placement prevents unsafe remote attach.

### P0: Inventory Must Explain Placement

If a user opens a support bundle, they should see PVC, PV, app node,
blockvolume node, server ID, frontend/status endpoints, desired vs observed
replicas, and support bundle path.

Close requirement: inventory assertions include node and endpoint fields, not
just PASS lines.

### P1: Cleanup Must Cover Both Nodes

Multi-node tests can leave iSCSI sessions or processes on either node.

Close requirement: cleanup and residue assertions cover every participating lab
node.

## Deliverables

### D1: Multi-Node Attach Reality Audit

Run and document a read-only audit of current behavior:

- available lab nodes and labels,
- current generated `blockvolume` node placement,
- current app pod node placement,
- current frontend/status addresses,
- whether remote-node attach works, fails, or is unsafe,
- current inventory fields for node/server/frontend/status endpoint.

Output: update this plan with the honest current state and choose the supported
alpha model for D2-D6.

### D2: Placement And Endpoint Contract

Add or update a reference doc under `internal/docs/ref/` describing:

- supported RF=1 multi-node alpha placement model,
- node labels/selectors/tolerations used by generated workloads,
- frontend address strategy for iSCSI,
- status endpoint strategy for inventory,
- what is deliberately not claimed.

### D3: Fast Tests

Add tests for:

- generated `blockvolume` Deployment placement fields,
- endpoint rendering for the supported model,
- inventory node/server/frontend/status fields,
- failure wording for unreachable endpoint or unsupported cross-node attach.

### D4: Runner-Native Multi-Node Attach Gate

Add a runner-native gate that:

```text
pre_clean all nodes
build/import alpha images
install alpha stack
create PVC
wait for generated blockvolume placement
run writer pod under the supported node placement
run replacement reader pod
collect inventory
assert PVC/PV/app node/blockvolume node/frontend/status/support bundle fields
delete PVC and cleanup
collect_and_cleanup(always)
```

### D5: Partial-State / Negative Fixture

Add one focused fixture for a realistic multi-node mistake:

- app pod scheduled away from an unsupported loopback blockvolume endpoint, or
- generated blockvolume placed on an unavailable node, or
- status endpoint unreachable from inventory.

The result should be an actionable inventory/support bundle, not just a timeout.

### D6: Operations Manual Update

Update `docs/operations-v1.md` and `docs/quickstart-kubernetes.md` to explain:

- when multi-node alpha is supported,
- required node labels or scheduling constraints,
- expected inventory placement fields,
- how to collect a bundle,
- non-claims for HA, node loss, RF=2/RF=3, and failover.

### D7: QA Close Gate

Ask QA to validate:

- the runbook is understandable without knowing implementation details,
- the runner-native multi-node attach gate passes,
- inventory explains placement and endpoint reachability,
- the negative fixture produces a useful bundle,
- cleanup covers all participating nodes,
- docs do not over-claim HA or node-loss durability.

## Gates To Close

This plan closes only when:

1. The supported multi-node alpha placement model is documented.
2. Fast tests cover placement and endpoint rendering.
3. A live runner-native two-node gate proves app write/read through the normal
   PVC path under the supported placement model.
4. Inventory/support bundles expose node/server/frontend/status/support-bundle
   evidence.
5. A negative fixture proves unsupported placement or endpoint failure is
   actionable.
6. Cleanup covers all participating nodes.
7. QA validates independently and reports no blocking usability issue.

## Success Statement

After this plan, Seaweed Block can make a stronger light-use claim:

```text
On a supported two-node alpha Kubernetes lab, users can create an RF=1 iSCSI
PVC, run an app through the normal CSI path under the documented placement
model, and use inventory/support bundles to understand exactly where the volume
is running and how it is attached.
```
