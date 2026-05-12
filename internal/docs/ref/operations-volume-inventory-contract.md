# Operations Contract: Volume Inventory

Status: active D1 contract for
`current-plan.md` (`Cluster Operations Inventory And Lifecycle Visibility MVP`).

This contract is a read-only inventory of Seaweed Block volumes and replicas.
It is not a repair API, cleanup API, scheduler, failover command, backup,
restore point, or data snapshot.

## Purpose

Operators need one compact answer to:

```text
Which Seaweed Block volumes exist, which Kubernetes PVC/PV objects own them,
which replicas are expected, which replicas are observed, where are their
frontend/status endpoints, and what evidence should block a false OK?
```

The first live implementation may run on the single-node alpha Kubernetes path,
but the schema is intentionally multi-volume and RF-aware. Single-node execution
must not create a single-volume or single-replica data model.

## Top-Level Shape

`volume-inventory.json` is an append-only JSON object:

- `schema_version`
- `captured_at`
- `source`
  - `component`
  - `host`
  - `scenario`
- `product_revision`
- `runner_revision`, when available
- `status`
  - `ok`
  - `unhealthy`
  - `invalid`
- `collection_errors`
- `volumes`
- `non_claims`

Top-level `status` is derived from the contents. A report with missing or
invalid identity is `invalid`; a report with valid identity but unhealthy,
missing, or partially collected evidence is `unhealthy`.

## Volume Shape

Each `volumes[]` entry contains:

- `volume_id`
- `namespace`
- `pvc_name`
- `pv_name`
- `replication_factor`
- `desired_replicas`
- `observed_replicas`
- `primary_replica_id`
- `protocols`
- `product_revision`
- `status`
- `residue`
- `issues`
- `unchecked`
- `collection_errors`
- `support_bundle`
- `replicas`

`desired_replicas` is the operator/product intent. `observed_replicas` is what
the inventory can actually see. If `observed_replicas < desired_replicas`, the
volume is not clean even when the primary is healthy.

## Replica Shape

Each `replicas[]` entry contains:

- `replica_id`
- `server_id`
- `node_name`
- `generated_deployment`
- `protocol`
- `frontend_address`
- `status_address`
- `support_bundle`
- `data_addr`
- `ctrl_addr`
- `observed`
- `status`
  - `ok`
  - `unhealthy`
  - `invalid`
  - `missing`
- `authority_role`
- `healthy`
- `frontend_primary_ready`
- `replication_role`
- `epoch`
- `endpoint_version`
- `residue`
- `issues`
- `collection_errors`

A desired but unseen replica is represented as `observed=false`,
`status=missing`; it must not be silently dropped from the volume row.

## RF Contract

The schema must support RF=1, RF=2, and RF=3 shapes:

- RF=1: one desired replica, normally one observed primary.
- RF=2: two desired replicas; missing or stale secondary is explicit evidence.
- RF=3: three desired replicas; partial observation remains one volume with
  three replica slots, not three independent volumes.

Live RF=2/RF=3 Kubernetes operation is not claimed by this contract. It is a
live product claim only after a runner gate proves it.

## Master / Node Join Semantics

Current product behavior:

- `blockvolume` processes dynamically register with master by heartbeat and
  subscribe for assignments.
- Master does not infer open placement from heartbeats. Topology,
  cluster-spec, or lifecycle placement admits the slot.
- Inventory should report both sides honestly: observed processes are evidence,
  but authority only applies to admitted replicas.

## Human Summary

`RenderVolumeInventorySummary` emits stable plain text suitable for logs and
support bundles. It must include:

- top-level inventory status,
- source and product revision,
- volume counts by status,
- one line per volume with PVC/PV, RF, desired/observed counts, primary, and
  status,
- one line per replica with server/node, observed flag, role, replication role,
  health, epoch, endpoint version, frontend, status endpoint, and status
  support-bundle pointer,
- issue lines that name the volume and replica.

## Support Bundle Manifest

`sw-block ops inventory` writes a small manifest beside the inventory:

```text
ops-inventory-bundle.json
```

Required artifacts:

- `volume-inventory.json`
- `volume-inventory-summary.txt`
- `ops-inventory-bundle.json`

When `sw-block ops inventory --master <addr>` discovers a live replica with a
status endpoint, it also writes that replica's normal `sw-block ops status`
bundle under:

```text
volumes/<volume_id>/<replica_id>/
```

The replica row's `support_bundle` field points at that nested directory. The
nested bundle must keep the same semantics as standalone `sw-block ops status`;
inventory may aggregate it, but must not redefine its fields or issue meaning.

Required manifest fields:

- `schema_version`
- `command`
- `captured_at`
- `product_revision`
- `runner_revision`, when available
- `exit_code`
- `status`
- `inventory_status`
- `volume_count`
- `artifacts`
- `collection_errors`
- `non_claims`

An empty cluster is a valid inventory result. It should emit `status=ok`,
`volume_count=0`, and an empty `volumes` array; it must not look like a hang or
a failed command.

## Non-Claims

Every `non_claims` block uses stable machine-readable prefixes:

- `read-only-observation`: inventory does not mutate product state.
- `single-cluster-alpha-scope`: discovery is scoped to one alpha Kubernetes cluster.
- `best-effort-partial-discovery`: missing inputs are reported as issues or unchecked evidence, not inferred as healthy.
- `no-mutating-admin`: inventory is not repair, cleanup, failover, backup, or restore.
- `no-multi-node-scheduling`: inventory observes placement; it does not schedule or rebalance replicas.
- `rf2-rf3-live-kubernetes-operation`: non-claim unless a runner gate explicitly proves it.
