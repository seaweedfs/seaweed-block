# Blockvolume Lifecycle Ownership Contract

Status: D1 contract for
`current-plan.md` Product-Owned Blockvolume Lifecycle MVP.

## Product Goal

The supported alpha Kubernetes path should let a user create and delete PVCs
without running a separate generated-manifest apply script.

Current manual loop:

```text
PVC created -> blockmaster renders /manifests/*.yaml -> user runs apply script
```

Target product-owned loop:

```text
PVC created -> blockmaster/lifecycle reconciler materializes owned Deployment
PVC deleted -> blockmaster/lifecycle reconciler removes or stops owning only
               the matching generated Deployment
```

The scope is intentionally narrow. This contract covers generated
`blockvolume` workload ownership for the supported single-cluster alpha path.
It is not a full operator contract, upgrade contract, multi-node scheduler, RF
repair contract, or mutating admin surface.

## Existing Ownership Inputs

The current code already exposes the durable identity needed for a safe first
reconciler.

| Source | Existing field or behavior | Owner |
|---|---|---|
| CSI CreateVolume | `VolumeSpec.VolumeID`, `PVCName`, `PVCNamespace`, `PVCUID`, `PVName`, `ReplicationFactor`, `Protocol` | CSI/controller path |
| Lifecycle store | one JSON volume record per volume ID; `DeleteVolume` is idempotent | `core/lifecycle.FileStore` |
| Placement store | one placement intent per volume; `DeletePlacement` is idempotent | `core/lifecycle.PlacementIntentStore` |
| Workload planner | `BlockVolumeWorkloadPlan` and `BlockVolumeReplicaWorkload` | lifecycle planner |
| Renderer | one Kubernetes Deployment per volume replica | `core/launcher` |
| Inventory | PVC/PV/Deployment/status endpoint mapping and residue issues | `sw-block ops inventory` |

The workload plan is not authority-shaped. It must not mint epoch,
endpoint_version, primary, health, or readiness. Authority remains owned by
blockmaster assignment logic and blockvolume heartbeats.

## Generated Workload Identity

Every product-owned generated `blockvolume` Deployment must be identifiable
without parsing logs or TestOps artifacts.

Required Deployment metadata:

```text
metadata.name = sw-blockvolume-<volume-id-dns>-<replica-id-dns>
metadata.labels.app = sw-blockvolume
metadata.labels.sw-block.seaweedfs.com/volume = <volume-id>
metadata.labels.sw-block.seaweedfs.com/replica = <replica-id>
```

Required Pod template labels:

```text
metadata.labels.app = <deployment-name>
metadata.labels.sw-block.seaweedfs.com/volume = <volume-id>
metadata.labels.sw-block.seaweedfs.com/replica = <replica-id>
```

When PVC metadata is available and `--launcher-pvc-owner-ref` is enabled, the
Deployment must also carry:

```text
ownerReferences[0].apiVersion = v1
ownerReferences[0].kind = PersistentVolumeClaim
ownerReferences[0].name = <pvc-name>
ownerReferences[0].uid = <pvc-uid>
ownerReferences[0].controller = true
metadata.namespace = <pvc-namespace>
```

If PVC metadata is missing, the reconciler must not invent an owner reference.
It may still render a workload in the configured launcher namespace, but
inventory must be able to report the missing ownership metadata.

## Runtime Arguments

Every generated `blockvolume` container must retain these args because they
are the machine-readable bridge for inventory and support bundles:

```text
--master=<master-grpc-addr>
--server-id=<server-id>
--volume-id=<volume-id>
--replica-id=<replica-id>
--data-addr=<host:port>
--ctrl-addr=<host:port>
--durable-root=<root>/<volume-id>/<replica-id>
--durable-impl=walstore
--durable-blocks=<size/4096>
--durable-blocksize=4096
--recovery-mode=<mode>
--status-addr=127.0.0.1:<frontend-port+20000>   # when diagnostics enabled
```

Protocol-specific args:

```text
iSCSI: --iscsi-listen=127.0.0.1:<port>
       --iscsi-iqn=iqn.2026-05.io.seaweedfs:<volume-id>

NVMe:  --nvme-listen=127.0.0.1:<port>
       --nvme-subsysnqn=nqn.2026-05.io.seaweedfs:<volume-id>
       --nvme-ns=1
```

Port allocation must remain per-node and per-workload so two PVCs on one alpha
node do not collide. The inventory hard gate already proved this for two live
PVCs; future lifecycle work must preserve it.

## Lifecycle States

The product-owned reconciler should expose these states through inventory,
logs, or both. The exact JSON field can evolve, but the issue vocabulary must
stay machine-readable.

| State | Meaning | Required inventory evidence |
|---|---|---|
| desired | PVC/PV/lifecycle record exists; workload may not exist yet | volume row with PVC/PV and `observed_replicas` |
| materialized | expected Deployment exists | replica row with `generated_deployment`, protocol, frontend/status endpoints |
| ready | Deployment available and nested status bundle collected | support bundle path and status evidence |
| degraded | workload exists but status endpoint or authority state is not healthy | `replica_degraded`, `ops_status=... reason=...` |
| missing | PVC/PV exists but expected Deployment is absent | `generated_deployment_missing`, `observed_replicas=0 desired=...` |
| orphan | Deployment or process exists without matching PVC/PV | `orphan-blockvolume-deploy` or `blockvolume-process-without-placement` |
| deleting | PVC/PV/lifecycle record removed; matching owned workload being removed | delete event or inventory row until absent |
| deleted | matching owned workload absent | inventory no longer shows the volume, or shows scoped cleanup evidence |

## Reconcile Algorithm

The first implementation should be conservative and idempotent.

For each tick:

1. Read desired lifecycle volumes and placement intents from the lifecycle
   stores.
2. Build workload plans with `RunLifecycleWorkloadPlanTick`.
3. Render expected Deployment manifests with stable names and labels.
4. Apply or update only Deployments that match the expected
   `sw-block.seaweedfs.com/volume` and `sw-block.seaweedfs.com/replica` pair.
5. List existing `app=sw-blockvolume` Deployments in the managed namespace.
6. Delete only Deployments that satisfy all of:
   `app=sw-blockvolume`, Seaweed Block volume label present, generated name
   matches the expected name format, and no matching desired lifecycle volume
   remains.
7. Never delete unrelated resources, unlabeled Deployments, or Deployments
   whose volume label does not map to this lifecycle store.
8. Emit enough event/log text for TestOps to distinguish create, update,
   no-op, scoped delete, and skipped-unowned.

The reconciler must be safe to run repeatedly. A second tick with unchanged
inputs should be a no-op except for normal Kubernetes apply metadata changes.

## Delete Semantics

PVC/PV deletion should remove product ownership in this order:

1. CSI/controller deletes or marks the lifecycle volume record.
2. Placement intent for the volume is removed or becomes non-desired.
3. Reconciler removes only generated Deployments for that volume.
4. Kubernetes garbage collection may also remove PVC-owned Deployments when
   ownerReference is present; the reconciler must treat already-missing
   workloads as success.
5. Persistent data under `/var/lib/sw-block` is not automatically wiped by this
   plan unless a later plan explicitly adds a safe data-retention policy.

Deletion must be retry-safe. Missing lifecycle records, missing placement
records, and already-deleted Deployments are successful no-ops.

## Inventory Contract

`sw-block ops inventory` is the proof surface for this plan.

For created workloads it must show:

```text
PVC namespace/name -> PV -> volume_id -> generated Deployment -> replica
protocol/frontend/status endpoint -> support_bundle
```

For deleted or partial workloads it must show one of:

```text
generated_deployment_missing
orphan-blockvolume-deploy=<deployment>
blockvolume-process-without-placement=<server>
heartbeat-without-placement=<server> state=unadmitted-by-master reason=<reason>
status_endpoint_unavailable
status_endpoint_unreachable=<addr>
```

The reconciler should not introduce lifecycle behavior that only logs can
explain. If inventory cannot name the state, the implementation is not ready
for the live gate.

## Non-Claims

This contract does not claim:

- full Kubernetes operator semantics,
- CRD ownership,
- leader election,
- multi-node scheduling,
- live RF=2/RF=3 Kubernetes lifecycle,
- repair, rebuild, promote, backup, or restore,
- upgrade or uninstall safety,
- automatic deletion of persistent volume data under `/var/lib/sw-block`,
- mutation of unrelated resources.

## D2 Acceptance Checklist

D2 can start when this contract is linked from `current-plan.md`.

D2 is complete only when fast tests prove:

- expected manifest labels and owner references are preserved,
- apply/update selection is scoped to generated Seaweed Block Deployments,
- delete selection skips unrelated or unlabeled Deployments,
- repeated reconcile ticks are idempotent,
- two PVCs on one node keep distinct ports and identities,
- inventory can still observe the lifecycle state.

## QA Boundary

No QA run is needed for this D1 contract. QA should re-enter at the first live
gate after D6, when the product-owned path can be exercised from the runbook.
