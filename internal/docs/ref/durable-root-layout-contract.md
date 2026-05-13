# Durable Root Layout Contract

Status: active reference for
`current-plan.md` Durable Volume Restart And Reattach MVP.

Purpose: define how product-owned generated `blockvolume` workloads map durable
state onto Kubernetes storage. This is the contract behind the alpha claim that
an RF=1 generated `blockvolume` can restart and reattach to the same PVC without
losing data when a durable host path is explicitly configured.

## Product Rule

`emptyDir` is allowed only for explicit throwaway smoke tests.

Any scenario or manual run that claims data survives `blockvolume` restart must
use a durable state volume and must capture the rendered manifest proving it.
The normal throwaway `emptyDir` alpha path can prove attach and I/O, but not
restart durability.

Durable restart is not only a local storage check. For Kubernetes product
behavior, the gate must include:

```text
blockvolume restart
-> local durable recovery
-> blockmaster re-observation
-> refreshed frontend target facts
-> CSI rediscovery / reattach
-> checksum verification
```

## Container Layout

Inside the `blockvolume` container:

```text
/var/lib/sw-block/
  <volume-id>/
    <replica-id>/
      durable backend files
```

The launcher passes:

```text
--durable-root=/var/lib/sw-block/<volume-id>/<replica-id>
```

The container mount point remains:

```text
/var/lib/sw-block
```

The generated Deployment must still carry the product lifecycle identity from
`blockvolume-lifecycle-ownership-contract.md`:

```text
metadata.labels.app = sw-blockvolume
metadata.labels.sw-block.seaweedfs.com/volume = <volume-id>
metadata.labels.sw-block.seaweedfs.com/replica = <replica-id>
ownerReferences[0].kind = PersistentVolumeClaim   # when PVC owner-ref mode is enabled
```

Durable restart evidence is only product-facing when this identity is present,
because inventory uses it to map:

```text
PVC -> PV -> volume_id -> generated Deployment -> replica -> durable status bundle
```

## Kubernetes State Volume Modes

### Throwaway Smoke Mode

Default rendering uses:

```yaml
volumes:
- name: state
  emptyDir: {}
```

Claims allowed:

- protocol smoke,
- manifest rendering,
- checksum write/read during one pod lifetime.

Claims not allowed:

- blockvolume pod restart durability,
- node reboot durability,
- returned-replica durability,
- beta data-retention behavior.

### Durable Lab Mode

When `K8sRenderConfig.StateHostPathBase` is set, rendering uses:

```yaml
volumes:
- name: state
  hostPath:
    path: <StateHostPathBase>
    type: DirectoryOrCreate
```

The container still receives:

```text
--durable-root=/var/lib/sw-block/<volume-id>/<replica-id>
```

Example mapping:

```text
host:      <StateHostPathBase>/pvc-a/r1
container: /var/lib/sw-block/pvc-a/r1
```

`blockmaster` exposes this through:

```text
--launcher-state-hostpath=/var/lib/sw-block
```

The alpha install/dynamic scripts expose the same choice through:

```text
SW_BLOCK_LAUNCHER_STATE_HOSTPATH=/var/lib/sw-block
```

Leaving the flag empty keeps the throwaway `emptyDir` rendering. Durable
restart gates must set the flag explicitly and capture the generated manifest.

For TestOps and local lab runs, prefer a run-scoped path:

```text
SW_BLOCK_LAUNCHER_STATE_HOSTPATH=/var/lib/sw-block/testops-<run-id>
```

This lets cleanup remove only the run-owned state and prevents one test from
silently reusing another test's data.

This is acceptable for single-node and node-local lab evidence. Multi-node
behavior must ensure the same replica is scheduled back onto the node that owns
its hostPath state, or use a real persistent volume per replica.

## Required Evidence

A durable restart/reattach QA bundle must include:

- generated `blockvolume` manifest,
- proof the state volume is not `emptyDir`,
- `--durable-root` argument,
- `hostPath.path` and `hostPath.type=DirectoryOrCreate`,
- PVC owner reference or explicit explanation why owner-ref mode is disabled,
- blockmaster status before and after restart,
- proof the restarted replica re-registered frontend target facts,
- CSI ControllerPublish / NodeStage evidence after restart or reattach,
- pre-restart checksum,
- post-restart checksum,
- `sw-block ops inventory` bundle after restart,
- nested `sw-block ops status` bundle with `durable_entry` evidence,
- blockvolume version,
- cleanup state.

The inventory/status bundle should show durable state in both JSON and summary
form:

```text
durable_entry: impl=walstore path=/var/lib/sw-block/<volume>/<replica>
  replica=<replica> latched=true operational=true epoch=<n> endpoint_version=<n>
```

If the nested status bundle cannot be collected, inventory must name the
reason with `status_endpoint_unavailable` or `status_endpoint_unreachable=...`
instead of reporting a false healthy state.

## Replica-Factor Scope

### RF=1 Reliable Restart

RF=1 proves:

```text
same replica + same node + same durable root + master re-observation
=> CSI can reattach and bytes survive
```

Master behavior:

- observe heartbeat loss,
- mark frontend unavailable,
- keep volume intent,
- observe the same replica after restart,
- record fresh status / endpoint version / frontend facts,
- allow CSI to rediscover the target.

Master does not promote another replica because none exists.

Close evidence for the current plan must go through the Kubernetes user path:

```text
writer pod writes checksum
-> generated blockvolume Deployment rolls out/restarts
-> durable status reports latched+operational
-> writer is removed
-> replacement reader pod mounts the same PVC
-> reader verifies checksum
-> inventory records lifecycle_owner=pvc-owner-ref and durable support bundle
```

### RF=2 / RF=3 Restart And Reintegration

RF=2/3 is a different gate. It must prove:

```text
primary loss
-> master promotion
-> old primary fenced on return
-> returned replica reports recovered frontier
-> catch-up completes
-> ready / ACK / promotion eligibility restored
```

The old primary must not serve writes just because its local durable root
recovered. It must first be classified and caught up.

## Non-Claims

- hostPath durable lab mode is not a production storage recommendation.
- This contract does not define backup, snapshots, migration, or encryption.
- This contract does not solve returned-replica rebuild semantics.
- This contract does not claim node-loss durability unless the hostPath remains
  on the same node and the scheduler returns the replica there.
- This contract does not claim live RF=2/RF=3 Kubernetes restart/reintegration.
- This contract does not claim upgrade or broad uninstall safety.

## D1/D2 Acceptance Checklist

D1/D2 for the current durable restart plan is complete when fast tests prove:

- durable hostPath rendering uses `DirectoryOrCreate`,
- the generated container still receives
  `--durable-root=/var/lib/sw-block/<volume>/<replica>`,
- PVC owner references still render with durable hostPath enabled,
- status endpoint args still render with durable hostPath enabled,
- throwaway mode still renders `emptyDir` and is not over-claimed,
- the component gate includes the combined durable+owner-ref+status rendering
  case.
