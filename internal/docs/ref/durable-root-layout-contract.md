# Durable Root Layout Contract

Status: active reference for the beta-hardening plan.

Purpose: define how generated `blockvolume` workloads should map durable state
onto Kubernetes storage. This closes the gap where the workload had a
`--durable-root` argument but the pod state volume was still `emptyDir`.

## Product Rule

`emptyDir` is allowed only for explicit throwaway smoke tests.

Any scenario that claims data survives `blockvolume` restart must use a durable
state volume and must capture the rendered manifest proving it.

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
host:      /var/lib/sw-block/pvc-a/r1
container: /var/lib/sw-block/pvc-a/r1
```

This is acceptable for single-node and node-local lab evidence. Multi-node
behavior must ensure the same replica is scheduled back onto the node that owns
its hostPath state, or use a real persistent volume per replica.

## Required Evidence

A durable restart/reattach QA bundle must include:

- generated `blockvolume` manifest,
- proof the state volume is not `emptyDir`,
- `--durable-root` argument,
- pre-restart checksum,
- post-restart checksum,
- blockvolume version,
- cleanup state.

## Non-Claims

- hostPath durable lab mode is not a production storage recommendation.
- This contract does not define backup, snapshots, migration, or encryption.
- This contract does not solve returned-replica rebuild semantics.
- This contract does not replace the future operator/controller ownership
  model.
