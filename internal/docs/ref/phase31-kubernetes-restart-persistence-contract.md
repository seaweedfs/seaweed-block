# Phase 31 Kubernetes Restart Persistence Contract

Date: 2026-05-25

Purpose: define the minimum storage and control-plane state that must survive a
Kubernetes/product restart before Seaweed Block can claim a credible beta user
path.

## Required Restart Claim

Supported claim after Phase 31:

```text
On the documented durable Helm install mode, the same Kubernetes cluster and
same block-capable nodes can restart and continue serving existing PVC data.
If a volume was promoted before restart, the promoted primary/epoch/publish
target are preserved or a later valid failover is explicitly evidenced.
```

This is not disaster recovery. It does not cover deleting the Kubernetes
cluster, losing all nodes, losing host disks, or restoring from backup.

## Persisted State Requirements

| State | Owner | Persisted Where | Restart Rule |
|---|---|---|---|
| Volume data / walstore | blockvolume durable backend | per-node durable hostPath | Must reopen existing WAL/store for the same volume/replica. |
| Authority primary | EngineMaster / AuthorityLineAuthority | master authority store | Must reload latest primary, epoch, endpoint_version. |
| Promotion history | EngineMaster | authority events/store | Must not resurrect old primary after promotion. |
| Lifecycle registration | blockmaster lifecycle store | master lifecycle store | Must reload PVC/PV to generated workload relationship. |
| Placement intent | launcher / cluster spec / Kubernetes objects | Helm values + Kubernetes objects + lifecycle store | Must reconcile existing desired runtime, not mint a different topology. |
| Publish target | AuthorityLineAuthority + PlacementAuthority | authority line + generated Deployment args | CSI must reattach to current target. |
| CSI stage evidence | CSI node / master event ingestion | events/report bundle | Restart report must show reattach or ready path. |
| Cleanup evidence | CleanupAuthority | cleanup-summary/report | Restart gate must still clean without residue. |

## Failure Rules

- If authority store is missing but durable data exists, status must be
  `Blocked` or `Unknown`; do not infer an old primary from topology.
- If durable store is missing for the selected replica, status must be
  `Blocked`; do not silently recreate empty storage for an existing PVC.
- If publish target after restart differs from persisted authority, status must
  be `Blocked` unless a newer valid failover event explains the change.
- If the old primary was stopped before restart, it must not regain primary
  role merely because the launcher sees its Deployment again.

## Evidence Required

Minimum bundle/report fields for restart gates:

- `restart_persistence_mode=hostpath`
- `before_restart_primary`
- `before_restart_epoch`
- `before_restart_publish_target`
- `after_restart_primary`
- `after_restart_epoch`
- `after_restart_publish_target`
- `reader_verified_after_restart=true`
- `authority_reloaded=true`
- `lifecycle_reloaded=true`
- `durable_reopened=true`
- `old_primary_resurrected=false`
- `cleanup_status=ok`

## Non-Claims

- No fresh-cluster restore.
- No backup/snapshot/restore.
- No host disk loss survival.
- No all-replica loss survival.
- No broad production SLO.
- No automatic rebuild/failback of returned replicas.

