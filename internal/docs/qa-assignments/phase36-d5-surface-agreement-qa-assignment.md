# Phase 36 D5 QA Assignment - Surface Agreement And Negative-First Gates

Status: ready for QA.

Source scope:

- `SwBlockCluster.status`
- `SwBlockVolume.status`
- Kubernetes Events emitted by operator-status
- `summary.txt`
- `index.html`
- `operator-snapshot.json`
- dashboard `/operator-snapshot.json`
- `sw-block ops explain`

## Goal

Verify that the user-facing status surfaces agree on the same operational truth
for healthy, blocked, unknown/stale, cleanup-required, and multi-volume paths.

This gate must preserve the negative-first rule: blocked, unknown, stale, or
cleanup-required evidence must never be shown as false `Ready=True`.

## Required Gates

### G1: Healthy First Volume

Use the Helm first-volume path with operator-status write mode enabled.

Expected:

```text
SwBlockVolume.status.status=ready
SwBlockVolume.status.reasonCode=first_volume_verified
Ready=True reason=first_volume_verified
SwBlockCluster.status.readyVolumeCount=1
summary.txt managed_volume line matches the CRD
operator-snapshot.json matches the CRD
dashboard /operator-snapshot.json matches the CRD
Kubernetes Event is Normal / first_volume_verified
```

### G2: Blocked Path

Use the existing CSI image-pull blocked bundle or another controlled blocked
path.

Expected:

```text
SwBlockVolume.status.status=blocked
Ready=False
Blocked=True
reasonCode=csi_node_image_pull_failed or the controlled blocker reason
no Ready=True anywhere in CRD/report/dashboard/operator-snapshot/explain
safe next steps are read_only or dry_run, mutationAllowed=false
Kubernetes Event is Warning with the same reason
```

### G3: Unknown / Stale Evidence

Use the live or replay status-endpoint-unreachable gate.

Expected:

```text
SwBlockVolume.status.status=unknown
Ready=Unknown
EvidenceStale=True
reasonCode=status_endpoint_unreachable
no Ready=True anywhere
no inappropriate Blocked=True for pure unreachable evidence
summary.txt includes managed_volume and managed_volume_condition lines even if
  the bundle has managed_volumes without raw volumes[]
ops explain names the same status_endpoint_unreachable reason
dashboard /operator-snapshot.json agrees
```

### G4: Cleanup Required

Use Phase 36 D4 residue evidence.

Expected:

```text
SwBlockCluster.status.cleanup.status=failed
CleanupRequired=True
reason=cleanup_required or verifier reason when present
safeNextSteps has observe.verify_cleanup mode=scripted mutationAllowed=false
summary.txt / operator-snapshot.json / index.html / dashboard agree
operator-status does not execute cleanup
```

### G5: Multi-Volume Sanity

Use the existing multi-volume RF3 smoke/readiness path or a replayed equivalent.

Expected:

```text
volumeCount=3
readyVolumeCount=3 when all are healthy
three distinct SwBlockVolume identities
three distinct volume_id/pvc_name entries in operator-snapshot.json
no duplicate publish target for distinct volumes
no cross-volume reason/status mix-up
summary.txt includes one managed_volume line per volume
```

### G6: Event Identity And Boundary

Across G1-G4:

```text
Events use stable bounded identity; repeated reconcile does not mint unbounded
  duplicates for the same object/type/reason
operator-status ServiceAccount can patch /status and create Events only
no spec, pod, PVC, PV, secret, deployment, storageclass, iSCSI, multipath, or
hostPath mutation power
```

## Pass Criteria

```text
G1 PASS
G2 PASS
G3 PASS
G4 PASS
G5 PASS
G6 PASS
final cleanup verifier returns cleanup_status=ok
```

## Non-Claims

- No mutating lifecycle.
- No automatic cleanup.
- No automatic support-bundle collection or upload.
- No repair/rebuild/failback/promote/delete execution.
- No finalizer/delete safety.
