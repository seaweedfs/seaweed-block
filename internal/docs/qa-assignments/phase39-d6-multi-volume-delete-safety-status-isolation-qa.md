# Phase 39 D6 QA Assignment: Multi-Volume Delete-Safety Status Isolation

Status: pending; run only after D4/D5 status-boundary validation passes on
`1630de2` or newer.

## Goal

Validate that delete-safety evidence for one volume does not contaminate other
volumes. This is a status/events-only gate. Do not test finalizer add/remove.

The operator-status controller may write only:

```text
SwBlockCluster/status
SwBlockVolume/status
Kubernetes Events
```

It must not patch `SwBlockVolume.metadata.finalizers`, `SwBlockVolume.spec`,
PVCs, PVs, Pods, Deployments, StorageClasses, Secrets, Nodes, iSCSI, multipath,
dmsetup, hostPath data, or Helm resources.

## Preflight

- Use a clean lab.
- Restore `tp01` first if running on the 3-node RF=3 lab.
- Install with `operatorStatus.create=true` and `operatorStatus.dryRun=false`.
- Confirm RBAC:
  - `patch swblockclusters/status`: yes
  - `patch swblockvolumes/status`: yes
  - `create events`: yes
  - main `patch swblockvolumes`: no
  - `patch swblockvolumes --subresource=finalizers`: no
  - pod/PVC/PV/deployment/storageclass mutations: no

## Scenario Shape

Create or replay evidence for three managed volumes:

- Volume A: delete requested + cleanup residue.
- Volume B: healthy and not deleting.
- Volume C: clean delete-safety evidence or healthy, depending on available
  fixture.

Minimum expected status:

```text
volume A: deleteSafety.state=blocked, decision=rejected
volume B: status=ready, reasonCode=first_volume_verified
volume C: status remains independent; if clean-delete evidence is used,
          deleteSafety.state=releasable, decision=allowed
```

## Pass Criteria

- Volume A has `deleteSafety.state=blocked` and a stable residue reason.
- Volume B keeps its original `volumeID`, `pvcName`, status, reason, and publish
  target identity.
- Volume C keeps independent status; if it has clean delete-safety evidence, it
  becomes `releasable` without changing A/B.
- `SwBlockCluster.status.volumeCount` and ready/blocked/stale counts match the
  three-volume evidence.
- No cross-volume reason-code mix-up appears in CRD status, report,
  operator-snapshot, dashboard, or explain output.
- Events, if emitted, reference the correct `SwBlockVolume` object only.
- `operator_status=... finalizer_patches=0`.
- No finalizer-added or finalizer-released Events appear.
- Cleanup verifier ends with `cleanup_status=ok` and all residue counters zero.

## Failure Conditions

- Any volume other than A shows A's residue reason.
- A blocked delete-safety decision prevents B/C status publication.
- Any finalizer patch is attempted.
- Any workload/storage/host mutation occurs from operator-status.
- Any surface claims object deletion completed because of operator-status.

## Report

Write the sign-off to:

```text
internal/docs/qa-assignments/phase39-d6-multi-volume-delete-safety-status-isolation-qa-signoff.md
```

Include:

- source commit,
- lab node health,
- evidence shape used,
- per-volume CRD status,
- cross-surface agreement matrix,
- RBAC boundary result,
- final cleanup audit,
- blocking/non-blocking findings,
- recommendation for Phase 39 close.
