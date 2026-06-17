# QA Assignment — Phase 44 D3/D4 Delete Lifecycle Close Gate

Goal: validate the integrated delete lifecycle after D2 proved that a normal PVC
creates a protected `SwBlockVolume` CR.

This gate must use a VAP-capable cluster. It must run with fresh `sw-block` and
`sw-block-csi` images from the candidate branch because both the CSI CR creation
path and the operator-status delete-safety bridge are new.

## Scope

Validate this product path:

```text
Helm install with operatorStatus + lifecycleOwner
-> first PVC writer/reader
-> SwBlockVolume exists with protection finalizer
-> delete requested
-> unsafe or missing cleanup evidence holds the finalizer
-> clean cleanup-summary evidence makes deleteSafety releasable
-> lifecycle-owner releases only the protection finalizer
-> SwBlockVolume deletion completes
-> uninstall leaves zero residue
```

Out of scope: automatic cleanup execution, PVC/PV/workload mutation by
operator-status or lifecycle-owner, rebuild/failback/backup/NVMe.

## Required Setup

Install with both controllers enabled:

```text
--set operatorStatus.create=true
--set operatorStatus.dryRun=false
--set lifecycleOwner.create=true
--set lifecycleOwner.dryRun=false
```

Confirm the ownership split before delete testing:

```text
CSI service account:
  can create/patch swblockvolumes main object
  cannot patch swblockvolumes/status
  cannot patch swblockvolumes/finalizers

operator-status service account:
  can patch swblockvolumes/status and create events
  cannot patch swblockvolumes main object or finalizers

lifecycle-owner service account:
  can patch swblockvolumes main object
  VAP confines the patch to block.seaweedfs.com/swblockvolume-protection
  cannot patch swblockvolumes/status
```

## G1 — Day-1 Baseline

1. Install the chart.
2. Create the first PVC with the standard basic-app path.
3. Verify writer and reader both pass.
4. Verify exactly one `SwBlockVolume` exists for the PVC.
5. Verify the protection finalizer exists exactly once.
6. Verify operator-status writes:

```text
status=ready
reasonCode=first_volume_verified
Ready=True
deleteSafety=null
```

## G2 — Missing Evidence Holds

Request deletion of the `SwBlockVolume` while the protection finalizer is present
and before providing cleanup evidence.

Expected:

```text
SwBlockVolume remains Terminating
status.deleteSafety.decision=unknown
status.deleteSafety.state=requested
status.deleteSafety.reason=cleanup_evidence_missing
status.status=unknown
status.reasonCode=cleanup_evidence_missing
lifecycle-owner finalizer_held > 0
finalizer_released = 0
```

Fail if the finalizer is released or any surface claims Ready=True for the
deleting object with missing cleanup evidence.

## G3 — Blocked Residue Holds

Provide a verifier summary with residue, for example an iSCSI node DB record, and
run operator-status with:

```text
sw-block ops operator-status --namespace kube-system \
  --master-api <blockmaster:9333> \
  --cleanup-summary <cleanup-summary.txt>
```

Expected:

```text
status.deleteSafety.decision=rejected
status.deleteSafety.state=blocked
CleanupRequired=True
allowedActions includes observe.verify_cleanup mode=scripted mutationAllowed=false
lifecycle-owner holds the protection finalizer
no cleanup is executed by either controller
```

Fail if blocked residue releases the finalizer.

## G4 — Clean Evidence Releases

Clear residue with the documented cleanup path, then produce a fresh
`cleanup-summary.txt` from `scripts/verify-helm-cleanup.sh`. Run operator-status
with `--cleanup-summary` again.

Expected:

```text
status.deleteSafety.decision=allowed
status.deleteSafety.state=releasable
status.deleteSafety.finalizerReleaseAllowed=true
CleanupRequired=False
lifecycle-owner removes only block.seaweedfs.com/swblockvolume-protection
foreign finalizers, if present, are preserved
SwBlockVolume deletion completes
```

Fail if lifecycle-owner changes `.spec`, `.status`, labels, annotations, owner
references, PVCs, PVs, pods, storage classes, or any finalizer other than the
Seaweed Block protection finalizer.

## G5 — Surface Agreement

For G2/G3/G4, compare:

```text
kubectl get/describe SwBlockVolume
sw-block ops report
sw-block ops explain
sw-block ops dashboard /operator-snapshot.json
Kubernetes Events
```

Required:

```text
same status
same reasonCode
same deleteSafety decision/state/reason
same safe action mode and mutationAllowed=false
no false Ready=True in missing/stale/blocked delete states
```

## G6 — Cleanup

Uninstall the chart and run the cleanup verifier.

Required:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
multipath_residue_count=0
process_residue_count=0
hostpath_residue_count=0
failure_count=0
```

The lab must have no stuck `SwBlockVolume`, PVC/PV, pod, helm, VAP, iSCSI,
multipath, dmsetup, or per-host process residue.

## Verdict

D3/D4 pass only if unsafe evidence holds the finalizer, clean fresh evidence
releases it, all user surfaces agree, and the final cleanup verifier is clean.
