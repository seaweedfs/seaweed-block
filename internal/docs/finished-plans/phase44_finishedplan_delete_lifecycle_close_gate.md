# Phase 44 Finished Plan: Delete Lifecycle Close Gate

Status: closed on 2026-06-17.

Branch: `phase41-lifecycle-owner-foundation`

## Outcome

Phase 44 closes the integrated Kubernetes delete lifecycle for the bounded
`SwBlockVolume` protection finalizer:

```text
install -> first PVC -> CSI-created SwBlockVolume CR -> lifecycle-owner
protection finalizer -> operator-status evidence -> delete request ->
hold on missing/blocked evidence -> release on clean evidence -> CR deletion
completes -> uninstall leaves zero residue
```

This is the first end-to-end product path where CSI identity ownership,
operator-status judgment, lifecycle-owner mutation, Kubernetes Events, user
surfaces, and cleanup evidence compose as one operation.

## Delivered

- CSI creates or updates the `SwBlockVolume` identity CR after successful
  `CreateVolume` when operator/lifecycle surfaces are enabled.
- Ownership is split cleanly:
  - CSI owns metadata/spec identity for the `SwBlockVolume`,
  - operator-status owns `.status` and Events,
  - lifecycle-owner owns only
    `block.seaweedfs.com/swblockvolume-protection`.
- Normal Day-1 PVC path now creates the protected CR without manual stubs.
- operator-status projects live delete-safety for terminating
  `SwBlockVolume` objects.
- `cleanup-summary.txt` remains external evidence:
  - operator-status, report, dashboard, and explain can consume it,
  - no controller runs cleanup,
  - unsafe or missing evidence holds the finalizer,
  - clean fresh evidence allows release.
- lifecycle-owner releases only the Seaweed Block protection finalizer and
  preserves foreign finalizers.
- Deleting CR status is negative-first:
  - missing evidence -> `Ready=Unknown`,
  - blocked residue -> `Ready=False` / `Blocked=True`,
  - no false `Ready=True`.
- Multi-volume delete-safety is isolated:
  - held volumes do not block releasable volumes,
  - non-deleting protected volumes are not released,
  - Events/status keep the correct volume identity.
- operator-status skips a disappeared CR status patch instead of aborting the
  whole reconcile.
- `ops explain volume --cleanup-summary` can explain a deleting CR even when
  the live inventory no longer contains the managed volume.

## QA Evidence

- D2 integrated SwBlockVolume creation and protection:
  `internal/docs/qa-assignments/phase44-d2-integrated-swblockvolume-cr-qa-signoff.md`
  - QA PASS on m02 (`k3s v1.34.4+k3s1`) with real
    ValidatingAdmissionPolicy enforcement,
  - normal PVC creates exactly one `SwBlockVolume` CR,
  - lifecycle-owner adds exactly one protection finalizer,
  - operator-status writes `Ready=True` / `first_volume_verified`,
  - CSI/operator-status/lifecycle-owner RBAC ownership split holds,
  - cleanup verifier reports zero residue.
- D3/D4 delete hold/release close gate:
  `internal/docs/qa-assignments/phase44-d3-d4-delete-lifecycle-close-gate-qa-signoff.md`
  - QA PASS on m02 with fresh images from `8669d4a`,
  - missing cleanup evidence holds the finalizer,
  - blocked residue evidence holds the finalizer,
  - clean fresh evidence releases only the protection finalizer,
  - CR deletion completes,
  - no cleanup is executed by the controllers,
  - final cleanup verifier reports zero residue.
- D5/D6 surface agreement and multi-volume isolation:
  `internal/docs/qa-assignments/phase44-d5-d6-surface-isolation-close-qa-signoff.md`
  - QA PASS on m02 with fresh images from `874d0cf`,
  - report/dashboard agree with CRD status for deleting volumes,
  - multi-volume hold/release is isolated,
  - disappeared CR status patch is skipped,
  - final cleanup verifier reports zero residue.

Post-QA polish is committed in `041b084`: `ops explain` now renders deleting CR
delete-safety from `--cleanup-summary` instead of returning "not found in
inventory".

## Release Claim

Phase 44 supports this narrow claim:

```text
Seaweed Block can protect its SwBlockVolume CR with a bounded finalizer and
release that finalizer only after externally supplied cleanup evidence is clean.
```

The claim includes the integrated Day-1 path, CRD status, Events, report,
dashboard, explain, multi-volume isolation, and zero-residue cleanup verification
on the validated lab.

## Non-Claims

Phase 44 does not claim automatic cleanup execution, PVC/PV/workload deletion,
host repair, iSCSI/multipath repair, returned-replica rebuild, failback,
backup/restore, NVMe ANA parity expansion, upgrade execution, production SLOs,
or broad cluster compatibility.

The lifecycle-owner owns only the Seaweed Block `SwBlockVolume` protection
finalizer. It does not own PVC/PV finalizers.

## Required Carry-Forward

- Publish `sw-block` and `sw-block-csi` images from the same commit before a
  public release note claims the integrated lifecycle path.
- Run a release-candidate smoke against the pinned images if image digests
  change after `041b084`.
- Keep the release wording narrow: bounded finalizer lifecycle and
  evidence-driven hold/release, not automatic cleanup or full operator
  automation.
