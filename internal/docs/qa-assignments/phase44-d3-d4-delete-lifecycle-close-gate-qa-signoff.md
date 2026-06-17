# QA Sign-off — Phase 44 D3/D4 Delete Lifecycle Close Gate

Verdict: **PASS.** The integrated delete lifecycle works end-to-end on a real
VAP-capable cluster: a normal PVC yields a protected `SwBlockVolume`; a delete
request **holds** the protection finalizer while cleanup evidence is missing or
shows residue; clean fresh `--cleanup-summary` evidence makes `deleteSafety`
releasable; the lifecycle-owner removes **only** the protection finalizer; the CR
deletion completes; and uninstall leaves zero residue. No surface shows a false
`Ready=True` in any missing/blocked delete state.

Date: 2026-06-16
Source: branch `phase41-lifecycle-owner-foundation` @ `8669d4a phase44: project
live delete safety status`
Images: fresh `sw-block:local` **and** `sw-block-csi:local` built from `8669d4a`,
imported to m01+m02.
Environment: m02 k3s **v1.34.4+k3s1** (ValidatingAdmissionPolicy enforced).
Install: `--set operatorStatus.create=true --set operatorStatus.dryRun=false
--set lifecycleOwner.create=true --set lifecycleOwner.dryRun=false`.

Ownership split confirmed (live `can-i`): CSI SA `sw-block-seaweed-block-csi`
create/patch main `swblockvolumes`, no `/status`, no `/finalizers`;
operator-status SA patch `/status` + events only, no main/finalizers;
lifecycle-owner SA main patch VAP-confined to the protection finalizer, no
`/status`.

## G1 — Day-1 Baseline — PASS

Install + first-volume writer/reader ok; exactly one CR `sw-block-example-pvc`
(`.spec.pvcName` matches), `finalizers=[protection]` (one), `status=ready`,
`reasonCode=first_volume_verified`, `Ready=True`, `deleteSafety=null`.

## G2 — Missing Evidence Holds — PASS

Deleted the `SwBlockVolume` (protection finalizer present, no cleanup evidence);
the running operator-status projected, within its interval:

```text
SwBlockVolume Terminating (held by protection finalizer)
status.status=unknown   reasonCode=cleanup_evidence_missing
deleteSafety.decision=unknown  state=requested  reason=cleanup_evidence_missing
Ready=Unknown   (no false Ready=True)
lifecycle-owner: finalizer_held=1  finalizer_released=0
Event: Warning cleanup_evidence_missing "...held until delete-safety evidence allows release"
```

## G3 — Blocked Residue Holds — PASS

Fed a residue `--cleanup-summary` (`iscsi_residue_count=1`,
`reason_codes=iscsi_node_records_present`) to a one-shot
`operator-status --cleanup-summary` (run in-cluster as the operator-status SA;
the in-cluster controller scaled to 0 so its loop did not overwrite):

```text
deleteSafety.decision=rejected  state=blocked
status.status=blocked  reasonCode=iscsi_node_records_present
CleanupRequired=True   Ready=False   (no false Ready=True)
allowedActions observe.verify_cleanup  mode=scripted  mutationAllowed=false
operator-status: mutation_allowed=false  finalizer_patches=0   (no cleanup executed)
lifecycle-owner: finalizer_held continues, finalizer_released=0  (finalizer still held)
```

Blocked residue did not release the finalizer.

## G4 — Clean Evidence Releases — PASS

Fed a clean `--cleanup-summary` (`cleanup_status=ok`, all residue 0, fresh
`cleanup_observed_at`):

```text
deleteSafety.decision=allowed  state=releasable  finalizerReleaseAllowed=true
CleanupRequired=False
lifecycle-owner: finalizer_released=1  -> removes only the protection finalizer
SwBlockVolume reaches NotFound (deletion completes)
Event: Normal finalizer_released "...released after clean delete-safety evidence"
```

Only the protection finalizer was removed; no `.spec`/`.status`/labels/
annotations/ownerReferences or any PVC/PV/pod/storageclass mutation. (Foreign-
finalizer preservation was proven separately in Phase 43 D3/D4 and the
lifecycle-owner code is unchanged here.)

## G5 — Surface Agreement — PASS (with a scope note)

For each deleting state the authoritative surfaces operator-status writes — the
**CRD status** (`kubectl get`) and **Kubernetes Events** — agree, and **no surface
shows a false `Ready=True`** in the missing/blocked states:

```text
missing  : CRD status=unknown/Ready=Unknown/cleanup_evidence_missing  + Warning cleanup_evidence_missing
blocked  : CRD status=blocked/Ready=False/iscsi_node_records_present/CleanupRequired=True
           + Warning iscsi_node_records_present "delete is blocked until cleanup evidence is clean"
releasable: CRD deleteSafety allowed/releasable/finalizerReleaseAllowed=true/CleanupRequired=False
           + Normal finalizer_released
```

Scope note (non-blocking): `ops explain` / `ops report` do **not** accept
`--cleanup-summary`, so the CLI/dashboard consumer surfaces do not independently
re-derive a *deleting* CR's `deleteSafety` from a cleanup-summary — that
projection is operator-status's job and is surfaced via CRD status + Events. The
core G5 requirement (consistent state, no false `Ready=True`) is met on those
surfaces.

## G6 — Cleanup — PASS

```text
cleanup_status=ok
k8s_residue_count=0  iscsi_residue_count=0  multipath_residue_count=0
process_residue_count=0  hostpath_residue_count=0  failure_count=0
swblockvolumes=0  pvc=0  pods=0  helm=0  lifecycle-owner VAP=0
```

## Blocking Findings

None. Unsafe (missing/blocked) evidence held the finalizer; clean fresh evidence
released only the protection finalizer and deletion completed; surfaces agree with
no false `Ready=True`; the final cleanup verifier is clean; neither controller
executed cleanup or mutated PVC/PV/workload/storage.

## Non-Blocking Findings

1. **Deleting-CR `deleteSafety` is not re-derivable from `ops explain`/`report`.**
   Those CLI surfaces lack `--cleanup-summary`; the deleting projection is visible
   only via CRD status + Events (which agree). Consider plumbing the cleanup
   evidence (or reading the live CRD `deleteSafety`) into report/explain/dashboard
   so a cold support bundle of a deleting volume shows the same hold/release
   reasoning.
2. **Operator-status 404 on a managed volume whose CR was already deleted.** After
   G4 released + deleted the CR while the PVC/managed volume still existed, the
   master-observation projection tried to patch the gone CR and the one-shot
   exited 2. Harmless in the real ordering (PVC delete removes the managed volume),
   but a deleted-CR-for-still-managed-volume should be skipped rather than abort
   the reconcile.
3. New CSI CR-registration + delete-safety bridge require publishing both
   candidate images together. tp01 `NotReady` — unrelated to this single-node gate.

## Recommendation

**Phase 44 D3/D4 pass.** The bounded delete lifecycle is now real-API-proven
end-to-end: install → PVC → protected CR → delete-request → hold-on-unsafe →
release-on-clean → deletion completes → zero residue, with the three-way ownership
split intact and no false `Ready=True`. Combined with D2, the integrated Day-1 and
delete paths are complete. The two non-blocking items (consumer-surface coverage
of the deleting projection; skip-deleted-CR in the master projection) are good
polish before the public delete-lifecycle release note.

## Post-QA Dev Follow-up

Addressed after this QA pass:

- `ops report`, `ops explain`, and `ops dashboard` now accept
  `--cleanup-summary` and, when run in-cluster, use the live `SwBlockVolume` list
  to project deleting-CR `deleteSafety` with the same logic as operator-status.
- operator-status now skips a `404 NotFound` volume status patch when the
  `SwBlockVolume` CR disappeared between observation and write, instead of
  aborting the whole reconcile.
