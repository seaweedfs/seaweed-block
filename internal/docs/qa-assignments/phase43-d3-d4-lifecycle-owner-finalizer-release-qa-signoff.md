# QA Sign-off — Phase 43 D3/D4 Delete-Safety Gated Finalizer Release

Verdict: **PASS.** The lifecycle-owner release half works and is correctly gated:
it **holds** the Seaweed Block protection finalizer while delete-safety is
missing, blocked, or stale, and **releases only** that finalizer when
`status.deleteSafety` says release is allowed — preserving foreign finalizers,
mutating no non-finalizer field, and executing no cleanup. Validated live on a
real VAP-capable cluster. With D1/D2 (add) already PASS, the bounded delete
lifecycle (add + release) is complete.

Date: 2026-06-15
Source: branch `phase41-lifecycle-owner-foundation` @ `252ec35 phase43: gate
finalizer release on delete safety`
Image: fresh local build from `252ec35` (`sw-block:local`), imported to m01+m02.
Environment: m02 k3s **v1.34.4+k3s1** (ValidatingAdmissionPolicy enforced).
Install: `helm install ... --set operatorStatus.create=true
--set operatorStatus.dryRun=false --set lifecycleOwner.create=true
--set lifecycleOwner.dryRun=false --set lifecycleOwner.interval=15s` (pods 1/1).

## G1 — Local Contract — PASS

`go test ./core/ops ./cmd/sw-block` → `ok` (both); `helm lint
charts/seaweed-block` → 1 chart linted, 0 failed.

## G2 — Hold On Missing / Blocked / Stale Evidence — PASS

Three deleting `SwBlockVolume` objects (each with the protection finalizer and a
deletionTimestamp), reconciled once:

```text
hold-missing  (no status.deleteSafety)                              -> finalizers stay [protection]
hold-blocked  (state=blocked, decision=rejected)                    -> finalizers stay [protection]
hold-stale    (state=requested, decision=unknown,
               reason=cleanup_evidence_stale)                       -> finalizers stay [protection]

controller log (steady state): volumes=4 finalizer_held=3 finalizer_released=0
Warning Events (each names the hold reason, none claim release):
  cleanup_evidence_missing  swblockvolume/hold-missing
  iscsi_node_records_present swblockvolume/hold-blocked
  cleanup_evidence_stale    swblockvolume/hold-stale
  message: "Seaweed Block protection finalizer held until delete-safety evidence allows release"
```

All three keep the protection finalizer; no patch removes it; the hold is reported
with a stable reason via a Warning Event; no cleanup command is executed (the
lifecycle-owner only patches `metadata.finalizers`).

## G3 — Release On Clean Fresh Evidence — PASS

One deleting `SwBlockVolume` with `finalizers=[example.com/foreign,
block.seaweedfs.com/swblockvolume-protection]` and
`status.deleteSafety={state=releasable, decision=allowed,
finalizerReleaseAllowed=true}`, reconciled once:

```text
finalizers: [example.com/foreign, protection]  ->  [example.com/foreign]
spec.pvcName=release-clean  labels.keep=true  annotations.keep=true  deleteSafety.state=releasable  (all unchanged)
controller log: finalizer_released=1   (first iteration)
Event: Normal finalizer_released swblockvolume/release-clean
       "Seaweed Block protection finalizer released after clean delete-safety evidence"
after removing the foreign finalizer (admin): object reaches NotFound (deletion finishes)
```

Only the protection finalizer is removed; the foreign finalizer is preserved; no
non-finalizer field changes; the object can finish deletion once the Seaweed Block
finalizer is gone; a single bounded Normal `finalizer_released` Event is emitted.

## G4 — Admission / RBAC Boundary — PASS

`252ec35` does not touch `lifecycle-owner-rbac.yaml`,
`lifecycle-owner-admission.yaml`, or `operator-status-rbac.yaml` (git diff empty
vs D1/D2 `1244285`), so the D1/D2 full matrix (operator-status no main patch;
lifecycle-owner finalizer-only main patch; 36/36 forbidden resource×verb denied;
spec/labels/annotations/ownerReferences/foreign/mixed denied) still applies. Live
spot-check confirmed:

```text
operator-status patch swblockvolumes(main)   => no
lifecycle-owner patch swblockvolumes(main)   => yes (VAP-confined to finalizers)
lifecycle-owner spec patch                    => denied (VAP)
lifecycle-owner patch pods                     => no
lifecycle-owner delete persistentvolumeclaims  => no
```

## G5 — Cleanup — PASS

```text
helm uninstall: release uninstalled
verify-helm-cleanup.sh: cleanup_status=ok   cleanup_observed_at=2026-06-16T05:17:05Z
swblockvolumes=0  lifecycle-owner VAP/binding=0/0  RBAC=0/0  pods=0  helm=0
```

No stuck `SwBlockVolume`, VAP, binding, RBAC, pods, PVCs, PVs, iSCSI, multipath,
or dmsetup residue.

## Blocking Findings

None. No blocking condition: missing/blocked/stale evidence never released the
finalizer; clean releasable evidence removed only the protection finalizer (foreign
preserved); no non-finalizer field changed; no cleanup was executed; the
lifecycle-owner gained no workload/storage mutation power.

## Non-Blocking Findings

1. Release requires the exact triple `finalizerReleaseAllowed=true` **and**
   `decision=allowed` **and** `state=releasable` (`lifecycleOwnerReleaseAllowed`).
   A partial/clean-but-not-fully-releasable decision correctly holds. Worth a doc
   note so operators understand which delete-safety shape releases.
2. The `status.deleteSafety` here was set directly for the gate; in production it
   is projected by operator-status from real cleanup evidence. The end-to-end
   path (real residue → operator-status projects blocked/releasable →
   lifecycle-owner holds/releases) is the natural Phase 44 close-gate scenario.
3. tp01 `NotReady` — unrelated to this single-node gate.

## Recommendation

**Phase 43 D3/D4 pass.** The delete-safety-gated finalizer release is
real-API-proven: hold on missing/blocked/stale, release-only-protection on
clean/releasable, foreign-preserving, non-mutating, cleanup-free, with
operator-status untouched and zero residue. Combined with D1/D2, Phase 43 delivers
the full bounded `SwBlockVolume` protection-finalizer lifecycle (add + release).
The remaining work toward a delete-lifecycle release is the end-to-end close gate
(Phase 44): real cleanup evidence → operator-status projection → lifecycle-owner
hold/release as one user-visible path.
