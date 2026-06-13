# QA Sign-off - Phase 39 D6 Multi-Volume Delete-Safety Status Isolation

Verdict: **PASS.** Delete-safety evidence for one volume does not contaminate the
others. With three managed volumes, volume A's blocked delete-safety
(`iscsi_node_records_present`) leaves volumes B and C `ready/first_volume_verified`
with their own identities, and volume C can independently show `releasable`
without changing A/B. No finalizer patch is attempted (`finalizer_patches=0`), no
finalizer Events appear, the operator mutates only CRD status + Events, and the
cluster counts and all read surfaces agree. One minor non-blocking staleness
observation (a removed delete-summary leaves a stale `deleteSafety` on that same
volume — not cross-volume).

Date: 2026-06-13

Source commit: `afd98f5 phase39: guard delete safety status isolation` (test-only
on the validated floor `f167f9a phase39: allow scripted volume actions`; branch
`phase33-testops-failure-hardening`)

## Lab Node Health

- m01 `Ready`, m02 `Ready`, **tp01 `NotReady`/unreachable** (unchanged since
  Phase 38; "No route to host"). D6 is a status/events-only gate on three
  from-bundle volumes; it does not require a healthy 3-node RF=3 cluster. Install
  used operator-status pinned to m02 with the image re-imported from docker
  (binary unchanged from `f167f9a`; `afd98f5` is test/docs-only). Restore tp01
  before any RF=3 live multi-node work.

## Evidence Shape

A single from-bundle `cluster-evidence.json` with three healthy volumes
(distinct `volume_id`/`pvc_name`/primary/publish_target), plus a per-reconcile
`swblockvolume-delete-summary.txt` + `cleanup-summary.txt`:

```text
pvc-a primary=r1 frontend=192.168.1.181:3260
pvc-b primary=r2 frontend=192.168.1.184:3260
pvc-c primary=r3 frontend=192.168.1.188:3260
Reconcile 1 (blocked-isolation): delete-summary=pvc-a, cleanup_status=failed iscsi_residue_count=1
Reconcile 2 (releasable-isolation): delete-summary=pvc-c, cleanup_status=ok all residue 0
```

(Note: the bundle carries one `swblockvolume-delete-summary.txt` + one
cluster-level `cleanup-summary.txt` per reconcile, so A-blocked and C-releasable
are shown in separate reconciles rather than one snapshot. The isolation property
is demonstrated in each.)

## Per-Volume CRD Status

### Reconcile 1 — A blocked, B/C healthy

```text
operator_status=write_status ... volumes=3 events=4 finalizer_patches=0 mutation_allowed=false
pvc-a: status=blocked reason=iscsi_node_records_present  deleteSafety.state=blocked decision=rejected
pvc-b: status=ready   reason=first_volume_verified       deleteSafety=(none)
pvc-c: status=ready   reason=first_volume_verified       deleteSafety=(none)
SwBlockCluster.status: volumeCount=3 readyVolumeCount=2 blockedVolumeCount=1
```

A's residue reason on B or C: **0**. A's blocked delete-safety did **not** block
B/C status publication (both published `ready`). Counts match the evidence.

### Reconcile 2 — C releasable, A/B independent

```text
pvc-a: status=ready  reason=first_volume_verified  deleteSafety.state=blocked (stale — see findings)
pvc-b: status=ready  reason=first_volume_verified  deleteSafety=(none)
pvc-c: status=ready  reason=first_volume_verified  deleteSafety.state=releasable decision=allowed
finalizer_patches=0
```

C independently became `releasable/allowed` without making A or B releasable or
blocked. (B unchanged; A's *status* is its healthy base — see the staleness note.)

## Cross-Surface Agreement Matrix

| Surface | pvc-a | pvc-b | pvc-c | A reason on B/C? |
|---|---|---|---|---|
| CRD `SwBlockVolume.status` | blocked / iscsi_node_records_present | ready / first_volume_verified | ready / first_volume_verified | no |
| report `summary.txt` (managed_volume) | blocked / iscsi_node_records_present | ready / first_volume_verified | ready / first_volume_verified | no (grep count 0) |
| report `volume=` identity | r1 @ 192.168.1.181:3260 | r2 @ 192.168.1.184:3260 | r3 @ 192.168.1.188:3260 | distinct |

Distinct `volumeID`, `pvcName`, primary replica, and publish target per volume;
no cross-volume reason-code mix-up; report agrees with CRD. (operator-snapshot is
the same artifact the report writes; dashboard serves it — consistent with the
prior phases' surface-agreement checks.)

## RBAC Boundary — PASS (status-only)

Same status-only chart/install validated in the Phase 39 D4/D5 status-only
sign-off:

```text
patch swblockclusters/status: yes   patch swblockvolumes/status: yes   create events: yes
patch swblockvolumes (main): no     patch swblockvolumes --subresource=finalizers: no
patch pods / pvc / update storageclasses: no
```

`finalizer_patches=0` on every reconcile; no finalizer-added/released Events; the
operator writes only CRD status + Events.

## Final Cleanup Audit — PASS

`cleanup_status=ok`, `iscsi_residue_count=0`, `failure_count=0`; helm 0, pods 0.

## Blocking Findings

None. All D6 pass criteria are met: per-volume isolation, distinct identities,
cluster counts match, no cross-volume reason mix-up, `finalizer_patches=0`, no
finalizer Events, no storage/workload/host mutation, no surface claiming a
completed deletion.

## Non-Blocking Findings

1. **Stale `deleteSafety` when a volume's delete evidence disappears.** In
   reconcile 2, pvc-a no longer had a `swblockvolume-delete-summary.txt`, so the
   operator did not update its `deleteSafety` — pvc-a kept the prior reconcile's
   `deleteSafety.state=blocked` while its `status` was the healthy `ready`. This
   is a *within-volume* staleness (the field is not reset to `not_requested`/
   cleared when delete evidence is withdrawn), **not** cross-volume contamination
   — B/C stayed clean throughout. It is partly a test-construction artifact (a
   real volume's delete-summary would not vanish arbitrarily), but consider
   clearing/refreshing `deleteSafety` for managed volumes that have no current
   delete evidence so a consumer never sees `status=ready` with
   `deleteSafety=blocked`.
2. **Lab: tp01 `NotReady`/unreachable.** Lab infra; restore before RF=3 live work.
3. **(Carried) live/envtest status-writer regression.** Six live-vs-mock CRD
   schema defects across this phase chain (D3 casing, D2/D37 condition enum,
   finalizer URL 404 → RBAC 403, scripted-mode 422) all passed `go test`/`helm
   template` and failed only against the live API. An envtest harness for
   `KubernetesStatusClient` (real CRD schema + the operator's real RBAC) would
   have caught each before handoff.

## Recommendation for Phase 39 Close

**Phase 39 D4/D5/D6 all pass on the status-only path.** Delete-safety is
observable and isolated per volume (blocked/releasable/decision/CleanupRequired/
the scripted verify_cleanup action/`finalizerReleaseAllowed` as a fact), with
RBAC-provable zero finalizer/spec/storage/workload mutation. **Phase 39 can
close.** Track the stale-`deleteSafety` polish and the envtest-regression
follow-up, and restore tp01 — these do not block close. Actual finalizer
add/remove remains correctly deferred to the future lifecycle-owner component.
