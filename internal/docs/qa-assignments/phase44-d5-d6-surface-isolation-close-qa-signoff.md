# QA Sign-off — Phase 44 D5/D6 Surface Agreement, Multi-Volume Isolation, Close

Verdict: **PASS** (one non-blocking gap in the `ops explain` surface). The post-QA
fixes from `874d0cf` are validated: the report/dashboard consumer surfaces now
project a deleting CR's `deleteSafety` from `--cleanup-summary` and agree with the
CRD; operator-status no longer aborts when a managed volume's CR has disappeared;
multiple deleting volumes are held and released independently with no cross-volume
contamination; and the lab closes with zero residue.

Date: 2026-06-16
Source: branch `phase41-lifecycle-owner-foundation` @ `874d0cf phase44: polish
delete lifecycle surfaces`
Images: fresh `sw-block:local` + `sw-block-csi:local` from `874d0cf`, m01+m02.
Environment: m02 k3s **v1.34.4+k3s1** (VAP enforced). Install: operator-status +
lifecycle-owner enabled, non-dry-run.

## D5 — Surface Agreement With `--cleanup-summary`

`--cleanup-summary` is now wired into `ops report`, `ops explain`, and `ops
dashboard` (new `applyCleanupSummaryProjection` → `ProjectSwBlockVolumeDeleteSafety`,
in-cluster). With a deleting `SwBlockVolume` projected blocked:

```text
CRD (kubectl):            status=blocked  deleteSafety=rejected/blocked  Ready=False
report operator-snapshot: status=blocked  reason_code=iscsi_node_records_present
                          blocked_volume_count=1  evidence_ref=/tmp/blocked.txt
                          Ready condition=False   (no false Ready=True)
dashboard:                serves the same operator-snapshot
```

**report + dashboard agree with the CRD** and show no false `Ready=True` for the
deleting/blocked volume — fix verified for those surfaces.

**Non-blocking gap:** `ops explain volume <name>` consistently returns
`volume "<name>" not found in inventory` for a *deleting* volume (reproduced with
`--master`, `--master-api`, on both a real managed+deleting volume and a pure
synthetic deleting CR). `explain` accepts `--cleanup-summary` but its volume
lookup does not include deleting CRs, so a user cannot `ops explain` a deleting
volume's hold/release reasoning. CRD status, Events, report, and dashboard all
surface it, so this is non-blocking — but the `explain` half of the `874d0cf` fix
is incomplete.

## D6 — Multi-Volume Isolation

Three deleting `SwBlockVolume` CRs (d6a/d6b/d6c, each with the protection
finalizer) plus one non-deleting CR (d6keep). One `operator-status
--cleanup-summary` projection each:

```text
blocked summary -> d6a/d6b/d6c: each independently  status=blocked
                   deleteSafety=rejected/blocked  Ready=False
                   d6keep (not deleting): no deleteSafety, untouched
                   lifecycle-owner: holds all three
clean summary   -> d6a/d6b/d6c: each releasable -> lifecycle-owner removes only the
                   protection finalizer -> all three reach GONE (deleted)
                   d6keep: still EXISTS with [protection] (never released)
                   lifecycle-owner log: volumes=4 finalizer_patches=3 finalizer_released=3
                   next iteration: volumes=1 released=0
```

Per-volume isolation holds in both hold and release: each deleting volume gets its
own decision/Event, the non-deleting volume is never given a `deleteSafety` and is
never released, and the lifecycle-owner released exactly the three deleting volumes
(not the protected one). No cross-volume contamination.

## 404-Skip Fix (post-QA finding #2)

Re-provisioned a real PVC (managed volume + CR), then deleted the CR while keeping
the PVC (master still tracks the volume, CR gone) and ran operator-status:

```text
operator_status=write_status  volumes=0  events=0  one_shot_exit=0
```

operator-status **skips** the disappeared CR and exits 0, instead of the D3/D4
behavior of aborting the reconcile with exit 2 on the `404 NotFound` status patch.
Fix verified.

## Close — Zero Residue

```text
helm uninstall: release uninstalled
verify-helm-cleanup.sh: cleanup_status=ok  k8s/iscsi/multipath/process/hostpath residue=0  failure_count=0
swblockvolumes=0  pvc=0  pods=0  helm=0  lifecycle-owner VAP=0
```

## Blocking Findings

None. Surface agreement holds on CRD/Events/report/dashboard with no false
`Ready=True`; multi-volume hold/release is per-volume isolated; the 404-skip fix
works; the lab is clean.

## Non-Blocking Findings

1. **`ops explain` cannot look up a deleting volume** ("not found in inventory")
   despite accepting `--cleanup-summary`. The `report`/`dashboard` half of the
   `874d0cf` surface fix works; the `explain` half does not surface deleting-CR
   `deleteSafety`. Recommend including deleting CRs (the projected cluster
   evidence) in `explain`'s volume inventory so `ops explain <deleting-volume>`
   shows the same hold/release reasoning.
2. `ops explain` uses `--master` (not `--master-api`); the slim image has no
   python3 — minor tooling notes for future QA scripting.
3. tp01 `NotReady` — unrelated to this single-node gate.

## Recommendation

**Phase 44 D5/D6 pass.** The delete-lifecycle surface projection (report/dashboard)
and multi-volume isolation are real-API-proven, and the two D3/D4 follow-ups
(consumer-surface `--cleanup-summary`, 404-skip) are addressed — report/dashboard
fully, `explain` partially. With D2 + D3/D4 + D5/D6, the integrated Day-1 and
bounded delete lifecycle are complete end-to-end. The single remaining polish is
the `ops explain` deleting-volume lookup before the public delete-lifecycle
release note; it does not block, since four surfaces (CRD, Events, report,
dashboard) already agree.

## Post-QA Dev Follow-up

Addressed after this QA pass:

- `ops explain volume --cleanup-summary <file> <volume>` now allows a missing
  live inventory volume when cleanup-summary evidence is supplied, then uses the
  live deleting `SwBlockVolume` CR projection to render the same delete-safety
  hold/release reasoning as report/dashboard.
- Regression: empty live inventory + deleting `SwBlockVolume` CR + residue
  cleanup-summary now renders `status=blocked`,
  `reason=iscsi_node_records_present`, and the dry-run
  `safe_k8s.release_swblockvolume_finalizer` action instead of
  `volume "<name>" not found in inventory`.
