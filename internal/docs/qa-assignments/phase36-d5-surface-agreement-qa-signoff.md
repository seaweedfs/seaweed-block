# QA Sign-off - Phase 36 D5 Surface Agreement And Negative-First Gates

Verdict: **PASS (all six gates).** Across healthy, blocked, unknown/stale,
cleanup-required, and multi-volume paths, the user-facing surfaces (CRD status,
operator-snapshot, summary.txt, index.html, dashboard, `ops explain`, Events)
agree on the same operational truth, and the negative-first rule holds: no
blocked/unknown/stale/cleanup-required evidence is ever shown as a false
`Ready=True`. Event identity stays bounded and the SA holds no mutation power.

Three environment/build findings surfaced during the live G1 path (none are D5
surface *disagreements* — the surfaces stayed internally consistent — but they
are real and two reinforce the open D2 live-node-evidence gap). See Findings.

Date: 2026-06-05

Source commit: `2390bbb phase36: add surface agreement gate`
(branch `phase33-testops-failure-hardening`; includes the D4 polish
`531d124 phase36: close cleanup visibility gate`)

Environment: 3-node k3s `v1.34.4+k3s1`, write-mode operator-status, fresh
`2390bbb` images. G1 live; G2–G5 from-bundle under dedicated cluster names so the
live controller did not race the gate.

## G1 — Healthy First Volume — PASS

Live Helm first-volume (PVC `d5-vol` + writer/reader pod):

```text
SwBlockVolume.status: status=ready reason=first_volume_verified Ready=True/first_volume_verified
SwBlockCluster.status.readyVolumeCount=1 volumeCount=1
operator-snapshot.json: status=ready ; summary.txt: managed_volume=… status=ready reason=first_volume_verified
dashboard /operator-snapshot.json: first_volume_verified, status=ready
Event: Normal / first_volume_verified  (name d5-vol-normal-first-volume-verified)
writer/reader: pod 1/1 Running, log "D5_DATA" (wrote + read back on m02)
```

All surfaces agree; the consuming pod actually writes+reads (after the two
environment fixes in Findings). PASS.

## G2 — Blocked Path — PASS

CSI image-pull blocked bundle → `SwBlockVolume/unknown`:

```text
status=blocked reason=csi_node_image_pull_failed  Ready=False  Blocked=True
safeNextSteps: observe.collect_bundle(read_only,mut=false), safe_k8s.import_csi_image(dry_run,mut=false)
report ready=true count across all artifacts: 0
ops explain: reason=csi_node_image_pull_failed
Event: Warning / csi_node_image_pull_failed
```

No `Ready=True` anywhere; safe steps are read_only/dry_run with
`mutationAllowed=false`; Warning event reason matches. PASS.

## G3 — Unknown / Stale Evidence — PASS

`status_endpoint_unreachable` bundle → `SwBlockVolume/pvc-stale`:

```text
status=unknown reason=status_endpoint_unreachable  Ready=Unknown  EvidenceStale=True  (no Blocked condition)
summary.txt: managed_volume=pvc-stale status=unknown reason=status_endpoint_unreachable
             managed_volume_condition=Ready status=Unknown ; =EvidenceStale status=True
ops explain: reason=status_endpoint_unreachable
```

`Ready=Unknown` (not True/False), `EvidenceStale=True`, and **no inappropriate
`Blocked=True`** for pure unreachable evidence.

**Managed-only sub-case (the new D5 behavior):** with `volumes[]` emptied and only
`managed_volumes[]` present, `summary.txt` still renders `managed_volume=` and
both `managed_volume_condition=` lines (0 raw `volume=` lines). The managed-only
stale rendering works. PASS.

## G4 — Cleanup Required — PASS

Verifier-format residue summary (`cleanup_status=failed, iscsi_residue_count=1`)
→ `SwBlockCluster/g4-cluster`:

```text
status.cleanup.status=failed
CleanupRequired=True reason=cleanup_required
safeNextSteps: observe.verify_cleanup mode=scripted mutationAllowed=false
```

(Full cleanup surface agreement — CRD/snapshot/summary/index.html — was proven
exhaustively in the D4 sign-off; D5 confirms the residue path under the
surface-agreement lens.) PASS.

## G5 — Multi-Volume Sanity — PASS

Three healthy RF3 volumes (pvc-a/b/c, distinct primaries r1/r2/r3, distinct
publish targets 181/184/188):

```text
volumeCount=3  readyVolumeCount=3
SwBlockVolume pvc-a/pvc-b/pvc-c: all status=ready reason=first_volume_verified, distinct volume_id
operator-snapshot.json: 3 distinct volume_ids
publish targets: 3 distinct (192.168.1.181/184/188:3260) — no duplicate
summary.txt: 3 managed_volume lines; volume lines show distinct primary+frontend, no cross-volume mix-up
```

Three distinct identities, three distinct volume_id/pvc_name, no duplicate
publish target, one managed_volume line per volume, no reason/status mix-up. PASS.

## G6 — Event Identity And Boundary — PASS

```text
Event identity: 3 total reconciles of the blocked volume -> 1 distinct Event object
  (stable object+type+reason name; no unbounded duplicates)  [confirms D6]
Boundary (operator-status SA):
  ALLOWED:  create events: yes   patch swblockclusters --subresource=status: yes
  FORBIDDEN: patch swblockvolumes (spec): no   create pods: no   create persistentvolumeclaims: no
             delete persistentvolumes: no   create secrets: no   patch deployments.apps: no
             create storageclasses.storage.k8s.io: no
```

PASS.

## Final Cleanup Verifier — cleanup_status=ok

After teardown (and after clearing the residue noted in Finding 3), the real
`verify-helm-cleanup.sh` reports `cleanup_status=ok, iscsi_residue_count=0,
failure_count=0`. Pass criterion met.

## Findings (environment/build; not D5 surface disagreements)

### F1 — build-alpha-images.sh does not import the CSI image into the build host's k3s (recurring; masked by status)

`sw-block-csi:local` was present on m01/tp01 (imported via
`SW_BLOCK_IMPORT_K3S_NODES`) but **absent from m02's k3s containerd** — only
present in m02's *docker*. The build host (m02) imports `sw-block:local` locally
but not `sw-block-csi:local`. Result on the default install: `sw-block-csi-node`
on m02 is `Init:ErrImageNeverPull`, `CSINode m02` lists no driver, and any pod
scheduled on m02 fails `AttachVolume` with
`CSINode m02 does not contain driver block.csi.seaweedfs.com`.

This is **recurring** — it is almost certainly the same condition I previously
mis-attributed to "iSCSI mount latency" in the Phase 35 D3 live first-volume
(its pod never reached Running either). Worked around by
`docker save sw-block-csi:local | k3s ctr -n k8s.io images import -` on m02 +
restarting the m02 csi-node, after which the driver registered.

Crucially, the status surface showed the volume `ready/first_volume_verified`
and node m02 `ready/node_ready` the whole time — a live instance of the **D2
node-evidence gap** (live node evidence hardcodes ready/schedulable and does not
detect a missing CSI image). Recommend: (a) fix the build to import the CSI image
into the build host's k3s; (b) the D2 live-node follow-up (detect
missing-image/CSINode-unregistered) so this is not masked.

### F2 — default single-node install uses a loopback publish target (cross-node unusable)

The default chart `blockNodes[0].internalIP=127.0.0.1`, so the iSCSI target
publishes on `127.0.0.1:3260`. A consumer pod on any node other than m02 attaches
but fails to mount: `iscsiadm: cannot make connection to 127.0.0.1: Connection
refused`. The volume is only usable by pods pinned to m02. The product already
has a `publish_target_loopback_cross_node` reason for this class; the default
install simply ships the loopback value. Pinning `d5-rw` to m02 made it mount and
write/read cleanly. Non-blocking for D5, but worth a note in the default-install
docs (single-node default = m02-local consumers only).

### F3 — force-deleting a pod with a mounted volume leaves a stale iSCSI node DB record

After `kubectl delete pod --force --grace-period=0` on `d5-rw` and deleting its
PVC, the final verifier flagged `iscsi_residue_count=1`: a stale node record
`iqn.2026-05.io.seaweedfs:pvc-…@127.0.0.1:3260` (no active session — orderly CSI
`NodeUnstageVolume` was skipped by the force delete). Cleared with
`iscsiadm -m node -o delete`; re-verify `cleanup_status=ok`. Same residue class
as the Minimal-Release B1 finding (node DB records not scrubbed). Operationally,
prefer graceful pod deletion; the cleanup verifier correctly catches the residue.

## Lab State

Clean — live pod/PVC deleted, all `SwBlockVolume`/`SwBlockCluster` stubs deleted,
Events deleted, helm uninstalled, both CRDs deleted, stale iSCSI node record
removed; final verifier `cleanup_status=ok`; 0 sw-block pods, 0 CRDs.

## Bottom Line

- **D5 PASS (G1–G6).** Healthy/blocked/unknown-stale/cleanup-required/multi-volume
  surfaces all agree; negative-first holds (no false `Ready=True` for blocked,
  unknown, stale, or cleanup-required evidence); the new managed-only stale
  rendering works; Event identity is bounded; the SA can only patch `/status` and
  create Events. Final cleanup verifier `cleanup_status=ok`.
- **Three environment/build findings**, none a surface disagreement: F1
  (CSI image not imported to build-host k3s — recurring, masked by the D2 gap),
  F2 (default loopback publish target = m02-local only), F3 (force-delete leaves a
  stale iSCSI node record). F1 is the most actionable — it has been silently
  failing live consumer pods since at least Phase 35 D3.
- **D5 can close.** Recommend filing F1 (build import) and re-confirming the D2
  live-node follow-up, which would have surfaced F1 as a real node blocker instead
  of a hidden one.
