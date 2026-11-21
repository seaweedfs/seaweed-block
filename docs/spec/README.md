# Formal specifications (TLA+)

This directory holds TLA+ models of the sw-block protocols where a bug would
lose or corrupt data: volume and snapshot lifecycle, epoch fencing and
takeover, local WAL crash recovery, the cut (HEAD publish) pipeline, the
control-plane authority store, RF=2 failover, and GC timing.

The models check **designs**, not the Go code. Each one is small, each
abstracts a lot, and each is mapped to the code by hand (see
[Limits](#limits)). They have already paid for themselves: several of the
counterexamples below were turned into fixes (#152, #156, #158, #159).

Code references are `path:line` at main **`5c7c1a7f`** unless a row says
otherwise. Comments inside the `.tla` files cite lines at the commit named in
each file's header; this README is the current map.

## TLA+ and TLC in one paragraph

[TLA+](https://github.com/tlaplus/tlaplus) describes a system as a
state machine: an initial state (`Init`) and a relation (`Next`) that says
which steps may follow any state. Concurrency is modelled as interleaving, so
every order of remote calls, crashes, lost replies and timeouts that the spec
allows is a possible behaviour. **TLC** is the explicit-state model checker:
for a configuration (`cfg/*.cfg`) that fixes small values for the constants
(2 volumes, 4 writes, ...), it enumerates *every* reachable state and checks
the invariants in each. A pass means "no violation exists within these
bounds"; a violation comes with the shortest trace that reaches it. **SANY**
is the parser and semantic checker that ships in the same jar.

## Running

**Tools.** Java 11 or newer (the runs below used Java 17 and 21) and
`tla2tools.jar` **v1.7.4** (TLC2 version 2.19):

```sh
curl -fLO https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
echo "936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88  tla2tools.jar" | sha256sum -c -
```

**Parse only** (seconds; useful after an edit):

```sh
cd docs/spec
java -cp /path/to/tla2tools.jar tla2sany.SANY WorkspaceTakeover.tla
```

**Model-check one configuration.** `run.sh` picks the spec from the config
name prefix:

```sh
cd docs/spec
TLA2TOOLS=/path/to/tla2tools.jar ./run.sh Fix_token
# equivalent raw command:
java -XX:+UseParallelGC -Xmx4g -cp /path/to/tla2tools.jar tlc2.TLC \
  -config cfg/Fix_token.cfg -deadlock -workers auto -metadir states/Fix_token \
  ChunkLifecycleFixed.tla
```

- `-deadlock` turns deadlock checking **off**. Several models have a process
  that loops forever (GC), and bounded models stop at their bounds.
- `-workers auto` uses every core; 8 workers is a good default on a laptop.
- Memory: most configs fit in `-Xmx4g`. The large ones (`Fix_P1`,
  `Fix_P2_aba`, `W4_drift_heads`, anything `_big` or `_large`) want
  `-Xmx8g` or more and a fast disk for `states/`. The `_large` configs ran for
  tens of minutes on 30 workers.
- `ChunkLifecycle.tla` and `ChunkLifecycleFixed.tla` are PlusCal. After
  editing the algorithm, re-translate with
  `java -cp tla2tools.jar pcal.trans -nocfg ChunkLifecycleFixed.tla`.
- For a quick random search of a config too large to finish, add
  `-simulate -depth 400` (not exhaustive).

**Reading results.** `Model checking completed. No error has been found.` is
a pass for that config. `Invariant X is violated` prints the trace.
Two kinds of config are **expected to violate**:

- `*_red_*`, `RED_*`, `Bug_*`: a guard is removed (a constant set to the
  "unsafe" value) or the pre-fix design is used. The violation shows that
  the guard or fix is load-bearing.
- `*_reach_*`, `W_*` invariants: reachability witnesses ("this good state is
  never reached"). Their violation shows that the model can actually do the
  thing (delete, take over, publish), so a pass elsewhere is not vacuous.

## Files

| Spec | Configs | Kind |
|---|---|---|
| `ChunkLifecycleFixed.tla` | `cfg/Fix_*` | current design (PlusCal) |
| `ChunkLifecycle.tla` | `cfg/Bug_*` | historical: the pre-fix design of #152, kept for its counterexamples |
| `WorkspaceTakeover.tla` | `cfg/W4_*`, `cfg/RED_*` | |
| `WALRecovery.tla` | `cfg/S3_wal_*`, `cfg/S3v*`, `cfg/S3r_*`, `cfg/S3m_*`, `cfg/S3f_*` | |
| `SmartWALRecovery.tla` | `cfg/S3_sw_*`, `cfg/S3_sw2_*` | |
| `WALReplica.tla` | `cfg/S3_rep_*` | |
| `CutCapture.tla` | `cfg/S4_*` | |
| `CutPipeline.tla` | `cfg/S4b_*` | |
| `CutCOW.tla` | `cfg/S4c_*` | earlier copy-window abstraction |
| `CutCOW2.tla` | `cfg/S4r2_*` | selected native COW: save I/O and abort/drain |
| `ControlPlane.tla` | `cfg/S5_*` | |
| `ReplicationFailover.tla` | `cfg/S6_*` | design-level |
| `RetentionGCTiming.tla` | `cfg/S9_*` | |
| `RetentionGCMulti.tla` | `cfg/S9b_*` | |

Each `.cfg` names its spec on its first line.

## Specs at a glance

State counts are distinct states for exhaustive passes. "N-step trace" is the
length of TLC's shortest counterexample.

| # | Spec | What it models | Key invariants | Results | Code (at `5c7c1a7f`) |
|---|---|---|---|---|---|
| S1 | `ChunkLifecycleFixed` (+ historical `ChunkLifecycle`) | Chunk HEAD, capture intents, snapshots (name record + attempt path), fork claims, tombstones, GC mark+sweep, and volume delete with a blockmaster deletion reservation. Actors: host, 0-2 snapshot callers, a fork, explicit DeleteSnapshot callers, 1-3 volume deleters with reruns, CreateVolume, GC. Filer semantics: conditional write on HEAD gen, exclusive create, conditional update/delete, uncancellable late create. | `GCSafe`, `SnapOnlyExplicitDelete`, `CommitClean`, `NoPublishAfterCommit`, `AckedChunksSurvive`, `NoIdReuse`, `GiveBackReleases`, temporal `HeadMonotone` | **Fixed design, all pass:** `Fix_P1` 51,375,412; `Fix_P2_aba` 46,245,848; `Fix_mix_small` 29,194,420; `Fix_token` 41,256; `Fix_create` 100. `Fix_mix` no violation in 138M states (20-min limit, incomplete). All `Fix_reach_*` reached. **Historical (pre-fix) counterexamples:** `Bug_P1_mark` (18-step), `Bug_P1_giveback` (23), `Bug_P1_data` (25: GC reclaims an acked snapshot's chunk), `Bug_P2_aba` (22), `Bug_token` (16), `Bug_create` (7). | `core/frontend/chunkstore/volume_delete.go:106` DeleteVolume (incarnation loop `:150`), `:269` markHeadDeleting, `:400` snapshotsBlock, `:314` revertHeadDeleting, `:347` commitHeadPurging; `catalog.go:824` CreateSnapshot (gate `:863`), `:943` claimName, `:771` createResolved, `:1058` DeleteSnapshot, `:1157` Fork, `:1337` claimForkParent, `:709` AbortSnapshot, `:1438` AbortFork; `retention.go:448` markDeleting, `:570` liveClaims; `gc.go:108` Collector.run; `chunk_remote.go:404` CASHead refuses delete states; `core/host/master/services.go:53` CreateVolume (holds workspaceMu `:64`), `:132` MarkVolumeDeleting; `core/lifecycle/deletion_reservations.go:111` Admit; `cmd/sw-block/chunk_volume_delete.go:32` `--request-id` |
| S2 | `WorkspaceTakeover` | A workspace volume moving from host A (epoch 1) to host B by a clean move (detach, revoke, ACK, assign) or a forced move (lease expiry). Blockmaster clock, host clock lag up to `Drift`, lossy and reordered messages, master restart, each host's cut as one remote call per step, takeover CAS, GC intent expiry. Code pinned at `5c293821`. | `NoDualServing`, `NoOldCasAfterTakeover`, `NoLateChunkReachable`, `GCSafe`, `NoLiveIntentExpired`, `CleanNoLoss`, `ForcedLossBounded`, `SeedMatchesTakeover`, temporal `HeadMonotone` | **Rerun with the durable lease horizon (#205, PR #209; model `7074febd`, 2026-11-18; TLC 2.19, 27 configurations, BFS).** **Pass:** `W4_clean` 18,378,104 (depth 63); `W4_forced` 33,690,232 (54); `W4_drift_heads` 82,446,907 (55); `W4_hz_clean` 25,464,387 (63) and `W4_hz_forced` 47,511,490 (54), with failing extension writes (landed or not), one restart, and the new `HostWithinHorizon`, `DiskCoversMemory`; `RED_hz_noclamp_safety` 25,511,482 (no clamp, split-brain invariants: passes, the restart rule covers grants). **Time limit, no violation (incomplete):** `W4_clean_large` 592,158,353 and `W4_forced_large` 592,791,914 (100 min each); `W4_both` 124,916,894 and `W4_hz_both` 132,152,744 (40 min each). **RED:** `RED_hz_noclamp` HostWithinHorizon (9-step: a renewal without the clamp grants validity past the durable horizon); `RED_early_assign` NoDualServing (13); `RED_gc_ageonly` GCSafe (12); `RED_epoch` NoOldCasAfterTakeover (23); `RED_lsn` HeadMonotone (25). `RED_epoch_storeonly`/`_hostonly` pass (213,607 / 322,067); `RED_lsn_storeonly` passes (33,690,232). **Witnesses (expected violated):** `W4_reach_clean`, `W4_reach_forced`, `W4_reach_bcut`, `W4_reach_restart`, `W4_hz_reach_ext` (an extension raises the horizon), `W4_hz_reach_landed` (a failed extension lands on disk), `W4_hz_reach_uncertain_restart`. **Findings (unchanged):** `W4_drift` NoDualServing (17-step: the model's forced move has no clock-skew margin; the code adds it, #158); `W4_forced_exact` ForcedLossExact (17-step, benign, open). Earlier results (before #205): `W4_clean` 6,048,496; `W4_forced` 22,619,644; `W4_drift_heads` 56,242,345; `W4_clean_large` 590,137,925 (depth 70); `W4_forced_large` no violation in 910,907,693 (incomplete). | `core/frontend/chunkstore/chunk_remote.go:390` CASHead (epoch guard `:391`, gen/epoch/LSN guard `:409`, conditional update `:420`); `core/host/volume/workspace_lease.go:57` permits, `:70` Refresh (anchor `:73`, accept `:117`); `core/authority/workspace.go:95` RenewWorkspaceLease, `:222` BeginWorkspaceRevocation, `:237` AckWorkspaceRevocation, `:309` prepareWorkspaceMoveLocked (forced-move margin `:316-320`); `core/host/master/workspace_service.go:109` MoveWorkspace (HEAD read `:116`); `core/authority/authority.go:251` reloadFromStore (deadline reset `:261`); `core/frontend/durable/provider_chunk.go:289` PrepareRestore, `:309` ClaimChunkHead; `core/storage/chunk_restore.go:14`; `core/storage/chunk_layer.go:680` captureLocked (admit `:695`, lsn_behind `:800`, PutIntent `:853`, re-admit `:911`, supersededBy `:916`, CAS `:926`, settle `:972`); `core/frontend/chunkstore/gc.go:221` intent-expiry epoch rule |
| S3 | `WALRecovery`, `SmartWALRecovery`, `WALReplica` | Local WAL crash recovery. **walstore:** a ring of K pages, 1-2 page records, page cache written back in any order, Sync, flusher checkpoint, crash at any point with optionally one corrupted record page; recovery is a pure function of the durable image, with a selectable `Rule`. **smartwal:** in-place extent write plus a metadata ring slot, written back in any order, torn slots. **Replica:** RF=2 classification and promotion eligibility, BASE jump. | `I1` every synced write is recovered (or superseded) or recovery fails closed; `I2` a crash without corruption never reports corruption; `ReplayLastWriter`; `I0Prefix`; `I1Frontier`; `NoForgedReplay`; `I3` the promoted node has every acked write | **walstore, current main** (rule `rootLww`, reuse bounded by the persisted tail): `S3r_rootLww_I1_crashonly`/`_I2`/`_replay` pass (849,003); with an in-memory rebuild jump `S3r_rebuild_rootLww_*` pass (1,460,772). **Pre-fix code:** `S3_wal_current_I1_crashonly` I1 violated (synced write lost after a plain crash, #156); `S3v_code_lww_I1_crashonly` I1 violated (LWW replay alone is not enough, #159). **Model findings, resolved in v3 code (see Open items 1-2):** one corrupted synced record `S3v1_root_lww_I1` violated; Sync-persisted watermark `S3m_*` pass (1,320,984 / 3,955,400; rerun on `d0e3895a`: unchanged); `S3f_*` NoForgedReplay violated (the model has no v3 nonce/cursor binding). **smartwal, current:** `S3_sw_current_I2` violated (false `wal_corrupt`), `_I1`, `_I1frontier` violated; proposal `S3_sw2_ordered_marker_repair_I2` pass (65,401), `_I1slot` pass (345,084). **Replica:** `S3_rep_c1_I2` pass (29,196), `S3_rep_c1_g2_I2` pass (12,712), `S3_rep_c1_g1_I3` pass (74,527); RED `S3_rep_c2_I2`, `S3_rep_c1_g2jump_I2`, `S3_rep_nogate_I3` violated. | `core/storage/recovery.go:43` recoverWAL (head==tail scan from its position `:63-73`, defensive scan `:74-90`, a range stops at its first bad entry `:141`, `:154`, hole warning `:437`), LWW replay `:193` via `core/storage/dirty_map.go:56` putAtIfNewer; `core/storage/wal_writer.go:41` durableTail, `:70` spaceUsed, `:443` tailPersisted; `core/storage/walstore.go:390` checkpoint release, `:933` Sync, `:1065` AdvanceFrontier, `:1167` ApplyEntry; `core/storage/smartwal/store.go:297` writeAt (extent `:304`, then ring `:313`), `:345` Sync; `smartwal/recovery.go:47` runRecoveryCounting (fail closed `:99-100`); `smartwal/ring.go:45` appendAt, `:69` scanSlots; `core/replication/volume.go:680` RebaseShipLSN |
| S4 | `CutCapture`, `CutPipeline` | Which chunks the next cut must upload: the dirty bitmap vs a per-chunk last-write-LSN map, across RF=1 restart, crash mid-cut and RF=2 failover; dirty state persisted before a checkpoint passes WAL entries. Then the pipeline: precopy tagged with a per-chunk mutation generation, two-level manifest segments, root CAS, and `reconciling` after a restart without a bound bitmap. | `HeadImage` (HEAD is the exact image of the lineage at HEAD.lsn), `HeadLeDurable`, `SegConsistent` | **Pass:** `S4_rf1_bitmap` 47,670, `S4_rf1_lsnmap` 46,934, `S4_rf2_bitmapHash` 7,166,376, `S4_rf2_lsnmap` 8,226,004, `S4_rf2_lsnmap_base` 901,390; `S4b_design` 69,645, `S4b_design_big` 1,515,837. **RED:** `S4_rf2_bitmap` (failover without hashMode, 7-step), `S4_rf2_lsnmap_localCut` (6), `S4_rf1_*_noPersist` (7), `S4_rf1_bitmap_clearEarly` (8); `S4b_red_noGen`, `S4b_red_cutWhileRec` HeadImage; `S4b_red_segReuse` SegConsistent. | `core/storage/chunk_layer.go:356` BeforeCheckpointAdvance, `:299` OnExtentWrite, `:95` hashMode (set on attach `:184-230`), `:111` chunkMut, `:324` noteMutation, `:762` and `:1435` staged upload reused only if its generation is unchanged, `:702-704` cut refused while reconciling, `:877-881` PrepareManifest, `:1542` finishCut; `core/frontend/chunkstore/manifest.go:101` segment reused only if its chunk refs are unchanged |
| S4c | `CutCOW` | Copy-before-write in the cut window (proposed): today's barrier cut against COW, where the barrier only fixes the cut point H and the candidate set D, the first write to an uncopied candidate saves its at-H content to a local side store, a write to a chunk being copied waits, the copier reads the side store or the live chunk, and the side store is dropped at the CAS and on crash. A chunk copy is two steps (`CopyStart`, `CopyFinish`); read-visible content and the extent are separate; one crash. | `HeadImage`, `BufFrozen` (every uploaded chunk equals the image at H), `SideFrozen`, `SideOnlyInCut`, `ReadsNewest`, `HeadLeSynced` | `S4c_cow_safe` pass (34,342,214), `S4c_barrier_safe` pass (5,070,320), MaxLSN 6, 2 cuts. **RED**, each violates `BufFrozen`: `noChunkLock` (6,814), `noCopy` (6,617), and on the atomic-copy predecessor `copyAfter` (2,493), `earlyRelease` (1,067), `copyAtFlush` (2,191), `sideReuse` (272,908); those four carry over because every atomic-copy behaviour is a two-step behaviour with the steps adjacent. Witnesses reachable: `WriteInWindow` (508), `CASAfterWindowWrite`, `CrashDroppedSide` (atomic-copy runs). Atomic-copy COW at MaxLSN 8, 3 cuts: clean through depth 22 (605M states, time cap). | `core/storage/chunk_layer.go` `captureLocked`; `core/storage/walstore.go` `captureMu` (at `c5eb5819`) |
| S5 | `ControlPlane` | Blockmaster authority store: one volume record `{epoch, holder, move journal}`, two retried move requests, durable Puts that succeed, fail, or land with the reply lost, and restarts. | `UniqueEpoch` (issued epochs strictly increase), `Idempotent` (one epoch per request id), `JournalComplete` | `S5_code` pass (298). **RED:** `S5_red_noUncertain` UniqueEpoch violated (5-step: a lost-reply Put, then a reservation from stale memory puts the old assignment back). `S5_red_noReserve` passes (the reservation step is not needed for these invariants). | `core/authority/authority.go:436` applyWithWorkspaceMove (uncertain refusal `:442-444`, Put `:564`, mark uncertain `:568`); `core/authority/workspace.go:309` prepareWorkspaceMoveLocked (expected line `:313`, resume a reserved entry `:324-333`, reserve max+1 `:344-355`), journal replay `:192-219`, persist and mark uncertain `:276-288` |
| S6 | `ReplicationFailover` | RF=2: P ships to R, R acks what is durable, the client is acked when both are durable; promotion of R at a new epoch with an evidence gate; R refuses old-epoch shipping; cuts and snapshots cover only the replicated-durable frontier; an in-flight snapshot create from the old primary may still land after the failover. Design-level. | `AckedSurvive`, `HeadOnLineage`, `SnapOnLineage`, `NoStaleAck` | `S6_design` pass (52,897), `S6_design_big` (598,353). **RED:** `S6_red_localCut` HeadOnLineage; `S6_red_noFence` AckedSurvive. `S6_red_noEvidence` passes: with synchronous RF=2 acks the gate is redundant (it matters for quorum or async shipping, not modelled). | `core/transport/replica.go:362` replica rejects a stale-lineage ship entry; `core/replication/volume.go:680` RebaseShipLSN; takeover CAS as in S2 |
| S9 | `RetentionGCTiming`, `RetentionGCMulti` | GC timing with an explicit clock: a copy operation with deadline D, marks of duration up to MD, deletion only after two marks at least Grace apart; then marks across two volumes with a fork and a snapshot delete. | `NoLiveDelete` | **Single op:** `S9_rule_G4` (G > D + MD) pass (10,798; `_big` 43,780); G = D passes (`S9_red_G2_noguard`, 11,953). **RED:** `S9_red_G1_noguard`, `S9_red_G1_D3` (G < D) NoLiveDelete violated. **Two volumes:** `S9b_G1_MD2`, `_G2_MD2`, `_G2_MD3`, `_G3_MD1` pass (5,743-9,079). | `core/frontend/chunkstore/gc.go:89` grace > operation deadline, `:280` mark-duration margin, `:306-318` two-mark rule, `:214-221` intent expiry; `catalog.go:896-905` intent deadline re-checked before publish; `gc_copy.go:143`, `:177` a deleting attempt stays a root until grace |

## Limits

- **Design, not code.** Each action is a hand-written abstraction of one
  remote call or one local decision, mapped to code by the table above. A
  pass says the *design* is safe under the modelled assumptions; it does not
  say the Go code implements that design. Line mappings were re-checked
  against `5c7c1a7f`, but they rot as code moves.
- **Small scopes.** 2 volumes, 2-3 chunks, 2 snapshot names, 3-6 writes or
  LSNs, a 4-page WAL, 1-2 restarts, clocks up to 7-16 ticks. A pass is
  exhaustive *for that config only*. Some large configs stopped at a time
  limit; they are marked incomplete above.
- **Abstractions.** Data is "the LSN of the write it holds" (no bytes); a CRC
  is data identity; one corrupted record at most; GC mark+sweep is atomic in
  S1 and S2 (S9 models its timing); time in S1 is replaced by a "keys read by
  an in-flight operation are roots" assumption; S1 has no epoch (S2 covers
  it) and no crash (so fsync-durability bugs are out of scope); some reads
  are merged with the conditional write they guard, which can only hide
  interleavings, never invent them.
- **Not modelled.** RF=3 quorum, catch-up and rebuild, wall-clock jumps,
  master HA, an unknown-outcome HEAD CAS (`resolvePending`), the S2 forced
  move's skew margin added in #158, liveness under fairness.
- **Model errors happen.** Three early drafts of these specs produced
  spurious violations that were traced to the model, not the code. Every
  violation listed here was checked against the code.

## Findings that led to code fixes

| PR | Finding | Spec / configs |
|---|---|---|
| #152 | Volume delete (as first proposed): (1) the snapshot cascade's back-out could mark *another* caller's published snapshot deleting while the delete was still provisional, so a later GC could reclaim an acknowledged snapshot's only chunk; (2) a claim check and commit not bound to one deletion incarnation allowed an ABA across three concurrent deleters; (3) a rerun with a fresh token left the first reservation held; (4) CreateVolume released its lock between the reservation check and the create. Fixed by refusing while any snapshot name exists, binding check, commit and revert to one incarnation, using `--request-id` as the token, and keeping the lock. | S1 `Bug_P1_*`, `Bug_P2_aba`, `Bug_token`, `Bug_create` (violated); `Fix_*` (pass) |
| #156 | walstore recovery replayed in physical scan order. After a wrap, a newer record could be scanned before an older one for the same LBA and lose to it, so an acknowledged write was lost after a plain crash. Fixed by last-writer-wins replay by LSN. | S3 `S3_wal_current_I1_crashonly` |
| #159 | With LWW replay alone, the writer could reuse WAL space released only in memory; a torn unsynced record written there could stop the scan before a synced record that the on-disk superblock still retained. Fixed by reusing only space the *persisted* tail released, and by scanning an empty-superblock window from its position. | S3 `S3v_code_lww_I1_crashonly` (violated); `S3r_rootLww_*`, `S3r_rebuild_rootLww_*` (pass) |
| #158 | The workspace lease had no clock-skew margin: with a host clock one tick slow, the old and new host could both serve after a forced move (HEAD stayed safe because the old host's CAS fails on the epoch). Fixed by a skew margin: a forced move waits for lease expiry plus the margin (`core/authority/workspace.go:316-320`), and the lease validity reported to the host excludes half of it (`core/host/master/workspace_service.go:46-53`). | S2 `W4_drift` (violated against the pre-fix design; the margin is not yet in the model) |

The S2 epoch rule for GC intent expiry (`gc.go:221`), the S5 "uncertain
after a failed Put" guard, and the S4 generation and reconciling guards were
already in the code; their RED variants confirm they are load-bearing and
are good candidates for regression tests.

## Open items

These are model-level results on the current design. They are not confirmed
bugs in production paths unless stated.

1. **Corrupted synced WAL records: resolved in v3 (code), with one residual
   in mode C.** Before v3, if a synced record's bytes were corrupted,
   recovery stopped that range at the bad record and continued; data after
   it in the range was dropped and recovery reported success (a hole warning
   is logged since #155). An in-record durable watermark alone is not enough
   (a corrupt *last* synced record leaves no valid witness). The configs that
   pass (`S3m_*`) need Sync to persist the watermark and any frontier jump out
   of band before acknowledging, plus recovery that decodes every record
   position and replays by LSN. Format v3 does this: the acknowledged
   frontier W is published in the descriptor slot, and recovery checks the
   scan against it.
   - **Mode A (`WModeStrict`).** A corrupted acknowledged record fails
     recovery closed (`wal_corrupt`), and a crash without corruption never
     reports corruption: `TestWALStoreV3ModeCWindowIsExactlyTheNewestBatch`
     (its mode A cases), `TestWALStoreV3RecoversEveryAcknowledgedWriteInBothModes`,
     `TestWALStoreV3TornUnsyncedTailIsNeverAFalseCorrupt`.
   - **Open residual, mode C (`WModeLagged`, the default).** The slot W lags
     by one batch, so a corrupted *newest* acknowledged batch is truncated
     silently (documented by the same test). An idle refresh or
     `SetWMode(A)` closes the window
     (`TestWALStoreV3RefreshAndSetWModeCloseTheWindow`).
2. **Forged replay: resolved in v3 (code); the model is not updated.**
   Before v3, recovery accepted any well-formed record. A guest payload could
   contain bytes that decode as a valid WAL record (CRC32 is not keyed). If a
   newer, shorter record overwrote only the start of an old multi-block
   record, the scan continued into the old payload and replayed the forged
   record. Every rule modelled violates `NoForgedReplay` (`S3f_*`).
   - **Reproduced on the pre-v3 format.** A 3-block batch write carried an
     encoded WAL entry (LSN 999, LBA 7) in its guest data. After a newer
     1-block record wrapped over the batch's start, recovery decoded the
     embedded entry: LBA 7 read the forged data, and the frontier jumped to
     999, also after a clean close and reopen. The effect stayed inside the
     guest's own volume.
   - **Fix (format v3).** Every record is bound to the volume's nonce and to
     its exact logical cursor, lap included. The scanner checks the binding
     before it trusts a length or count, and never searches past a record it
     refused (`wal_io_v3.go`, `scanWALV3Accepted`).
   - **Tests.** `TestWALStoreV3StaleLapBytesAtTheWrapAreNotReplayed` replays
     the reproduction end to end on a mapped v3 WALStore, with the embedded
     record bound to the volume's own nonce at its lap-1 cursor. After the
     wrap the scan stops at it, recovery returns the newest real LSN, and LBA
     7 is never the forged data. With the lap dropped from the cursor check,
     the scan admits the record and the test fails. Also
     `TestWALIOV3_StopsAtBindingBeforeEmbeddedRecord` (foreign nonce, wrong
     cursor in the same lap, old lap, bad CRC, bad footer) and
     `TestWALIOV3_EmbeddedRecordInPayloadIsNotScanned`.
   - **Threat boundary.** The nonce lives in the descriptor slots, outside
     the guest's address space. A guest that knew it could place a record at
     a cursor it predicts; that is outside the boundary.
   - **Open, model.** `WALRecovery.tla` has no nonce, cursor or lap binding,
     so `S3f_*` still violate `NoForgedReplay`, and `S3v1_root_lww_I1` still
     violates I1 (rerun on main `d0e3895a` with TLC 2.19: unchanged). This
     stays open until the binding is modelled.
3. **SmartWAL.**
   - The extent write and the ring slot have no ordering barrier
     (`smartwal/store.go:304`, `:313`), and recovery fails closed on any CRC
     mismatch (`smartwal/recovery.go:99`). After a plain crash an *unsynced*
     write can therefore make the volume refuse to start with `wal_corrupt`
     (`S3_sw_current_I2`): an availability problem, not data loss.
   - There is no per-block CRC once a slot has been recycled, so extent
     corruption of such an LBA is not detected, and corrupting the only record
     of a synced write lowers the recovered frontier silently
     (`S3_sw_current_I1`, `_I1frontier`; a documented limitation).
   - A proposal passes `I2` and slot-corruption `I1`: make the record durable
     before the data write, persist gap markers for frontier jumps, and have
     recovery durably settle every torn tail it accepts (`S3_sw2_ordered_marker_repair_*`).
     Its frontier still needs an out-of-band durable watermark (item 1).
   - The positional torn-slot rule of an earlier revision of #155 (089d2d9)
     could report `wal_corrupt` after a plain crash (`S3_sw2_pr155o_I2`); the
     merged #155 only counts torn slots, so this does not apply to main.
4. **Design notes (no safety impact).** A forced move's journal records the
   HEAD read before the publisher lock (`workspace_service.go:116`), so it can
   over-report the loss window (`W4_forced_exact`). A refused snapshot attempt
   can leave an unpublished name record that blocks later volume deletes until
   `DeleteSnapshot` settles it (`Fix_reach_W_NoHiddenNameBlock`;
   `catalog.go:861`).

## Parse check

All 13 specs parse and pass SANY's semantic check with `tla2tools.jar` v1.7.4
(`java -cp tla2tools.jar tla2sany.SANY <file>`), checked on 2026-11-18:
`ChunkLifecycle`, `ChunkLifecycleFixed`, `ControlPlane`, `CutCapture`,
`CutCOW`, `CutPipeline`, `ReplicationFailover`, `RetentionGCMulti`, `RetentionGCTiming`,
`SmartWALRecovery`, `WALRecovery`, `WALReplica`, `WorkspaceTakeover`. The state counts above come from earlier
TLC runs of these specs; comment-only edits were made since.

## Format-v3 activation correspondence

The public selector now reaches the mapped impl3 paths in `walstore_v3.go`, `descriptor_v3.go`, `wal_io_v3.go` and `walstore_map_checkpoint_v3.go`. This is activation of the previously reviewed format contract, not a new abstract transition. Its canonical signed design is r17.4 (`e2a5304ef8fdc29f7e109883b8664a5c93d37a559e7b93e1d24964cb09cb7824`), with the composed external S3/BASE/install/map TLC receipts referenced by that design. The in-repository `WALRecovery.tla` remains an abstraction and is not a proof of every physical nonce/cursor/map/dual-slot rule. Pending external bounded runs remain pending; activation tests and source correspondence do not turn them into exhaustive proof.

Native Go controls and the temporary frozen-v2 fixture disposition are listed in [the activation inventory](../design/format-v3-legacy-test-inventory.md). Old-codec tests are preserved, and both default and tagged migration/caller suites are validation gates. No model equation is changed by the selector wiring; a new counterexample still requires an amendment before affected integration.

## Cut-window implementation correspondence

The mapped v3 cut now uses `chunk_cut_cow.go`. `CutCOW2.tla` is the unchanged
S4c r2 model (SHA-256 `0b9bc89a9aed6c42dda6b1d23ba8515c406b1c2cb265cd83e78122238df6bcca`);
its header records its pre-implementation status at the time of the run.
The existing bounded verdict is not a new run: Chunks={a,b,c}, MaxLSN=6,
MaxCuts=2, MaxCrash=1, MaxAborts=1. `S4r2_cow_safe` checked seven invariants,
252,733,280 states, depth 36; barrier checked 27,790,736, depth 31.
`abortNoJoin` violates SideOnlyInCut at 31,835 states; `noChunkLock` and
`noCopy` violate BufFrozen at 6,982 and 7,484. The three witness configs
reach AbortWhileSaving (1,682), SaveUsedByCopier (28,250), CASAfterAbort
(614,292); their exit 12 is expected reachability, not a safe-config pass.

SaveStart/SaveFinish correspond to the saving owner's validated read and
side-store publication. AbortStart is terminal invalidation/wakeup; AbortFinish
is reference drain followed by one cleanup. The model has one copier and
whole-chunk writes. Actual multi-writer/batch ownership, first-attach/Close,
Recover/reset snapshot recheck and saved-file readers are covered by the
native `TestCutCOWV3_*` caller controls, not claimed as model proofs.
The immutable cut image is represented by live extents protected by per-chunk
COW ownership, saved-before-write bytes, and pinned HEAD references for lazily
absent blocks. Bounded workers acquire one image at upload time; after its local
entry is released, the worker-owned immutable buffer survives remote waits.
The window protects not-yet-read candidates until workers settle. Abort joins
local owners, and the outer cut registration joins uploads before cleanup.
Byte budgets and transport resource costs remain native test obligations, not
claims from a new model run.

The earlier thirteen-spec parse statement above predates this imported model;
no new independent TLC/parse or cloud run is claimed here.


### WAL pressure progress without guest Sync

`WALReclaim.tla` models native local pressure persistence and checkpoint/reuse
progress. `InternalSync` is weakly fair and does not take the capture lock;
`GuestSync` has no fairness requirement. `REC_safe.cfg` and `REC_safe_big.cfg`
check I1/I2/I3, ring/pin safety, `AllWritten` and `NoStuckWaiter`. Removing internal
sync (`REC_red_noInternalSync.cfg`) violates liveness; removing the durable clamp
violates I2. The capture-lock mutant and permanently held pin also violate
liveness. The Go implementation bounds a permanent pin with retryable admission
expiry rather than assuming that it always releases.

Run, from `docs/spec`, with the existing TLC tool:

```sh
java -cp /path/to/tla2tools.jar tlc2.TLC -deadlock -config cfg/REC_safe.cfg WALReclaim
java -cp /path/to/tla2tools.jar tlc2.TLC -deadlock -config cfg/REC_red_noInternalSync.cfg WALReclaim
```

The authored LIGHT receipts report exhaustive safe searches at 2,700 and 25,550
states, plus the expected mutant/witness failures. Source and configs are copied
from the sealed `claude05-walreclaim-tla-r0-20261119` artifact with line-ending
normalization only. Those are prior author runs, not a claim that this PR reran
TLC. This is an abstraction: physical fsync success, byte layout, crash recovery,
both-slot retirement, timing and multiple writers remain covered by their other
models and actual Go controls. Do not treat finite liveness checking as an
end-to-end wall-clock latency guarantee. Keep long model runs off tp04.

## HostBudget

`HostBudget.tla` abstracts cooperating process slots, COW reservation and joined release. Config `HostBudget.cfg` checks three owners/two slots; `HostBudget_early_release.cfg` must violate HeldUntilJoined. It excludes degraded static fallback and does not prove filesystem locks, rates or physical-I/O deadlines. Native subprocess tests cover lock lifetime.

HostBudget also models guarded configuration initialization: staged bytes are invisible until atomic publication; an initializer crash permits a successor. `HostBudget_partial_config.cfg` must violate NoPartialConfig. The syscall-level crash boundary is covered by TestKilledInitializerDoesNotPublishPartialConfiguration.
## Replica apply ordering

`ReplicaApplyOrder.tla` models two admitted sessions on one LBA with three versions, plus the two events of one probe and a delayed post-completion probe. `R292_safe` checks `NoRegression`, `NoDuplicateRecovery` and `NoFalseKnown`; the four `R292_red_*` configurations remove one guard each. It abstracts successful physical applies only: it does not establish restart persistence, authority, byte-level framing, or I/O deadlines.

`ReplicaPrefixFloor.tla` checks two LBAs/three source versions with explicit
covered-prefix certification, suffix pruning and stale checks against the floor.
`R293_prefix_safe` checks `NoRegression`, `CertifiedCoverage`, and `OnlySuffix`;
its two negative controls infer a prefix from the synced maximum or omit the
floor after pruning. It does not establish the wire certificate's provenance,
restart persistence, live-prefix extension, or a fixed memory cap without closes.

`ReplicaReviewGuards.tla` checks two logical sessions with one admitted frame
per session, settled-coverage retirement, active-recovery readiness refusal and
one missing-probe/newer-ACK schedule. `R293_review_safe` checks four invariants;
four negative controls separately remove retirement, the frame join, the
readiness guard or ACK progress restoration. This abstracts successful admitted
frames and does not prove physical-I/O, authority rollback or timing behavior.

`PrecopyFixedAdmission.tla` checks cost admission over five physical rounds.
A known estimate fits the 1.2-second allowance, or uses the five-second declared
freeze limit when it is at least 90% of the previous estimate or on the final
round. `SafeCommit` checks the declaration and ceiling; `FinalWithinBudget`
checks that an eligible final round cannot refuse. This model does not model
sample freshness, measurement availability, publication/data safety or physical
latency; those remain separate checks.

### PrecopyFixedAdmission checked configurations

Local amendment-4 run, 2026-11-21 01:46 UTC, TLC 2.19 (tools jar SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`),
from `docs/spec`:

```sh
java -Xmx512m -cp /path/to/tla2tools.jar tlc2.TLC -deadlock -config cfg/PrecopyFixedAdmission.cfg PrecopyFixedAdmission
```

The positive configuration exited 0: 6,467 generated / 1,893 distinct states,
queue empty, depth 6. The same invocation with each negative configuration
exited 12: `no_stall` violates `SafeCommit` (24 / 20, depth 2), `no_cap`
violates `SafeCommit` (417 / 390, depth 3), and `no_final` violates
`FinalWithinBudget` (5,759 / 1,605, depth 6). The older quarter-tail model
results are superseded by this rule. These finite-state checks do not establish
physical latency or train qualification.

## Plain detach admission

`PlainDetach.tla` models the explicit plain-detach idle gate and atomic closure
of frontend admission, with up to two I/O readers. `IdleAtStop` checks that
stopping or detached states have no admitted/entering reader. HEAD publication,
authority, physical I/O, wall-clock bounds, and failure settlement are outside
this small admission abstraction and retain their existing native controls.

Local run 2026-11-21 00:00 UTC, tools distribution v1.7.4 (TLC 2.19), jar SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.
From `docs/spec`, SANY `PlainDetach.tla` exited 0; TLC with
`-deadlock -config cfg/PlainDetach.cfg PlainDetach` exited 0 (8 generated,
5 distinct, queue empty, depth 3). Replacing the config with
`cfg/PlainDetach_no_idle.cfg` exited 12: `IdleAtStop` fails on
BeginIO -> Plain (7 generated / 5 distinct, depth 3). The negative uses a
distinct `-metadir` to avoid timestamp-directory collisions between short runs.
