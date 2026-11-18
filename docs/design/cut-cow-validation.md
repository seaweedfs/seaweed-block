# Cut-window validation and mount rehearsal

The native v3 cut fixes H, candidate versions and a window under captureMu,
then copies with bounded workers outside it. It preserves whole chunks, upload
intents, manifest/CAS behavior and the local durability contract. The legacy
engine retained only by test constructors still uses the older barrier cut.

## Lock and lifetime graph

```text
BASE begin/complete: installMu X -> capMu -> captureMu -> lifecycleMu
                    -> syncMu/store.mu -> descMu -> fileSyncMu
completed handle write: installMu R -> captureMu R -> COW claim/release
                       -> validated Read -> release reference
                       -> lifecycleMu/admission/store.mu -> WAL/dirty map
cut: capMu -> captureMu X (Sync, H/D, window); release captureMu -> copy
claim/finish: layer.cowMu -> window.mu -> entry.mu; release before all I/O
Close: captureMu X (permanent attach shutdown + stable hook snapshot); RELEASE
       -> layer abort + actual reference/waitgroup join -> lifecycleMu X
Recover/reset: admission lease + abort/join, without lower store locks
               -> captureMu (hook snapshot recheck) -> existing lower locks
```

Only short state transitions hold window/entry locks. No read, file create,
spill read/write, wait, callback into the store or reference join holds them.
A saver that aborts releases its own reference before any join. A terminal
notification wakes waiters immediately; late I/O cannot publish a saved result.
The active-window fast path is an atomic pointer load. With no window there
is no chunk lock, allocation or side I/O.

Recover/reset keep their lease until their lower locks have been released;
first-attach hook snapshots are retried under captureMu. BASE resume is
excluded by BaseInstalling, not by a nonexistent capMu drain. Close atomically
shuts attachment admission with its hook snapshot and all callers share the
same completed shutdown result.

## Space and measurements

The 64 MiB budget covers retained in-memory saved payload only, not immutable
upload buffers or transient owner reads. Spill offsets are compact candidate
slots, checked against MaxInt64 before installation. Spill files are per cut,
not fsynced or replayed, and are removed only after terminal plus zero actual
references. A later exclusively locked RW open removes regular files with
this volume's reserved `.cut-cow-` prefix; other prefixes, directories and
symlinks are left alone. Cleanup failure fails the cut diagnostically, without
changing any guest write outcome.

`barrierPauseNs` includes Sync/window setup, not chunk reads. `copyNs` is the
outside-barrier copy and drain. Native `barrierCopyChunks`/`barrierOverBound`
are zero; the configured threshold still bounds staging, not a hard COW-copy
limit. `cowSavedChunks`, `cowSaveNs`, `cowWaitNs` are mirrored into the cut
body. Save/wait times sum concurrent owners and must not be added to interpret
a wall-clock pause. `cut_aborted` is retryable (503); a wrapped ordinary fence,
shutdown or authority cause retains its existing classification.

## Real-mount rehearsal (planned, not executed)

This is a rehearsal note, not a lab packet or permission to run a mount. The
existing frozen M5 ublk/NVMe packet and its host/volume allocation remain the
starting point. Freeze the reviewed build SHA, weed SHA, map geometry,
UploadConcurrency and frontend options in that packet before execution.
Use only the assigned disposable volume, never a production/user filesystem.

1. Rebuild the existing blockvolume/sw-block binaries at the reviewed SHA and
   replay the unchanged M5 mount/setup. Record the device, filesystem and
   status URL. Verify the frontend reads/writes the expected v3 volume.
2. Populate 8 then 64 distinct 4 MiB chunks with deterministic patterns and
   fsync. Capture a baseline HEAD and preserve its manifest/hash receipts.
3. On the mounted file, run the existing 4K random-write/fsync workload at
   QD1 and QD8 while issuing cuts through the real route:

   ```sh
   sw-block ops chunk cut --volume "$VOLUME_ID" \
     --status-addr "$STATUS_URL" --timeout 2m
   ```

   Record fio JSON, all cut JSON, frontend durability-stage histograms,
   writer p50/p99/max, barrier/copy/total times, upload bytes, save/wait counts,
   RSS and spill bytes. Compare interleaved parent/candidate runs with identical
   setup. Backend proxy timings are not guest/mount results.
4. Read back literal acknowledged patterns after the concurrent run. Validate
   each HEAD against the write-log prefix at its returned H, then make another
   cut and restore it on the packet's second host; verify the complete image.
5. Repeat Close/cancel during a long cut and an abrupt process stop while a
   side file exists. Reopen the local v3 image, verify acknowledged data, then
   cut and restore again. Side data must not be imported; HEAD can only be
   either the last complete CAS or a complete newer CAS, never a partial image.
6. Stop/join workloads before the packet's normal unmount/cleanup. Preserve
   receipts and report actual overhead/stalls; do not claim COW reduces upload
   volume. Save failure must abort the cut while ordinary writes keep working.

Deterministic native tests exercise held saving, saved-file and live-copy
owners, first attach vs Close, hook-snapshot races in Recover/reset, overlapping
public batches, cancellation and observed fencing. They complement the
bounded one-copier/whole-chunk S4c r2 verdict; they are not a substitute for the
future real-mount run.

## Caller closure

| Caller/path | COW admission or exclusion | Control |
|---|---|---|
| Write, WriteBatch | after immutable argument checks and captureMu R; before lifecycle/admission/store publication | MutationCompletesWhileEarlierChunkCopyIsHeld (including a checkpointed post-H rewrite); OverlappingRealBatchesPreserveCut; InvalidInputsDoNotSaveOrAbortWindow |
| ordinary ApplyEntry, completed handle ApplyInstallEntry | actual applyEntryV3 hook; completed handle's existing install admission stays outside it | MutationCompletesWhileEarlierChunkCopyIsHeld / ApplyEntry and ApplyInstallEntry |
| pending/saving/copying collisions | one owner, wait on per-entry notification or terminal | CopyingChunkWaitsButPendingChunkSaves; TwoWritersOneSaveAndCloseJoinsSavedReader |
| Cut, Capture, Detach, MakeRemoteDurable | same capMu/captureLocked path; immutable buffers after terminal/drain | existing C1/C3 image, staged reuse, Detach, MakeRemoteDurable and unknown-CAS/intent controls remain native |
| Recover and reset | beforeDestructive lease, abort/join, capture barrier snapshot recheck | RecoveryAndCloseDrainSavingOwner; DestructiveSnapshotRetriesFirstAttachment |
| Store Close / standalone layer.Close / second Close | permanent attachment gate + stable snapshot; terminal and actual owner/waitgroup drain | CloseCannotMissFirstAttachment; CloseSnapshotRejectsFirstAttachAndSharesCompletion; CloseJoinsHeldLiveCopy; RecoveryAndCloseDrainSavingOwner; TwoWritersOneSaveAndCloseJoinsSavedReader |
| BASE begin/complete | existing installMu/capMu transition drains whole cut | existing attached BASE lifecycle/capture-generation controls |
| BASE WriteBase / resume | installing state excludes a cut; resume is not capMu-drained | existing install-handle state/owner controls; native COW hook does not weaken them |
| SeedChunkFrontier / WriteExtentDirect | attached seed refused; public v3 direct write refused | existing seed/direct caller controls |
| Flusher / lazy fill | materialize existing visible bytes; no new read-visible mutation, no new hook | SaverUsesValidatedWALBatchAndLazyHEAD (WAL-only and flushed batch); existing PostReadMissRechecksWALFormat |
| Temporary v2 fixture mutation paths | remain behind the full-copy barrier; no production C11 file | existing legacy fixture controls and production reachability guard |

Test names above have prefix `TestCutCOWV3_` unless an existing control is named.
The abrupt-process control `ProcessCrashDropsSideAndRestoresSyncedImage` exits
with an OPEN spill and no cleanup, then the parent reopens, validates the
synced post-H bytes, observes the actual old HEAD and publishes the next image.
`SpillIsEphemeralAndCleanupIsPrefixScoped` additionally covers ignored corrupt
side payloads and preservation of other-prefix files/directories.

## Backend proxy result

The source-identical `BenchmarkCutCOWProxy` was run parent/candidate interleaved
three times, each row with three cuts, on tp04 (i7-13700, Go reports GOMAXPROCS
24; actual work stays under the runner's quota). Parent is main d0e3895a plus
only the benchmark, ref 3faecbd4. Candidate timing head is ee6c6f3b. Staging is
disabled, copy/upload concurrency is 8, chunks are 4 MiB and each released
writer performs one 4K write+Sync per cut. Writers start at the real Sync/H
handshake. A memory ChunkRemote keeps identical upload work in both arms;
this is file-backed storage with an in-process remote, not network, guest or
mount performance. No throughput/fsync guarantee is inferred from it.

The table reports medians of the three per-run values, in milliseconds. Each
per-run p99 has only 3 samples at QD1 or 24 at QD8; these are sample percentiles,
not estimates of a production tail. The full raw stage/timing rows are kept
with the review receipts. Later source changes are fence-observation ordering,
cleanup-error reporting and additional caller controls; they are not timed by
these earlier-head measurements.

| Geometry / writers | Barrier ms parent -> COW | Writer p50 ms parent -> COW | Writer p99 ms parent -> COW | Total cut ms parent -> COW |
|---|---:|---:|---:|---:|
| chunks8/QD1 | 41.15 -> 0.54 | 41.72 -> 8.38 | 44.79 -> 8.62 | 74.12 -> 45.44 |
| chunks8/QD8 | 40.46 -> 0.47 | 43.24 -> 9.71 | 43.27 -> 12.48 | 71.84 -> 43.63 |
| chunks64/QD1 | 312.68 -> 0.50 | 324.78 -> 8.35 | 346.51 -> 8.61 | 554.93 -> 356.54 |
| chunks64/QD8 | 308.86 -> 0.54 | 309.59 -> 15.29 | 347.69 -> 20.42 | 531.13 -> 346.85 |

Each 8-chunk cut uploads 32 MiB and each 64-chunk cut uploads 256 MiB in both
arms. COW reduces the barrier/copy stall and parallelizes materialization;
it does not reduce remote upload volume. The native mutation control reports
one real save (and positive save time), zero barrier copies, and verifies
post-H map dirt even after the new bytes have checkpointed. No mounted result
is claimed; use the rehearsal above for that separate measurement.
