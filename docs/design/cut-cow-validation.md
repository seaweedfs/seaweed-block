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
