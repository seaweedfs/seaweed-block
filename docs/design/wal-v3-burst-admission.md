# Native WAL burst admission

A fresh v3 walstore-chunk volume has a 64 MiB WAL by default. Before this fix,
the common admission callback sampled `s.wal`, the legacy writer. V3 appends go
to `s.v3.w`, so the inactive legacy occupancy stayed zero. A real provider and
NVMe handler writing 128 KiB requests with Flush every 48 MiB hit WRITE
InternalError / WAL region full at 66,977,792 submitted bytes. The same schedule
reproduced on root WSL and non-root tp04. The preceding Flush and a direct Sync
after the failed Write succeeded. This does not establish the cause of the
reported guest SyncAll EIO; a host fsync error is not by itself an observed
backend Sync error.

Admission now samples native physical head minus the both-slot reuse floor.
The existing soft/hard watermarks, flusher notification and deadline stay in
place. The sampler holds store.mu shared while selecting the writer, snapshots
the descriptor/floor, releases descMu, then samples head under writer.mu. It
releases all these locks before admission can sleep or wait. Existing publication
order is unchanged; there is no fsync, slot/map write, early acknowledgement or
new reuse permission in this callback. A missing/unbound writer remains an append-binding refusal, not capacity pressure;
the existing typed guard runs without a pressure wait.
Legacy fixtures keep the original legacy occupancy. Very large batches,
concurrent already-admitted requests or a fully unsynced ring can still exhaust
space; this change is not a reservation scheme or a guarantee for every burst.

The real NVMe session logs failed Flush status and IOResult.Reason using its
existing target logger. This preserves the backend diagnostic that the CQE
cannot carry. Success emits no failure log; the status/error mapping is
unchanged. Use that target log to attribute a future Flush failure rather than
inferring its cause from a guest EIO alone.

Controls: `TestWALStoreV3AdmissionUsesNativeOccupancy` asserts exact encoded
occupancy and release after checkpoint/rollover; the public provider + actual
NVMe handler `TestWALStoreChunkV3_SequentialBurstFlush` writes 320 MiB and verifies
every byte against submitted literals; `TestSessionFlushLogsBackendReason`
uses actual session dispatch, asserts error status, reason logging exactly once,
and no failure log on success. INV-WAL-V3-ADMISSION-001 and
INV-FE-FLUSH-SYNC-001 record these checks. The durability/format model is unchanged;
no new TLC or mounted performance claim is made.

The fault-at-ACK-boundary control parks writer B after existing capacity
admission, before A's held slot publication, then requires B to own store.mu
exclusively (TryRLock fails) before releasing A. A shared sampling lock cannot
stand in for that handshake. Its injected failing append and no-W-advance
assertions remain unchanged. The half-reset control keeps its original typed
binding-refusal assertion.
