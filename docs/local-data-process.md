# Local Data Process

The local data process is the bounded execution institution that
owns one node's read, write, flush, checkpoint, and recovery
mechanics. It lives behind the `LogicalStorage` seam and serves the
semantic engine through that seam — without holding any semantic
authority itself.

This doc names what the institution IS, what it owns, what it does
NOT own, and what crash model is actually proven against it.

## What it owns

| Responsibility | Where it lives | Notes |
|---|---|---|
| Local read | `WALStore.Read`, `BlockStore.Read`, `smartwal.Store.Read` | Returns current bytes for an LBA. Reads check the dirty map first, fall back to the extent for any LBA the flusher has already drained. |
| Local write | `Write` on each implementation | Allocates next LSN, appends to WAL, updates dirty map. NOT durable until Sync returns. |
| Local sync | `Sync` on each implementation | Forces the underlying file to disk. The durability boundary. Group-committed across concurrent callers. |
| Local flush | `core/storage/flusher.go` | Background goroutine: snapshots dirty map → writes data into extent → fsyncs → advances on-disk checkpoint LSN → drops the flushed entries from the dirty map. |
| Local checkpoint | `WALStore.persistCheckpoint` + `superblock.WALCheckpointLSN` | Pinned by the flusher. Recovery skips records whose LSN ≤ checkpoint. |
| Local recover | `WALStore.Recover` + `core/storage/recovery.go` | Replays WAL records past the checkpoint into the dirty map. Defensive past-head scan picks up writes that landed after the last superblock update. Idempotent. |

## What it does NOT own

The institution is execution, not authority. It must never:

- decide recovery class (`catch_up` vs `rebuild`) — that's the engine
- decide mode transitions (`healthy` / `recovering` / `degraded`) — that's the engine
- emit engine commands or interpret events
- read engine projection as control input
- redefine `targetLSN` — the engine sets it, storage honors it

Specifically: the local data process exposes `Boundaries() (R, S, H)`
so the engine can reason about durability/retention/head, but the
**interpretation** of those numbers belongs to the engine. The
storage just reports them.

## Crash model — what is actually proven

The institution is proven against the following abrupt-termination
windows. "Abrupt termination" means the file handle is dropped
without going through `Close()` — no implicit fsync, no superblock
update, no graceful flusher drain.

### `WALStore` (regular WAL+extent backend)

| Crash window | Tested by | What survives |
|---|---|---|
| Pre-Sync (writes returned, Sync not called) | `TestCrashFamily_AbruptKillAtMultipleWindows/kill_pre_sync` | Nothing required. Acked = none. |
| Post-Sync, pre-flush | `kill_post_sync_pre_flush` | All Sync'd writes (recovered from WAL on Recover) |
| Post-flush (data in extent, checkpoint advanced) | `kill_post_flush` | All flushed writes (extent direct on read; recovery skips checkpointed records) |
| Post-flush + further unacked writes | `kill_post_flush_then_more_unacked_writes` | Flushed writes always; further unacked writes may or may not survive but never corrupt acked data |

### `smartwal.Store` (extent-direct backend)

SmartWAL has fewer windows because there is no separate flush step
— writes go directly to extent, and one fsync per Sync covers both
the extent and the metadata ring.

| Crash window | Tested by | What survives |
|---|---|---|
| Pre-Sync (writes returned, Sync not called) | `TestSmartWAL_CrashFamily_AbruptKillAtMultipleWindows/kill_pre_sync` | Nothing required. Reads return either the written bytes (if extent pwrite reached disk) or zeros. |
| Post-Sync (writes + Sync, then kill) | `kill_post_sync` | All Sync'd writes |
| Acked + later unacked overwrite of the same LBA | `kill_after_acked_then_unacked_overwrite` | LBA reads either the acked bytes (A) or the unacked-overwrite bytes (B) — never garbage, never another LBA's data. **Failure-atomicity claim verified.** |

For every window:

- **Acked writes survive.** A write whose enclosing `Sync()` returned
  with no error is recoverable.
- **Unacked writes never corrupt acked data.** A pre-Sync write that
  survives the crash returns its written bytes; a pre-Sync write that
  doesn't survive returns the LBA's prior bytes (zeros if never
  written before). Never garbage, never another LBA's bytes.
- **Recovery is deterministic.** Two Reopen+Recover cycles on the
  same on-disk state produce identical results. Verified by
  `TestRecovery_DeterministicAcrossMultipleOpens`.

## Crash model — what is NOT proven

These are intentional boundary choices. None are implied by the
"acked writes survive" claim:

- **Power loss with disabled write cache.** We rely on `fsync`
  semantics holding at the OS+device boundary. If the device has a
  volatile write cache and `fsync` doesn't flush it (cache-lying
  device or misconfigured controller), acked writes can still be
  lost. Mitigation lives at the device-management layer
  (write-cache disable / FUA), not in this code.
- **Distributed durability.** Single-node only. Replication and
  cross-node ack live in `core/transport`; this doc and these
  tests do not cover them.
- **Bit rot / silent data corruption from the device.** We CRC each
  WAL record but do not CRC the extent itself. A surface-level
  bit flip in the extent goes undetected at this layer.
- **Network partition / shared-storage races.** Out of scope.

## Carry-forward

| Item | Where it'd land |
|---|---|
| Extent-side per-block CRC | inside the local data process; would let recovery detect bit rot in the extent |
| Compaction (rewrite extent + truncate WAL) | inside the local data process; bounded WAL today via circular buffer + checkpoint + tail advance, but no actual compaction yet |
| Raw block device backing | small additive change, see `docs/persistence.md` Backing-store options |
| `O_DIRECT` + FUA writes | larger change, brings device-managed durability into our contract |
| SmartWAL as a swappable backend | already exists in `core/storage/smartwal/`; adopting it as default would be a separate decision |

## Interaction with peer institutions (carry-forward)

The local data process is one of four named execution institutions
the architect identifies. The others are deliberately deferred to
later phases:

| Institution | What it would own | Currently lives as |
|---|---|---|
| **DataCommunicator** | replication + rebuild data movement | informally in `core/transport` |
| **RecoveryExecutor** | command execution lifecycle (StartCatchUp / StartRebuild / session close handling) | `core/adapter` (`VolumeReplicaAdapter`) |
| **ProgressFeed** | typed progress/trace surface separate from `engine.TraceEntry` | every consumer reads `adapter.Trace()` ad hoc |

Naming and tightening these is future work. The local data process
is bounded today and the seam (`LogicalStorage`) is correct, so the
others can be carved out independently when their phase arrives.

## How to read the surface from outside

Anything a caller wants from the local data process goes through
`LogicalStorage`. The compile-time assertion `var _ LogicalStorage =
(*WALStore)(nil)` (and analogous ones for `BlockStore` and
`smartwal.Store`) is the source of truth: a backend either satisfies
the seam or does not. There is no other way in.

Specifically: the flusher is an **internal** detail of the
implementation. Callers do not invoke it, do not configure it, do
not see it. It runs because the institution needs to keep its WAL
bounded and its checkpoint current. Diagnostic accessors like
`WALStore.CheckpointLSN()` and `WALStore.FlushCount()` exist for
tests and observability but are not part of `LogicalStorage`.
