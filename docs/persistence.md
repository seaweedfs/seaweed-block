# Persistence

This doc describes the single-node persistence backend in
`core/storage/`. It is one implementation behind the `LogicalStorage`
interface; the in-memory `BlockStore` is the other. Both satisfy the
same contract; tests run against both via a shared factory table so a
new backend slots in without test duplication.

For the broader institutional view of read / write / flush /
checkpoint / recover as one bounded execution institution with an
explicit crash model and named carry-forward boundaries, see
[docs/local-data-process.md](local-data-process.md).

## What this proves

1. **Acked writes survive crash.** A write whose enclosing `Sync()`
   returned with no error is recoverable after a process kill or
   clean stop. Verified by `TestWALStore_AckedWritesSurviveSimulatedCrash`
   which simulates a kill by dropping the file handle without going
   through `Close()`.
2. **Recovery is deterministic.** Calling `Recover()` twice on the
   same on-disk state yields identical results. Verified by
   `TestWALStore_RecoverIsIdempotent`.
3. **Pre-Sync writes can be lost but never corrupted.** A write that
   completed but was not followed by a successful `Sync()` may or
   may not be present after recovery. If absent, the LBA reads as
   zeros. If present, it reads as the bytes that were written —
   never some other LBA's bytes, never garbage.
4. **Boundaries report current truth.** `Boundaries()` returns the
   live `(R, S, H)` triple — synced LSN, retained-window tail, and
   newest written LSN — so the engine can classify recovery without
   the storage taking a position on what those numbers mean.

## What this does NOT prove

These are intentional scope choices. The scope statement
(`sparrow --help`) lists them as not supported:

- Distributed durability across nodes. This backend is single-node.
- Replicated persistence. The `core/transport` primary→replica path
  still operates on in-memory stores in the demos and calibration;
  WAL persistence is admitted as a backend, not adopted as the
  default everywhere.
- SmartWAL semantics. SmartWAL is a future implementation behind the
  same interface; not part of this slice.
- Compaction. The WAL is bounded by configured size, but there is
  no background defragger that rewrites old extents.
- Concurrent writer per LBA. The interface assumes serialized writes
  per LBA.

## The interface

```go
type LogicalStorage interface {
    Write(lba uint32, data []byte) (lsn uint64, err error)
    Read(lba uint32) ([]byte, error)
    Sync() (stableLSN uint64, err error)
    Recover() (recoveredLSN uint64, err error)
    Boundaries() (R, S, H uint64)
    NextLSN() uint64
    NumBlocks() uint32
    BlockSize() int
    AdvanceFrontier(lsn uint64)
    AdvanceWALTail(newTail uint64)
    ApplyEntry(lba uint32, data []byte, lsn uint64) error
    AllBlocks() map[uint32][]byte
    Close() error
}
```

Two implementations satisfy it today:

| Impl | File | Notes |
|---|---|---|
| `BlockStore` | `core/storage/store.go` | in-memory; default for runnable slice and calibration |
| `WALStore` | `core/storage/walstore.go` | WAL+extent, crash-safe; backs `--persist-demo` |

Adding a third implementation: provide a constructor and add it to
`implementations()` in `contract_test.go`. Every contract test runs
against it automatically.

## Execution-layer scope decision

`LogicalStorage` is the **accepted persistence seam** for this slice.
The direction is correct and the contract is honored by both
implementations behind it (in-memory `BlockStore` and crash-safe
`WALStore`, plus the experimental `smartwal.Store` subpackage). The
seam describes what storage IS, not what storage DECIDES.

Three other named execution institutions are **deliberately deferred**
to later phases — not built, not implied as done:

| Institution | What it would own | Why deferred |
|---|---|---|
| `DataCommunicator` | replication + rebuild data movement | currently lives informally in `core/transport`; promoting to a named interface needs its own scope |
| `RecoveryExecutor` | command execution lifecycle (StartCatchUp / StartRebuild / session close handling) | currently lives in `core/adapter` as `VolumeReplicaAdapter`; the boundary is the right shape but the interface name and method set deserve a dedicated phase |
| `ProgressFeed` | a structured progress/trace surface separate from `engine.TraceEntry` | currently every consumer reads `adapter.Trace()` ad hoc; a typed feed would tighten this |

Recording these here so later phases can pick them up explicitly,
not silently inherit "we did this already". The decision matches
the architect-defined V3 execution-layer pattern in
`v3-phase-development-model.md` §16; this slice closes the storage
slot of that pattern only.

Some `LogicalStorage` methods are **replication-facing today** —
`ApplyEntry`, `AllBlocks`, `AdvanceFrontier`, `AdvanceWALTail` — and
exist on the seam because the current `core/transport` rebuild
path needs them somewhere. When the future `DataCommunicator` seam
is named, splitting those methods into a sibling interface
(`ReplicationSink` or similar) is a clean follow-up. They satisfy
contract tests against both backends today, which is enough for
this slice.

## Storage is not authority

`LogicalStorage` is execution machinery. It must not:

- decide recovery class (`catch_up` vs `rebuild`)
- decide mode transitions (`healthy` / `recovering` / `degraded`)
- emit engine commands or interpret events
- read engine projection as control input

Specifically: `WALStore` exposes `Boundaries() (R, S, H)`, but
**interpreting** those boundaries to choose recovery is the engine's
job. The engine reads R/S/H via the adapter and classifies; storage
just reports them.

`Recover()` recovers data, not policy. After `Recover()` returns,
the engine still has no opinion until an `AssignmentObserved` event
lands and the normal route runs.

## On-disk layout (`WALStore`)

A `WALStore` is one preallocated file with three regions:

```
[0 .. 4096)                                 superblock
[4096 .. 4096+walSize)                      circular WAL region
[4096+walSize .. 4096+walSize+volumeSize)   block-indexed extent
```

The superblock records geometry (block size, volume size, WAL size)
and a UUID. The WAL is a circular buffer; head/tail are
monotonically-increasing logical counters so head==tail unambiguously
means empty. The extent is preallocated to hold every addressable
block at its natural offset.

### WAL record format

Each record is variable-size:

```
prefix (30 bytes):
  LSN(8) Reserved(8) Type(1) Flags(1) LBA(8) Length(4)
data (Length bytes for Write, 0 for Trim/Barrier)
trailer (8 bytes):
  CRC32(4) EntrySize(4)
```

CRC covers prefix + data. The trailer's `EntrySize` lets a torn-write
scanner detect mid-record truncation independently of the CRC. The
recovery scanner walks the WAL region, validating CRC and
`EntrySize`; the first invalid record marks the end of useful data.

### Read path

A `Read(lba)`:

1. Looks up `lba` in the dirty map (sharded for low contention).
2. If present: fetches the record from the WAL at the recorded
   offset and returns the data section.
3. If absent: fetches the block from the extent at
   `extentBase + lba * blockSize`.

### Write path

A `Write(lba, data)`:

1. Allocates the next LSN.
2. Encodes a WAL record with that LSN.
3. Appends to the circular WAL (the writer handles wrap-around with
   padding records).
4. Updates the dirty map so subsequent reads see the new bytes.
5. Returns the LSN — **not** durable yet.

### Durability

`Sync()` enqueues an fsync request through the group committer.
Concurrent `Sync()` callers are batched into one fsync per
millisecond (default), so write throughput scales with concurrency
not fsync count. When `Sync()` returns nil, every Write whose LSN is
≤ the returned `stableLSN` is on durable media.

### Recovery

`Recover()` rebuilds the dirty map from the WAL. It:

1. Reads the superblock for WAL geometry and head/tail hints.
2. Scans the known head→tail range, replaying records past the
   checkpoint LSN into the dirty map.
3. Continues scanning past the recorded head, in case writes landed
   after the last superblock update — the first invalid CRC stops
   the extension.

`Recover()` is idempotent.

## How to run the demo

```bash
mkdir -p /tmp/sparrow-persist
go run ./cmd/sparrow --persist-demo --persist-dir /tmp/sparrow-persist
```

Expected output ends with `result: PASS — all blocks survived restart`.

For machine-readable evidence:

```bash
go run ./cmd/sparrow --persist-demo --persist-dir /tmp/sparrow-persist --json
```

```json
{
  "path": "/tmp/sparrow-persist/sparrow-persist.dat",
  "num_blocks": 16,
  "block_size": 4096,
  "n_writes": 4,
  "synced_before_close": 4,
  "recovered_after_open": 4,
  "all_matched": true
}
```

Exit codes:

| Code | Meaning |
|---|---|
| 0 | All blocks round-tripped intact through Sync + Close + Recover |
| 1 | One or more blocks did not survive restart |
| 2 | Usage / flag error (e.g. missing `--persist-dir`) |
| 3 | Runtime error (file create / open / read / write / fsync failed) |

## Backing-store options (carry-forward)

The current `WALStore` and `smartwal.Store` both take a `path string`
and treat it as a regular file: `Truncate` for preallocation,
`O_CREATE|O_EXCL` for create-fresh, kernel page cache for buffering,
`fsync` for the durability boundary.

The `*os.File` API preserves the option of pointing at a raw block
device (`/dev/sdb`, `/dev/nvme0n1`, `\\.\PhysicalDriveN` on Windows)
without an interface change. Going there is **not** in the current
implementation but is **not blocked** by the design. Adding it is a
progression of independent steps, each useful on its own.

| Step | Effort | Net benefit (rough order) |
|---|---|---|
| File-or-device branch in Create/Open: skip `Truncate`, query device size via seek-to-end / `BLKGETSIZE64` | ~2 h | Works on `/dev/sdb`; still page-cached, still fsync-bound. ~3-5x perf vs filesystem |
| Add `O_DIRECT` mode + sector-align WAL records | ~6-8 h | Bypass page cache; predictable latency; ~10x perf |
| Use FUA (Force Unit Access) on writes; drop the group committer | ~4-6 h on top of O_DIRECT | Per-write durability without separate fsync; concurrent writers scale linearly; ~20-50x perf |
| User-space NVMe (SPDK / vfio-user style) | weeks/months | Dedicated CPU, polling queues; ~100-500x perf; different operational model (pinned cores, hugepages) |

These are perf-scale changes only — they do not alter the
`LogicalStorage` contract. A consumer of `LogicalStorage` cannot
tell which backing-store option was chosen, only that the contract
is satisfied.

### NVMe controller features worth tracking

Modern NVMe devices expose hardware features that simplify and
speed up our design once we go device-direct:

| Feature | What it enables |
|---|---|
| FUA (Force Unit Access) bit per write | Drop the group committer; per-write durability with no extra syscall |
| NVM Atomic Write Unit (often 16KB) | Drop per-record CRC for blocks that fit; hardware guarantees torn-free writes |
| T10 PI (8-byte protection info per sector) | End-to-end CRC the device verifies; replaces our application-level CRC for raw devices |
| ZNS (Zoned Namespaces) | Append-only zones with GC-aware semantics; perfect substrate for the WAL region |
| Multiple submission/completion queues | Lock-free per-writer parallelism |
| SR-IOV | Per-volume queue isolation; no noisy-neighbor on hot writers |

None of these are required for Phase 07 closure. They define the
trajectory if and when V3 needs production-class storage performance.

## Future work behind this seam

- **Background flusher.** Currently the WAL is the active storage
  for any LBA that has ever been written; the extent is preallocated
  but unused except as the fallback for never-written LBAs. A
  background flusher that drains dirty entries to extent and
  advances the checkpoint LSN keeps WAL space bounded. The hooks
  (dirty map, group committer, checkpoint LSN field in the
  superblock) are already in place.
- **SmartWAL backend.** A second `LogicalStorage` implementation
  with a denser metadata-only WAL record and extent-first writes.
  No interface change; old data files cannot be read by SmartWAL
  and vice versa.
- **Snapshots.** Out of scope.
- **Replicated persistence.** The transport already replicates
  in-memory; switching the replica side to `WALStore` is a small
  follow-up.
