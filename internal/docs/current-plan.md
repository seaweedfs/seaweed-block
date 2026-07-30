# Current Plan: Phase 170 Default WALStore Staged Commit Pipeline

Status: active evidence-first product-path milestone.

Phase 168 proved that replacing positioned writes with `io_uring` does not
improve a one-request-per-round WAL. Phase 169 proved that a separate segmented
format can group queued requests correctly, but one owner that serializes
encode, CRC, `WriteAt`, and publication still does not scale:

```text
segmented writers=1       101.98 MiB/s
segmented writers=4        78.68 MiB/s
positioned writers=4       79.51 MiB/s
segmented entries/segment   1.348
four/single                 0.772x
four/positioned             0.990x
```

The next experiment must improve the shipped default path rather than add a
third backend. The existing `walstore` format already has the useful primitive:
`walWriter.appendBatch` preserves independently recoverable records while
coalescing adjacent encoded bytes into fewer positioned writes. Phase 170 asks
whether a bounded staged pipeline can use that primitive for concurrent
ordinary writes without changing the disk format, durability contract, or
recovery model.

## Goal

Build one evidence-gated staged commit candidate inside the default
`WALStore` implementation that:

- retains the current superblock, WAL entry, circular-region, dirty-map,
  flusher, checkpoint, and recovery formats;
- validates and owns caller payloads before asynchronous admission;
- assigns one contiguous LSN range only when a bounded batch is ready to
  commit;
- uses the existing `walWriter.appendBatch` path for several ordinary
  existing-format records;
- updates dirty-map and visible head state atomically from the complete append
  result;
- completes each caller only after its record is appended and readable;
- gives Sync an exact fence over requests admitted before the call;
- keeps queue, payload, encoded bytes, waiter count, and batch size bounded;
- proves a material ordinary-write gain before any mounted or RF3 claim.

This is one large milestone. Profiling, owner semantics, Sync/failure,
flusher/recovery compatibility, comparable performance, and mounted admission
are deliverables inside Phase 170.

## Non-Goals

- No new backend name or file-format version.
- No segmented record, direct I/O, `io_uring`, SQPOLL, fixed buffer, FUA, raw
  device, or NVMe atomic-write claim.
- No arbitrary batching sleep. The owner may consume already admitted work;
  an isolated request must not wait for a timer.
- No change to replication protocol, authority, frontend, CSI, or operation
  status semantics.
- No weakening of the rule that `Write` is readable after return but becomes
  crash durable only after a covering successful `Sync`.
- No mounted test used to excuse a failed local engine gate.

## Existing Product Seam

Today `WALStore.Write` holds `s.mu` across:

```text
assign tentative LSN
-> wal.append
   -> encode + copy + CRC
   -> WAL writer lock
   -> one WriteAt
-> dirty-map update
-> advance nextLSN/H
```

The storage instrumentation already exposes:

- WAL copy, encode, and checksum operation/byte/time totals;
- WAL append time and append-lock wait;
- whole-write commit-lock wait;
- `WriteAt` calls, bytes, maximum size, wrap count, and padding;
- dirty-map update operation/time totals.

`walWriter.appendBatch` already validates all entries, plans wrap/capacity
before publication, encodes existing-format entries into a bounded buffer, and
coalesces adjacent bytes into one `WriteAt` where possible. Phase 170 should
reuse it, not create another codec.

## Required Invariants

1. The bytes accepted by existing recovery remain unchanged.
2. Every admitted request owns immutable payload bytes until terminal
   completion.
3. Queue-full or validation rejection consumes no LSN.
4. A failed batch consumes no visible LSN range, publishes no dirty-map entry,
   and returns an error to every request in that batch.
5. A successful batch assigns consecutive LSNs in request order and updates
   WAL offsets, dirty-map entries, `nextLSN`, `S/H`, and caller results as one
   logical commit.
6. Same-LBA requests remain ordered. Read-after-Write observes the latest
   completed LSN.
7. Sync fences the highest request admitted before the call, waits for that
   prefix to finish, then preserves the existing fsync and superblock rules.
8. Flusher/admission wakeups cannot deadlock with the owner, Sync, Close, or
   WAL-pressure callbacks.
9. Circular wrap may split physical writes, but never split logical success or
   expose padding as a record.
10. Close drains or terminally completes every admitted request before file
    close.
11. Recovery, `ScanLBAs`, `R/S/H`, checkpoint, recycle-floor, direct BASE
    install, and retention behavior remain byte-for-byte or semantically
    equivalent.
12. Disabled or rejected candidate code leaves the current synchronous
    `walstore` path unchanged.

## Deliverables

### D1. Product-Path Baseline And Headroom Decision

- Extend the existing `BenchmarkPhase167WALStoreContention` evidence rather
  than creating a synthetic backend.
- Run 4 KiB ordinary Write with 1/2/4/8 writers and explicit final Sync.
- Run the existing explicit `WriteBatch` control to show whether fewer
  `WriteAt` calls have measurable value on the same machine.
- Report median/range, p50/p95/p99, allocations, CPU profile, syscall counts,
  flusher/checkpoint activity, all write-instrumentation stages, and exact
  Sync cadence.
- Separate cumulative CPU work from wall-clock lock wait; do not sum
  overlapping concurrent durations into a false percentage.
- Admit D2 only if the baseline has a reproducible concurrency deficit and
  either record encode/copy/checksum/lock work has material parallel headroom
  or the existing-format batch control proves material syscall/coalescing
  headroom.
- Stop if the profile instead shows an already saturated device or no
  existing-format batch advantage.

### D2. Bounded Existing-Format Commit Owner

- Add one internal, disabled-by-default candidate mode for tests and
  benchmarks; do not add a CLI/product selector yet.
- Reserve request and payload capacity before copying caller bytes.
- Use a fixed queue and explicit maximum entries/bytes per append batch.
- Form batches only from requests already admitted when the owner drains the
  queue; no batching timer.
- Assign a tentative contiguous LSN range, call `walWriter.appendBatch`, and
  publish offsets/dirty-map/frontier only on complete success.
- Prove one writer, concurrent writers, same-LBA order, wrap, queue-full,
  admission failure, partial physical write failure, Close, and bounded memory.
- Run an early same-run performance gate. Remove the owner immediately if it
  neither reduces WriteAt calls nor preserves one-writer cost.

### D3. Sync, Failure, And Read Visibility

- Add an admission/publication fence so Sync covers exactly requests admitted
  before its call.
- Preserve the current logical-Sync semantics and group-committer behavior.
- Prove that a failed lower batch cannot allow a higher LSN to become visible
  or durable.
- Prove read-after-Write, concurrent same-LBA overwrite, Write/Sync races,
  Sync/Close races, and no orphaned waiters.
- Inject encode, WAL-full, wrap-padding, short-write, `WriteAt`, fsync, and
  superblock metadata failures.
- Keep status/instrumentation truthful for queued, batched, failed, and
  completed operations.

### D4. Flusher, Recovery, Retention, And Replication Equivalence

- Run the existing WAL recovery and corruption suites against candidate-created
  files; no new recovery branch is allowed.
- Exercise background flush during active admission and while Sync fences a
  target.
- Prove dirty-map offsets for batched records, circular wrap, checkpoint
  advancement, WAL admission pressure, recycle-floor pins, and restart.
- Prove `ScanLBAs`, catch-up, direct BASE install, `AdvanceFrontier`, and
  rebuild use the unchanged current contracts.
- Run logical replication/RF3 component tests without changing the wire
  protocol.

### D5. Comparable Linux Performance Decision

- Compare candidate and unchanged synchronous `walstore` in one rotated m02
  session with five one-second repetitions.
- Include ordinary 4 KiB Write at 1/2/4/8 writers and explicit 16-block
  WriteBatch controls.
- Report median/range, p50/p95/p99, allocations, CPU, selected syscalls,
  WriteAt calls/bytes, entries per append batch, queue high-water/rejections,
  flusher/checkpoint work, and exact Sync cadence.
- Admit only if:
  - one-writer throughput is at least 90% of baseline;
  - four-writer throughput is at least 1.5x baseline four-writer throughput;
  - candidate four-writer throughput is at least 1.3x candidate one-writer
    throughput, unless both paths are shown to hit the same measured device
    ceiling;
  - ordinary writes average more than one entry per append batch and reduce
    `WriteAt` calls per entry;
  - p99 and five-run range remain bounded;
  - queue-full, fallback, partial-write, durability, and recovery counters are
    clean.

### D6. Mounted RF1 And RF3 Close Gate

- Run only if D5 admits the candidate.
- Build matching product and CSI images from the exact candidate commit.
- Run mounted NVMe/TCP RF1 concurrent Write/Flush/Read/checksum and durable
  restart recovery.
- Run RF3 sync-quorum with a delayed non-quorum peer, catch-up/rebuild,
  restart, status honesty, and continued mounted I/O.
- Compare baseline and candidate in the same lab session.
- Finish with product-owned cleanup verification and zero Kubernetes, NVMe,
  iSCSI, process, and durable-path residue.
- Add a product setting or change the default only after this gate passes.

## Stop Rules

Stop and remove the staged owner if:

- D1 cannot identify a measured parallelizable or coalescible cost;
- owner batching again serializes four writers below the synchronous baseline;
- batching requires an arbitrary latency delay;
- LSN assignment, dirty-map publication, or Sync needs ambiguous rollback;
- WAL pressure can deadlock because the owner waits for the flusher while the
  flusher waits for owner-held state;
- performance depends on disabling normal flusher/checkpoint/recovery work;
- only explicit WriteBatch improves while ordinary Write remains unscaled.

An honest rejection is a valid Phase 170 outcome. It must leave the default
`walstore`, on-disk format, and all operation/frontend contracts intact.

## Exit Criteria

```text
default-product baseline and headroom proof
-> bounded existing-format staged owner
-> exact LSN/visibility/Sync/failure semantics
-> unchanged flusher/recovery/retention/rebuild contracts
-> same-run Linux performance decision
-> mounted RF1/RF3 only if admitted
-> promote or remove
```

The milestone succeeds through an evidence-backed product-path decision, not
through another backend name.
