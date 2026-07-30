# Current Plan: Phase 171 Default WALStore Checkpoint Pipeline

Status: active evidence-first shipped-backend milestone.

Phase 170 proved that WAL append-call coalescing is mechanically effective but
not sufficient under the complete default `walstore` workload:

```text
ordinary writers=4                  64.09 MiB/s
explicit batch writers=4           47.22 MiB/s
batch/ordinary                       0.737x
ordinary WriteAt calls/entry         1.000
batch WriteAt calls/entry            0.06250
paired batch gains                   2/5
ordinary four-writer range           2.936x
batch four-writer range              3.500x
```

The batch control reduced WAL `WriteAt` calls by about 16x, yet throughput and
stability did not improve. A staged append owner was therefore rejected before
implementation.

The next measured seam is the other half of the shipped persistence pipeline.
The default flusher snapshots the complete dirty map and, for each current
entry, reads and validates WAL location metadata, reads block data, issues one
extent `WriteAt`, fsyncs the shared file, persists the checkpoint, and advances
the recyclable tail. This work competes with foreground WAL append on the same
file and currently exposes only flush-count and byte-count evidence.

The plan review also found correctness debt in this path that must be fixed
before optimization:

- `persistCheckpoint` currently updates the in-memory checkpoint before its
  superblock write succeeds, so a later cycle can incorrectly treat a failed
  checkpoint as reusable;
- checkpoint metadata is not fsynced before in-memory WAL-tail reuse;
- `Close` marks the store closed before `flusher.Stop` requests its final
  cycle, so that promised final flush currently exits without work;
- default `walstore` has no explicit extent-write ownership rule between
  flusher writeback and direct rebuild BASE installation;
- a WAL-slot mismatch currently deletes the dirty entry and continues, even
  though the flusher has not proved that entry was already durably
  checkpointed.

Phase 167 already proved that bounded contiguous checkpoint writes can reduce
writeback syscall amplification in the opt-in `parallel-walstore`. Phase 171
tests the corresponding mechanism in the default `walstore`, without changing
its disk format, recovery rules, or public storage contract.

## Goal

Build one evidence-gated checkpoint/writeback improvement for the shipped
`WALStore` that:

- first makes checkpoint publication, tail reuse, Close, and direct BASE
  overlap failure-safe;
- measures foreground WAL and background checkpoint work independently;
- preserves the current complete-snapshot checkpoint invariant;
- materializes every verified latest dirty block before advancing checkpoint;
- combines contiguous extent LBAs into bounded positioned writes;
- retains compare-and-delete protection against concurrent overwrite;
- keeps circular-WAL validation, retention pins, Sync, recovery, direct BASE
  install, and rebuild semantics unchanged;
- reduces checkpoint I/O operations and stabilizes ordinary-write throughput
  before any mounted or RF3 claim.

This is one large milestone. Instrumentation, bounded writeback, failure and
concurrency semantics, recovery equivalence, Linux performance, and conditional
mounted admission are deliverables inside Phase 171.

## Non-Goals

- No new WAL format, backend name, frontend protocol, or product selector.
- No staged append owner from Phase 170.
- No `io_uring`, direct I/O, raw device, SQPOLL, fixed buffer, FUA, or NVMe
  atomic-write claim.
- No partial dirty-map snapshot that advances checkpoint past omitted entries.
- No disabled-flusher performance claim.
- No arbitrary sleep added to make benchmark samples look stable.
- No replication, authority, CSI, or Operation Layer semantic change.
- No WAL-read coalescing unless D2 evidence shows it is the next measured
  limiter after extent-write coalescing.

## Existing Product Seam

The current `flusher.flushOnce` path is:

```text
snapshot all latest dirty-map entries
-> read one WAL header per entry and validate LSN/location
-> read each current block from WAL
-> WriteAt one block into its natural extent slot
-> fsync the shared file
-> persist checkpoint metadata
-> advance WAL tail
-> compare-and-delete each flushed dirty entry
```

This shape is correct but syscall-heavy. It also rereads WAL metadata when
`readFromWAL` materializes data after the first validation pass. The initial
change must target only extent writeback; WAL read materialization remains a
separate measured decision.

The proven helper shape in `core/storage/parallelwal`:

- sorts checkpoint LBAs;
- groups only physically contiguous destination blocks;
- caps each positioned extent write at 1 MiB;
- returns an explicit operation count;
- does not alter the WAL record format.

Phase 171 may reuse the algorithm, not the parallel backend's header, lane,
extent-switch, or recovery model.

## Required Invariants

1. The current superblock and WAL bytes remain unchanged.
2. A flush cycle advances in-memory or on-disk checkpoint only after every
   current entry in its complete snapshot has been verified, materialized,
   written, covered by a successful extent fsync, and followed by a successful
   checkpoint-metadata write and fsync.
3. A failed extent write or fsync advances neither checkpoint nor recyclable
   tail. Partial extent writes remain safe because recovery replays the
   retained WAL.
4. A concurrent newer write to the same LBA remains visible and dirty.
   Compare-and-delete may remove only the exact flushed LSN.
5. A WAL-slot/header mismatch fails the flush cycle closed. It removes no dirty
   entry and advances no checkpoint unless durable checkpoint evidence proves
   that exact entry is already obsolete.
6. Contiguous coalescing never crosses an LBA gap, block-size boundary,
   configured maximum write size, or extent-region boundary.
7. Circular WAL wrap, padding, stale-slot detection, trim, ordinary records,
   and multi-block record data offsets retain current behavior.
8. Foreground append and background extent writes may overlap only in disjoint
   file regions. Flusher writeback and direct rebuild BASE installation need
   an explicit extent ownership rule so a coalesced range cannot overwrite a
   newer BASE block.
9. Recovery-retention and active rebuild pin floors continue to clamp
   checkpoint/tail advancement exactly as today.
10. Sync retains its current target-head and fsync semantics. Checkpoint
   optimization may not claim a higher durable frontier or hide a failed
   flusher cycle.
11. Close rejects new writes, performs one final best-effort complete flush
    while the file remains usable, and never closes the file while writeback
    is active.
12. Instrumentation distinguishes WAL append, WAL readback, extent writeback,
    fsync, checkpoint metadata, and cleanup work. Counters cannot turn a
    partial cycle into a successful cycle.
13. A rejected candidate leaves default flusher behavior and all external
    contracts unchanged.

## Deliverables

### D1. Correctness Hardening And Full-Pipeline Baseline

- Add failure injection around checkpoint metadata write/fsync. Prove failure
  advances neither in-memory checkpoint nor WAL tail, retry succeeds, and
  crash/reopen uses only a durably published checkpoint.
- Require a checkpoint metadata fsync before any corresponding WAL-tail reuse.
- Repair or explicitly replace the current Close sequencing so the promised
  final flush can run after new writes are fenced but before file close. Prove
  dirty close/reopen and final-flush failure behavior.
- Define and test flusher-versus-direct-BASE extent ownership. A direct block
  and a coalesced range that overlap must resolve according to the rebuild
  bitmap/frontier contract rather than goroutine timing.
- Make an unexplained WAL-slot/header mismatch fail the cycle closed with no
  dirty deletion, checkpoint advance, or tail reuse. Only explicit durable
  checkpoint evidence may classify an entry as safely obsolete.

- Add product-owned counters for snapshot entries, validated/stale entries,
  WAL header/data read operations and bytes, extent write operations and
  bytes, maximum extent write size, fsync time/count, checkpoint metadata
  operations, successful/failed cycles, and cycle latency.
- Extend the Phase 170 ordinary-Write benchmark rather than creating a
  synthetic storage backend.
- Run 4 KiB ordinary Write and explicit 16-block WriteBatch with 1/2/4/8
  writers, normal 100 ms flusher cadence, explicit final Sync, and complete
  final drain.
- Report five-run median/range, p50/p95/p99, allocations, foreground stage
  metrics, flusher metrics, CPU, selected syscalls, checkpoint coverage, and
  exact Sync cadence.
- Report foreground Write duration, final Sync duration, and final drain
  duration separately. Throughput admission uses the foreground interval,
  while drain improvement remains a separately named operational result.
- Report contiguous-run count/distribution and the theoretical minimum bounded
  extent-write operations for both sequential and randomized-LBA workloads.
  D2 is not admitted when realistic snapshots have no material coalescing
  opportunity.
- Verify counters against deterministic small fixtures and external
  `strace` where available. Product counters are authoritative for scoped
  work; whole-process `strace` remains qualitative unless a dedicated test
  binary isolates the operation.

### D2. Bounded Contiguous Extent Writeback

- Add one internal, disabled-by-default comparison mode before changing the
  shipped default.
- Sort the existing complete dirty snapshot by LBA without changing which
  entries are included.
- Materialize and write contiguous destination LBAs in bounded chunks of at
  most 1 MiB.
- Keep only one bounded data chunk live at a time; do not build an additional
  volume-sized data map.
- Preserve fail-closed stale-slot handling, trim zeroing, multi-block
  `dataOffset`, max-LSN tracking, extent-fsync then
  checkpoint-write/fsync ordering, WAL-tail advancement, and
  compare-and-delete behavior.
- Prove contiguous, gapped, reverse snapshot order, overwrite, trim,
  multi-block, wrap, maximum chunk, final partial chunk, and empty-cycle
  behavior.
- Inject first/middle/final extent-write failure and fsync failure. Each must
  retain recovery through the unchanged WAL and publish no false checkpoint.

### D3. Concurrency, Scheduling, And Optional WAL Readback Decision

- Exercise foreground Write/WriteBatch while a large flush cycle is active,
  including repeated same-LBA overwrite and WAL pressure wakeups.
- Exercise direct BASE installation against overlapping and adjacent flusher
  ranges; no result may depend on physical write completion order.
- Prove admission, flusher, Sync, retention pin, and Close cannot deadlock.
- Measure flusher-cycle occupancy, foreground p99, extent operations per block,
  and WAL read operations per block after D2.
- If WAL readback is then a material limiter, add one separately gated bounded
  read-materialization change that retains per-record LSN/type/length checks
  and handles ring wrap. Otherwise record the no-change decision.
- Do not combine fsync scheduling or group-committer redesign into this
  deliverable unless a dedicated trace proves duplicate/concurrent fsync is
  the remaining dominant cost.

### D4. Recovery, Retention, Rebuild, And Replication Equivalence

- Run existing WAL recovery, corruption, typed-failure, flusher, retention,
  recycle-pin, direct-frontier, and close/reopen suites against
  candidate-created files.
- Add crash windows after extent fsync, after checkpoint pwrite, after
  checkpoint fsync, and before/after WAL-tail publication.
- Prove `ScanLBAs`, catch-up, direct BASE install, `AdvanceFrontier`, and
  returned-replica rebuild retain their current contracts.
- Exercise checkpoint advancement under an active recycle-floor pin and after
  pin release.
- Run storage race coverage and RF1/RF3 logical replication component tests.
- No new recovery or replication branch is allowed.

### D5. Comparable Linux Performance Decision

- Compare candidate and unchanged default flusher in one rotated m02 session
  with five one-second repetitions.
- Include ordinary 4 KiB Write and explicit 16-block WriteBatch at 1/2/4/8
  writers with identical flusher, Sync, file, and checkpoint settings.
- Report median/range, p50/p95/p99, allocations, CPU, scoped/selected syscalls,
  append and flusher stage metrics, extent operations per block, checkpoint
  coverage, pressure waits, and failed-cycle counters. Foreground, final Sync,
  and final drain durations must remain separate.
- Admit the candidate only if:
  - one-writer ordinary throughput is at least 95% of baseline;
  - four-writer ordinary throughput improves by at least 1.25x;
  - candidate four-writer median is not below candidate one-writer median
    unless measured device saturation explains the ceiling;
  - extent `WriteAt` operations per contiguous block fall materially and match
    deterministic counter/trace evidence;
  - ordinary four-writer range is at most `1.50x` and improves materially from
    the Phase 170 `2.936x` baseline;
  - p99, WAL pressure, failed cycles, checkpoint coverage, and cleanup remain
    bounded and correct.

### D6. Mounted RF1 And RF3 Close Gate

- Run only if D5 admits the candidate.
- Build matching product and CSI images from the exact candidate commit.
- Run mounted NVMe/TCP RF1 concurrent Write/Flush/Read/checksum, durable
  restart, and sustained writeback pressure.
- Run RF3 sync-quorum with a delayed non-quorum peer, catch-up/rebuild,
  restart, honest status, and continued mounted I/O.
- Compare baseline and candidate in the same lab session.
- Finish with product-owned cleanup verification and zero Kubernetes, NVMe,
  iSCSI, process, and durable-path residue.
- Change the default flusher only after this gate passes.

## Stop Rules

Stop and remove the candidate if:

- D1 does not show meaningful extent-write or WAL-read amplification;
- D1 realistic snapshots have too few contiguous runs for bounded coalescing
  to reduce extent operations materially;
- operation reduction does not improve ordinary Write throughput or stability;
- coalescing requires omitting dirty entries or advancing checkpoint over
  incomplete work;
- bounded writeback cannot retain trim, multi-block, wrap, stale-slot, and
  overwrite semantics;
- a WAL-slot mismatch can delete dirty state or permit checkpoint progress
  without durable obsolescence evidence;
- pressure, Sync, Close, or recycle pins can deadlock with the flusher;
- a failed partial cycle can become a visible checkpoint;
- checkpoint metadata is not durable before the corresponding WAL bytes become
  reusable;
- Close cannot fence new writes while still completing its promised final
  flush;
- direct BASE and flusher overlap has no deterministic ownership rule;
- performance depends on disabling normal flush, fsync, checkpoint, recovery,
  or retention work;
- only explicit WriteBatch improves while ordinary Write remains unstable.

An honest rejection is a valid Phase 171 outcome. It must leave the current
disk format, default flusher, and all frontend/replication/operation contracts
intact.

## Exit Criteria

```text
checkpoint/Close/BASE correctness hardening plus full-pipeline evidence
-> bounded contiguous extent writeback
-> concurrency and optional WAL-read decision
-> unchanged recovery/retention/rebuild/replication semantics
-> same-run Linux performance decision
-> mounted RF1/RF3 only if admitted
-> promote or remove
```

The milestone succeeds by proving or disproving a complete shipped-path
improvement, not by reducing one counter in isolation.
