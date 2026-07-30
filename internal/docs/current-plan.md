# Current Plan: Phase 169 Segmented WAL Group-Commit Engine

Status: active design and implementation milestone.

Phase 167 proved that lane ownership, COW checkpointing, recovery, and bounded
WAL I/O can be made correct, but its ordinary 4 KiB path did not scale. Phase
168 then proved that replacing positioned writes with buffered-file
`io_uring` does not fix the underlying shape: a synchronous single request
still becomes one submission/completion round, and the extra completion path
can cost more syscalls than `pwrite64`.

Phase 169 changes the unit of persistence rather than the syscall API. It tests
whether multiple independently admitted logical writes can become one bounded
contiguous WAL segment and one commit round while retaining exact per-LSN
completion, Sync, corruption, recovery, and rebuild semantics.

## Goal

Build one opt-in segmented group-commit candidate that:

- preserves the global monotonically increasing LSN and contiguous publication
  frontier;
- admits concurrent logical writes into a bounded owner queue;
- encodes multiple records into one checksummed contiguous segment;
- performs one positioned WAL write per segment, without io_uring, direct I/O,
  or registered buffers;
- returns each write only after its own segment write succeeds and never
  publishes through a failed lower LSN;
- gives Sync an explicit target-LSN fence and durable segment/header barrier;
- proves whether fewer persistence rounds improve four-writer scaling without
  penalizing the one-writer path.

This is one large engine milestone. Segment format, owner, failure model,
recovery, benchmark decision, and mounted admission are deliverables inside
Phase 169, not separate phases.

## Why This Follows Phase 168

Phase 168's final evidence was:

```text
native single/legacy        = 0.960x
native four-writer scaling  = 0.963x
native four/positioned      = 0.942x
native batch/positioned     = 0.961x
native selected syscalls    = 1553
positioned selected syscalls= 1052
```

The result rejects a syscall-substitution theory. It does not reject
asynchronous admission or batching. Vitastor's useful lesson is not simply
"use io_uring"; it is to keep explicit operation stages, bounded queues, and
journal-sector/segment aggregation so several logical operations share
persistence work. Phase 169 borrows that mechanism while retaining Seaweed
Block's own LSN, recovery, replication, and frontend contracts.

## Assumptions And Boundaries

- `walstore` remains the default.
- `parallel-walstore` remains opt-in. The segmented format is a separate
  internal candidate and cannot silently open an existing Phase 167 file.
- The initial implementation uses positioned buffered-file I/O. No io_uring,
  `O_DIRECT`, FUA, SQPOLL, registered files, fixed buffers, or raw-device claim
  is part of this milestone.
- No artificial batching delay is allowed for an isolated one-writer request.
  Grouping may use already queued work or a measured bounded handoff, never an
  unbounded timer.
- A segment may contain multiple LSNs and LBAs, but user-visible completion
  remains per request.
- Sync cadence and durability semantics cannot be weakened to pass a
  benchmark.
- Existing iSCSI, NVMe/TCP, NVMe/RDMA, CSI, operation-layer, and replication
  APIs remain unchanged until local engine admission passes.

## Required Invariants

1. Every admitted request owns immutable bytes until terminal completion.
2. Every LSN appears in exactly one committed segment or receives a terminal
   error.
3. Segment decode validates geometry, bounds, header CRC, entry CRC, monotonic
   LSN order, and duplicate LSN/LBA metadata before replay.
4. Torn or corrupt committed segments fail closed; an uncommitted tail is
   ignored only under an explicit tail rule.
5. The publication frontier advances only through the contiguous successful
   LSN prefix, regardless of segment or checkpoint completion order.
6. A failed segment fails every request it owns and prevents higher LSN
   success from escaping.
7. Sync fences the highest LSN admitted before the call and returns only after
   its segment and durable header generation are stable.
8. Queue entries, segment bytes, request count, waiters, and retained WAL bytes
   are bounded.
9. Close drains or terminally completes all admitted requests before closing
   the file. Recover rejects active work.
10. Recovery, catch-up scan, checkpoint, COW rebuild, and source-frontier
    behavior remain equivalent to the accepted Phase 167 contracts.

## Deliverables

### D1. Segment Format And Executable Recovery Proof

- Define the minimum versioned segment header and entry table.
- Bound segment bytes and entry count; reject integer overflow before
  allocation or I/O.
- Prove clean decode, torn tail, bad header CRC, bad entry CRC, duplicate LSN,
  non-monotonic LSN, invalid LBA, and truncated payload.
- Reopen and recover mixed one-entry and multi-entry segments without a product
  selector.

### D2. One Bounded Group-Commit Owner

- Add one owner queue for the candidate.
- Form a segment from already queued requests up to explicit byte/count bounds.
- Avoid a forced delay when no other request is ready.
- Complete every request from the segment result and expose admitted requests,
  segments, entries/segment, bytes/segment, queue-full, and high-water metrics.
- Prove one writer, concurrent writers, same-LBA order, queue saturation, and
  no goroutine/buffer growth.

### D3. Publication, Sync, And Terminal Failure

- Feed segment completions into the existing contiguous global-LSN ledger.
- Implement target-LSN Sync and dual-header durability without per-request
  fsync.
- Inject short segment writes, `EIO`, fsync failure, header failure, and lower
  segment failure with completed higher work.
- Prove Close, recovery exclusion, and no orphaned waiter.

### D4. Checkpoint, Retention, Rebuild, And Replication Equivalence

- Reuse the accepted COW extent/checkpoint design where possible.
- Define segment-level retention and wrap/reuse fences.
- Prove catch-up scan and source-frontier behavior across segment boundaries.
- Run corruption, header fallback, wrap, aborted rebuild, and restart matrices.
- Run RF3 logical replication tests without changing the replication protocol.

### D5. Comparable Linux Performance Decision

- Compare segmented candidate, Phase 167 positioned parallel WAL, and legacy
  WAL in one rotated time-driven m02 session.
- Include 4 KiB Write and 16-block WriteBatch with 1/2/4/8 writers where
  applicable.
- Report median/range, p50/p95/p99, allocations, CPU, queue depth,
  entries/segment, segments, write syscalls, checkpoint/recycle I/O, and exact
  Sync cadence.
- Use external strace/perf where available.
- Admit only if:
  - one-writer throughput is at least 90% of same-run legacy;
  - four-writer throughput is at least 1.5x candidate one-writer throughput;
  - four-writer candidate throughput is not below positioned parallel WAL;
  - p99 and sample range remain bounded;
  - zero fallback, queue saturation, short write, or durability weakening is
    present.

### D6. Mounted And RF3 Admission

- Run only if D5 admits the candidate.
- Build matching product/CSI images.
- Run mounted NVMe/TCP RF1 concurrent write/read/flush/checksum and restart
  recovery.
- Run RF3 sync-quorum with delayed peer, catch-up/rebuild, restart, status
  honesty, and zero-residue cleanup.
- Keep NVMe/RDMA performance and reconnect claims separate.

## Stop Rules

Stop and remove the segmented candidate if:

- one-writer cost cannot stay within 90% of legacy without weakening
  durability;
- concurrent writes still map approximately one-to-one to persistence rounds;
- batching requires an arbitrary latency sleep rather than queued work;
- corruption or recovery needs ambiguous tail rules;
- the candidate only wins synthetic WriteBatch while ordinary Write remains
  unscaled;
- mounted correctness would be used to excuse a failed local performance gate.

An honest rejection remains a valid outcome. It must leave the Phase 167
positioned backend and default `walstore` intact.

## Exit Criteria

```text
segment format proof
-> bounded group-commit owner
-> exact publication/Sync/failure semantics
-> checkpoint/rebuild/replication equivalence
-> same-run scaling decision
-> mounted RF1/RF3 only if admitted
-> promote, retain research-only, or remove
```

The milestone succeeds only through an evidence-backed decision, not by adding
another backend name.
