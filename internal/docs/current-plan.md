# Current Plan: Phase 172 WAL Materialization Pipeline

Status: active evidence-first shipped-backend milestone.

## Why This Is Next

Phase 171 rejected bounded contiguous extent writeback without weakening its
gate. Its instrumentation exposed a different stable amplification in the
default `walstore` flusher:

```text
ordinary dirty entry:
  ReadAt(header)       = 1 operation
  ReadAt(full record)  = 1 operation
  extent WriteAt       = 1 operation

Linux strace control:
  workload             = BenchmarkPhase167WALStoreContention/writers_4
  logical writes       = 67,539
  pread64              = 155,282 calls
  pwrite64             = 155,304 calls
  fsync                = 44 calls
```

The header is read once to discover and validate the record, then read again as
part of the full record. Product counters report one header read and one record
read per validated dirty entry across sequential and scattered workloads. This
is deterministic work, unlike the snapshot-dependent coalescing opportunity
that failed Phase 171 admission.

The next candidate therefore changes only WAL materialization: provide enough
trusted in-memory record geometry to issue one bounded read, then run the
existing decode, CRC, LSN, type, LBA, length, data-offset, wrap, and stale-slot
checks before using any bytes or advancing checkpoint.

This is one large milestone. Record identity, single-read materialization,
shared-record reuse, corruption and concurrency behavior, Linux performance,
and conditional mounted admission are deliverables inside Phase 172.

## Goal

Build one disabled-by-default comparison path for the shipped `WALStore` that:

- reduces a current ordinary dirty entry from two WAL reads to one;
- reads a multi-block WAL record once for all snapshot entries that reference
  that exact record;
- preserves the current disk format and complete-snapshot checkpoint rule;
- retains fail-closed validation before extent write or checkpoint progress;
- bounds temporary memory to one WAL record or one configured read chunk;
- keeps trim, wrap, padding, overwrite, direct BASE, retention, Sync, Close,
  recovery, rebuild, and replication semantics unchanged;
- earns promotion only through same-run Linux and mounted evidence.

## Non-Goals

- No new WAL format, backend name, frontend, or public selector in D1-D4.
- No extent-write coalescing rejected by Phase 171.
- No staged append owner rejected by Phase 170.
- No `io_uring` candidate rejected by Phase 168.
- No `O_DIRECT`, raw device, fixed buffer, SQPOLL, FUA, or atomic-write claim.
- No trusting dirty-map metadata without revalidating on-disk record bytes.
- No volume-sized record cache or unbounded grouping map.
- No disabled-flusher or checkpoint-free performance claim.

## Required Invariants

1. Dirty metadata is an address hint, never proof that the WAL slot still
   contains the expected record.
2. Every materialized record passes the existing decode and CRC validation
   before any extent write.
3. Ordinary write validation matches exact LSN, LBA, type, length, flags, and
   data range.
4. Trim validation produces one zero block only after exact trim identity is
   proven.
5. Multi-block entries sharing a WAL offset may reuse bytes only when record
   identity and each block's LSN/LBA/data offset are independently validated.
6. Ring padding and wrap are never exposed as a record and no read crosses the
   WAL-region boundary.
7. A stale slot, short read, CRC mismatch, unsupported type, or metadata
   disagreement fails the complete cycle closed: no dirty deletion,
   checkpoint advance, or tail reuse.
8. Concurrent overwrite keeps the newer dirty entry through the existing
   compare-and-delete contract.
9. Direct BASE and flusher ownership remains governed by the existing extent
   locks; materialization does not create a second extent writer.
10. Checkpoint publication remains extent write -> extent fsync -> checkpoint
    write -> checkpoint fsync -> tail publication.
11. Close, admission, pressure wakeup, recycle pins, and Sync cannot deadlock
    with the candidate path.
12. A rejected candidate leaves the default path and all external contracts
    unchanged.

## Deliverables

### D1. Record Geometry And Baseline Contract

Implementation status: complete. Linux exact-commit D1 admission passed on
`6dd89e7` with sequential/scattered duplicate-read counts `5/5` and `5/5`,
product/`strace` exact-path reads `2048/2048`, race and vet green, and actual
reuse hits zero. D2 was admitted without changing thresholds.

- Extend the dirty-entry in-memory contract with only the geometry needed to
  size one record read. Populate it at ordinary append, trim, WriteBatch,
  multi-block append, and recovery replay.
- Prove this metadata is reconstructed after reopen and is never serialized as
  a new disk format.
- Add deterministic counters for logical dirty entries, unique WAL records,
  materialization reads/bytes, reused-record hits, and validation failures.
- Add a five-run Linux baseline for ordinary, explicit batch, multi-block
  opt-in, wrap, and scattered workloads. Cover legacy trim records with a
  deterministic recovery fixture because `WALStore` has no public trim API.
- Admit D2 only when at least four of five sequential samples and four of five
  scattered samples each report:
  - header reads per validated record in `[0.95, 1.05]`;
  - full-record reads per validated record in `[0.95, 1.05]`;
  - combined materialization reads per validated record at least `1.90`.
- Require a dedicated scoped `strace` control to corroborate the product
  counter shape. Whole-process counts may be reported as qualitative context
  but cannot satisfy admission.

The D1 review exposed and fixed a pre-existing legacy range-trim inconsistency:
recovery expanded the record per block, while dirty reads treated trim length
as payload and the flusher accepted only the base LBA. The corrected fixture
starts from nonzero extents and proves read-only, dirty-read, writeback,
checkpoint, crash/reopen, and explicit-close behavior for a three-block trim.
This changes no disk format or default materialization mode.

### D2. Single-Read Fail-Closed Materialization

Implementation status: complete. Exact-commit Linux D2 correctness gate passed
on `a46af56`: focused and race repetitions were green, the shipped default
remained at `2048` exact-file reads for 1024 records, and the candidate used
exactly `1024` product/`strace` reads with zero shared-record reuse.

- Add one internal disabled-by-default comparison mode.
- Read each ordinary or trim record once at its exact bounded size.
- Decode and validate with the existing CRC and semantic checks; do not add a
  fast parser that bypasses `decodeWALEntry`.
- Return one block view/copy only after validation.
- Prove ordinary write, trim, gapped/reverse snapshot, ring-end padding, wrap,
  short read, stale slot, corrupt header, corrupt payload, flags, and
  unsupported-type behavior.
- Inject first/middle/final materialization failures and prove no false
  checkpoint or dirty deletion.

### D3. Shared Multi-Block Record Materialization

Implementation status: complete. Exact-commit Linux D3 correctness gate passed
on `7d09924`: seven focused and CGO race tests each passed 20 repetitions,
1024 logical blocks produced exactly `64/64` product/`strace` record reads and
`960` actual reuse hits, and D4 was admitted.

- Order the complete snapshot by WAL record identity without omitting entries.
- Read one multi-block record once and validate every referenced block's
  expected LSN, LBA, length, and data offset.
- Keep at most one bounded decoded record live; release it before moving to the
  next record.
- Prove partial overwrite of a shared record leaves newer blocks dirty while
  safely flushing still-current blocks.
- Prove malformed `Reserved`, total length, offset, and wrap metadata fail the
  whole cycle closed.

### D4. Concurrency, Recovery, And Lifecycle Equivalence

Implementation status: complete locally; Linux exact-commit race, SIGKILL,
recovery, and replication gates remain pending. Candidate-on lifecycle fixtures
cover large concurrent snapshots, direct BASE ownership, recycle-floor
recovery, Close final flush/failure, and malformed batch recovery.

D4 exposed and fixed four recovery-contract gaps rather than weakening the
fixtures: a checkpoint inside one multi-block record now replays only its
uncheckpointed suffix; valid-CRC malformed batch geometry returns typed
`WALIntegrity`; recovery restores byte-based writer head/tail before any new
append; and legacy `head==tail` metadata can reconstruct a retained window that
crosses ring wrap. Checkpoint metadata now carries conservative byte boundaries
in its existing write/fsync without adding a new disk field or sync.

- Run foreground Write/WriteBatch and repeated same-LBA overwrite during large
  materialization cycles.
- Exercise direct BASE overlap, pressure wakeups, recycle-floor pins, Sync,
  Close, and final-flush failure under race instrumentation.
- Reopen candidate-created files and run corruption, typed-failure, retention,
  `ScanLBAs`, catch-up, returned-replica rebuild, RF1, and RF3 component suites.
- Add crash windows before materialization, after extent write/fsync, and
  around checkpoint/tail publication.
- No new recovery or replication branch is allowed.

### D5. Comparable Linux Performance Decision

- Compare candidate and unchanged default in one rotated m02 run with five
  one-second repetitions and identical 100 ms flusher/Sync/drain settings.
- Cover ordinary 4 KiB, scattered 4 KiB, explicit 16-block batch, and
  multi-block opt-in at 1/2/4/8 writers where applicable; keep trim as a
  correctness fixture rather than inventing a benchmark-only product API.
- Report foreground throughput and p50/p95/p99 separately from final Sync and
  drain, plus allocations, CPU, scoped syscalls, failed cycles, checkpoint
  coverage, and exact materialization counters.
- Admit only if:
  - WAL materialization reads per validated ordinary entry fall by at least
    45% and match scoped `strace`;
  - one-writer ordinary throughput is at least 95% of baseline;
  - four-writer ordinary median improves by at least 1.15x;
  - candidate four-writer range is at most 1.50x;
  - p99 does not regress by more than 10%;
  - multi-block reuse reduces physical reads without increasing corruption or
    recovery ambiguity;
  - failed cycles remain zero and checkpoint coverage remains complete.

### D6. Mounted RF1 And RF3 Close Gate

- Run only if D5 admits the candidate.
- Build exact matching product and CSI images.
- Run mounted NVMe/TCP RF1 concurrent write/flush/read/checksum, durable
  restart, and sustained writeback pressure.
- Run RF3 sync-quorum with delayed peer, catch-up/rebuild, restart, honest
  status, and continued mounted I/O.
- Compare baseline and candidate in the same lab session and finish with
  product-owned zero-residue cleanup.
- Promote the candidate only after this gate passes; otherwise remove it.

## Stop Rules

Stop and remove the candidate if:

- D1 does not reproduce stable duplicate WAL reads;
- exact record sizing requires trusting unverified disk metadata;
- one-read materialization weakens CRC/type/LSN/LBA/length/offset checks;
- grouping can omit a dirty entry or advance checkpoint over incomplete work;
- temporary memory grows with volume size rather than one bounded record;
- any failure deletes dirty state or publishes checkpoint/tail progress;
- concurrency or lifecycle tests deadlock;
- physical read reduction does not improve ordinary throughput or stability;
- only the opt-in multi-block path improves.

An honest rejection is a valid Phase 172 result. It must leave the current disk
format, default flusher, and all frontend, recovery, replication, and Operation
Layer contracts intact.

## Exit Criteria

```text
record geometry and duplicate-read evidence
-> disabled single-read materialization
-> bounded shared-record reuse
-> concurrency/recovery/lifecycle equivalence
-> same-run Linux performance decision
-> mounted RF1/RF3 only if admitted
-> promote or remove
```
