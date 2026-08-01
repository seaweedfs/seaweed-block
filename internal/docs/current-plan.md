# Current Plan: Phase 173 Storage Execution Architecture Decision

Status: active; D1 fixed-work measurement is closed and D2 attribution is next.

## Why This Is Next

Phases 167-172 rejected five plausible optimizations without weakening their
gates:

- a parallel WAL backend did not beat the shipped path;
- native `io_uring` reduced no dominant persistence round;
- segmented group commit did not scale;
- staged append ownership had no stable batching headroom;
- extent coalescing opportunity was workload-dependent;
- WAL materialization cut physical reads but improved four-writer throughput
  only `1.068x` and produced a `2.031x` sample range.

The Phase 172 CPU profile explains why another local patch is unlikely to
close the gap:

```text
flusher.flushOnceInternal cumulative CPU: 38.6%
WALStore.Write cumulative CPU:             34.2%
syscall flat CPU:                          32.9%
foreground WAL pwrite cumulative CPU:      17.8%
flusher pread cumulative CPU:              13.3%
mutex lock/spin cumulative CPU:            about 11%
```

No single stage dominates. Foreground append, background WAL readback, extent
writeback, fsync, and lock scheduling share one file and one volume-level
execution context. Vitastor's useful lesson is not a specific algorithm to
copy; it is to make journal/data ownership, queue depth, durability boundaries,
and completion order explicit before optimizing.

Phase 173 is therefore one large decision-and-delivery milestone. It first
stabilizes the measurement model, then attributes the complete shipped path,
then selects at most one architectural candidate. Implementation, recovery,
replication, mounted validation, and promotion remain in this phase rather
than being split into many semantic micro-phases.

## Goal

Produce one defensible answer to:

```text
Which architectural boundary, if any, can materially improve shipped
WALStore throughput while preserving its durability and recovery contract?
```

The eligible outcomes are:

1. implement and promote one evidence-selected architecture;
2. implement and retain one opt-in candidate with named blockers;
3. reject all candidates and publish the measured shipped-backend envelope.

All three are valid. Repeating a previously rejected mechanism without new
evidence is not.

## Non-Goals

- No immediate `io_uring`, SQPOLL, fixed-buffer, `O_DIRECT`, FUA, or raw-device
  implementation.
- No automatic switch from `walstore` to `parallel-walstore` or `smartwal`.
- No disk-format change before a format/recovery design is selected.
- No disabled-flusher, checkpoint-free, page-cache-only, or sink-only number
  may be presented as product throughput.
- No frontend, replication, or mounted result may be inferred from an engine
  microbenchmark.
- No threshold changes after candidate results are visible.

## Required Invariants

1. Acknowledged writes remain recoverable after process kill and host restart.
2. Checkpoint and recyclable tail never advance over incomplete work.
3. Same-LBA order is deterministic; cross-LBA concurrency cannot publish a
   non-contiguous durable frontier.
4. Direct BASE/rebuild ownership cannot race with background writeback.
5. WAL corruption and malformed geometry fail closed with typed evidence.
6. Sync fences every write admitted before the call and does not wait for work
   admitted later.
7. Close blocks new mutation, drains owned work, and reports terminal failure.
8. RF3 acknowledgement and catch-up semantics remain unchanged.
9. Memory and queue ownership are bounded and visible.
10. Rejected candidates leave the shipped format and defaults unchanged.

## Deliverables

### D1. Deterministic Fixed-Work Measurement Contract

Status: closed at `29897cc`. Exact Linux QA passed on `/dev/nvme0n1p1` with
64 measured rows, 32 precondition runs, and combined four-writer max/min ratios
of `1.143`, `1.129`, `1.078`, and `1.143` across the four required shapes. See
`internal/docs/qa-assignments/phase173-d1-fixed-work-baseline-qa-signoff.md`.

- Replace Go benchmark auto-calibration as the admission source with a
  fixed-operation harness. Keep Go benchmarks as diagnostics only.
- Use a predeclared operation count, warmup, file size, LBA sequence, writer
  count, 100 ms flusher, one final Sync, and complete drain.
- Run ordinary sequential/scattered 4 KiB, explicit 16-block batch, and
  mounted-sized mixed write shapes at 1/2/4/8 writers.
- Record foreground wall time and p50/p95/p99 separately from Sync/drain.
- Record CPU affinity, scheduler, kernel, filesystem, mount options, device,
  page-cache policy, free space, temperature/throttling evidence, and
  background load.
- Require two independent five-run baseline sets with four-writer max/min
  range at most `1.25x` before any architecture admission. If the harness
  cannot meet that bound, fix the harness/lab and stop.

### D2. Complete Shipped-Path Attribution

- Correlate product counters with exact-path `strace`, `perf stat`, CPU/memory
  profiles, lock wait, `iostat`, and final checkpoint evidence.
- Attribute foreground WAL encode/copy/CRC/pwrite/lock, flusher
  snapshot/pread/decode/extent-pwrite/fsync/checkpoint, replication wait, and
  frontend cost separately.
- Report time and operation count, not percentages alone.
- Prove all counters reconcile with fixed logical operations and bytes.

### D3. Diagnostic Architecture Controls

Run test-only controls in the same session; none are product candidates:

- foreground append ceiling with writeback deferred, clearly labeled
  non-durable/non-product;
- prefilled flusher-only drain with no foreground writers;
- current shared-file WAL/extent path versus a same-device split-file scratch
  control;
- current volume lock versus a bounded no-contention single-writer control;
- engine-only versus NVMe/TCP frontend and RF1 versus RF3 acknowledgement.

Select at most one direction:

- **Owner/queue redesign** only if lock/scheduling is a stable dominant cost;
- **WAL/extent media separation** only if same-file I/O/fsync interference is
  a stable dominant cost;
- **No backend change** if frontend, replication, device, or benchmark
  variance dominates or the shipped path already meets the target envelope.

### D4. Selected Architecture Contract

Required only when D3 selects a candidate.

- Define owner, queue, backpressure, durability frontier, checkpoint, recycle,
  Close, and failure semantics before code.
- If storage layout changes, version it explicitly and define create, reopen,
  upgrade refusal, rollback refusal, support-bundle evidence, and cleanup.
- Add one dry-run/diagnostic proof and one fail-closed rejected operation
  before implementing mutation.
- Independent design review must find no unresolved correctness blocker.

### D5. Bounded Candidate Implementation

- Implement only the selected architecture behind an internal or explicit
  opt-in boundary.
- Add no second candidate and no speculative configurability.
- Keep allocations, queues, buffers, and in-flight operations bounded.
- Add exact counters for ownership, queue depth, I/O rounds, bytes, wait,
  completion, failure, and fallback.

### D6. Correctness, Recovery, And Replication Gate

- Same-LBA and cross-LBA concurrency, partial writes, batch wrap, WAL pressure,
  Sync/Close, direct BASE, recycle floors, malformed records, and crash
  windows.
- Reopen, `ScanLBAs`, catch-up, returned-replica rebuild, RF1, and RF3
  component suites.
- CGO race repetition and real Linux SIGKILL windows.
- No fallback or recovery branch may hide candidate failure.

### D7. Same-Run Performance Admission

Use the D1 fixed-work harness and unchanged thresholds:

- one-writer ordinary throughput at least `0.95x` shipped baseline;
- four-writer ordinary median at least `1.30x` shipped baseline;
- candidate four-writer max/min range at most `1.25x`;
- ordinary four-writer p99 no worse than `1.10x`;
- no workload regresses more than 10%;
- failed operations/cycles zero and checkpoint coverage complete;
- product counters agree with OS/device evidence.

Reject and remove the candidate if any required condition fails.

### D8. Mounted RF1/RF3 Close Gate

Run only after D7 admits the candidate:

- exact matching product/CSI images;
- mounted NVMe/TCP RF1 concurrent write/read/checksum, sustained pressure,
  restart, and zero-residue cleanup;
- RF3 sync-quorum with delayed peer, catch-up/rebuild, restart, honest status,
  and continuous mounted I/O;
- same-session shipped baseline/candidate comparison;
- no default promotion until both correctness and performance gates pass.

## Stop Rules

Stop before implementation when:

- fixed-work baseline remains noisier than `1.25x`;
- counters do not reconcile with logical work;
- diagnostic controls identify no dominant architecture boundary;
- the proposed direction repeats a Phase 167-172 rejection without new
  evidence;
- the candidate requires weaker durability, recovery, corruption, cleanup,
  or RF3 semantics;
- benefit exists only in a non-product control.

## Exit Criteria

```text
stable fixed-work baseline
-> complete shipped-path attribution
-> diagnostic architecture controls
-> one selected design or honest no-change decision
-> bounded implementation only if selected
-> correctness/recovery/replication
-> same-run admission
-> mounted RF1/RF3 only if admitted
-> promote, retain opt-in with blockers, or remove
```
