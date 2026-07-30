# Current Plan: Phase 168 Linux Native Async WAL Execution Milestone

Status: active design and implementation milestone.

Phase 167 removed replication waits under the whole-volume lock, built an
opt-in parallel WAL format, and passed its ordering, corruption, recovery, and
rebuild gates. It also removed two large avoidable syscall amplifications:

- stable checkpoint writeback changed from one `pwrite64` per LBA to bounded
  contiguous writes;
- recycle verification changed from one `pread64` per record to bounded
  contiguous reads while retaining per-record decode and CRC validation.

The exact Phase 167 gate still rejected promotion:

| Shape | `parallel-walstore` | legacy `walstore` |
|---|---:|---:|
| 4 KiB, 1 writer | 49.79 MiB/s | 107.85 MiB/s |
| 4 KiB, 4 writers | 39.25 MiB/s | 104.08 MiB/s |
| 16-block batch, 4 writers | 116.95 MiB/s | 80.00 MiB/s |

The batch path is useful, but ordinary writes still issue one positioned write
per request when each writer maps to a different lane. Four-writer scaling was
`0.788x`, so the backend remains opt-in and the default remains `walstore`.

Phase 168 is the next evidence-gated execution experiment: submit independent
lane writes asynchronously on Linux so multiple non-contiguous WAL records can
share submission/completion machinery without changing the Phase 167 disk
format or global LSN contract.

## Goal

Build one Linux-native asynchronous execution candidate that:

- keeps Phase 167's deterministic lane ownership and contiguous global LSN
  publication;
- aggregates non-contiguous lane writes into bounded asynchronous submission
  rounds;
- models write, durability barrier, completion, and terminal failure
  explicitly;
- retains the existing positioned-I/O backend as the portable and runtime
  fallback;
- proves or disproves a concrete 4 KiB throughput/scaling hypothesis before
  any default switch or mounted RF3 claim.

This is one large milestone. Capability probing, queue ownership, submission,
barriers, metrics, and live gates are deliverables inside Phase 168, not
separate phases.

## Industry And Vitastor Lessons

The local Vitastor review at `C:\work\vitastor-review` shows mechanisms worth
testing:

- `src/util/ringloop.*` owns one `io_uring`, reserves SQEs, submits a batch, and
  drives completion callbacks from CQEs;
- `src/blockstore/blockstore_impl.*` keeps a submit queue and a bounded
  `max_write_iodepth` instead of letting callers issue unbounded disk work;
- blockstore write/sync code models data write, journal write, metadata, fsync,
  and stable completion as explicit stages;
- journal sector buffers combine updates before submission instead of paying
  one syscall for every logical operation.

Seaweed Block should borrow those execution mechanisms, not Vitastor's whole
architecture. This phase does not copy its etcd placement, PG peering,
object-version model, raw-device layout, immediate-commit assumptions, or
licensing-sensitive source.

## Assumptions And Boundaries

- `walstore` remains the default.
- `parallel-walstore` remains opt-in. A Linux-native mode must be separately
  explicit until it passes all fallback, recovery, and mounted gates.
- The on-disk Phase 167 header, WAL record, lane mapping, COW extents, and
  recovery algorithm do not change merely to add asynchronous submission.
- Global LSN allocation and publication remain contiguous. CQE completion
  order may differ from LSN order; user-visible success may not cross a hole.
- The first prototype uses ordinary file semantics. `O_DIRECT`, registered
  files, fixed buffers, SQPOLL, FUA, and device atomic-write support require
  separate evidence and are not bundled into the initial comparison.
- Linux capability or policy refusal must be explicit and must fall back only
  when the user selected an automatic mode. An explicitly requested native
  mode fails clearly rather than silently changing paths.
- No new third-party dependency is accepted until a small executable spike
  proves kernel support, shutdown safety, and a maintainable license/build
  boundary.
- Existing iSCSI, NVMe/TCP, NVMe/RDMA, CSI, recovery, and operation-layer
  contracts remain unchanged.

## Required Invariants

1. Every admitted request owns immutable bytes until its completion is
   consumed.
2. One lane sequence maps to exactly one WAL slot and exactly one terminal
   completion.
3. A short write, negative CQE, canceled request, or missing completion
   terminal-faults the store; it never becomes a successful higher LSN.
4. `H` advances only through the contiguous completed global prefix.
5. `Sync` fences the highest request admitted before the call and completes
   only after every request through that fence and the required fsync/barrier
   complete successfully.
6. A later submission round may not reuse a ring slot until the existing
   checkpoint, dual-header seal, and reuse fence permit it.
7. Queue saturation returns typed backpressure. It may not allocate
   unbounded goroutines, SQEs, or buffers.
8. Close drains or terminally completes all admitted work before releasing the
   file and ring. Recover refuses active work.
9. Capability fallback is observable through metrics and logs. A fallback run
   cannot pass a native-path performance gate.
10. The positioned-I/O and native backends recover byte-identical data and
    report the same `R/S/H`, retention, and corruption results.

## Deliverables

### D1. Executable Capability And Dependency Spike

Status: complete at `ea1a44c`. The exact m02 gate, independent QA, Linux race
repetition, and adversarial review passed. The selected D1 implementation uses
the existing `x/sys/unix` dependency with no CGO or new module; unsupported
platforms remain explicit. No product selector or `parallelwal` integration was
added.

- Add a Linux-only executable test that creates a bounded ring, submits
  multiple non-contiguous writes to a temporary file, consumes every
  completion, fsyncs, and verifies bytes after reopen.
- Record kernel/version, supported opcodes, queue depth, submission count,
  completion count, and exact refusal reason.
- Compare a small raw `io_uring`/`x/sys` implementation with one maintained Go
  wrapper only if necessary. Document license, CGO, cross-compile, and shutdown
  implications before selecting either.
- Prove Windows and unsupported Linux builds remain unchanged.
- Exit D1 with one selected implementation or an explicit rejection. Do not
  create a permanent abstraction for a backend that cannot pass the spike.

### D2. One Bounded Submission Owner

- Introduce the minimum internal execution seam needed by `parallelwal`; do
  not generalize all storage backends.
- Run one long-lived owner for the native ring. Callers enqueue immutable
  requests into bounded per-lane queues; the owner reserves SQEs across active
  lanes and submits them in one round.
- Keep lane sequence ordering while allowing different lanes to have writes
  in flight concurrently.
- Bound ring depth, request depth, owned bytes, and completion bookkeeping.
- Eliminate per-batch goroutine creation from the native path.
- Expose product metrics for admitted requests, queue-full rejects, SQEs,
  submit syscalls, completions, short completions, and in-flight high-water.

### D3. Completion And Durability State Machine

- Convert CQEs into the existing `writeRequest` completion ledger without
  advancing over lower-LSN holes.
- Treat partial/negative completions as terminal substrate I/O failures.
- Implement a target-LSN Sync barrier: drain writes through the fence, submit
  the durability operation, wait for its completion, then persist the same
  alternate CRC header protocol.
- Define Close and terminal-error drain behavior with no orphaned waiter and no
  ring use after file close.
- Prove an old ring generation cannot complete work into a reopened/recovered
  store.

### D4. Execution Shape And Buffer Ownership

- Reuse bounded owned buffers; remove avoidable request-channel and batch-slice
  allocation only after profiles identify them.
- Keep the Phase 167 bounded checkpoint writes and recycle reads. They may use
  the positioned path initially unless asynchronous execution gives a measured
  benefit without weakening extent locks or CRC checks.
- Test ring wrap, multiple SQE rounds, queue saturation, a full submission
  queue, short completion injection, fsync failure, and shutdown with work in
  flight.
- Consider registered files/fixed buffers only if the first native profile
  shows registration can remove a named remaining cost.

### D5. Comparable Linux Performance Gate

- Run the exact Phase 167 candidate and legacy controls in the same isolated
  m02 session with 1/2/4/8 writers.
- Include 4 KiB `Write`, 16-block `WriteBatch`, Sync cadence, p50/p95/p99,
  allocations, CPU, queue depth, SQEs, submit syscalls, CQEs, fallback count,
  `pwrite64`/`pread64`, and `io_uring_enter`.
- Use external `strace`/perf evidence where available; product counters alone
  cannot prove syscall reduction.
- Run enough repetitions to report median and range. Do not select the best
  sample.
- Keep the candidate only if single-writer throughput is at least 90% of the
  same-run legacy result and four-writer aggregate is at least 1.5x the
  candidate's own single-writer result, or a measured device queue limit
  explains the plateau.

### D6. Recovery, Mounted, And RF3 Admission

- Before mounted use, pass Linux race plus the Phase 167 corruption, header
  fallback, ring wrap, COW rebuild, and source-frontier matrix on both native
  and fallback execution.
- Build matching product/CSI images only after D5 admits the candidate.
- Run mounted NVMe/TCP RF1 concurrent write/read/flush/checksum and restart
  recovery.
- Then run RF3 sync-quorum with a delayed non-quorum peer, catch-up/rebuild,
  restart, status honesty, and zero-residue cleanup.
- Keep NVMe/RDMA performance and Phase 166's third-RoCE-host reconnect gate
  separate.

## Acceptance And Stop Rules

The native path may remain in the tree only if:

- D1 proves a maintainable capability/build boundary;
- all correctness and recovery invariants pass with zero acknowledged-data
  loss, frontier hole, or false readiness;
- native-path counters prove no fallback during its performance gate;
- 4 KiB single-writer and four-writer thresholds pass;
- p99 remains bounded;
- mounted restart and RF3 slow-peer gates pass before any default proposal.

Stop and remove the native implementation if:

- buffered-file `io_uring` does not outperform positioned I/O after obvious
  queue/allocation issues are removed;
- it requires silent fallback to pass;
- completion handling duplicates the storage state machine without a clear
  owner;
- shutdown, cancellation, or fsync semantics cannot be made deterministic;
- the only gain comes from changing durability or Sync cadence.

An honest rejection is a valid Phase 168 result. It should leave Phase 167's
opt-in backend and the default `walstore` behavior intact.

## Out Of Scope

- New WAL format or vector frontier.
- Raw-device deployment, SPDK, DPDK, or userspace NVMe.
- `O_DIRECT`, FUA, atomic writes, fixed buffers, or SQPOLL without a separate
  measured decision.
- New frontend protocols or NVMe/RDMA performance claims.
- PG placement, etcd control plane, erasure coding, snapshots, backup, or
  restore.
- Changes to Kubernetes operator ownership or lifecycle mutation boundaries.

## Exit Criteria

Phase 168 closes with one of two evidence-backed outcomes:

```text
Phase 167 profile
-> native capability proof
-> bounded submission/completion owner
-> exact Sync and failure semantics
-> same-run 4 KiB scaling decision
-> mounted RF1/RF3 admission if performance passes
-> promote, retain opt-in, or remove
```

Documentation must state which outcome occurred. A ring that compiles, an
`io_uring_enter` counter, or a batch-only gain is not a product capability.
