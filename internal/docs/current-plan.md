# Current Plan: Phase 167 Parallel Write Engine Milestone

Status: active design and implementation milestone.

Phase 166 NVMe/RDMA multipath reconnect is implemented but remains open because
its honest live gate needs a third RoCE-capable Kubernetes initiator. That
infrastructure-blocked gate is preserved in
`internal/docs/ref/phase166-nvme-rdma-kubernetes-multipath-reconnect-hold.md`;
it does not block independent engine development.

Phase 122-156 established the reason for this milestone:

- the 100GbE network comparator reached about 4.1 GiB/s;
- mounted NVMe/TCP sequential write remained about 175-208 MiB/s;
- local-path write was about 1.1 GiB/s in the comparable profile;
- larger frontend requests and multi-block WAL records reduced record and call
  counts but did not remove the backend write bottleneck;
- the RF1 performance gates did not exercise the stricter replication path,
  where batching is currently disabled and per-write fan-out is serialized.

The next useful change is therefore architectural concurrency, not another
single-counter or single-copy optimization.

## Goal

Build and live-gate a parallel write engine candidate that removes avoidable
whole-volume serialization while preserving the existing block contract:
global LSN identity, durable `R/S/H` frontiers, per-LBA ordering, authority
fencing, sync-quorum/sync-all acknowledgement, crash recovery, catch-up,
rebuild, and negative-first status.

This is one large milestone. Its implementation may use several reviewable
commits or PR slices, but individual lock, counter, queue, and syscall changes
do not become separate phases.

## Current Progress

The D1 local baseline slice is implemented and repeatable through
`scripts/run-phase167-parallel-write-engine-local-baseline-gate.sh`:

- direct WAL and real TCP RF3 sync-quorum benchmarks cover 1, 2, 4, and 8
  writers with throughput, p99, WAL-lock wait, and replication-fanout timing;
- the benchmark asserts the replication observer processed every accepted
  write, so a detached observer cannot produce a false performance PASS;
- repeated same-run Windows baselines measured WAL four-writer scaling at
  about `0.82-0.99x`, while RF3 four-writer scaling was only about
  `0.17-0.21x`;
- concurrent RF3 execution exposed a real pre-existing ordering gap: storage
  could assign LSN N before N+1 while observer callbacks arrived in the
  opposite order, causing the replica ship cursor to reject the gap;
- `ReplicationVolume` now resequences concurrent callbacks by storage-assigned
  LSN, owns queued buffers, makes `Sync(targetLSN)` wait for the ordered ship
  frontier, and unblocks pending work safely on shutdown;
- WALStore and SmartWAL now advance `nextLSN` only after a successful append,
  so a failed local write cannot leave a permanent hole in the replication
  ingress sequence; caller cancellation after local commit no longer drops the
  corresponding replication fact;
- WALStore and SmartWAL `Sync` capture the head covered by their fsync request
  and never promote a concurrent later write into the returned durable
  frontier;
- direct-write frontier metadata keeps the superblock WAL head as a byte
  cursor, and only clears pending checkpoint state after the metadata write and
  fsync succeed, so a failed metadata commit remains retryable;
- repeated RF3 benchmark runs and the storage/frontend/replication test suites
  passed without another cursor gap.

The D2 ordered asynchronous replication slice is implemented and repeatable
through `scripts/run-phase167-ordered-async-replication-gate.sh`:

- each replica lineage owns one bounded FIFO shared by writes and barriers, so
  per-peer LSN order is retained without holding the whole-volume mutex across
  network I/O;
- the global resequencer dispatches every contiguous committed LSN before
  waiting for individual acknowledgements, allowing multiple writes and
  independent peers to remain in flight;
- sync-quorum returns after local durability plus the required peer frontier,
  while a slow non-quorum queue continues independently; sync-all still waits
  for every peer;
- queue saturation fails closed with typed evidence and degrades that peer
  lineage rather than skipping one LSN and sending later writes;
- terminal queue failures cannot be hidden by an earlier ordinary transport
  error; terminal replacement and enqueue are atomic with queue identity, and
  the failed committed write is retained on the replacement queue so the
  recovery boundary cannot lose its first LSN;
- write and barrier queues are closed before a removed or replaced peer
  lineage is torn down; the gate verifies an in-flight old-lineage write has
  stopped before replacement returns;
- strict observer-ack mode now retains full-block storage batching through an
  explicit batch observer seam, including correct observation of a partially
  committed batch prefix;
- repeated slow-peer, sync-all, saturation, ordering, and batch tests pass,
  and the real-TCP RF3 gate verifies eventual frontier and byte agreement on
  both replicas with zero normal-path queue saturation.

D2 is a correctness and slow-peer isolation improvement, not yet a throughput
win. The latest same-host result measured about `49.38 MiB/s` for one RF3
writer and `6.48 MiB/s` for four writers (`0.131x` scaling); ACK wait dominates
the multi-writer path. Linux race, mounted NVMe/TCP, and same-run lab evidence
remain open. D3 parallel local WAL is now the active implementation target.

The first D3 candidate slice is implemented behind the explicit
`parallel-walstore` selector:

- one file contains alternating CRC headers, four fixed WAL rings, and dual
  data extents; lane geometry, durable heads/tails, `R`, checkpoint, `S`, and
  the authoritative extent are persisted;
- deterministic LBA lanes execute positioned writes independently while a
  global ledger publishes only contiguous LSN completion;
- pressure-triggered checkpointing copies only `LSN <= R`, then advances
  recyclable lane tails after extent durability;
- recovery merges durable lane records by LSN and fails closed on CRC,
  mapping, duplicate, or committed-hole evidence;
- provider, blockvolume flag, launcher rendering, adapter/reopen matrices,
  canonical `LogicalStorage`, source-LSN jumps, direct rebuild frontier, ring
  wrap, and per-LBA serialization across partial/full/batch writes are covered;
- adversarial review drove explicit Sync admission fencing, terminal append
  drain before close/recover, dual-header provider probing, checked persisted
  geometry, and protection against retained pre-checkpoint WAL overriding
  rebuilt BASE extents;
- rebuild BASE installation now clears and writes an inactive COW extent,
  overlays session-live WAL, and switches the authoritative extent only in the
  final fsynced header; failed commits retain prior acknowledged data and a new
  session cannot inherit an abandoned BASE stage;
- `scripts/run-phase167-parallel-wal-candidate-gate.sh` passes the correctness
  stress gate with multiple lanes and steady-state WAL recycle observed.

The candidate has not earned a performance claim. The latest Windows
positioned-I/O control, now including checkpoint/recycle work, measured
`84.02 MiB/s` for one candidate writer and `94.06 MiB/s` for four (`1.119x`
scaling), versus `345.27 MiB/s` and `275.77 MiB/s` for legacy `walstore`. The
gate therefore records
`performance_claim_allowed=false`. D4 Linux/device profiling and execution
redesign are required before RF3 or mounted promotion gates; the default
backend remains unchanged.

D4 Linux profiling at exact commit `ac2b4d5` passed the race suites and showed
why `io_uring` is not the first change:

- three same-run rounds measured the candidate at about `88-101 MiB/s` and
  legacy `walstore` at `174-211 MiB/s`;
- an 8000-write candidate run issued `16009 pwrite64` calls versus `11852` for
  legacy, because pressure checkpointing wrote each stable block separately;
- candidate CPU samples were dominated by syscall (`32%`), memory copy
  (`21%`), CRC (`11%`), and lock spinning (`11%`);
- sustained loops could drive the local NVMe device above `90%` utilization,
  so the evidence does not support treating the device as idle or adding an
  asynchronous API without first removing avoidable write amplification.

The first D4 execution change therefore coalesces contiguous checkpoint LBAs
into bounded 1 MiB positioned writes. This is simpler than a new Linux-only
backend, preserves the established WAL/header protocol, and gives a direct
gate metric (`checkpoint_write_ops`) before any `io_uring` decision.

## Assumptions And Boundaries

- `walstore` remains the default backend until the candidate passes the full
  correctness, recovery, RF3, and mounted close gates.
- Existing iSCSI, NVMe/TCP, and NVMe/RDMA frontend contracts do not change.
- Keep the global LSN and current `LogicalStorage` recovery vocabulary in the
  first implementation. Do not introduce a vector frontier merely to enable
  parallel writes.
- Any queued write owns an immutable copy or an explicitly transferred buffer.
  Existing borrowed-buffer lifetimes must not leak into asynchronous work.
- `io_uring`, `O_DIRECT`, fixed buffers, FUA, and NVMe atomic-write support are
  evidence-gated optimizations, not assumptions.
- This milestone does not copy Vitastor's etcd PG placement, full object-list
  peering, or raw-device operational model.
- No performance SLO or default-backend switch is allowed from a local
  microbenchmark alone.

## Required Invariants

1. Two writes to the same LBA are applied in assigned-LSN order.
2. Different write lanes may execute concurrently, but an acknowledged write
   cannot disappear after a successful covering `Sync`.
3. `Sync(targetLSN)` waits for every local submission at or below the target
   and for the acknowledgement set required by the selected durability mode.
4. A slow or failed non-quorum replica must not hold the whole volume mutex or
   block a satisfied sync-quorum acknowledgement.
5. Replica-set generation, epoch, or endpoint-version change fences old queued
   work before the new lineage can acknowledge writes.
6. Recovery produces a monotonic contiguous durable frontier. It must not
   expose holes as a higher stable LSN.
7. `ScanLBAs`, catch-up, and rebuild preserve their current externally visible
   semantics even if records are stored in multiple lanes.
8. Partial writes retain read-modify-write correctness; a multi-lane request
   completes only after every child write reaches the required acknowledgement
   state.
9. Cancellation, queue saturation, shutdown, and WAL pressure fail with typed
   evidence. They must not silently downgrade durability or fall back to an
   untracked path.

## Deliverables

### D1. Comparable Baseline And Contention Evidence

- Add one reusable benchmark/gate for direct engine and mounted NVMe/TCP writes.
- Run RF1 and RF3 sync-quorum shapes with 1, 2, 4, and 8 writers.
- Record throughput, IOPS, p50/p95/p99 latency, CPU, queue depth, WAL append
  wait, storage completion wait, replication wait, and sync wait.
- Require strict path counters so a missing observer, fallback backend, or
  transport change cannot produce a false PASS.
- Preserve the Phase 122-156 evidence as the historical comparator; take a
  same-run baseline before judging the candidate.

### D2. Ordered Asynchronous Replication

- Replace the `ReplicationVolume` mutex held across network fan-out and `Sync`
  with a short critical section for membership/lineage snapshots plus
  per-peer ordered work queues.
- Preserve global LSN send order per peer while allowing multiple writes to be
  in flight and allowing independent peers to progress concurrently.
- Add a batch observer path so sync-quorum/sync-all no longer disables
  full-block storage batching and then re-expands every batch into a serialized
  network call.
- Make `Sync(targetLSN)` a barrier over local completion and the required peer
  acknowledgement frontier, not a mutex held while waiting on network I/O.
- Make replica-set changes drain or reject old-lineage work deterministically.

### D3. Parallel Local WAL Candidate

- Add an explicit opt-in parallel backend; do not silently alter `walstore`.
- Route striped LBA groups to a fixed number of owner lanes so sequential and
  independent-LBA workloads can use more than one lane while same-LBA writes
  retain one owner.
- Give each lane an append queue and non-overlapping WAL storage ownership.
- Allocate global LSN ranges centrally with minimal synchronization.
- Track completion in a contiguous frontier ledger so out-of-order lane
  completion cannot advance `R` over a hole.
- Persist geometry, lane cursors, checkpoint, and durable frontier through
  alternating CRC-protected headers. `Sync` may publish `R=N` only after every
  record through `N` is complete and the corresponding lane writes are
  durable; a lower-LSN failure terminal-faults the store instead of allowing a
  higher lane to publish success.
- Keep one logical shared extent, materialized as two physical COW copies for
  rebuild commit safety, and flush only the stable `LSN <= R` prefix. Extent
  writeback, checkpoint advance, WAL recycling, `ApplyEntry`, and
  `AdvanceFrontier` must preserve the same global ordering contract.
- Merge lane recovery scans by LSN and return the existing typed
  `ErrWALRecycled`/recovery failures where the old contract requires them.
- Use a format-specific recovery scanner. Do not reuse legacy recovery logic
  that mixes byte-ring cursors with logical LSNs.
- Keep lane count and mapping format on disk so reopen does not reinterpret
  existing data.
- Serialize partial-block read-modify-write by LBA (or the same deterministic
  stripe owner) so frontend concurrency cannot lose overlapping updates before
  they reach the WAL.

### D4. Storage Execution Backend

- First prove the design with positioned I/O and bounded queue depth.
- Profile syscall time, device utilization, queue occupancy, and CPU after D2
  and D3. Only add `io_uring` if this evidence shows the synchronous positioned
  I/O layer is now limiting progress.
- If selected, keep `io_uring` behind a Linux build/runtime capability gate,
  use aligned owned buffers, and retain a tested positioned-I/O fallback.
- Investigate device atomic-write/FUA only through explicit capability
  evidence. Never infer atomicity from device type or NVMe transport.

### D5. Correctness And Recovery Matrix

- Unit/component tests: repeated same-LBA writes, cross-lane batches, partial
  writes, queue saturation, cancellation, shutdown, and lineage change with
  queued work.
- Durability tests: RF1, RF3 sync-quorum, RF3 sync-all, slow peer, failed peer,
  group commit, and barriers at exact target LSNs.
- Dirty-failure tests: kill before local completion, after local completion but
  before peer quorum, after quorum but before sync, and during lane recovery.
- Recovery tests: close/reopen, WAL wrap, `ScanLBAs`, catch-up, returned-replica
  rebuild, and corruption fail-closed with no false `Ready=True`.
- Run existing storage, replication, rebuild, failback, iSCSI, NVMe, and cleanup
  regression gates that exercise the changed contracts.

### D6. Live RF3 And Mounted Close Gate

- Build fresh matching product and CSI images from the candidate commit.
- Run a mounted NVMe/TCP RF3 sync-quorum workload with concurrent writers,
  flushes, reads, and checksum verification.
- Inject one slow/non-quorum replica and prove quorum progress remains bounded;
  then restore it and prove catch-up/rebuild convergence.
- Restart the primary/backend with hostPath persistence and verify acknowledged
  data, `R/S/H`, status, and workload recovery.
- Compare old and candidate engines in the same lab run.
- Finish with product-owned cleanup verification and zero Kubernetes, NVMe,
  iSCSI, process, and durable-path residue.

## Acceptance And Decision Rules

The candidate is eligible to become the default only when all of the following
are true:

- all D5/D6 correctness and cleanup gates pass;
- no acknowledged-data loss, frontier hole, stale-lineage ACK, or false
  readiness is observed;
- RF1 single-writer throughput regresses by no more than 10%;
- four-writer aggregate throughput improves by at least 1.5x over the same-run
  old-engine baseline, or the evidence identifies a new external device limit
  with queue-depth saturation;
- RF3 sync-quorum continues making progress when one non-quorum replica is
  delayed or unavailable;
- p99 latency is reported and bounded; an aggregate-throughput gain that causes
  unbounded tail latency is not accepted;
- the candidate remains opt-in until a matching-image mounted recovery and
  upgrade/rollback smoke passes.

If D2 removes replication serialization but D3 does not improve RF1 scaling,
keep D2 only if its RF3 slow-peer and latency evidence is independently useful.
If `io_uring` does not outperform the positioned-I/O candidate after D2/D3,
do not retain it for architecture appearance.

## Out Of Scope

- Phase 166's infrastructure-blocked RDMA multipath close gate.
- New frontend protocols, Docker integration, GPU Direct, cuFile, or cuObject.
- New placement/PG control plane or replacement of the authority model.
- Snapshot/backup/restore.
- Compression, deduplication, erasure coding, or distributed transactions.
- Broad hardware compatibility or production performance SLOs.

## Exit Criteria

Phase 167 closes when a single opt-in candidate demonstrates the complete
fact-to-capability loop:

```text
measured contention
-> asynchronous ordered replication
-> parallel local WAL ownership
-> preserved durability/recovery semantics
-> RF3 mounted workload scaling and slow-peer tolerance
-> restart/rebuild convergence
-> zero residue
```

Documentation must then state one of three honest outcomes: promote the
candidate, keep it opt-in with named blockers, or reject it and retain the
existing engine. A partial implementation is not reported as a parallel-engine
performance claim.
