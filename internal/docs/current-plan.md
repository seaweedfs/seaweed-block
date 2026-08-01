# Current Plan: Phase 174 Frontend And Replication Execution Architecture

Status: active. Phase 173 closed with no storage-backend change. Phase 174 D1
completed its local fixed-work matrix at `ed270f3`: direct WALStore was stable,
but durable-adapter RF1 remained above the unchanged `1.25x` range limit. D1 is
HOLD. D2's RF1 slice at `4034f37` reconciled all counters and attributed the
variance to foreground/background flusher overlap rather than an adapter CPU
hotspot. The distinct-node RF3 slice passed at `7d75a47`: 15 primary samples,
30 live probes, eight real rebuilds, six independently reopened remote stores,
and zero residue. Its one-writer shape remained unstable and the management-LAN
numbers select no candidate. The NVMe/TCP RF1 slice passed at `706b173`: all 30
rows reconciled from protocol command through recovered WAL bytes, and the
four-writer admission shape was stable at `1.035x`. Detailed phase attribution
passed at `c588800`: all 30 rows reconciled six target phases, Linux race passed,
and the current run was stable at `1.019x/1.037x/1.121x` for one/four/eight IO
queues. Four-queue accumulated latency splits into R2T collection `30.10 us/op`,
client/wire residual `15.92 us/op`, handler `8.20 us/op`, completion send
`3.71 us/op`, and smaller receive/dispatch/queue phases. Cross-run D1 stability
remains HOLD because the prior semantically identical one-queue run reached
`1.530x`; instrumentation alone is not a stability fix. R2T is a bounded mounted
kernel-initiator diagnostic next, not yet an architecture candidate. No
architecture implementation is eligible.

## Why This Is Next

Phase 173 proved the shipped WALStore engine is stable enough to measure and
found no evidence for an owner/queue rewrite or WAL/extent media split. The
remaining diagnostic gap is above the backend:

```text
WALStore shipped control, 4 writers: 255.743 MiB/s
RF1 durable adapter diagnostic:       61.28 MiB/s
RF3 TCP diagnostic, 1 writer:         91.56 MiB/s
RF3 TCP diagnostic, 4 writers:        14.69 MiB/s
mounted NVMe/TCP sequential write:    127.49 MiB/s
```

These values use different work shapes and cannot be divided into a valid
performance ratio. They do show that another backend patch is the wrong next
move. Phase 174 creates one comparable contract across frontend, adapter,
replication, and mounted paths, attributes each boundary, and selects at most
one non-backend architecture candidate.

## Goal

Answer one product question:

```text
Which frontend, adapter, or replication ownership boundary limits concurrent
shipped writes, and can one bounded change improve it without weakening ACK,
ordering, recovery, fencing, or mounted I/O semantics?
```

Valid outcomes are promote one candidate, retain one opt-in candidate with
named blockers, or select no change. A diagnostic with different durability or
payload semantics is not a candidate.

## Non-Goals

- No WALStore format or backend ownership redesign.
- No RDMA transport optimization; Phase 174 first establishes the TCP shipped
  path contract.
- No weakened RF3 quorum, asynchronous acknowledgement, sink peer, disabled
  persistence, or loopback-only number as a product claim.
- No batch/request-size change before identical logical work and durability
  are proven.
- No broad rewrite of CSI, NVMe target, replication, and adapter together.

## Required Invariants

1. RF1 and RF3 ACK profiles remain explicit and unchanged.
2. Acknowledged writes survive process restart and required replica recovery.
3. Same-LBA order, global durable frontier, and fencing remain deterministic.
4. Per-peer and frontend queues are bounded, observable, and fail closed.
5. Backpressure cannot silently drop, reorder, or acknowledge work early.
6. Sync fences all prior admitted work and excludes later work.
7. Peer failure, timeout, close, and reconnect have terminal evidence.
8. Mounted data and checksums remain correct through restart and catch-up.
9. No fallback path may hide candidate failure.
10. Rejected work leaves defaults and shipped behavior unchanged.

## Deliverables

### D1. Comparable Fixed-Work Pipeline Contract

- Define one fixed logical payload, LBA sequence, operation count, writer
  matrix, queue limits, ACK profile, warmup, Sync, and complete drain.
- Run engine, durable adapter RF1, RF3 real TCP, NVMe/TCP target, and mounted
  controls with identical logical work and an explicit unchanged ACK profile
  at each layer. Compare throughput ratios only inside compatible profiles.
- Record one/four/eight writers, throughput, p50/p95/p99, Sync/drain, CPU
  affinity, network route, device, and run stability.
- Require two independent five-run sets with the admission shape at or below
  `1.25x` max/min before selecting a candidate.
- Where a layer cannot implement the identical contract, label it diagnostic
  and do not compare its throughput ratio.

### D2. Complete Boundary Attribution

- Measure frontend parse/copy/queue/completion, adapter mapping and commit,
  replication fanout, per-peer enqueue/send/ACK, backend commit, Sync, and
  completion publication.
- Reconcile logical operations and bytes with every queue, peer, backend, TCP,
  and acknowledgement counter.
- Correlate product counters with CPU/heap/block profiles, socket statistics,
  `perf`, and network/device evidence.
- Separate accumulated concurrent wait from wall time.

### D3. Architecture Controls And Selection

Run test-only controls for:

- direct backend versus durable adapter with identical fixed work;
- frontend request/completion overhead without bypassing durability;
- RF1 versus RF3 with one peer delayed and with queue limits reached;
- per-request versus bounded batch submission where ordering is unchanged;
- one shared replication owner versus bounded per-peer ownership.

Select at most one direction only when the effect is stable and at least
`1.30x` on the four-writer admission shape. Otherwise select no change.

### D4. Selected Architecture Contract

Required only if D3 selects a candidate. Define owner, queue, admission,
backpressure, ordering, ACK frontier, Sync, Close, reconnect, failure, and
evidence semantics before implementation. Require independent correctness
review and one fail-closed rejected operation.

### D5. Bounded Candidate Implementation

Implement only the selected direction behind an internal or explicit opt-in
boundary. Add exact counters for queue depth, wait, batches, bytes, completion,
failure, timeout, and fallback. Keep all memory and in-flight work bounded.

### D6. Correctness, Failure, And Recovery

- Same/cross-LBA concurrency, partial requests, queue pressure, Sync/Close,
  timeout, peer loss, reconnect, process kill, and malformed completion.
- RF1 and RF3 recovery, catch-up, returned-replica rebuild, and authority
  fencing.
- Race tests and real Linux failure windows; zero hidden fallback.

### D7. Same-Run Performance Admission

- one-writer throughput at least `0.95x` shipped;
- four-writer median at least `1.30x` shipped;
- candidate max/min at most `1.25x`;
- four-writer p99 no worse than `1.10x` shipped;
- no required shape regresses more than 10%;
- zero failed work and complete counter reconciliation.

Reject and remove the candidate if any required gate fails.

### D8. Mounted RF1/RF3 Close Gate

- matching product/CSI images and 100 GbE data-plane route;
- mounted NVMe/TCP fixed-work comparison against the same-session shipped
  path;
- RF1 restart and RF3 delayed-peer/catch-up while mounted checksums continue;
- honest status, no false Ready, safe detach, exact PV deletion before CSI
  teardown, and zero residue;
- promote no default until correctness and performance both pass.

## Stop Rules

Stop before implementation if the comparable baseline is unstable, counters
do not reconcile, no single boundary dominates, the gain exists only in a
weakened/non-product control, or the candidate requires weaker durability,
recovery, fencing, or cleanup semantics.

## Exit Criteria

```text
comparable fixed-work contract
-> complete frontend/adapter/replication attribution
-> one selected design or honest no-change decision
-> bounded implementation only if selected
-> correctness and recovery
-> same-run performance admission
-> mounted RF1/RF3 close
-> promote, retain opt-in with blockers, or remove
```
