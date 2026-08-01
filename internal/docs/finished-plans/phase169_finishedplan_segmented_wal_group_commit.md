# Finished Plan: Phase 169 Segmented WAL Group-Commit Engine

Status: complete as a rejected persistence-format candidate. D1-D3 proved the
format, bounded owner, recovery, Sync, and terminal-failure contracts. The
D4-0 Linux pre-admission gate then rejected further integration. Candidate
code and executable gates were removed; D5 and D6 were intentionally skipped.

## Final Outcome

Phase 169 tested a different hypothesis from Phase 168. Instead of replacing
`pwrite64` with `io_uring`, it changed the persistence unit so several
independent logical writes could share one checksummed contiguous segment and
one positioned write.

The exact m02 gate used five rotated one-second samples and the same final
logical Sync cadence for segmented, Phase 167 positioned, and default legacy
controls:

```text
segmented writers=1          101.98 MiB/s
segmented writers=4           78.68 MiB/s
positioned writers=1          91.80 MiB/s
positioned writers=4          79.51 MiB/s
legacy writers=1              45.57 MiB/s
legacy writers=4              43.06 MiB/s
segmented entries/segment      1.348
segmented four/single          0.772x
segmented four/positioned      0.990x
```

Grouping occurred, and the incomplete candidate had a strong one-writer
optimistic upper bound. It did not solve the target problem: four concurrent
writers were slower than one and did not beat the accepted positioned control.
The candidate had not yet paid dirty-map, checkpoint, retention/reuse, rebuild,
recovery-index, or replication costs, so adding full engine semantics was not
a credible route to reverse the result.

## What Was Proved

The rejected performance result does not erase the correctness work:

- a bounded versioned segment header and per-entry table;
- CRC validation, canonical offsets, LSN/sequence continuity, and invalid-LBA
  rejection;
- a trusted committed-prefix manifest that fails closed inside the boundary
  and ignores only bytes after it;
- a streaming recovery scanner with bounded memory;
- admission and payload reservations before copy, queue bounds, and no LSN
  consumption on queue-full rejection;
- one positioned write per segment and exact per-request completion;
- target-LSN Sync with data-fsync, alternate-header write, header-fsync order;
- an owner-held publication barrier that prevents later success escaping a
  failed durability round;
- terminal behavior for short write, data fsync, header write, header fsync,
  and externally injected failure;
- Linux race, Windows cross-compile, and four-package storage regression.

The exact evidence remains in:

- `phase169-d1-segment-format-recovery-qa-signoff.md`;
- `phase169-d2-segment-owner-qa-signoff.md`;
- `phase169-d3-segment-durability-qa-signoff.md`;
- `phase169-d4-segment-performance-qa-signoff.md`.

## Why The Candidate Was Removed

The implementation had one `segmentOwner.run()` stage responsible for queue
drain, segment encoding, CRC, `WriteAt`, and publication. Multiple callers
could create a modest batch, but they could not make those stages progress in
parallel. The extra queue/channel/copy coordination reduced four-writer
throughput instead of scaling it.

The stop rules intentionally reject this shape:

- grouping alone is not an ordinary-write capability;
- mounted correctness cannot excuse a failed local performance gate;
- a candidate that loses before checkpoint/rebuild integration must not gain
  complexity in the hope that more work will make it faster.

The code and gates remain available in Git history through `ddd69e9`.

## Surviving Product State

- `walstore` remains the default backend.
- Phase 167 `parallel-walstore` remains explicit and opt-in.
- No segmented format selector, dead owner, recovery branch, or unused gate
  remains in the product tree.
- Existing iSCSI, NVMe/TCP, NVMe/RDMA, CSI, replication, and operation-layer
  contracts are unchanged.
- D5 comparable full-engine performance and D6 mounted RF1/RF3 gates were
  skipped because D4-0 denied admission.

## Next Direction

Phase 169 rejects both “change the syscall” and “add a new segmented backend”
as sufficient answers to the current write bottleneck. The next experiment
must work on the default product path and preserve the existing WAL format.

The useful seam is already present: `walWriter.appendBatch` can encode
independently recoverable existing-format records and coalesce adjacent bytes
into fewer positioned writes. Phase 170 therefore starts with measurement of
the default `WALStore.Write` pipeline, then may introduce a bounded staged
commit owner only if evidence proves that CRC/copy/global-lock work has enough
parallelizable headroom. It must not add another backend selector or weaken
Sync, flusher, recovery, or retention behavior.
