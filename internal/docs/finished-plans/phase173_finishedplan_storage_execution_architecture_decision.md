# Finished Plan: Phase 173 Storage Execution Architecture Decision

Status: closed with the evidence-selected **no backend change** outcome.

## Product Question

Phase 173 asked which storage execution boundary, if any, could materially
improve shipped WALStore throughput without weakening durability, recovery,
checkpoint, replication, or cleanup semantics.

## Delivered

- D1 replaced auto-calibrated admission data with a deterministic fixed-work
  Linux harness. Two independent sets and all four required four-writer shapes
  passed the `1.25x` stability gate.
- D2 reconciled 16,000 logical blocks with product counters, exact `strace`,
  perf, CPU/memory profiles, iostat, and complete checkpoint/drain evidence.
- D3 measured shipped concurrent execution, deferred foreground work,
  flusher-only drain, shared/split-file scratch, RF1 durable adapter, RF3 TCP,
  and mounted NVMe/TCP.
- The mounted close gate now waits for the exact PV to be deleted before CSI
  teardown and proves zero residue.

## Decision

No local backend architecture was selected:

```text
owner_queue_signal=false
writeback_interference_signal=false
media_separation_signal=false
counterfactual_control_stability_gate=inconclusive
diagnostic_controls_candidate_eligible=false
architecture_candidate_selected=false
```

The stable shipped control reached `255.743 MiB/s`; the split-file scratch path
was `0.993x` the shared path. Deferred-writeback controls did not expose a
useful product ceiling and two counterfactual groups were too noisy for
candidate admission. Implementing a storage owner, media split, `io_uring`, or
format change would therefore be speculation.

D4-D8 were intentionally not run because they require an admitted candidate.
WALStore, its format, durability behavior, and defaults remain unchanged.

## Next Boundary

The diagnostic adapter/frontend/replication results are materially below the
local engine envelope and RF3 four-writer ACK wait increased sharply. These
numbers are not directly comparable throughput claims, but they move the next
investigation above the backend: Phase 174 measures the complete
frontend/adapter/replication execution path with one fixed contract before
selecting any implementation.

## Evidence

- `internal/docs/qa-assignments/phase173-d1-fixed-work-baseline-qa-signoff.md`
- `internal/docs/qa-assignments/phase173-d2-shipped-path-attribution-qa-signoff.md`
- `internal/docs/qa-assignments/phase173-d3-architecture-decision-qa-signoff.md`

No user-visible feature or release claim changed in this phase.
