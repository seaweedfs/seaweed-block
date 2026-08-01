# Phase 168 D4 Native WAL Execution Shape

Status: PASS at exact commit `428a0de`.

## Contract

D4 closes the bounded execution and buffer-ownership matrix before performance
work:

- per-lane saturation returns typed `ErrQueueFull`;
- an oversized executor request is rejected before raw SQ enqueue, leaves no
  stale SQE, and does not poison the reusable ring;
- a full-depth request completes through the real ring;
- partial submission, negative/short CQE, fsync failure, and eventfd failure
  remain fail-closed;
- every short CQE in a round is counted and all affected requests fail;
- an accepted buffer owner cannot be finalized before its terminal CQE;
- the owner respects ring depth across multiple rounds and survives WAL/ring
  wrap with portable recovery;
- `Store.Close` cannot close the executor while an accepted operation is in
  flight;
- a separate real-io_uring close/reopen test recovers the submitted record.

No product selector, fallback, registered file, fixed buffer, SQPOLL, or
default change was added.

## Exact Evidence

```text
phase168_native_wal_shape_status=ok
typed_queue_backpressure=pass
all_short_completions_counted=pass
later_write_after_short_completion=denied
close_waits_for_inflight=pass
fsync_failure_terminal=true
partial_submission_executor_poisoned=true
eventfd_error_executor_poisoned=true
oversized_submission_rejected_without_stale_sqe=true
full_submission_queue=pass
accepted_buffer_forced_gc=pass
depth_one_round_accounting=pass
bounded_multiple_rounds=pass
native_ring_wrap_recovery=pass
real_native_close_reopen_recovery=pass
shape_matrix_linux_race=pass
race_repetitions=10
fallback_count=0
```

Independent QA also passed 20 race repetitions, 100 accepted-buffer finalizer
repetitions, 100 real native Close/reopen repetitions, `go vet`, and external
io_uring/eventfd syscall confirmation. Adversarial review ran a larger
repetition set and accepted the exact commit with no remaining finding.

## Review Progression

The first gate was rejected because the fake Close test checked only delayed
return, the SQ-full case was absent, and buffer ownership had no active
lifetime test. The next revision exposed an executor bug: oversized requests
were rejected only after entering the raw path and needlessly poisoned the
ring. That request is now rejected at the executor boundary.

A later review rejected two self-proving tests. Reading bytes through a saved
SQE address did not prove reachability and failed `go vet`; a fake also wrote a
hard-coded valid record before recovery. The accepted version uses an inline
array owner plus finalizer signal with no unsafe address reconstruction, keeps
the fake only for deterministic close ordering, and proves persistence through
a separate real-io_uring close/reopen test.

D5 must now measure before adding buffer reuse or registration complexity.
