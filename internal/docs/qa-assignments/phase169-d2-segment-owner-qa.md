# Phase 169 D2 Bounded Segment Owner QA

Validate the internal single-owner group-commit path at the exact D2 commit.
This is a local engine gate. It must not add a product selector or alter the
existing Store path.

## Run

Use a clean exact-commit source snapshot on Linux with CGO enabled:

```bash
cd /tmp/seaweed_block
bash scripts/run-phase169-segment-owner-gate.sh /tmp/seaweed_block
cat results/phase169-segment-owner-gate/phase169-segment-owner-summary.txt
```

## Required Evidence

The command must exit zero, each named log must contain real test execution,
and the summary must contain:

```text
queued_group_commit=pass
queue_bound_and_lsn_continuity=pass
short_write_terminal_failure=pass
log_byte_ceiling=pass
close_drains_admitted=pass
config_and_input_bounds=pass
storage_regression=pass
segment_owner_race=pass
windows_compile=pass
batching_timer_present=false
product_selector_added=false
segment_queue_depth_hard_limit=4096
queue_full_consumes_lsn=false
one_write_at_per_segment=true
phase169_segment_owner_status=ok
```

## Review Checks

- The first isolated request reaches the writer without waiting on a timer.
- Requests already queued behind a blocked write form one later segment and
  one `WriteAt`.
- LSN assignment follows queue admission order; a queue-full rejection leaves
  no persisted LSN hole.
- Request count, payload bytes, segment entries, and log bytes are bounded.
- Short writes and log-full fail admitted requests and become terminal.
- Close cannot race a sender into a closed channel or leave a waiter orphaned.
- Metrics count successful persisted segments/entries, not merely attempted
  writes.

Write the verdict to
`internal/docs/qa-assignments/phase169-d2-segment-owner-qa-signoff.md`.
