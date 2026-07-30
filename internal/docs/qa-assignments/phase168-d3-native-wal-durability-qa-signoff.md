# Phase 168 D3 Native WAL Durability State Machine

Status: PASS at exact commit `85d3336`.

## Contract

D3 routes native WAL durability through the same bounded owner as write
submission:

- `Sync` first waits for its target LSN to enter the contiguous published
  prefix;
- the owner processes at most one write round before rechecking the barrier
  channel, so later admitted writes cannot indefinitely starve the fence;
- the owner submits and consumes the fsync CQE before stable-header
  publication continues;
- fsync, short-CQE, negative-CQE, and completion-notification failures
  terminal-fault the store and reject later writes;
- accepted SQEs retain their Go buffers until every terminal CQE is consumed,
  including after a permanent eventfd wait error;
- completion waits use registered eventfd notification and do not use
  `IORING_ENTER_GETEVENTS`;
- the default execution remains `positioned`, native execution has no product
  selector, and fallback remains zero.

## Exact Evidence

The exact commit was archived and executed on m02, Linux/amd64:

```text
phase168_native_wal_durability_status=ok
default_execution=positioned
product_selector_added=false
target_lsn_barrier=pass
durability_barriers=2
fsync_completions=2
portable_reopen_recovery=pass
sync_under_continuous_later_writes=pass
fsync_failure_terminal=true
later_write_after_fsync_failure=denied
close_reports_terminal_failure=true
eventfd_error_terminal_drain=pass
affected_linux_race=pass
completion_notification_registration=eventfd
getevents_wait_calls=0
external_native_syscall_validation=strace
fallback_count=0
```

Independent QA repeated the Sync-liveness, fsync-terminalization, and
eventfd-terminal-drain tests 20 times under the race detector. Adversarial
review additionally ran 100 Sync-liveness repetitions and accepted the exact
commit with no blocking finding.

## Review Progression

The first D3 review rejected an unbounded queue drain because continuous later
writes could starve a pending Sync barrier. It also found that a permanent
eventfd wait error was ignored and that the gate overstated wakeup evidence.

The accepted implementation processes one bounded round per wake and checks
barriers between rounds. After an eventfd wait failure it switches to CQ-only
polling until all accepted operations reach terminal CQEs, then returns the
error so the executor is poisoned. The gate now claims only the notification
registration and observed absence of `IORING_ENTER_GETEVENTS`.

D4 carries two non-blocking test-strengthening items: make post-fence barrier
ordering deterministic rather than timing-based, and assert executor poisoning
directly after the eventfd-error terminal drain.
