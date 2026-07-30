# Phase 168 D2 Native WAL Submission Owner

Status: PASS at exact commit `240bff8`.

## Contract

D2 adds one internal, runtime-only execution seam:

- the default remains `positioned`;
- `ExecutionIOUring` must be selected explicitly through the `parallelwal`
  construction API;
- one long-lived owner drains bounded per-lane queues;
- one round may submit one contiguous WAL batch from each active lane;
- every accepted SQE reaches a terminal CQE before its Go-owned buffer is
  released;
- an ambiguous or partial submission poisons and closes the ring;
- the store terminal-faults rather than falling back;
- the Phase 167 disk format and portable reopen/recovery path are unchanged.

No blockvolume flag, Helm value, chart enum, or product default was added.

## Exact Evidence

```text
phase168_native_wal_owner_status=ok
default_execution=positioned
product_selector_added=false
single_owner_cross_lane_round=pass
admitted_requests=4
submission_rounds=1
sqes=4
completions=4
fallback_count=0
portable_reopen_recovery=pass
affected_linux_race=pass
external_native_syscall_validation=strace
windows_cross_compile=pass
unsupported_explicit_no_fallback=true
```

`strace` observed a real four-SQE `io_uring_enter` submission. The same store
was synced, closed, reopened through the default positioned path, and recovered
all four blocks. The depth-one lane-rotation test and partial-submit poison test
also pass under Linux race repetition.

## Review Progression

The first adversarial review rejected D2 for two real lifetime hazards:

1. executor buffer lifetime relied on the caller retaining its slices;
2. a partial submission could leave stale SQEs in a reusable ring.

The final implementation adds explicit `runtime.KeepAlive`, derives accepted
work from the SQ head, drains accepted CQEs, poisons/closes the executor on any
submission ambiguity, and rejects reuse. Independent QA passed and adversarial
review accepted the corrected path.

The remaining hard carry-forward is D3: deterministic durability barriers and
terminal completion/cancellation behavior for a permanently failing wait path.
Native execution cannot gain a product selector before that is resolved.
