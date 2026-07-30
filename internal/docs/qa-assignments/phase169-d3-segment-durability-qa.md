# Phase 169 D3 Publication And Durability QA

Validate the internal target-LSN Sync and dual-header durability coordinator at
the exact D3 commit. This remains an unselected engine candidate; no image or
Kubernetes install is required.

## Run

Use an isolated exact-commit Linux source tree with CGO enabled:

```bash
cd /tmp/seaweed_block
bash scripts/run-phase169-segment-durability-gate.sh /tmp/seaweed_block
cat results/phase169-segment-durability-gate/phase169-segment-durability-summary.txt
```

## Required Evidence

```text
dual_header_fallback=pass
durable_header_bounds=pass
trusted_prefix_recovery=pass
sync_waits_for_target=pass
sync_excludes_future_admission=pass
segment_write_error_terminal=pass
data_sync_failure_terminal=pass
failure_barrier_blocks_future_publish=pass
header_write_failure_terminal=pass
header_sync_failure_terminal=pass
external_failure_blocks_active_publish=pass
short_header_write_rejected=pass
storage_regression=pass
segment_durability_race=pass
windows_compile=pass
product_selector_added=false
sync_order=data_fsync_then_alternate_header_then_header_fsync
sync_target=highest_lsn_admitted_before_call
uncommitted_physical_tail_recovered=false
terminal_failure_allows_later_publish=false
phase169_segment_durability_status=ok
```

## Review Checks

- The committed header snapshot is coherent: LSN, byte boundary, segment
  count, and first/last anchors come from one owner-lock observation.
- Sync waits for its pre-call target and does not wait for a later admission
  whose segment remains blocked.
- Physical segment bytes beyond the selected header boundary do not recover.
- No header generation becomes in-memory durable before the final Sync
  succeeds.
- Data Sync, header WriteAt, and final header Sync errors each terminally fault
  the owner.
- An external durability failure racing an active segment prevents that
  segment from publishing or returning success.
- Recovery callbacks populate private staging; callers publish only after the
  complete manifest validates.

Write the verdict to
`internal/docs/qa-assignments/phase169-d3-segment-durability-qa-signoff.md`.
