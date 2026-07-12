# Phase 147 Finished Plan: WAL Multi-Block Record Design Gate

Status: **closed 2026-07-06, local gate PASS**.

## Problem

Phase 146 showed the Phase 145 allocation reduction is visible, but further
small local materialization tweaks are unlikely to be the highest-leverage path.
The current append path already coalesces many encoded records into fewer
`WriteAt` calls. The remaining structural cost is per-record encode/checksum and
recovery work.

## Work

Phase 147 added:

- `internal/docs/protocol/phase147-wal-multiblock-record-design.md`;
- `scripts/run-phase147-wal-multiblock-record-design-gate.sh`.

The design gate pins the current WAL format as unchanged, runs current
storage/durable recovery tests, and documents the required invariants for a
future multi-block WAL record.

## Evidence

```text
phase147_wal_multiblock_record_design_status=ok
current_wal_format_unchanged=true
current_recovery_compatibility=pass
candidate_design=multi_block_record
candidate_reduces_record_count=true
candidate_reduces_write_calls=false
durability_invariant_documented=true
recovery_invariant_documented=true
phase147_decision=prototype_next
next_recommendation=phase148_wal_multiblock_record_local_prototype
cleanup_status=ok
```

## Conclusion

The next WAL optimization should be a local, feature-gated multi-block record
prototype. The prototype must not change defaults or live Kubernetes behavior
until encode/decode, dirty reads, recovery splitting, flusher splitting, and
compatibility tests pass.
