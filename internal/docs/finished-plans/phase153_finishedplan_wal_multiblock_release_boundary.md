# Phase 153 Finished Plan: WAL Multi-Block Release Boundary

Status: **closed 2026-07-06, local contract gate PASS**.

## Problem

Phases 150-152 moved multi-block WAL records from local prototype to a
disabled-by-default runtime opt-in with mounted NVMe/TCP profile evidence and
mounted restart/recovery compatibility. That still was not a release claim: the
project needed a clear boundary that says what was proven, how to opt in for lab
testing, and what must not be claimed.

## Work

Phase 153 added:

- a release-boundary document under `docs/releases/`;
- release README, root README, chart README, and NVMe/TCP lab release-note
  updates;
- a Helm/schema contract fix for `durableWALMultiBlockRecords`;
- a local gate that verifies defaults, explicit Helm rendering, documented
  non-claims, cited Phase 151/152 evidence, and remaining follow-ups.

## Evidence

```text
phase153_wal_multiblock_release_boundary_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
default_wal_format_unchanged=true
feature_gate_default=false
runtime_opt_in_name=durable-wal-multiblock-records
runtime_opt_in_documented=true
mounted_profile_gate_passed=true
mounted_recovery_gate_passed=true
release_note_non_claims_documented=true
remaining_followups_listed=true
phase153_decision=document_opt_in
next_recommendation=phase154_durable_status_head_lsn_cleanup
```

## Conclusion

The multi-block WAL record path remains a source-gated opt-in. The release
boundary is now explicit: no default format change, no performance/SLO claim, no
RoCE or NVMe/RDMA claim, and no user-facing recovery-test flusher-disable
guidance. The next follow-up is the durable-status `HeadLSN` diagnostic cleanup
observed in Phase 152.
