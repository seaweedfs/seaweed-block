# Phase 156 Finished Plan: WAL Multi-Block Release-Smoke Decision

Status: **closed 2026-07-17, QA PASS**.

## Problem

Phases 150-155 proved the disabled-by-default multi-block WAL record opt-in from
runtime wiring through mounted restart/recovery and durable-status confirmation.
The remaining risk was documentation drift: treating source-gated evidence as a
public release-image claim.

## Work

Phase 156:

- kept `durableWALMultiBlockRecords` and the recovery-test flusher hook default
  off;
- documented that the opt-in remains source-gated;
- documented the exact future published-image smoke scope required before any
  public release-image claim;
- added a local/remote-renderable gate that verifies chart defaults, explicit
  opt-in rendering, Phase155 citation, and non-claim wording.

## Evidence

```text
phase156_wal_multiblock_release_smoke_decision_status=ok
source_gated_status=kept
published_image_smoke_required=true
published_image_smoke_scope=explicit_opt_in_recovery_status
default_wal_format_unchanged=true
recovery_test_disable_flusher_user_claim=false
performance_slo_claim_allowed=false
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
phase156_decision=keep_source_gated_until_matching_image_smoke
next_recommendation=phase157_nvme_rdma_capability_boundary
cleanup_status=ok
```

## Conclusion

The WAL multi-block opt-in remains a source-gated backend optimization. Future
work can proceed to the next storage track unless a release manager requests the
matching-image explicit opt-in smoke.
