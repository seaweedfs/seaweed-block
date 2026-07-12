# Phase 127 Finished Plan: NVMe ANA Change Notice

Status: closed source/component gate, 2026-07-03.

## Problem

The NVMe target already implemented ANA Identify fields and the ANA log page,
but `OAES` stayed zero because the target had no async event producer. That was
the correct fail-closed state, but it left a protocol gap: hosts could poll ANA
state, but the target could not complete an Asynchronous Event Request when ANA
state changed.

This mattered before Kubernetes reconnect work because dynamic failover should
not rely only on polling or incidental I/O errors.

## Implementation

- `core/frontend/nvme/controller.go`
  - adds a bounded `completePendingAER()` path for the existing single pending
    AER slot.
- `core/frontend/nvme/admin_features.go`
  - keeps the first AER non-blocking;
  - preserves the second-AER limit error;
  - watches the ANA provider's change count;
  - completes the parked AER as Notice / ANA Change / ANA log page when the
    count advances.
- `core/frontend/nvme/identify.go`
  - advertises OAES bit 11 only when an `ANAProvider` is wired.
- `core/frontend/nvme/admin_async_ana_test.go`
  - pins OAES conditional advertisement;
  - pins AER completion CID, status, and DW0 encoding.
- `scripts/run-phase127-nvme-ana-change-notice-gate.sh`
  - provides the repeatable local gate and non-claim summary.
- `testops/scenarios/nvme-ana-change-notice-chain.yaml`
  - gives QA/subagents a runner-native entry point.

## Evidence

```text
phase127_nvme_ana_change_notice_status=ok
ana_provider_oaes_ana_change_notice=true
no_provider_oaes_zero=true
aer_completes_on_ana_change=true
aer_completion_event_type=notice
aer_completion_event_info=ana_change
aer_completion_log_page=ana
aer_limit_still_enforced=true
projection_change_count_source=lineage
host_live_aer_claim=false
k8s_dynamic_reconnect_claim=false
cleanup_status=ok
```

Verification:

```text
go test ./core/frontend/nvme ./cmd/blockvolume
bash -n scripts/run-phase127-nvme-ana-change-notice-gate.sh
swblock validate testops/scenarios/nvme-ana-change-notice-chain.yaml
bash scripts/run-phase127-nvme-ana-change-notice-gate.sh .
```

## Boundaries

Phase 127 does not claim live Linux host AER behavior, Kubernetes dynamic
reconnect/restage, NVMe/RDMA, RoCE, DSM/TRIM, Write Zeroes, authentication, or
performance/SLO.

## Next

The next phase should validate one of the two remaining correctness layers:

1. Linux host receives ANA Change Notice and refreshes ANA state; or
2. Kubernetes mounted NVMe path handles primary failover through dynamic
   reconnect/restage.

Only after those correctness gates should durable backend write optimization
resume.
