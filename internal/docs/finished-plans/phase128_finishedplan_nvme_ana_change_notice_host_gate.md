# Phase 128 Finished Plan: NVMe ANA Change Notice Host Gate

Status: closed 2026-07-03, runner PASS.

## Problem

Phase 127 proved the target-side ANA Change Notice implementation in
source/component tests, but it still did not prove that a real Linux NVMe/TCP
initiator would keep an Async Event Request outstanding, receive the completion,
and refresh path state during failover.

Without that host proof, the NVMe protocol surface was still incomplete: the
target could look correct in unit tests while the Linux host continued to rely
only on polling or path-loss side effects.

## Implementation

Phase 128 adds a live host gate:

- `scripts/run-phase128-nvme-ana-change-notice-host-gate.sh`
  - enables the Linux `nvme:nvme_async_event` tracepoint;
  - runs the standalone mounted NVMe/TCP failover smoke;
  - parses the host AER result;
  - validates OAES, ANA change count, host path refresh, mounted I/O, and
    cleanup.
- `scripts/run-nvme-mounted-failover-smoke.sh`
  - now captures Identify Controller OAES from raw identify data;
  - captures ANA log snapshots before and after failover;
  - writes ANA change-count and mounted-I/O evidence to the summary.
- `testops/scenarios/nvme-ana-change-notice-host-chain.yaml`
  - exposes the same gate through the runner for QA/subagent use.

The wrapper builds fresh binaries under the artifact directory by default. This
prevents stale `/tmp/sw-block-nvme-failover/bin` binaries from hiding new
protocol behavior.

## Evidence

Runner bundle:

```text
results/20260703-085706-6487
```

Summary:

```text
phase128_nvme_ana_change_notice_host_gate_status=ok
host_aer_observed=true
host_aer_result=0x000c0302
host_aer_event_type=notice
host_aer_event_info=ana_change
host_aer_log_page=ana
oaes_ana_change_notice_advertised=true
ana_log_change_count_before=4294967297
ana_log_change_count_after=8589934593
ana_log_change_count_advanced=true
host_path_state_refreshed=true
mounted_io_after_notice=ok
cleanup_status=ok
```

The host tracepoint line was:

```text
nvme_async_event: nvme2: NVME_AEN=0x0c0302 [NVME_AER_NOTICE]
```

## Close Criteria

Phase 128 closes because:

- a real Linux host observed the ANA Change Notice AER;
- target-side OAES advertisement was visible to the host;
- ANA change count advanced across failover;
- mounted I/O remained correct;
- the gate is runner-addressable and not just manual;
- cleanup was clean.

## Remaining Work

Phase 128 does not close Kubernetes dynamic reconnect/restage. The next phase
must decide and prove who owns host-session mutation after a mounted PVC's
published NVMe path set changes: CSI NodeStage, a node reconciler, or a future
lifecycle owner.

Performance work, including durable backend write batching from Phase 126,
stays deferred until that Kubernetes reconnect ownership question is explicit.
