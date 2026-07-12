# Phase 128 NVMe ANA Change Notice Host Gate QA Sign-Off

Date: 2026-07-03

Verdict: PASS.

Source: `phase128-nvme-ana-change-notice-host-chain` runner on m02
(`192.168.1.184`), Linux `6.17.0-23-generic`, native NVMe multipath enabled.

Run bundle:

```text
results/20260703-085706-6487
remote artifacts:
results/20260703-085706-6487/artifacts/remote-phases.tgz
```

## Gate Result

```text
=== nvme-ana-change-notice-host-chain === PASS (1m16.233s)
14 actions: 14 passed, 0 failed
```

## Terminal Evidence

From `phase128-nvme-ana-change-notice-host-summary.txt`:

```text
phase128_nvme_ana_change_notice_host_gate_status=ok
nvme_transport=tcp
host_aer_event_count=1
host_aer_observed=true
host_aer_result=0x000c0302
host_aer_event_type=notice
host_aer_event_info=ana_change
host_aer_log_page=ana
host_aer_trace_line=kworker/8:2H-2025406 [008] ..... 3867053.727566: nvme_async_event: nvme2: NVME_AEN=0x0c0302 [NVME_AER_NOTICE]
inner_phase101_status=ok
oaes_ana_change_notice_advertised=true
ana_log_change_count_before=4294967297
ana_log_change_count_after=8589934593
ana_log_change_count_advanced=true
host_path_state_refreshed=true
mounted_io_after_notice=ok
target_aer_parked_count=3
target_aer_completed_count=1
cleanup_status=ok
```

The host evidence came from the real kernel tracepoint:

```text
nvme:nvme_async_event
```

The observed result `0x000c0302` decodes as:

```text
event_type=0x02  Notice
event_info=0x03  ANA Change
log_page=0x0c    ANA log page
```

## What This Proves

- The target advertises OAES ANA Change Notice to a real Linux NVMe/TCP host.
- The Linux host posts AERs; the target parks and completes at least one AER
  during the role/path transition.
- The host receives a real ANA Change Notice AEN through the kernel initiator.
- The ANA log change count advances across r1->r2 failover.
- Host path state refreshes to the one remaining path after r1 loss.
- Mounted I/O remains correct after the notice and failover.
- Cleanup leaves no Seaweed Block NVMe subsystem residue.

## Non-Claims

This is still a standalone Linux host gate. It does not claim:

- Kubernetes CSI dynamic reconnect/restage after published path changes;
- NVMe/RDMA or RoCE;
- performance/SLO;
- broad distro compatibility.

Those remain separate gates.

## Notes

The first manual run reused stale binaries from `/tmp/sw-block-nvme-failover/bin`
and correctly showed `oaes_ana_change_notice_advertised=false`. The Phase 128
wrapper now defaults `SW_BLOCK_BIN_DIR` to the artifact directory so each gate
builds fresh binaries from the synced `product_root`.
