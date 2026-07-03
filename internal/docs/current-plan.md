# Current Plan: Phase 128 NVMe ANA Change Notice Host Gate

Status: planning.

Phase 127 closed the source/component gap: when an ANA provider exists, the
target advertises OAES ANA Change Notice and completes a parked AER when
`ANAChangeCount()` advances.

Phase 128 should prove whether a real Linux host receives and reacts to that
event through the NVMe/TCP initiator stack.

## Goal

Run a live host gate that observes ANA Change Notice behavior from the Linux
NVMe initiator:

```text
connect two NVMe/TCP paths
-> host posts/keeps AER through kernel initiator
-> target ANA change count advances during path/role transition
-> host receives ANA Change Notice
-> host refreshes ANA log / path state
-> mounted I/O remains honest
```

## Required Evidence

```text
phase128_nvme_ana_change_notice_host_gate_status=ok
oaes_ana_change_notice_advertised=true
host_aer_observed=true
ana_log_change_count_before=<number>
ana_log_change_count_after=<greater-number>
host_path_state_refreshed=true
mounted_io_after_notice=ok
cleanup_status=ok
```

If the Linux host does not expose AER evidence cleanly, the phase may close as
`blocked_host_observability` only if the target-side source/component evidence
is preserved and the missing host proof is documented.

## Non-Claims

Phase 128 still does not claim Kubernetes dynamic reconnect/restage after
primary/node failover, NVMe/RDMA/RoCE, broad distro compatibility, or a
performance/SLO.

## Next After Phase 128

If the host gate passes, move to Kubernetes NVMe dynamic reconnect/restage. If
it fails for product reasons, fix the target/host interaction before any
backend write optimization.
