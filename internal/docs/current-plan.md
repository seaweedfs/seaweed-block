# Current Plan: Phase 131 Kubernetes NVMe Live Reconnect Close Gate

Status: planning.

Phase 129 closed the mounted restage primitive: repeated `NodeStageVolume` can
refresh publish context and connect missing NVMe paths without remounting.
Phase 130 closed the ownership contract: the CSI node plugin now has an opt-in
owner loop that invokes that primitive from refreshed publish evidence.

Phase 131 should prove the full live Kubernetes user path rather than another
component contract.

## Goal

Prove a live Kubernetes path for mounted NVMe PVC reconnect after a path-set
change:

```text
mounted NVMe PVC starts with two paths
-> one path is removed
-> desired replacement/restored path appears in publish evidence
-> CSI-node reconnect owner detects the desired path set
-> owner invokes bounded reconnect
-> missing replacement path is connected without remount
-> mounted pod keeps identity and I/O remains correct
```

## Required Evidence

```text
phase131_nvme_k8s_reconnect_live_status=ok
initial_path_count=2
path_loss_detected=true
desired_path_set_changed=true
reconnect_owner=csi-node
reconnect_invoked=true
replacement_path_connected=true
stale_path_disconnect_claim=<true|false-with-reason>
pod_uid_preserved=true
mounted_io_after_reconnect=ok
crd_status_agrees=true
report_dashboard_agree=true
cleanup_status=ok
```

## Boundaries

- Do not claim automatic reconnect if the gate manually calls NodeStage.
- Do not use `nvme disconnect-all`; mutation must stay scoped to the affected
  NQN/path.
- Do not claim NVMe/RDMA/RoCE or performance/SLO.
- Do not hide a missing owner behind TestOps helper logic. The product log or
  event must prove the CSI-node owner invoked reconnect.
- If the kernel auto-reconnects before the owner acts, record that as
  inconclusive and redesign the trigger stimulus rather than marking PASS.

## Candidate Implementation / Gate

1. Enable `csiNode.nvmeReconnect.enabled=true` and `stage2Multipath.enabled=true`
   in the NVMe RF=2 Kubernetes scenario.
2. Start a mounted writer/reader pod and record pod UID plus initial host NVMe
   path count.
3. Remove one frontend path or replace it with a different reachable path while
   keeping the pod mounted.
4. Wait for publish evidence to carry the desired path set.
5. Assert CSI-node logs/events show the reconnect owner invocation.
6. Assert the replacement path is connected, no remount happened, pod UID is
   preserved, mounted I/O still works, and support surfaces agree.

## Next After Phase 131

If Phase 131 passes, the NVMe Kubernetes correctness loop is complete enough to
return to Phase 126's performance direction: durable backend write batching and
write-path optimization. If Phase 131 blocks, document the live trigger gap
before adding backend/performance work.
