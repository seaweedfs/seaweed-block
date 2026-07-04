# Current Plan: Phase 130 Kubernetes NVMe Reconnect Owner / Trigger Gate

Status: planning.

Phase 129 closed the mounted restage contract: if NodeStage is invoked again
for an already-mounted NVMe staging path, the CSI node plugin refreshes the
publish context, connects missing NVMe paths, rejects NQN mismatch, and does
not remount or reformat. It deliberately did not claim an automatic Kubernetes
trigger.

Phase 130 should close the remaining ownership gap: who notices a mounted PVC's
published NVMe path set changed, and who safely calls the reconnect/restage
path.

## Goal

Prove a live Kubernetes path for mounted NVMe PVC reconnect after path set
change:

```text
mounted NVMe PVC starts with two paths
-> one path is removed
-> replacement path appears in publish evidence
-> explicit owner detects changed desired path set
-> owner invokes bounded reconnect/restage
-> missing replacement path is connected without remount
-> mounted pod keeps identity and I/O remains correct
```

## Required Evidence

```text
phase130_nvme_k8s_reconnect_owner_status=ok
initial_path_count=2
path_loss_detected=true
desired_path_set_changed=true
reconnect_owner=<csi-node|node-reconciler|other-explicit-owner>
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

- Do not claim automatic reconnect if the gate manually calls NodeStage without
  a product owner/trigger.
- Do not use `nvme disconnect-all`; mutation must be scoped to the affected
  NQN/path.
- Do not claim NVMe/RDMA/RoCE or performance/SLO.
- Do not hide a missing owner behind TestOps helper logic.

## Candidate Implementation

1. Add an explicit node-side reconnect owner or controller loop, disabled by
   default if needed.
2. Feed it desired NVMe path-set evidence from the control plane.
3. Reuse the Phase 129 mounted NodeStage reconnect code for the actual bounded
   `nvme connect` operation.
4. Add a live gate that proves the owner invocation, not just the final path
   state.

## Next After Phase 130

If Phase 130 passes, the NVMe Kubernetes correctness loop is complete enough to
return to Phase 126's performance direction: durable backend write batching and
write-path optimization. If Phase 130 blocks, document the owner model before
adding backend/performance work.
