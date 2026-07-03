# Current Plan: Phase 129 Kubernetes NVMe Dynamic Reconnect / Restage

Status: planning.

Phase 128 closed the live Linux host ANA Change Notice gate. A real NVMe/TCP
initiator observed `NVME_AEN=0x0c0302` (Notice / ANA Change / ANA log page),
the ANA change count advanced, host path state refreshed, and mounted I/O
remained honest through standalone r1->r2 failover.

The remaining NVMe completion gap is Kubernetes behavior after a frontend
target address changes or a replacement path appears.

## Goal

Prove the Kubernetes CSI path can recover a mounted NVMe PVC when the published
NVMe path set changes:

```text
PVC mounted in app pod
-> active NVMe path/target changes
-> CSI/controller/operator surfaces publish updated path evidence
-> node-side reconciler disconnects stale path and connects replacement path
-> app pod keeps identity when possible
-> mounted I/O remains correct
```

## Required Evidence

```text
phase129_nvme_k8s_dynamic_reconnect_status=ok
initial_path_count=2
stale_path_removed=true
replacement_path_connected=true
host_path_state_refreshed=true
pod_uid_preserved=<true|documented_false>
mounted_io_after_reconnect=ok
crd_status_agrees=true
report_dashboard_agree=true
cleanup_status=ok
```

If current CSI ownership cannot safely mutate host NVMe sessions after initial
NodeStage, Phase 129 may close as a design-blocked gate only with concrete
evidence naming the missing owner, trigger, and bounded mutation policy.

## Non-Claims

Phase 129 still does not claim NVMe/RDMA/RoCE, performance/SLOs, broad distro
compatibility, or backend write optimization.

## Candidate Implementation Shape

1. Add a read-only detector for stale/replacement NVMe paths in the node-side
   status surface.
2. Define the owner for reconnect/restage. It must be explicit whether CSI
   NodeStage, a node reconciler, or a future lifecycle controller owns host
   `nvme connect/disconnect`.
3. Add a dry-run action contract first: desired disconnect/connect operations,
   preconditions, and evidence.
4. Only then enable the bounded mutating path in a lab gate.

## Next After Phase 129

If Phase 129 passes, the NVMe Kubernetes correctness loop is complete enough to
return to performance work: durable backend write batching and write-path
optimization from Phase 126. If Phase 129 blocks, fix the reconnect ownership
model before starting backend optimization.
