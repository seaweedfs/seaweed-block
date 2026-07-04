# Current Plan: Phase 132 Kubernetes NVMe Desired Path-Set Change Close Gate

Status: planning.

Phase 131 proved live host-path reconnect: a mounted RF=2 NVMe/TCP PVC starts
with two host paths, one controller/path is disconnected with scoped
`nvme disconnect -d`, and the CSI-node owner reconnects the missing path while
pod UID, I/O, and status surfaces stay correct.

Phase 132 should close the remaining NVMe Kubernetes failover semantics gap:
the desired frontend path set itself changes after replacement/failover, and
the mounted pod reconnects to the new desired path.

## Goal

```text
mounted RF=2 NVMe/TCP PVC starts with two desired paths
-> one frontend/replica path is replaced or failed over
-> control-plane publish evidence changes to the new desired path set
-> CSI-node reconnect owner observes the changed desired path set
-> owner connects the new desired path without remount
-> mounted pod UID is preserved and I/O still works
-> CRD/report/dashboard agree
```

## Required Evidence

```text
phase132_nvme_k8s_desired_path_change_status=ok
initial_path_count=2
old_desired_path=<addr>
new_desired_path=<addr>
desired_path_set_changed=true
path_loss_or_replacement_detected=true
reconnect_owner=csi-node
reconnect_invoked=true
new_desired_path_connected=true
pod_uid_preserved=true
mounted_io_after_reconnect=ok
crd_status_agrees=true
report_dashboard_agree=true
cleanup_status=ok
```

## Boundaries

- Do not reuse host-only path disconnect as proof; Phase 131 already covers
  that.
- Do not claim pass if Linux auto-reconnects the same old path without a
  control-plane desired path change.
- Do not use `nvme disconnect-all` for injected failure.
- Do not claim NVMe/RDMA/RoCE or performance/SLO.

## Candidate Gate Design

1. Start from the Phase131 live setup with `stage2Multipath.enabled=true` and
   `csiNode.nvmeReconnect.enabled=true`.
2. Force one frontend path to be replaced with a different reachable
   `addr/NQN`-compatible path, or add a controlled replacement frontend target
   for the same NQN/NSID while the old path is removed.
3. Assert `SwBlockVolume.status.nvme.nvmeAddrs` changes from old to new.
4. Assert CSI-node owner logs reconnect for the new path.
5. Assert mounted pod UID and I/O are preserved.
6. Assert CRD/report/dashboard agree and cleanup is clean.

## Next After Phase 132

If Phase 132 passes, the NVMe Kubernetes correctness loop is strong enough to
return to write-path performance optimization from Phase 126. If it blocks,
record whether the missing piece is frontend replacement machinery,
publish-evidence propagation, or CSI-node trigger semantics.
