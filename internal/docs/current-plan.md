# Current Plan: Phase 133 Kubernetes NVMe Stale Path Pruning Close Gate

Status: planning.

Phase 132 proved live desired path-set convergence: a mounted RF=2 NVMe/TCP PVC
started with two desired paths, one generated frontend path was replaced with a
different reachable address, `SwBlockVolume.status.nvme.nvmeAddrs` changed from
old to new, the CSI-node owner connected the new path, pod UID/I/O were
preserved, and CRD/report/dashboard agreed.

The live run also exposed the next gap:

```text
stale_old_host_path_after_desired_change=true
host_path_count_after_desired_change=3
```

The product connects new desired paths, but it does not yet prune stale mounted
NVMe paths for the same NQN that are no longer in the desired set.

## Goal

```text
mounted RF=2 NVMe/TCP PVC starts with two desired paths
-> one frontend/replica path is replaced with a different reachable address
-> control-plane desired path set changes old->new
-> CSI-node reconnect owner connects the new desired path
-> CSI-node reconnect owner disconnects the stale old path for the same NQN
-> mounted pod UID is preserved and I/O still works
-> CRD/report/dashboard agree
```

## Required Evidence

```text
phase133_nvme_k8s_stale_path_prune_status=ok
initial_path_count=2
old_desired_path=<addr>
new_desired_path=<addr>
desired_path_set_changed=true
reconnect_owner=csi-node
reconnect_invoked=true
new_desired_path_connected=true
stale_old_path_detected=true
stale_old_path_pruned=true
host_path_count_after_prune=2
host_paths_after_prune=<new + remaining desired only>
pod_uid_preserved=true
mounted_io_after_reconnect=ok
crd_status_agrees=true
report_dashboard_agree=true
cleanup_status=ok
```

## Boundaries

- Do not use `nvme disconnect-all` as proof or implementation.
- Do not disconnect paths for other NQNs or other volumes.
- Do not prune a path unless current control-plane evidence positively excludes
  that address from the desired set for the same NQN.
- Do not claim pass if only the new path connects while the old host path
  remains.
- Do not claim NVMe/RDMA/RoCE or performance/SLO.

## Candidate Gate Design

1. Extend the CSI-node mounted NVMe reconnect owner to list current host paths
   for the staged NQN.
2. After connecting all desired paths, compute stale connected paths:
   `connected_for_nqn - desired_addrs`.
3. Disconnect stale paths by scoped controller/path only.
4. Reuse the Phase132 live setup and desired-path replacement injection.
5. Assert the new desired path is connected and the old host path is gone.
6. Assert mounted pod UID/I/O, CRD/report/dashboard agreement, and cleanup.

If Phase 133 passes, the Kubernetes NVMe mounted failover path has a much
stronger correctness loop: fresh desired paths connect and stale paths are
bounded. Then return to write-path performance optimization from Phase 126.
