# Phase 133 QA Sign-off: Kubernetes NVMe Stale Path Pruning Close Gate

Verdict: PASS.

Runner:

```text
swblock run testops/scenarios/nvme-tcp-k8s-stale-path-prune-chain.yaml
run=20260704-145747-c73d
result=PASS 35/35
```

## Gate Result

| Check | Result |
|---|---|
| RF=2 NVMe/TCP PVC starts with two desired paths | PASS |
| Mounted pod exists before path change | PASS |
| One blockvolume frontend path is replaced with a different reachable address | PASS |
| `SwBlockVolume.status.nvme.nvmeAddrs` changes from old to new | PASS |
| CSI-node reconnect owner observes and connects the new desired path | PASS |
| Stale old host path for the same NQN is pruned by scoped path disconnect | PASS |
| Host path count returns to two desired paths | PASS |
| Mounted pod UID is preserved | PASS |
| Mounted I/O after reconnect/prune succeeds | PASS |
| CRD/report/dashboard agree | PASS |
| Cleanup verifier reports zero residue | PASS |

## Terminal Evidence

```text
phase133_nvme_k8s_stale_path_prune_status=ok
reconnect_owner_enabled=true
reconnect_owner_interval=5s
stage2_multipath_enabled=true
mounted_pod_uid_before=6b8147ce-6b10-4c38-9a88-8335a2ca7a09
volume_id=pvc-b11aa255-d807-4752-9db5-c388f7e79a22
nvme_nqn=nqn.2026-05.io.seaweedfs:pvc-b11aa255-d807-4752-9db5-c388f7e79a22
initial_path_count=2
initial_desired_paths=192.168.1.181:4420,192.168.1.184:4420
target_deployment=default/sw-blockvolume-pvc-b11aa255-d807-4752-9db5-c388f7e79a22-r2
target_replica=r2
old_desired_path=192.168.1.184:4420
new_desired_path=192.168.1.184:4520
desired_path_set_changed=true
path_loss_or_replacement_detected=true
reconnect_owner=csi-node
reconnect_invoked=true
new_desired_path_connected=true
host_path_count_after_desired_change=2
host_paths_after_desired_change=192.168.1.184:4520,192.168.1.181:4420
stale_old_host_path_after_desired_change=false
stale_old_path_detected=true
stale_old_path_pruned=true
host_path_count_after_prune=2
host_paths_after_prune=192.168.1.184:4520,192.168.1.181:4420
pod_uid_preserved=true
mounted_io_after_reconnect=ok
crd_status_agrees=true
report_dashboard_agree=true
surface_ready_reason=first_volume_verified
cleanup_status=ok
failure_count=0
```

`stale_old_host_path_after_desired_change=false` is expected in this run: the
owner connected the new desired path and pruned the old path before the first
post-change host-path sample. The stronger Phase 133 assertions are
`stale_old_path_pruned=true` and `host_path_count_after_prune=2`.

Cleanup verifier:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

## Product Meaning

Phase 132 proved that changed control-plane desired path evidence causes
CSI-node to connect the new mounted NVMe/TCP path without pod remount. Phase
133 closes the remaining stale-path gap: after the desired set changes, the
CSI-node owner now disconnects only stale host paths for the same NQN that are
no longer in the desired set.

The implementation uses scoped per-controller disconnects (`nvme disconnect -d
<controller>`), not `nvme disconnect-all` and not full-subsystem disconnect.

## Boundary

This gate does not claim NVMe/RDMA, RoCE, production HA, node-loss survival,
broad Linux compatibility, or a performance/SLO guarantee. It proves the
supported-lab Kubernetes NVMe/TCP mounted desired-path replacement and stale
path pruning loop.
