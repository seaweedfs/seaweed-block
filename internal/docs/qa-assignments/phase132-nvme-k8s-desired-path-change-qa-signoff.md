# Phase 132 QA Sign-off: Kubernetes NVMe Desired Path-Set Change Close Gate

Verdict: PASS.

Runner:

```text
swblock run testops/scenarios/nvme-tcp-k8s-desired-path-change-chain.yaml
run=20260704-143757-a76e
result=PASS 31/31
```

## Gate Result

| Check | Result |
|---|---|
| RF=2 NVMe/TCP PVC starts with two desired paths | PASS |
| Mounted pod exists before path change | PASS |
| One blockvolume frontend path is replaced with a different reachable address | PASS |
| `SwBlockVolume.status.nvme.nvmeAddrs` changes from old to new | PASS |
| CSI-node reconnect owner observes the new desired path | PASS |
| Host connects the new desired path | PASS |
| Mounted pod UID is preserved | PASS |
| Mounted I/O after reconnect succeeds | PASS |
| CRD/report/dashboard agree | PASS |
| Cleanup verifier reports zero residue | PASS |

## Terminal Evidence

```text
phase132_nvme_k8s_desired_path_change_status=ok
reconnect_owner_enabled=true
reconnect_owner_interval=5s
stage2_multipath_enabled=true
mounted_pod_uid_before=fe461dee-c11c-4c20-add4-9b456867e3a8
volume_id=pvc-20228828-11d7-4c8e-9fda-8475b7151ab7
nvme_nqn=nqn.2026-05.io.seaweedfs:pvc-20228828-11d7-4c8e-9fda-8475b7151ab7
initial_path_count=2
initial_desired_paths=192.168.1.181:4420,192.168.1.184:4420
target_deployment=default/sw-blockvolume-pvc-20228828-11d7-4c8e-9fda-8475b7151ab7-r1
target_replica=r1
old_desired_path=192.168.1.181:4420
new_desired_path=192.168.1.181:4520
desired_path_set_changed=true
path_loss_or_replacement_detected=true
reconnect_owner=csi-node
reconnect_invoked=true
new_desired_path_connected=true
host_path_count_after_desired_change=3
host_paths_after_desired_change=192.168.1.181:4520,192.168.1.181:4420,192.168.1.184:4420
stale_old_host_path_after_desired_change=true
pod_uid_preserved=true
mounted_io_after_reconnect=ok
crd_status_agrees=true
report_dashboard_agree=true
surface_ready_reason=first_volume_verified
cleanup_status=ok
failure_count=0
```

The CSI-node log confirms the owner, not pod restage, connected the new desired
path:

```text
NodeStageVolume: pvc-20228828-11d7-4c8e-9fda-8475b7151ab7 reconciled mounted NVMe paths portals=192.168.1.184:4420,192.168.1.181:4520 target=nqn.2026-05.io.seaweedfs:pvc-20228828-11d7-4c8e-9fda-8475b7151ab7
MountedNVMeReconnectOwner: iteration checked=1 reconnected=1 failed=0
```

The report surface agrees with the new desired path set:

```text
managed_volume_nvme=pvc-20228828-11d7-4c8e-9fda-8475b7151ab7 nqn=nqn.2026-05.io.seaweedfs:pvc-20228828-11d7-4c8e-9fda-8475b7151ab7 nsid=1 addr=192.168.1.181:4520 addrs=192.168.1.181:4520,192.168.1.184:4420 path_count=2 multipath_observed=true reason=-
```

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

## Finding

Non-blocking for Phase 132: the host still had the old path connected after the
desired set changed:

```text
stale_old_host_path_after_desired_change=true
host_path_count_after_desired_change=3
```

Phase 132 only claims that the new desired path is connected from changed
control-plane evidence while mounted I/O continues. It does not claim stale
host-path pruning. The next NVMe correctness phase should add scoped pruning of
NVMe paths for the same NQN that are no longer in the desired path set.

## Boundary

This gate does not use `nvme disconnect-all` as proof. It does not claim
NVMe/RDMA, RoCE, performance, production HA, or SLO behavior.
