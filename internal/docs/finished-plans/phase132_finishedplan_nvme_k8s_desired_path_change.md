# Phase 132 Finished Plan: Kubernetes NVMe Desired Path-Set Change Close Gate

Status: complete. Live QA PASS on 2026-07-04.

## Problem

Phase 131 proved that the CSI-node reconnect owner can restore a missing host
NVMe path when the control-plane desired path set is unchanged.

The remaining correctness gap was stronger:

```text
mounted RF=2 NVMe/TCP PVC
-> one frontend path is replaced
-> control-plane desired path set changes
-> mounted pod connects the new desired path without restage/remount
-> I/O continues
```

Without this gate, we could only claim host-path reconnect, not dynamic desired
path-set convergence.

## What Changed

Phase 132 adds a live gate:

```text
scripts/run-phase132-nvme-k8s-desired-path-change-gate.sh
testops/scenarios/nvme-tcp-k8s-desired-path-change-chain.yaml
```

The gate reuses the Phase 111/131 Kubernetes NVMe harness and adds a dedicated
`SW_BLOCK_NVME_DESIRED_PATH_CHANGE=1` branch. It:

1. Installs an RF=2 NVMe/TCP PVC with stage-2 multipath and the CSI-node
   reconnect owner enabled.
2. Creates a mounted pod and records its UID.
3. Replaces one generated blockvolume Deployment's `--nvme-listen` address
   with a different reachable address.
4. Waits for `SwBlockVolume.status.nvme.nvmeAddrs` to drop the old address and
   include the new address.
5. Waits for CSI-node owner logs proving the new desired address was connected.
6. Verifies mounted I/O and pod UID preservation.
7. Verifies CRD, report, and dashboard agreement.
8. Verifies zero-residue cleanup.

## Verification

Local:

```text
bash -n scripts/run-phase111-nvme-k8s-path-loss-crd-gate.sh
bash -n scripts/run-phase132-nvme-k8s-desired-path-change-gate.sh
swblock validate testops/scenarios/nvme-tcp-k8s-desired-path-change-chain.yaml
git diff --check
```

Live:

```text
swblock run testops/scenarios/nvme-tcp-k8s-desired-path-change-chain.yaml
run=20260704-143757-a76e
result=PASS 31/31
```

Key evidence:

```text
phase132_nvme_k8s_desired_path_change_status=ok
initial_path_count=2
old_desired_path=192.168.1.181:4420
new_desired_path=192.168.1.181:4520
desired_path_set_changed=true
reconnect_owner=csi-node
reconnect_invoked=true
new_desired_path_connected=true
pod_uid_preserved=true
mounted_io_after_reconnect=ok
crd_status_agrees=true
report_dashboard_agree=true
cleanup_status=ok
```

## Finding

The live run found that the old host path remains connected after the desired
path set changes:

```text
stale_old_host_path_after_desired_change=true
host_path_count_after_desired_change=3
```

That is outside Phase 132's claim, but it is the next correctness gap. The CSI
owner should eventually prune mounted NVMe paths for the same NQN that are not
in the current desired path set, using scoped disconnects only.

## Closed Boundary

Phase 132 proves live Kubernetes NVMe desired path-set convergence:

```text
desired path set changes
-> CSI-node owner sees fresh control-plane evidence
-> new desired path connects
-> mounted pod UID is preserved
-> mounted I/O works
-> CRD/report/dashboard agree
```

Non-claims:

- no stale path pruning yet;
- no `nvme disconnect-all` proof;
- no NVMe/RDMA/RoCE;
- no performance/SLO claim;
- no production HA claim.

## Next

Phase 133 should close the stale-path cleanup gap: when desired NVMe addrs
change, the CSI-node owner should disconnect only stale paths for the same NQN
that are no longer desired, while preserving mounted I/O and avoiding
`disconnect-all`.
