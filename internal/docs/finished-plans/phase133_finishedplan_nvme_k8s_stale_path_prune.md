# Phase 133 Finished Plan: Kubernetes NVMe Stale Path Pruning Close Gate

Status: complete. Live QA PASS on 2026-07-04.

## Problem

Phase 132 proved live desired path-set convergence:

```text
mounted RF=2 NVMe/TCP PVC
-> one frontend path is replaced
-> control-plane desired path set changes
-> CSI-node owner connects the new desired path
-> mounted pod UID and I/O are preserved
```

That run also exposed a correctness gap:

```text
stale_old_host_path_after_desired_change=true
host_path_count_after_desired_change=3
```

The product connected fresh desired paths, but did not remove mounted host
paths for the same NQN that were no longer present in the current desired set.

## What Changed

The CSI node reconnect owner now prunes stale mounted NVMe paths after it
connects all desired paths.

Implementation shape:

1. List current host NVMe paths for the staged NQN.
2. Compare connected path addresses against the current desired `nvmeAddrs`.
3. Disconnect only paths for the same NQN that are no longer desired.
4. Use scoped controller disconnects (`nvme disconnect -d <controller>`).
5. Preserve the existing bounded reconnect behavior and mounted staging
   markers.

Added/extended files:

```text
core/csi/node.go
core/csi/linux_util.go
core/csi/node_test.go
core/csi/linux_util_test.go
scripts/run-phase133-nvme-k8s-stale-path-prune-gate.sh
testops/scenarios/nvme-tcp-k8s-stale-path-prune-chain.yaml
```

The Phase 111/132 harness now has an optional
`SW_BLOCK_NVME_REQUIRE_STALE_PATH_PRUNE=1` branch. Phase 132 remains compatible;
Phase 133 turns the prune requirement into a hard gate.

## Verification

Local:

```text
go test ./core/csi ./cmd/blockcsi -count=1
bash -n scripts/run-phase111-nvme-k8s-path-loss-crd-gate.sh \
  scripts/run-phase132-nvme-k8s-desired-path-change-gate.sh \
  scripts/run-phase133-nvme-k8s-stale-path-prune-gate.sh
swblock validate testops/scenarios/nvme-tcp-k8s-stale-path-prune-chain.yaml
git diff --check
```

Live:

```text
swblock run testops/scenarios/nvme-tcp-k8s-stale-path-prune-chain.yaml
run=20260704-145747-c73d
result=PASS 35/35
```

Key evidence:

```text
phase133_nvme_k8s_stale_path_prune_status=ok
initial_path_count=2
old_desired_path=192.168.1.184:4420
new_desired_path=192.168.1.184:4520
desired_path_set_changed=true
reconnect_owner=csi-node
reconnect_invoked=true
new_desired_path_connected=true
stale_old_path_detected=true
stale_old_path_pruned=true
host_path_count_after_prune=2
host_paths_after_prune=192.168.1.184:4520,192.168.1.181:4420
pod_uid_preserved=true
mounted_io_after_reconnect=ok
crd_status_agrees=true
report_dashboard_agree=true
cleanup_status=ok
```

## Closed Boundary

Phase 133 proves the supported-lab Kubernetes NVMe/TCP mounted desired-path
replacement loop:

```text
desired path set changes old->new
-> CSI-node owner connects new desired path
-> CSI-node owner prunes stale old path for the same NQN
-> mounted pod UID and I/O are preserved
-> CRD/report/dashboard agree
-> cleanup is clean
```

Non-claims:

- no `nvme disconnect-all` proof;
- no disconnect of other NQNs or other volumes;
- no NVMe/RDMA/RoCE;
- no production HA or node-loss survival claim;
- no performance/SLO claim.

## Next

The Kubernetes NVMe/TCP mounted correctness loop is now strong enough to return
to the Phase 126 performance finding: backend writes dominate the mounted
NVMe/TCP write gap. The next phase should work on durable backend write
batching/aggregation and rerun the same product-owned counter evidence before
making any optimization claim.
