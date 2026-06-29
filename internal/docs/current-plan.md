# Current Plan: Phase 107 NVMe/TCP Multi-Volume Cross-Node Isolation

Status: closed. Live gate passed on 2026-06-29
(`nvme-tcp-cross-node-multivolume-isolation-chain`, run
`20260629-142400-2032`, 30/30 PASS) with a separate strict cleanup audit
returning `cleanup_status=ok`.

## Why This Is Next

Phase 106 proved the positive single-PVC cross-node NVMe/TCP path:

```text
blockvolume on m01
workload on m02
publish_target=192.168.1.181:4420
protocol=nvme
writer/reader verified
managed_volume=ready reason=first_volume_verified
```

The next risk is not another transport claim. It is isolation: when more than
one PVC uses the new routable NVMe/TCP path, the product must not collapse
volume identity, reuse the wrong NQN, or surface one volume's target as another
volume's authority.

## Product Goal

Prove the supported-lab NVMe/TCP cross-node path handles multiple PVCs with
independent identities.

Required behavior:

- two PVCs can be provisioned with `protocol=nvme`;
- workload pods are pinned to a different Kubernetes node from the primary
  blockvolume node;
- every writer and reader verifies data through its own PVC;
- each managed volume reports `ready/first_volume_verified`;
- NVMe NQNs remain distinct per volume;
- no loopback target is used for cross-node attach;
- cleanup leaves zero residue in the strict verifier.

## D1: Multi-Volume Helper Protocol Selection

`scripts/run-multi-volume-example.sh` now accepts:

```text
SW_BLOCK_MULTI_VOLUME_PROTOCOL=iscsi|nvme
SW_BLOCK_MULTI_VOLUME_NODE_SELECTOR=<kubernetes node name>
```

For `nvme`, the helper renders both CSI protocol keys in the StorageClass:

```yaml
sw-block.seaweedfs.com/protocol: "nvme"
protocol: "nvme"
```

The node selector pins all generated writer/reader pods to the selected
application node.

## D2: Live Multi-Volume Cross-Node Gate

Scenario:

```text
testops/scenarios/nvme-tcp-cross-node-multivolume-isolation-chain.yaml
```

Terminal evidence:

```text
phase107_nvme_tcp_multivolume_isolation_status=ok
app_node=m02
protocol=nvme
managed_volume_count=2
writer_verified_count=2
reader_verified_count=2
managed_volume_status=ready
managed_volume_reason=first_volume_verified
distinct_volume_ids=2
distinct_nvme_nqns=2
publish_target_loopback=false
cross_volume_identity_mixup=false
```

Strict cleanup audit:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

## Non-Claims

Phase 107 still does not claim:

- RoCE/NVMe-RDMA;
- performance or SLO;
- broad distro/kernel compatibility;
- production HA;
- multi-path failover across real hosts;
- more than the supported-lab two-PVC isolation gate.
