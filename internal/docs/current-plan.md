# Current Plan: Phase 108 NVMe/TCP Multi-Volume Lifecycle Soak

Status: closed. Live gate passed on 2026-06-29
(`nvme-tcp-cross-node-multivolume-lifecycle-soak-chain`, run
`20260629-143934-a9f5`, 19/19 PASS) with final strict cleanup returning
`cleanup_status=ok`.

## Why This Is Next

Phase 106 proved one cross-node NVMe/TCP PVC can be mounted and verified.
Phase 107 proved two cross-node NVMe/TCP PVCs keep distinct volume identities
and distinct NVMe NQNs.

The next risk is lifecycle residue. A product can pass a single attach test but
still leak NVMe sessions, stale PVC/PV objects, StorageClasses, or generated
blockvolume Deployments after repeated create/delete cycles. That would make
the feature unreliable for users even if the happy path is correct.

## Product Goal

Prove the supported-lab NVMe/TCP cross-node multi-volume path can run repeated
create/write/read/delete cycles without accumulating residue.

Required behavior:

- install the NVMe/TCP chart path with external NVMe/TCP enabled;
- run two lifecycle cycles;
- each cycle provisions two PVCs with `protocol=nvme`;
- writer and reader pods are pinned to the application node and verify data;
- helper cleanup deletes pods, PVCs, generated blockvolume Deployments, and the
  StorageClass;
- each cycle leaves zero SeaweedFS NVMe subsystems and zero multi-volume
  Kubernetes residue before the next cycle starts;
- final Helm uninstall plus strict cleanup returns `cleanup_status=ok`.

## D1: Lifecycle Soak Scenario

Scenario:

```text
testops/scenarios/nvme-tcp-cross-node-multivolume-lifecycle-soak-chain.yaml
```

Terminal evidence:

```text
phase108_nvme_tcp_multivolume_lifecycle_soak_status=ok
cycle_count=2
volume_count_per_cycle=2
cycle_1_writer_verified_count=2
cycle_1_reader_verified_count=2
cycle_1_cleanup_status=ok
cycle_1_nvme_residue_count=0
cycle_1_k8s_residue_count=0
cycle_2_writer_verified_count=2
cycle_2_reader_verified_count=2
cycle_2_cleanup_status=ok
cycle_2_nvme_residue_count=0
cycle_2_k8s_residue_count=0
```

The first live run failed usefully: helper cleanup reported `cleanup_status=ok`
while a SeaweedFS NVMe subsystem was still `deleting`, a matching PV was still
`Released`, and generated blockvolume pods were still terminating/creating. The
helper cleanup path now waits for generated blockvolume pods, matching PVs, and
SeaweedFS NVMe subsystems to drain before declaring success.

## D2: Final Cleanup Gate

After the two lifecycle cycles, uninstall the Helm release and run the strict
cleanup verifier.

Expected evidence:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

Observed evidence:

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

Phase 108 still does not claim:

- RoCE/NVMe-RDMA;
- performance or SLO;
- broad distro/kernel compatibility;
- production HA;
- multi-path failover across real hosts;
- more than the supported-lab two-PVC, two-cycle lifecycle gate.
