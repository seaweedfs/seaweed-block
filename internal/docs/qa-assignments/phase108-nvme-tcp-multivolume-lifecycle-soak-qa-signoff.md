# Phase 108 NVMe/TCP Multi-Volume Lifecycle Soak QA Sign-off

Status: PASS.

Validated source branch:
`phase108-nvme-tcp-multivolume-lifecycle-soak`.

Scenario:
`testops/scenarios/nvme-tcp-cross-node-multivolume-lifecycle-soak-chain.yaml`

QA run: `20260629-143934-a9f5`

Result: 19/19 PASS.

## Scope

This gate validates lifecycle residue for the supported-lab NVMe/TCP
multi-volume path. Phase 107 proved two cross-node NVMe/TCP PVCs can verify
data with distinct identities. Phase 108 repeats that create/write/read/delete
path twice and requires cleanup to drain before the next cycle can start.

The gate proves:

- two consecutive lifecycle cycles complete;
- each cycle provisions two PVCs with `protocol=nvme`;
- writer and reader pods pinned to the app node verify data for both PVCs;
- helper cleanup waits for generated blockvolume pods, matching PVs, and
  SeaweedFS NVMe subsystems to drain;
- each cycle leaves zero NVMe and Kubernetes multi-volume residue;
- final Helm uninstall plus strict cleanup leaves zero residue.

## Useful First-Run Failure

The first live run (`20260629-143510-956b`) failed for a real reason:

```text
cycle 1 residue nvme=1 pvc=0 pod=0 pv=1 sc=0
```

Artifacts showed the helper had reported `cleanup_status=ok` while:

- one SeaweedFS NVMe subsystem was still in `deleting`;
- one matching PV was still `Released`;
- generated blockvolume pods were still terminating/creating.

The fix was not to relax the gate. The helper cleanup path now waits for
generated blockvolume pods, matching PVs, and SeaweedFS NVMe subsystems before
claiming cleanup success.

## Terminal Evidence

```text
phase108_nvme_tcp_multivolume_lifecycle_soak_status=ok
cycle_count=2
volume_count_per_cycle=2
cycle_1_status=ok
cycle_1_writer_verified_count=2
cycle_1_reader_verified_count=2
cycle_1_cleanup_status=ok
cycle_1_nvme_residue_count=0
cycle_1_k8s_residue_count=0
cycle_2_status=ok
cycle_2_writer_verified_count=2
cycle_2_reader_verified_count=2
cycle_2_cleanup_status=ok
cycle_2_nvme_residue_count=0
cycle_2_k8s_residue_count=0
```

Both cycle summaries reported:

```text
multi_volume_status=ok
protocol=nvme
app_node_selector=m02
requested_volume_count=2
writer_verified_count=2
reader_verified_count=2
managed_volume_count=2
cleanup_status=ok
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

## Verdict

Phase 108 can close. The supported-lab NVMe/TCP path now has:

- Phase 106: single-PVC cross-node writer/reader attach;
- Phase 107: two-PVC cross-node identity and NQN isolation;
- Phase 108: repeated two-PVC lifecycle cleanup with zero per-cycle residue.

Non-claims remain: no RoCE/NVMe-RDMA, no performance/SLO, no broad
distro/kernel compatibility, no production HA, and no multi-path failover claim
across real hosts.
