# NVMe/TCP Supported-Lab Claim

Status: source-gated. Matching published `seaweed-block` and
`seaweed-block-csi` images still need a release-smoke run before this becomes a
public image claim.

## Claim

Seaweed Block can run an opt-in NVMe/TCP Kubernetes CSI path in the supported
lab:

```text
Helm values: protocol=nvme, stage2Multipath=true, replicationFactor=2
CSI CreateVolume -> blockmaster publish context -> CSI NodeStage
-> Linux NVMe/TCP native multipath -> mounted app pod write/read
```

The supported-lab claim is intentionally narrow:

- dynamic Kubernetes PVC provisioning with `protocol=nvme`;
- RF=2 volume layout with two NVMe/TCP frontend paths for one NQN/NSID;
- CSI publish context carries `nvmeAddrs` and stage-2 multipath intent;
- CSI NodeStage connects all expected NVMe/TCP paths and fails closed on a
  single-path stage-2 publish context;
- CRD/report/operator-snapshot/dashboard/explain surfaces expose protocol, NQN,
  namespace ID, address list, path count, and reason code;
- one path loss surfaces as `blocked/nvme_multipath_path_missing`, never false
  `Ready=True`;
- mounted workloads keep the same pod UID and continue write/read I/O through
  path loss;
- restored paths return to two live host NVMe paths and mounted write/read I/O
  still works;
- two mounted volumes remain isolated through path loss and restore;
- bounded multi-volume churn passes across three alternating loss/restore
  cycles;
- uninstall/cleanup leaves zero Seaweed Block Kubernetes, iSCSI, process,
  multipath, and hostPath residue.

## Evidence Chain

| Phase | Evidence | Result |
| --- | --- | --- |
| 99 | NVMe ANA/provider and CSI single-path baseline | PASS |
| 100 | Kubernetes CSI NVMe multipath attach | PASS |
| 101 | NVMe status, one-path failure honesty, repeated stage/unstage, bounded soak | PASS |
| 102 | Published-image NVMe smoke | Artifact-blocked until matching images publish |
| 103 | NVMe/TCP and RoCE host-capability preflight | PASS |
| 104 | RoCE live-I/O feasibility boundary, current target refuses RDMA clearly | PASS |
| 105 | Cross-node loopback NVMe/TCP topology blocker | PASS |
| 106 | Cross-node non-loopback NVMe/TCP live attach | PASS |
| 107 | Multi-volume cross-node NVMe identity isolation | PASS |
| 108 | Multi-volume NVMe lifecycle soak | PASS |
| 109 | NVMe status-surface evidence agreement | PASS |
| 110 | Path-loss support-surface honesty from replay evidence | PASS |
| 111 | Live Kubernetes CRD path-loss honesty | PASS |
| 112 | Mounted pod survives one observed path loss | PASS |
| 113 | Mounted pod survives path restore | PASS |
| 114 | Two mounted volumes stay isolated through one volume path loss/restore | PASS |
| 115 | Two mounted volumes pass three alternating path churn cycles | PASS |
| 120 | Management-LAN NVMe/TCP performance baseline | PASS |
| 121 | Explicit data-plane/frontend IP capability | PASS |
| 122 | 100GbE TCP frontend-address performance baseline | PASS |

Key QA sign-offs:

- `internal/docs/qa-assignments/phase114-nvme-k8s-multivolume-mounted-path-isolation-qa-signoff.md`
- `internal/docs/qa-assignments/phase115-nvme-k8s-multivolume-mounted-path-churn-soak-qa-signoff.md`
- `internal/docs/qa-assignments/phase122-nvme-tcp-100gbe-baseline-qa-signoff.md`

## Performance Baseline

Phase 122 is the current supported-lab baseline for the configured 100GbE TCP
frontend path:

```text
publish_target=10.0.0.1:4420
publish_target_network_class=100gbe_tcp
publish_target_route_dev=enp1s0np0
seq_write_mibps=115.11
seq_read_mibps=250.98
small_write_iops=606.64
cleanup_status=ok
```

This proves the target is no longer using the Kubernetes management LAN, but it
is still a baseline only. It does not create a throughput/SLO claim.

## Representative Release Smoke

When matching release images exist, the smoke should use the exact image pair:

```text
ghcr.io/seaweedfs/seaweed-block:<candidate>
ghcr.io/seaweedfs/seaweed-block-csi:<candidate>
```

Minimum release-smoke coverage:

1. Generate Helm values with:

   ```bash
   sw-block ops generate-helm-values \
     --protocol nvme \
     --stage2-multipath \
     --replication-factor 2 \
     --image ghcr.io/seaweedfs/seaweed-block:<candidate> \
     --csi-image ghcr.io/seaweedfs/seaweed-block-csi:<candidate> \
     --out values.nvme.yaml
   ```

2. Confirm render includes NVMe and stage-2 multipath flags.

3. Install the chart and create one RF=2 NVMe/TCP PVC.

4. Confirm the mounted writer/reader path passes.

5. Confirm `SwBlockVolume.status.nvme.pathCount=2` and
   `Ready=True/first_volume_verified`.

6. Run one representative path-loss/restore check, or rerun the Phase 114 gate
   if the release is intended to advertise multi-volume mounted isolation.

7. Verify cleanup returns:

   ```text
   cleanup_status=ok
   k8s_residue_count=0
   iscsi_residue_count=0
   process_residue_count=0
   multipath_residue_count=0
   hostpath_residue_count=0
   failure_count=0
   ```

Do not mark the NVMe/TCP path as a published-image release claim until this
image-pair smoke passes.

## Non-Claims

This evidence does not claim:

- RoCE or NVMe/RDMA data path. The current target is NVMe/TCP only, and
  `--nvme-transport=rdma` is a refusal path.
- Performance, throughput, latency, or production SLO.
- Broad Linux distro, kernel, initiator, or cloud compatibility.
- Production HA, node-loss survival, or arbitrary unbounded path churn.
- Backup, snapshot, restore, disaster recovery, or data migration.
- Automatic cleanup or host repair.
- Hosted production UI.

## User Guidance

Use NVMe/TCP only when you can accept the supported-lab boundary:

- you control the Kubernetes nodes;
- Linux NVMe/TCP host tooling is available;
- the cluster can route to the generated NVMe/TCP target addresses;
- you can run the cleanup verifier after tests;
- you treat failures as alpha/beta evidence, not production SLO violations.

Use the default iSCSI path when you want the broader, older compatibility path.
