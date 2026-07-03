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
| 123 | NVMe/TCP bottleneck triage with independent 100GbE network comparator | PASS |
| 124 | Same-shape local-path vs Block NVMe/TCP split | PASS |
| 125 | Block NVMe/TCP write-path profile with coarse target CPU evidence | PASS |
| 126 | Block NVMe/TCP backend write instrumentation with product-owned counters | PASS |
| 127 | OAES ANA Change Notice source/component gate | PASS |

Key QA sign-offs:

- `internal/docs/qa-assignments/phase114-nvme-k8s-multivolume-mounted-path-isolation-qa-signoff.md`
- `internal/docs/qa-assignments/phase115-nvme-k8s-multivolume-mounted-path-churn-soak-qa-signoff.md`
- `internal/docs/qa-assignments/phase122-nvme-tcp-100gbe-baseline-qa-signoff.md`
- `internal/docs/qa-assignments/phase123-nvme-tcp-bottleneck-triage-qa-signoff.md`
- `internal/docs/qa-assignments/phase124-nvme-tcp-target-backend-shape-split-qa-signoff.md`
- `internal/docs/qa-assignments/phase125-block-nvme-tcp-write-path-profile-qa-signoff.md`
- `internal/docs/qa-assignments/phase126-block-nvme-tcp-backend-write-instrumentation-qa-signoff.md`
- `internal/docs/qa-assignments/phase127-nvme-ana-change-notice-qa-signoff.md`

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

Phase 123 adds an independent network comparator over the same configured
100GbE TCP route:

```text
network_baseline_mibps=4106.55
k8s_mounted_seq_write_mibps=127.74
k8s_mounted_seq_read_mibps=248.06
k8s_mounted_small_write_iops=755.16
top_bottleneck=unknown
next_recommendation=phase124_target_backend_shape_split
cleanup_status=ok
```

This shows the configured 10.0.0.x data-plane network is not the immediate
bottleneck. It still does not identify whether the remaining limit is target
CPU, durable backend, Kubernetes mounted filesystem overhead, or the current
`dd` test shape; Phase 124 splits those before any NVMe/RDMA work.

Phase 124 compares the mounted Block NVMe/TCP path with a same-node Kubernetes
`local-path` PVC using the same `dd` shape:

```text
network_baseline_mibps=3769.28
local_path_seq_write_mibps=324.87
local_path_seq_read_mibps=235.29
block_nvme_seq_write_mibps=118.74
block_nvme_seq_read_mibps=273.50
block_vs_local_read_ratio=1.162
block_vs_local_write_ratio=0.366
shape_fsync_penalty=1.180
top_bottleneck=block_target_or_backend
next_recommendation=phase125_blockvolume_target_cpu_profile
cleanup_status=ok
```

This narrows the next engineering work to the Block write path. It still does
not create a performance/SLO claim, and it does not justify starting NVMe/RDMA
until the target/backend write-side gap is understood.

Phase 125 profiles a larger 512MiB write and captures coarse blockvolume CPU
samples during the write:

```text
network_baseline_mibps=3836.30
local_path_seq_write_mibps=1147.98
local_path_seq_read_mibps=513.54
block_nvme_seq_write_mibps=174.33
block_nvme_seq_read_mibps=544.10
block_vs_local_write_ratio=0.152
block_vs_local_read_ratio=1.060
blockvolume_cpu_sample_count=3
blockvolume_cpu_peak_percent=0.80
write_path_observation=backend_sync
next_recommendation=phase126_durable_backend_write_optimization
cleanup_status=ok
```

The CPU evidence is coarse and does not prove a specific backend function is
the bottleneck. It is enough to defer NVMe/RDMA and require product-owned
write-path instrumentation next.

Phase 126 adds that product-owned instrumentation to `/status/durable` and
runs the same mounted NVMe/TCP versus local-path comparison with target/backend
write counters:

```text
network_baseline_mibps=4180.60
local_path_seq_write_mibps=1115.47
local_path_seq_read_mibps=536.13
block_nvme_seq_write_mibps=177.72
block_nvme_seq_read_mibps=520.85
block_vs_local_write_ratio=0.159
block_vs_local_read_ratio=0.971
target_write_observed=true
target_write_bytes=588075008
target_write_ops=17972
target_write_duration_ms=34233
backend_write_bytes=588075008
backend_write_ops=17972
backend_write_duration_ms=33186
backend_sync_ops=9
backend_sync_duration_ms=73
write_path_observation=backend_write
top_bottleneck=backend_write
next_recommendation=phase127_durable_backend_write_batching
cleanup_status=ok
```

The duration fields are cumulative per-operation timing from the target and
durable backend, not wall-clock benchmark elapsed time. They are useful for
localizing the write-side cost and identify durable backend large-write
batching as the next performance optimization. That optimization is deferred
behind the Phase 127/128 NVMe correctness work and still does not create a
throughput/SLO, RoCE, or NVMe/RDMA claim.

Phase 127 closes the source/component side of the ANA Change Notice gap:

```text
ana_provider_oaes_ana_change_notice=true
no_provider_oaes_zero=true
aer_completes_on_ana_change=true
aer_completion_event_type=notice
aer_completion_event_info=ana_change
aer_completion_log_page=ana
aer_limit_still_enforced=true
host_live_aer_claim=false
k8s_dynamic_reconnect_claim=false
cleanup_status=ok
```

This means the target now has a concrete ANA-change async event source when an
ANA provider is wired. It is still not a live Linux host notification or
Kubernetes dynamic reconnect claim until those gates pass.

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
- Live Linux host AER/ANA notification behavior.
- Kubernetes dynamic NVMe reconnect/restage after primary/node failover.
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
