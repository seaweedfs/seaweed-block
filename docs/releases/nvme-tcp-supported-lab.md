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
- changed control-plane desired path sets cause CSI-node to connect the new
  desired NVMe/TCP path for a mounted pod without remounting;
- stale old host paths for the same NQN are pruned after desired path-set
  replacement using scoped path disconnects;
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
| 128 | Live Linux host ANA Change Notice AER gate | PASS |
| 129 | CSI mounted NVMe restage contract | PASS |
| 130 | CSI-node reconnect owner/trigger contract | PASS |
| 131 | Live Kubernetes host-path reconnect through CSI-node owner | PASS |
| 132 | Live Kubernetes desired path-set change through CSI-node owner | PASS |
| 133 | Live Kubernetes stale host-path pruning after desired path replacement | PASS |
| 134 | Durable backend full-block write batching with product-owned counters | PASS |
| 135 | Post-batch write-path retriage and next-bottleneck classification | PASS |
| 136 | WAL append/copy/checksum profile and backend-internal bottleneck classification | PASS |
| 137 | WAL record encode/copy reduction and next append-shape bottleneck classification | PASS |
| 138 | WAL append write-at shape profile and small-write classification | PASS |
| 139 | WAL append batch-shape analysis and frontend-request-limited classification | PASS |
| 140 | Frontend request-size owner classification: NVMe/TCP target MaxH2C limit | PASS |
| 141 | 64KiB NVMe/TCP MaxH2C opt-in boundary and live request-size movement | PASS |
| 142 | 64KiB NVMe/TCP post-H2C backend bottleneck retriage | PASS |
| 143 | 64KiB NVMe/TCP WAL append shape profile | PASS |
| 144 | 64KiB NVMe/TCP WAL encode/append pair profile | PASS |
| 145 | WAL record materialization allocation reduction | PASS |
| 146 | WAL materialization effectiveness profile | PASS |
| 147 | WAL multi-block record design gate | PASS |
| 148 | WAL multi-block record local prototype | PASS |
| 149 | WAL multi-block record local profile | PASS |
| 150 | WAL multi-block runtime opt-in | PASS |
| 151 | Mounted NVMe/TCP multi-block WAL opt-in profile | PASS |
| 152 | Mounted multi-block WAL recovery compatibility | PASS |
| 153 | WAL multi-block release-boundary documentation | PASS |
| 154 | Durable-status HeadLSN diagnostic cleanup | PASS |
| 155 | Mounted durable-status HeadLSN confirmation | PASS |
| 156 | WAL multi-block release-smoke decision keeps opt-in source-gated | PASS |
| 157 | NVMe/RDMA capability boundary keeps RDMA as non-claim | PASS |
| 158 | NVMe/RDMA volume capability probe reports TCP supported, RDMA unsupported | PASS |
| 159 | NVMe/RDMA standalone listener design gate rejects fake TCP-over-RDMA path | PASS |
| 160 | NVMe/RDMA transport adapter seam isolates TCP PDU wire path | PASS |
| 161 | NVMe/RDMA standalone preflight/refusal facts surface module/device/bind evidence | PASS |
| 162 | NVMe/RDMA listener start decision skeleton stays disabled by default | PASS |
| 163 | Standalone NVMe/RDMA live I/O through the Seaweed backend | PASS |
| 164 | Standalone NVMe/RDMA lifecycle, restart, isolation, refusal, and cleanup hardening | PASS |

Key QA sign-offs:

- `internal/docs/qa-assignments/phase114-nvme-k8s-multivolume-mounted-path-isolation-qa-signoff.md`
- `internal/docs/qa-assignments/phase115-nvme-k8s-multivolume-mounted-path-churn-soak-qa-signoff.md`
- `internal/docs/qa-assignments/phase122-nvme-tcp-100gbe-baseline-qa-signoff.md`
- `internal/docs/qa-assignments/phase123-nvme-tcp-bottleneck-triage-qa-signoff.md`
- `internal/docs/qa-assignments/phase124-nvme-tcp-target-backend-shape-split-qa-signoff.md`
- `internal/docs/qa-assignments/phase125-block-nvme-tcp-write-path-profile-qa-signoff.md`
- `internal/docs/qa-assignments/phase126-block-nvme-tcp-backend-write-instrumentation-qa-signoff.md`
- `internal/docs/qa-assignments/phase127-nvme-ana-change-notice-qa-signoff.md`
- `internal/docs/qa-assignments/phase128-nvme-ana-change-notice-host-qa-signoff.md`
- `internal/docs/qa-assignments/phase129-nvme-k8s-mounted-restage-qa-signoff.md`
- `internal/docs/qa-assignments/phase130-nvme-reconnect-owner-qa-signoff.md`
- `internal/docs/qa-assignments/phase131-nvme-k8s-reconnect-live-qa-signoff.md`
- `internal/docs/qa-assignments/phase132-nvme-k8s-desired-path-change-qa-signoff.md`
- `internal/docs/qa-assignments/phase133-nvme-k8s-stale-path-prune-qa-signoff.md`
- `internal/docs/qa-assignments/phase134-durable-backend-write-batching-qa-signoff.md`
- `internal/docs/qa-assignments/phase135-nvme-tcp-post-batch-retriage-qa-signoff.md`
- `internal/docs/qa-assignments/phase136-wal-append-copy-checksum-profile-qa-signoff.md`
- `internal/docs/qa-assignments/phase137-reduce-wal-record-encode-copy-qa-signoff.md`
- `internal/docs/qa-assignments/phase138-wal-writeat-shape-profile-qa-signoff.md`
- `internal/docs/qa-assignments/phase139-wal-append-batch-shape-coalescing-qa-signoff.md`
- `internal/docs/qa-assignments/phase140-frontend-request-size-profile-qa-signoff.md`
- `internal/docs/qa-assignments/phase141-nvme-tcp-max-h2c-boundary-qa-signoff.md`
- `internal/docs/qa-assignments/phase142-nvme-tcp-large-h2c-retriage-qa-signoff.md`
- `internal/docs/qa-assignments/phase143-wal-append-large-h2c-profile-qa-signoff.md`
- `internal/docs/qa-assignments/phase144-wal-encode-append-pair-profile-qa-signoff.md`
- `internal/docs/qa-assignments/phase145-wal-record-materialization-reduction-qa-signoff.md`
- `internal/docs/qa-assignments/phase146-wal-record-materialization-effectiveness-qa-signoff.md`
- `internal/docs/qa-assignments/phase147-wal-multiblock-record-design-qa-signoff.md`
- `internal/docs/qa-assignments/phase148-wal-multiblock-record-local-prototype-qa-signoff.md`
- `internal/docs/qa-assignments/phase149-wal-multiblock-record-profile-qa-signoff.md`
- `internal/docs/qa-assignments/phase150-wal-multiblock-runtime-opt-in-qa-signoff.md`
- `internal/docs/qa-assignments/phase151-wal-multiblock-mounted-nvme-profile-qa-signoff.md`
- `internal/docs/qa-assignments/phase152-wal-multiblock-recovery-compatibility-qa-signoff.md`
- `internal/docs/qa-assignments/phase153-wal-multiblock-release-boundary-qa-signoff.md`
- `internal/docs/qa-assignments/phase154-durable-status-head-lsn-cleanup-qa-signoff.md`
- `internal/docs/qa-assignments/phase155-mounted-durable-status-head-lsn-confirmation-qa-signoff.md`
- `internal/docs/qa-assignments/phase156-wal-multiblock-release-smoke-decision-qa-signoff.md`
- `internal/docs/qa-assignments/phase157-nvme-rdma-capability-boundary-qa-signoff.md`
- `internal/docs/qa-assignments/phase158-nvme-rdma-volume-capability-probe-qa-signoff.md`
- `internal/docs/qa-assignments/phase159-nvme-rdma-standalone-listener-design-qa-signoff.md`
- `internal/docs/qa-assignments/phase160-nvme-rdma-transport-adapter-seam-qa-signoff.md`
- `internal/docs/qa-assignments/phase161-nvme-rdma-standalone-preflight-refusal-qa-signoff.md`
- `internal/docs/qa-assignments/phase162-nvme-rdma-standalone-listener-skeleton-qa-signoff.md`

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

Phase 134 returns to that backend-write bottleneck and adds bounded full-block
write batching plus product-owned internal storage-call counters. The old
`backend_write_ops` counter remains useful, but it measures frontend
`StorageBackend.Write` calls, not internal per-block storage fan-out. The new
`backend_storage_*` counters are the evidence for batching:

```text
phase134_durable_backend_write_batching_status=ok
target_write_observed=true
target_write_bytes=118259712
backend_write_bytes=118259712
backend_write_ops=3634
backend_storage_write_calls=3634
backend_storage_write_blocks=28872
backend_storage_batch_calls=3613
backend_storage_batch_blocks=28851
backend_storage_batching_effective=true
strict_ack_batch_disabled=true
cleanup_status=ok
```

This proves the supported-lab NVMe/TCP write path exercises durable backend
batching and still cleans up. It is not a performance/SLO claim; the next
performance step is to compare wall-clock write behavior after batching and
identify the next dominant cost.

Phase 135 ran that comparable 512MiB post-batch profile:

```text
phase135_nvme_tcp_post_batch_retriage_status=ok
profile_comparable_with_phase126=true
network_baseline_mibps=4099.38
block_nvme_seq_write_mibps=172.80
block_nvme_seq_read_mibps=531.67
local_path_seq_write_mibps=1075.63
block_vs_local_write_ratio=0.161
backend_storage_batching_effective=true
backend_storage_batch_calls=17953
backend_storage_batch_blocks=143555
backend_sync_duration_ms=42
post_batch_bottleneck=backend_write
next_recommendation=phase136_wal_append_copy_checksum_profile
cleanup_status=ok
```

The result keeps NVMe/RDMA deferred: batching is active, network and sync are
not dominant, and the next evidence-backed step is to split WAL
append/copy/checksum/dirty-map cost inside the durable backend.

Phase 136 added those backend-internal counters and reran the same mounted
NVMe/TCP profile:

```text
phase136_wal_append_copy_checksum_profile_status=ok
target_write_observed=true
target_write_bytes=588075008
backend_write_bytes=588075008
backend_storage_batching_effective=true
backend_storage_batch_calls=17952
backend_storage_batch_blocks=143552
wal_copy_ops=143573
wal_copy_bytes=588075008
wal_copy_duration_ms=593
wal_encode_ops=143573
wal_encode_bytes=593530782
wal_encode_duration_ms=753
wal_checksum_ops=143573
wal_checksum_bytes=592382198
wal_checksum_duration_ms=100
wal_append_ops=17981
wal_append_bytes=593543918
wal_append_duration_ms=338
dirty_map_update_ops=143573
dirty_map_update_duration_ms=67
post_phase136_bottleneck=wal_encode
next_recommendation=phase137_reduce_wal_record_encode_copy
cleanup_status=ok
```

The named backend-internal cost is WAL record encode/copy. This keeps the next
work item inside the durable backend; it still does not create an NVMe/RDMA,
RoCE, throughput, latency, or production SLO claim.

Phase 137 reduced that encode/copy seam while preserving the WAL record format:

```text
phase137_reduce_wal_record_encode_copy_status=ok
phase136_wal_encode_copy_duration_ms=1346
preencode_data_copy_removed=true
batch_append_encodes_direct_to_pending=true
wal_copy_duration_ms=93
wal_encode_duration_ms=270
wal_encode_copy_duration_ms=363
wal_encode_copy_reduced_vs_phase136=true
wal_append_duration_ms=375
post_phase137_bottleneck=wal_append
next_recommendation=phase138_wal_writeat_shape_profile
cleanup_status=ok
```

The next measured cost is WAL append/write-at shape. The product still makes no
published performance/SLO, RoCE, or NVMe/RDMA claim from this source-gated lab
evidence.

Phase 138 split that append/write-at shape:

```text
phase138_wal_writeat_shape_profile_status=ok
wal_append_duration_ms=380
wal_append_writeat_calls=17979
wal_append_writeat_bytes=593543918
wal_append_writeat_max_bytes=33072
wal_append_writeat_avg_bytes=33013
wal_append_wrap_count=8
wal_append_padding_bytes=13136
post_phase138_bottleneck=wal_append_small_writes
next_recommendation=phase139_wal_append_batch_shape_coalescing
cleanup_status=ok
```

The write-at shape is many small writes of about 33KB, with negligible
wrap/padding. This keeps the next work inside WAL batching/coalescing rather
than transport work.

Phase 139 proved that the 33KB shape is coming from the frontend request path:

```text
phase139_wal_append_batch_shape_coalescing_status=ok
backend_write_request_max_bytes=32768
backend_write_request_avg_bytes=32723
backend_full_block_batch_max=8
backend_full_block_batch_avg=7
wal_append_writeat_max_bytes=33072
wal_append_writeat_avg_bytes=33012
phase139_shape_result=frontend_request_limited
post_phase139_bottleneck=wal_append_small_writes
next_recommendation=phase140_frontend_request_size_profile
cleanup_status=ok
```

This keeps the next experiment at the frontend/NVMe request-size seam. It is
still not a transport, RoCE, NVMe/RDMA, or production throughput/SLO claim.

Phase 140 named the owner of that request-size seam:

```text
phase140_frontend_request_size_profile_status=ok
nvme_tcp_max_h2c_data_length_bytes=32768
nvme_tcp_ioccsz_units=2052
target_write_request_max_bytes=32768
backend_write_request_max_bytes=32768
backend_full_block_batch_max=8
frontend_request_size_owner=target_limit
phase140_shape_result=target_limited
post_phase140_bottleneck=frontend_request_size
next_recommendation=phase141_nvme_tcp_max_h2c_boundary
cleanup_status=ok
```

The current 32KiB request shape is therefore an NVMe/TCP target advertised H2C
limit, not a WAL coalescing limit or a host-only choice. The next gate should
test any larger `MaxH2CDataLength` as an explicit protocol boundary before
changing defaults.

Phase 141 proved a 64KiB `MaxH2CDataLength` candidate as an explicit opt-in:

```text
phase141_nvme_tcp_max_h2c_boundary_status=ok
candidate_max_h2c_bytes=65536
host_connects_candidate=true
writer_verified=true
reader_verified=true
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
request_size_increase_observed=true
phase141_decision=add_opt_in
next_recommendation=phase142_nvme_tcp_large_h2c_retriage
cleanup_status=ok
```

This is source-gated lab evidence that the option can be wired and used by the
Linux host. It is not a default change, production compatibility claim, or
performance/SLO claim.

Phase 142 retriaged the write path under that 64KiB opt-in:

```text
phase142_nvme_tcp_large_h2c_retriage_status=ok
candidate_max_h2c_bytes=65536
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
backend_full_block_batch_max=16
wal_append_writeat_max_bytes=66144
wal_append_duration_ms=300
wal_encode_duration_ms=289
phase142_bottleneck=wal_append
phase142_decision=continue_backend_work
next_recommendation=phase143_wal_append_large_h2c_profile
cleanup_status=ok
```

This confirms the next bottleneck is no longer the NVMe/TCP request-size
boundary. The 64KiB path remains opt-in; the next work is backend WAL append
profiling, not a default change or performance/SLO claim.

Phase 143 decomposed that WAL append bucket:

```text
phase143_wal_append_large_h2c_profile_status=ok
candidate_max_h2c_bytes=65536
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
backend_full_block_batch_max=16
wal_append_duration_ms=290
wal_append_writeat_calls=9009
wal_append_writeat_avg_bytes=65883
wal_append_wrap_count=8
wal_append_padding_bytes=13136
wal_encode_duration_ms=285
phase143_append_shape=encode_close_second
next_recommendation=phase144_wal_encode_append_pair_profile
cleanup_status=ok
```

The append bucket is not primarily wrap/padding. WAL encode is nearly tied with
append duration, so the next backend work should profile encode+append as a
pair before changing WAL append semantics.

Phase 144 confirmed encode and append are tied:

```text
phase144_wal_encode_append_pair_profile_status=ok
candidate_max_h2c_bytes=65536
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
wal_encode_duration_ms=297
wal_append_duration_ms=295
phase144_pair_shape=encode_append_tied
next_recommendation=phase145_wal_record_materialization_reduction
cleanup_status=ok
```

The next backend work is a narrow WAL record materialization reduction, still
behind source-gated lab evidence.

Phase 145 made the first narrow materialization reduction:

```text
phase145_wal_record_materialization_reduction_status=ok
wal_record_materialization_change=writebatch_value_entries
unit_record_compatibility=pass
candidate_max_h2c_bytes=65536
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
writer_verified=true
reader_verified=true
phase145_decision=keep_change
cleanup_status=ok
```

The change removes per-record pointer allocation in `WALStore.WriteBatch`.
It preserves WAL format/recovery semantics and is not yet a performance claim.

Phase 146 measured that local change against the Phase 144 encode+append
baseline:

```text
phase146_wal_record_materialization_effectiveness_status=ok
candidate_max_h2c_bytes=65536
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
wal_encode_duration_ms=281
wal_append_duration_ms=280
phase146_baseline_pair_ms=592
phase146_current_pair_ms=561
phase146_pair_improvement_pct=5.24
phase146_effectiveness=visible
phase146_decision=keep_change
next_recommendation=phase147_wal_multiblock_record_design_gate
cleanup_status=ok
```

This keeps the Phase 145 allocation reduction, but it remains lab evidence and
not a throughput/SLO claim. The next backend gate should select a deeper WAL
design path, such as multi-block WAL records or vectored write-at.

Phase 147 selected that deeper path:

```text
phase147_wal_multiblock_record_design_status=ok
current_wal_format_unchanged=true
current_recovery_compatibility=pass
candidate_design=multi_block_record
candidate_reduces_record_count=true
candidate_reduces_write_calls=false
durability_invariant_documented=true
recovery_invariant_documented=true
phase147_decision=prototype_next
next_recommendation=phase148_wal_multiblock_record_local_prototype
cleanup_status=ok
```

The design gate keeps the current WAL format unchanged and documents the
required format-version, CRC, dirty-map, recovery, flusher, checkpoint, and
dirty-read invariants before any prototype writes a new record shape.

Phase 148 implemented that prototype locally behind a disabled-by-default test
gate:

```text
phase148_wal_multiblock_record_local_prototype_status=ok
default_wal_format_unchanged=true
feature_gate_default=false
multiblock_encode_decode=pass
multiblock_dirty_read=pass
multiblock_recovery_split=pass
multiblock_flusher_split=pass
single_block_compatibility=pass
current_recovery_compatibility=pass
phase148_decision=profile_next
next_recommendation=phase149_wal_multiblock_record_profile_gate
cleanup_status=ok
```

This is not wired to Kubernetes and not enabled by default.

Phase 149 profiled the local prototype:

```text
phase149_wal_multiblock_record_profile_status=ok
profile_scope=local_storage
single_block_wal_encode_ops=2048
multiblock_wal_encode_ops=128
single_block_wal_append_ops=128
multiblock_wal_append_ops=128
single_block_wal_writeat_calls=128
multiblock_wal_writeat_calls=128
record_count_reduction_visible=true
phase149_decision=wire_runtime_opt_in
next_recommendation=phase150_wal_multiblock_runtime_opt_in
cleanup_status=ok
```

This confirms the prototype reduces record/encode count in local storage, while
write-at call count is unchanged. A mounted NVMe/TCP profile is still required
before any user-facing performance statement.

Phase 150 wires the prototype as an explicit default-off runtime opt-in:

```text
phase150_wal_multiblock_runtime_opt_in_status=ok
runtime_opt_in_name=durable-wal-multiblock-records
runtime_opt_in_default=false
explicit_opt_in_reaches_walstore=true
helm_default_omits_opt_in=true
helm_explicit_renders_opt_in=true
phase150_decision=mounted_profile_next
next_recommendation=phase151_wal_multiblock_mounted_nvme_profile
cleanup_status=ok
```

The opt-in is not release-supported until the mounted NVMe/TCP profile passes.

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

Phase 127 means the target has a concrete ANA-change async event source when an
ANA provider is wired. By itself it was not yet a live Linux host notification
or Kubernetes dynamic reconnect claim.

Phase 128 closes the live Linux host notification half of that gap:

```text
host_aer_observed=true
host_aer_result=0x000c0302
host_aer_event_type=notice
host_aer_event_info=ana_change
host_aer_log_page=ana
oaes_ana_change_notice_advertised=true
ana_log_change_count_before=4294967297
ana_log_change_count_after=8589934593
ana_log_change_count_advanced=true
host_path_state_refreshed=true
mounted_io_after_notice=ok
cleanup_status=ok
```

This proves the standalone Linux NVMe/TCP initiator sees the ANA Change Notice
through the kernel `nvme_async_event` tracepoint during r1->r2 failover. It is
still not a Kubernetes dynamic reconnect/restage claim.

Phase 129 closes the mounted restage primitive, not the automatic trigger:

```text
mounted_nodestage_reconnects_missing_path=true
mounted_nodestage_rejects_nqn_mismatch=true
mounted_nodestage_does_not_remount=true
restage_owner=node_stage
host_mutation_scope=nvme_connect_missing_paths_only
automatic_k8s_reconnect_claim=false
automatic_trigger_required_next=true
cleanup_status=ok
```

Phase 130 closes the product owner/trigger contract, still without claiming the
full live Kubernetes failover close gate:

```text
scope=csi_node_owner_trigger_contract
live_k8s_failover_claim=false
desired_path_set_changed=true
reconnect_owner=csi-node
reconnect_invoked=true
replacement_path_connected=true
owner_loop_invokes_reconnect=true
default_enabled=false
host_mutation_scope=nvme_connect_missing_paths_only
stale_path_disconnect_claim=false-with-reason=no_stale_path_disconnect_primitive
live_k8s_gate_required_next=true
cleanup_status=ok
```

Phase 131 proves the same owner in a live mounted Kubernetes PVC path with
scoped host path loss:

```text
phase131_nvme_k8s_reconnect_live_status=ok
stage2_multipath_enabled=true
initial_path_count=2
path_loss_detected=true
after_disconnect_path_count=1
reconnect_owner=csi-node
reconnect_invoked=true
replacement_path_connected=true
reconnected_path_count=2
pod_uid_preserved=true
mounted_io_after_reconnect=ok
crd_status_agrees=true
report_dashboard_agree=true
cleanup_status=ok
```

Phase 131's injected loss is host-local and scoped:
`nvme disconnect -d <controller>`. Phase 132 then proved the changed desired
path-set half of the same loop:

```text
phase132_nvme_k8s_desired_path_change_status=ok
initial_path_count=2
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

Phase 133 closes the stale host-path gap left by Phase 132:

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
pod_uid_preserved=true
mounted_io_after_reconnect=ok
crd_status_agrees=true
report_dashboard_agree=true
cleanup_status=ok
```

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

- Kubernetes RoCE or NVMe/RDMA publish/attach. Phases 163-164 prove only an
  explicit, source-gated and lifecycle-hardened standalone Linux path; it is
  not yet part of CSI publish context, NodeStage/NodeUnstage, multipath, or
  failover.
- Performance, throughput, latency, or production SLO.
- Broad Linux distro, kernel, initiator, or cloud compatibility.
- Production HA, node-loss survival, or arbitrary unbounded path churn.
- Broad Linux host AER/ANA notification compatibility beyond the m02 supported
  lab gate.
- Published-image Kubernetes dynamic NVMe reconnect after control-plane desired
  path-set replacement/failover until matching release images pass a release
  smoke. The current evidence is source-gated.
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
