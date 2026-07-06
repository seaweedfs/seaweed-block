# Phase 151 QA Sign-off: WAL Multi-Block Mounted NVMe Profile

Status: **PASS**.

Branch: `phase151-wal-multiblock-mounted-nvme-profile`.

## Scope

Phase 151 profiles the Phase 150 runtime opt-in on the mounted NVMe/TCP path:

```text
blockmaster.durableWALMultiBlockRecords=true
```

This remains explicit opt-in evidence. It does not enable the feature by
default and does not claim RoCE, NVMe/RDMA, production performance, or an SLO.

## Checks

Local checks:

```text
bash -n scripts/run-phase120-nvme-tcp-performance-baseline-gate.sh \
  scripts/run-phase151-wal-multiblock-mounted-nvme-profile-gate.sh
go test ./core/storage ./core/frontend/durable ./cmd/blockvolume ./cmd/blockmaster -count=1
helm template sw-block charts/seaweed-block --namespace kube-system
helm template sw-block charts/seaweed-block --namespace kube-system \
  --set blockmaster.durableWALMultiBlockRecords=true
C:\work\swblock.exe validate testops/scenarios/nvme-tcp-wal-multiblock-mounted-profile-chain.yaml
git diff --check -- scripts/run-phase120-nvme-tcp-performance-baseline-gate.sh \
  scripts/run-phase151-wal-multiblock-mounted-nvme-profile-gate.sh \
  testops/scenarios/nvme-tcp-wal-multiblock-mounted-profile-chain.yaml
```

Live gate:

```text
C:\work\swblock.exe run testops/scenarios/nvme-tcp-wal-multiblock-mounted-profile-chain.yaml
```

Result:

```text
=== nvme-tcp-wal-multiblock-mounted-profile-chain === PASS (4m3.603s)
36 actions: 36 passed, 0 failed
run bundle: results\20260706-162114-11ce
```

## Evidence

```text
phase151_wal_multiblock_mounted_nvme_profile_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
default_wal_format_unchanged=true
feature_gate_default=false
runtime_opt_in_name=durable-wal-multiblock-records
runtime_opt_in_enabled=true
candidate_max_h2c_bytes=65536
unit_record_compatibility=pass
helm_default_omits_opt_in=true
helm_explicit_renders_opt_in=true
mounted_helm_extra_values=true
mounted_helm_renders_opt_in=true
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
wal_encode_ops=9002
wal_append_ops=9002
backend_storage_write_calls=9002
backend_storage_write_blocks=143570
backend_storage_batch_calls=8982
backend_full_block_batch_blocks=143550
multiblock_record_shape_observed=true
seq_write_mibps=204.96
seq_read_mibps=519.80
writer_verified=true
reader_verified=true
cleanup_status=ok
phase151_decision=keep_opt_in
next_recommendation=phase152_wal_multiblock_recovery_compatibility_gate
```

The important runtime-shape check is:

```text
wal_encode_ops == backend_storage_write_calls
wal_encode_ops < backend_storage_write_blocks
```

This proves the mounted path is no longer materializing one WAL entry per
written block under the opt-in. It is stronger than a Helm-render-only check.

Final cleanup:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

## Notes

- `tp01` remains NotReady in the lab, but this gate only requires the m01/m02
  NVMe/TCP path.
- Throughput was recorded as diagnostic evidence only. It is not a performance
  claim.
- The next gate should prove recovery/restart compatibility with actual
  multi-block WAL records before considering any broader release claim.
