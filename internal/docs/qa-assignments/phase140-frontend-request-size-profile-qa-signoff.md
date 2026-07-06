# Phase 140 QA Sign-Off: Frontend Request Size Profile

Status: **PASS**.

Validated source tree: local `phase140-frontend-request-size-profile` working
tree, synced to `/tmp/seaweed_block` on m02 with the Phase 140 overlay.

Run:

```text
C:\work\swblock.exe run testops/scenarios/nvme-tcp-frontend-request-size-profile-chain.yaml `
  -output results\phase140-request-size-run1.json `
  -html results\phase140-request-size-run1.html
```

Bundle:

```text
results\20260706-140706-9f75
28 actions: 28 passed, 0 failed
```

## Gate Result

```text
phase140_frontend_request_size_profile_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
unit_target_request_profile_passed=true
nvme_tcp_max_h2c_data_length_bytes=32768
nvme_tcp_ioccsz_units=2052
target_write_observed=true
target_write_request_max_bytes=32768
target_write_request_avg_bytes=32720
backend_write_request_max_bytes=32768
backend_write_request_avg_bytes=32720
backend_full_block_batch_max=8
backend_full_block_batch_avg=7
backend_storage_batching_effective=true
wal_append_writeat_max_bytes=33072
wal_append_writeat_avg_bytes=33010
frontend_request_size_owner=target_limit
phase140_shape_result=target_limited
post_phase140_bottleneck=frontend_request_size
next_recommendation=phase141_nvme_tcp_max_h2c_boundary
cleanup_status=ok
```

Final cleanup spot-check:

```text
cleanup_status=ok
k8s_residue_count=0
iscsi_residue_count=0
process_residue_count=0
multipath_residue_count=0
hostpath_residue_count=0
failure_count=0
```

## Interpretation

Phase 139 proved the WAL append small-write shape is caused by upstream
32KiB backend requests. Phase 140 names the upstream owner: the NVMe/TCP target
itself advertises `MaxH2CDataLength=32768` and Identify `IOCCSZ=2052`, and the
live target/backend counters match that exact 32KiB request size.

This is a target-limit finding, not a host, WAL, or backend coalescing finding.
No performance, RoCE, NVMe/RDMA, or SLO claim is made.

## Follow-Up

Phase 141 should test the `MaxH2CDataLength` boundary explicitly before any
default changes: verify ICResp/Identify consistency, Linux host compatibility,
writer/reader correctness, request-size movement, and cleanup. If larger H2C
requests are safe and useful, prefer an explicit opt-in before changing the
default.
