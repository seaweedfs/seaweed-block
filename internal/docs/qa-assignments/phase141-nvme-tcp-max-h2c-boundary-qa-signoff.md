# Phase 141 QA Sign-Off: NVMe/TCP MaxH2C Boundary

Status: **PASS**.

Validated source tree: local `phase141-nvme-tcp-max-h2c-boundary` working tree,
synced to `/tmp/seaweed_block` on m02 with the Phase 141 overlay.

Run:

```text
C:\work\swblock.exe run testops/scenarios/nvme-tcp-max-h2c-boundary-chain.yaml `
  -output results\phase141-h2c-run1.json `
  -html results\phase141-h2c-run1.html
```

Bundle:

```text
results\20260706-143800-c0ea
28 actions: 28 passed, 0 failed
```

## Gate Result

```text
phase141_nvme_tcp_max_h2c_boundary_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
baseline_max_h2c_bytes=32768
candidate_max_h2c_bytes=65536
h2c_contract_tests=pass
icresp_max_h2c_matches_candidate=true
identify_ioccsz_matches_candidate=true
helm_candidate_max_h2c_data_length=65536
host_connects_candidate=true
writer_verified=true
reader_verified=true
seq_write_mibps=208.21
seq_read_mibps=489.95
target_write_observed=true
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
request_size_increase_observed=true
phase141_decision=add_opt_in
next_recommendation=phase142_nvme_tcp_large_h2c_retriage
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

The 64KiB candidate is connected through the product path:

```text
Helm values nvme.maxH2CDataLength=65536
-> blockmaster --launcher-nvme-max-h2c-data-length=65536
-> generated blockvolume --nvme-max-h2c-data-length=65536
-> NVMe/TCP ICResp MaxH2CDataLength and Identify IOCCSZ/MDTS
-> Linux host connects and mounted writer/reader succeeds
-> /status/durable observes 65536-byte target/backend requests
```

Default behavior remains 32KiB; the new value is explicit opt-in only. The
gate does not make a performance/SLO, RoCE, or NVMe/RDMA claim.

## Follow-Up

Phase 142 should retriage the write path under the 64KiB opt-in shape, compare
the next backend/WAL bottleneck, and decide whether to document the option only,
run broader compatibility gates, or consider a later default change.
