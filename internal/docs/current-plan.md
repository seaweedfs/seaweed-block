# Current Plan: Phase 141 NVMe/TCP MaxH2C Boundary

Status: planning.

Phase 140 named the 32KiB frontend request-size owner:

```text
phase140_frontend_request_size_profile_status=ok
nvme_tcp_max_h2c_data_length_bytes=32768
nvme_tcp_ioccsz_units=2052
target_write_request_max_bytes=32768
target_write_request_avg_bytes=32720
backend_write_request_max_bytes=32768
backend_write_request_avg_bytes=32720
backend_full_block_batch_max=8
backend_storage_batching_effective=true
frontend_request_size_owner=target_limit
phase140_shape_result=target_limited
post_phase140_bottleneck=frontend_request_size
next_recommendation=phase141_nvme_tcp_max_h2c_boundary
cleanup_status=ok
```

The bottleneck thread has moved from WAL internals to the NVMe/TCP target's
advertised `MaxH2CDataLength`. The next phase must not blindly increase the
default. It should prove the protocol boundary, Linux host compatibility, live
I/O correctness, and request-size movement before deciding whether the limit
should remain 32KiB, become opt-in configurable, or become a new default.

## Goal

```text
baseline target limit is explicit
-> candidate MaxH2CDataLength is rendered consistently in ICResp and Identify
-> Linux host connects and mounted writer/reader still passes
-> live target/backend request counters show whether request size changed
-> cleanup remains clean
-> decision is named: keep 32KiB, add opt-in, raise default, or block
```

## Required Evidence

```text
phase141_nvme_tcp_max_h2c_boundary_status=ok
frontend_transport=tcp
roce_claim_allowed=false
nvme_rdma_claim_allowed=false
performance_slo_claim_allowed=false
baseline_max_h2c_bytes=32768
candidate_max_h2c_bytes=<bytes>
icresp_max_h2c_matches_candidate=true
identify_ioccsz_matches_candidate=true
host_connects_candidate=<true|false>
writer_verified=<true|false>
reader_verified=<true|false>
target_write_observed=true
target_write_request_max_bytes=<bytes>
backend_write_request_max_bytes=<bytes>
request_size_increase_observed=<true|false>
phase141_decision=<keep_32k|add_opt_in|raise_default|blocked>
next_recommendation=<specific next phase>
cleanup_status=ok
```

## Boundaries

- Do not claim performance/SLO, RoCE, NVMe/RDMA, GPU Direct, cuFile/cuObject,
  or NIXL.
- Do not change failover, reconnect, CSI publish, authority, or WAL recovery
  semantics.
- Do not raise the default unless the live gate proves host connect,
  writer/reader data, request-size movement, and clean teardown.
- Prefer explicit opt-in if compatibility or broader host behavior is still
  unknown.
- Keep evidence source-gated; matching release images are a separate release
  artifact question.

## Candidate Work

1. Introduce a minimal target-side limit seam for `MaxH2CDataLength` if needed.
2. Keep the current default at 32KiB unless the gate proves enough evidence to
   change it.
3. Ensure ICResp `MaxH2CDataLength` and Identify `IOCCSZ` derive from the same
   candidate value.
4. Rerun mounted NVMe/TCP writer/reader with a candidate larger request size.
5. Compare Phase 140 and candidate request-size counters.
6. Close with a concrete product decision and next phase.

## Exit Criteria

Phase 141 can close when the live supported-lab gate proves whether a larger
NVMe/TCP `MaxH2CDataLength` is safe and useful enough for opt-in/default
consideration, or blocks the change with a concrete compatibility/product
reason.
