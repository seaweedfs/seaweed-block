# Phase 140 Finished Plan: Frontend Request Size Profile

Status: **closed 2026-07-06, live gate PASS**.

## Problem

Phase 139 showed that WAL append/write-at calls were small because the durable
backend received 32KiB frontend requests. The remaining question was whether
that 32KiB shape came from the Linux host initiator, an explicit target limit,
or a backend limit.

## Work

Phase 140 added a read-only target-side request-size profile:

- `/status/durable` now exposes `TargetWriteRequestMaxBytes`;
- Phase 120 and Phase 126 summaries propagate target request max/average bytes;
- the Phase 140 gate extracts the NVMe/TCP target's advertised
  `MaxH2CDataLength` and Identify `IOCCSZ` from the product source, then
  compares those limits to live target/backend request counters.

No protocol behavior, WAL recovery semantics, CSI behavior, reconnect behavior,
or performance claim changed.

## Gate

Phase 140 added:

- `scripts/run-phase140-frontend-request-size-profile-gate.sh`
- `testops/scenarios/nvme-tcp-frontend-request-size-profile-chain.yaml`

The gate reruns the 512MiB mounted NVMe/TCP profile, verifies the target write
path is observed, verifies backend batching remains active, extracts the target
NVMe/TCP H2C limit, and classifies the request-size owner.

## Evidence

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

Run bundle:

```text
results\20260706-140706-9f75
28 actions: 28 passed, 0 failed
```

## Conclusion

The 32KiB write shape is a product target limit: the NVMe/TCP target advertises
`MaxH2CDataLength=32768`, and the live target/backend request counters match
that value. The next useful phase is not WAL coalescing or RDMA; it is a
bounded `MaxH2CDataLength` boundary gate to decide whether a larger H2C request
size is safe, compatible with Linux hosts, and worth making opt-in or default.
