# Phase 141 Finished Plan: NVMe/TCP MaxH2C Boundary

Status: **closed 2026-07-06, live gate PASS**.

## Problem

Phase 140 proved the 32KiB write request shape was owned by the NVMe/TCP
target's advertised `MaxH2CDataLength`. The open question was whether a larger
H2C limit could be safely wired through the product path and observed by a real
Linux host, without changing defaults or making a performance claim.

## Work

Phase 141 added an explicit opt-in H2C limit seam:

- `nvme.TargetConfig.MaxH2CDataLength`;
- blockvolume `--nvme-max-h2c-data-length`;
- blockmaster launcher `--launcher-nvme-max-h2c-data-length`;
- Helm `nvme.maxH2CDataLength`;
- launcher propagation into generated blockvolume args;
- unit coverage that ICResp `MaxH2CDataLength`, Identify `IOCCSZ`, and Identify
  `MDTS` are derived from the same value.

The default remains 32KiB and the chart renders no new launcher flag on the
default path, avoiding image flag-skew.

## Gate

Phase 141 added:

- `scripts/run-phase141-nvme-tcp-max-h2c-boundary-gate.sh`
- `testops/scenarios/nvme-tcp-max-h2c-boundary-chain.yaml`

The gate runs a 64KiB candidate through the existing mounted NVMe/TCP profile,
then verifies host connect, mounted writer/reader, target/backend request-size
movement, and cleanup.

## Evidence

```text
phase141_nvme_tcp_max_h2c_boundary_status=ok
baseline_max_h2c_bytes=32768
candidate_max_h2c_bytes=65536
icresp_max_h2c_matches_candidate=true
identify_ioccsz_matches_candidate=true
host_connects_candidate=true
writer_verified=true
reader_verified=true
seq_write_mibps=208.21
seq_read_mibps=489.95
target_write_request_max_bytes=65536
backend_write_request_max_bytes=65536
request_size_increase_observed=true
phase141_decision=add_opt_in
next_recommendation=phase142_nvme_tcp_large_h2c_retriage
cleanup_status=ok
```

Run bundle:

```text
results\20260706-143800-c0ea
28 actions: 28 passed, 0 failed
```

## Conclusion

The larger H2C limit is safe enough for an explicit opt-in in the supported lab:
Linux connects, mounted I/O passes, and live request counters move from 32KiB
to 64KiB. It is not enough evidence to raise the default or publish a
performance/SLO claim. Phase 142 should retriage the write path under the
64KiB opt-in shape.
