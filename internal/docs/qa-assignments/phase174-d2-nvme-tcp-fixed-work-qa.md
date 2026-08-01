# Phase 174 D2 NVMe/TCP Fixed-Work QA

## Purpose

Measure the real NVMe/TCP target over the same logical-work contract used by
the Phase 174 engine and durable-adapter slices. This is an RF1 local-durable
attribution gate, not a mounted or RF3 performance claim.

## Source

Run the gate from the exact assigned commit. Do not use a dirty shared tree.
The test binary must be built with `swblock_testtools`; no shipped binary gains
test-only reset or benchmark behavior.

## Environment

- Linux host: m02.
- Store root: a dedicated NVMe filesystem, normally `/data/nvme/block`.
- CPU control: `GOMAXPROCS=4`, `taskset -c 0,2,4,6` unless the host topology
  requires an explicitly documented replacement.
- Transport: real loopback NVMe/TCP admin and IO queues. Loopback removes the
  external network from this attribution slice; it does not make the result a
  mounted or 100 GbE claim.

## Command

```bash
SW_BLOCK_ARTIFACT_DIR=/tmp/<run-id> \
SW_BLOCK_PHASE174_NVME_STORE_DIR=/data/nvme/block/<run-id>-stores \
  bash scripts/run-phase174-nvme-tcp-fixed-work-gate.sh "$PWD"
```

## Required Checks

1. The fixed work is exactly 16,384 API writes of 4 KiB, with 1, 4, and 8
   independent NVMe IO queues.
2. Each writer shape has two independently ordered sets of five measured
   runs, after one precondition run per set and shape.
3. Every measured row reports exactly 16,384 NVMe Write commands, R2T writes,
   H2C PDUs, target writes, adapter requests/writes, storage calls/blocks, and
   WAL writes.
4. Every byte counter reports exactly 67,108,864 bytes.
5. The accumulated client request latency reconciles as target backend-call
   time plus NVMe/TCP round-trip non-backend time. The latter includes test
   initiator, loopback TCP, R2T/H2C receive, dispatch, and completion; do not
   relabel it as a pure target CPU measurement.
6. Final NVMe Flush succeeds, close/reopen/recovery reaches `R == H`, and at
   least five measured LBAs retain the expected bytes.
7. Capture merged CPU, block, and mutex profiles for five four-queue diagnostic
   runs.
8. Record the four-writer admission stability and all-shapes stability
   separately. A range above `1.25x` is HOLD evidence, not a reason to delete
   a slow run or select an architecture candidate. Correlate four/eight-writer
   foreground time with the existing WAL flusher counters.
9. All test stores and the test binary are removed after the gate.

## Mounted Boundary

The existing mounted fio/dd gates do not issue the same API/LBA/queue shape.
This gate must keep:

```text
mounted_shape_comparable=false
mounted_throughput_ratio_allowed=false
```

Mounted evidence remains a separate product diagnostic until a kernel
initiator workload can prove the same logical operation contract. Do not
divide this gate's throughput by historical mounted numbers.

## Verdict

- PASS: all operation, byte, recovery, and cleanup checks reconcile. Stability
  may be recorded as PASS or HOLD independently.
- FAIL: protocol/status error, counter mismatch, failed Flush/recovery/data
  check, or store residue.
- BLOCKED: the dedicated filesystem, CPU controls, or required Linux tooling
  is unavailable.

No result from this gate selects or implements an architecture change by
itself.
