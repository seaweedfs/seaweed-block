# Phase 174 D2 RF1 Boundary Attribution QA

Run the Phase 174 fixed-work pipeline gate from the assignment commit. D2
extends the D1 matrix with existing WALStore, flusher, and durable-adapter
counter deltas; it does not add hot-path instrumentation or change product
behavior.

## Purpose

Explain the unstable `adapter_rf1` four-writer baseline before any architecture
candidate is allowed. The same 16,384-operation, 64 MiB, `local_durable` work
must reconcile through:

- durable adapter requests, successful writes, bytes, storage calls, and
  blocks;
- WAL encode, append, dirty-map, and global commit-lock wait;
- foreground flusher cycles, extent writes, and extent syncs;
- merged CPU, block, and mutex profiles from five direct and five adapter
  diagnostic repeats.

Durations are accumulated concurrent work, not wall time. The gate reports
per-operation medians and correlation with foreground wall time; it must not
sum overlapping measurements into a throughput claim.

## Run

```bash
export SW_BLOCK_ARTIFACT_DIR=/path/to/results/phase174-d2-rf1-attribution
export SW_BLOCK_PHASE174_STORE_DIR=/path/on/dedicated/nvme/phase174-d2-stores
export SW_BLOCK_PHASE174_CPUSET=0,2,4,6
bash scripts/run-phase174-fixed-work-pipeline-gate.sh /path/to/seaweed_block
```

## Required Evidence

- all 90 D1 rows retain their ACK, recovery, correctness, and cleanup checks;
- every adapter four-writer row has exactly 16,384 requests, writes, storage
  calls, and blocks, plus 64 MiB at both adapter byte counters;
- `rf1_attribution_counter_reconciliation=true`;
- `rf1_attribution_status=ok` even if the unchanged D1 stability verdict is
  `hold`;
- per-operation medians and foreground correlations exist for adapter total,
  accounted storage, commit wait, encode, append, dirty map, unattributed
  adapter work, and foreground flusher work;
- merged CPU, block, and mutex tops exist for direct and adapter;
- `architecture_candidate_selected=false` and no product mutation is present;
- store residue is zero.

## Verdict

- `PASS`: attribution counters reconcile and identify the measured boundary;
  this does not override a D1 HOLD or authorize implementation.
- `FAIL`: any operation/byte count, profile, correctness, or cleanup evidence
  is missing.

D2 remains open for RF3 distinct-node and frontend attribution after this RF1
slice.
