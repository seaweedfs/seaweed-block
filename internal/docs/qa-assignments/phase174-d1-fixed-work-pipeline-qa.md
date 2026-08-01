# Phase 174 D1 Fixed-Work Pipeline QA

Source: use the commit that adds this assignment,
`core/replication/phase174_fixed_work_test.go`, and
`scripts/run-phase174-fixed-work-pipeline-gate.sh`.

## Purpose

Establish one fixed logical workload above WALStore before selecting another
performance implementation. The local gate compares only the two layers that
share the same `local_durable` ACK profile:

- direct WALStore;
- RF1 durable adapter over that WALStore.

It also runs current RF3 `sync_quorum` through real localhost TCP into two
durable WALStore replicas. That path is diagnostic, not admission: three
durable stores share one host/device and `sync_quorum` is allowed to return
after one peer ACK. Throughput ratios across `local_durable` and
`sync_quorum_rf3` are forbidden.

## Run

```bash
cd /path/to/seaweed_block
export SW_BLOCK_ARTIFACT_DIR=/path/to/results/phase174-d1-local
export SW_BLOCK_PHASE174_STORE_DIR=/path/on/dedicated/nvme/phase174-d1-stores
export SW_BLOCK_PHASE174_CPUSET=0,2,4,6
bash scripts/run-phase174-fixed-work-pipeline-gate.sh "$PWD"
```

The store directory must be on a dedicated local filesystem, not `/`, tmpfs,
overlay, or a network share.

## Fixed Contract

- 16,384 measured 4 KiB full-block writes per run;
- deterministic unique scattered LBAs in a 32K-block region;
- 1,024 warmup writes in a disjoint region;
- 1, 4, and 8 writers;
- one final Sync, close, reopen, Recover, frontier, and byte checks;
- two independent sets of five measured runs;
- exact primary WAL and replication operation counts;
- explicit `local_durable` versus `sync_quorum_rf3` ACK profiles.

For RF3, each sample uses new stores. A lagging non-quorum replica from one
sample is never reused as though it were healthy in the next sample.

## Required Evidence

- `phase174_fixed_work_pipeline_status=ok`
- `contract=phase174-fixed-work-v1`
- `cross_ack_profile_throughput_ratio_allowed=false`
- all 90 measured rows reconcile 16,384 writes and 64 MiB logical data
- recovered primary stable/head frontiers agree
- RF3 has exactly two configured replicas and at least one durable replica
- direct and adapter four-writer combined max/min ranges are at most `1.25x`
- `rf1_local_stability_gate=pass`
- `rf3_same_host_admission_eligible=false`
- `rf3_distinct_node_gate_required=true`
- `d1_close_allowed=false`
- `architecture_candidate_selected=false`
- `product_mutation_present=false`
- `store_residue_count=0`

Record, do not suppress, `rf3_queue_saturation_row_count` and
`rf3_same_host_healthy_baseline`. A saturated or lagging same-host replica is
evidence for the next distinct-node gate, not permission to call a degraded
RF3 run healthy.

## Verdict

- `PASS`: the local RF1 comparable contract is stable and all RF3 diagnostic
  facts are honest. D1 advances but does not close.
- `HOLD`: direct or adapter four-writer range exceeds `1.25x`; do not tune the
  threshold after results are visible.
- `FAIL`: counters, durability, recovery, byte correctness, ACK profile, or
  cleanup do not reconcile.

D1 closes only after a separate distinct-node RF3 and frontend/mounted gate
uses matching logical work and independent media.
