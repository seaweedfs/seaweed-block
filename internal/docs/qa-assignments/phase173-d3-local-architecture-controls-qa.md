# Phase 173 D3 Local Architecture Controls QA

Source: use the commit that adds this assignment and
`scripts/run-phase173-architecture-controls-gate.sh`.

## Purpose

Run test-only controls that distinguish writer ownership/lock effects,
foreground-versus-writeback interference, flusher-only capacity, and same-file
versus split-file I/O on one device. Also record fixed-iteration RF1 durable
adapter and RF3 real-TCP acknowledgement costs.

This is not a product candidate or performance promotion. The deferred
writeback control eventually performs a real Sync and complete real flusher
drain, but its foreground rate is a non-product ceiling because sustained
checkpoint/recycle is excluded from that timed window. The split-file control
has no WALStore format or recovery contract.

## Run

```bash
cd /path/to/seaweed_block
export SW_BLOCK_ARTIFACT_DIR=/path/to/results/phase173-d3-local
export SW_BLOCK_PHASE173_STORE_DIR=/path/on/evaluated/filesystem/phase173-d3-stores
export SW_BLOCK_PHASE173_CONTROL_CPUSET=0,2,4,6
bash scripts/run-phase173-architecture-controls-gate.sh "$PWD"
```

The store directory must resolve to the dedicated local block device. Do not
run the formal gate on a network share, tmpfs, overlay, or OS-root filesystem.
The CPU set must contain at least four comparable physical cores. Do not mix
performance and efficiency cores or sibling SMT threads; the gate records the
set and constrains both process affinity and `GOMAXPROCS`.
Each sample runs in a fresh process after `sync` and a 250 ms settle interval,
matching the Phase 173 D1 baseline's sample-isolation policy.

## Fixed Controls

The WALStore controls use 14,500 logical 4 KiB blocks (56.64 MiB). The scratch
comparison repeats the same 14,500-block layout three times per run so a single
filesystem sync does not dominate its variance. All controls use five runs,
persistent preconditioned files, and a predeclared max/min limit of `1.25x`:

- shipped 4-writer foreground with the normal 100 ms flusher;
- 1- and 4-writer foreground while writeback is deferred;
- real flusher drain of the 4-writer prefilled dirty set with no writers;
- shared-file and same-device split-file scratch with identical 87,000 preads,
  43,501 pwrites, two syncs, decode/CRC, and correctness samples.

The fixed work stays below the default 64 MiB WAL's 90% hard watermark while
running long enough on the admission host to cross normal flusher periods.

## Required Evidence

- `phase173_architecture_controls_status=ok`
- the expected `control_cpuset` and `control_gomaxprocs`
- `local_control_stability_gate=pass`
- all six `*_max_min_ratio` values at most `1.25`
- `rf1_rf3_component_attribution=complete`
- RF3 queue saturation zero for one and four writers
- `architecture_candidate_selected=false`
- `product_mutation_present=false`
- `deferred_foreground_product_claim_allowed=false`
- `split_file_scratch_product_claim_allowed=false`
- `mounted_nvme_tcp_control=pending_same_session_live_gate`
- `d3_close_allowed=false`
- `store_residue_count=0`

The local direction uses predeclared `1.30x` signals. It remains provisional
until the same-session mounted NVMe/TCP control runs:

- owner/queue signal: one deferred writer is at least `1.30x` four deferred
  writers;
- media signal: split-file scratch is at least `1.30x` shared-file scratch;
- writeback signal: four deferred writers are at least `1.30x` shipped
  concurrent writers.

## Verdict

- `PASS`: the local control gate and RF1/RF3 component diagnostics pass. This
  advances D3 but does not close it.
- `FAIL`: correctness, counter, queue, or residue evidence fails.
- `HOLD`: any control range exceeds `1.25x` or the dedicated-device condition
  is unavailable. Fix the control/lab before selecting a candidate.
