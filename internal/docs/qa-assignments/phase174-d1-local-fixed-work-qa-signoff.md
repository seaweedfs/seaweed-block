# Phase 174 D1 Local Fixed-Work QA Sign-Off

Verdict: **HOLD** at `ed270f3` on the dedicated m02 NVMe filesystem.

## Contract And Correctness

The gate completed all 90 fixed-work rows: direct WALStore and durable-adapter
RF1 used `local_durable`; RF3 used `sync_quorum_rf3` over real localhost TCP
and remained diagnostic-only. Every row reconciled 16,384 writes and 64 MiB,
reported `flusher_phase_reset=true`, recovered the primary frontier, and
passed sampled byte checks. Store residue was zero.

The final run corrected two harness confounders without changing product code,
work, ACK semantics, or the `1.25x` threshold:

- GC/settling happens before the flusher start handshake, so it cannot consume
  the first measurement interval;
- set 2 reverses layer and writer order, exposing device/time-order bias.

## Terminal Evidence

```text
commit=ed270f3
artifact=20260801T093246Z-phase174-d1-local-reversed
store_source=/dev/nvme0n1p1
store_filesystem=ext4
control_cpuset=0,2,4,6
control_gomaxprocs=4
second_set_order=reversed
direct_walstore_writers_4_set1_max_min_ratio=1.159
direct_walstore_writers_4_set2_max_min_ratio=1.054
adapter_rf1_writers_4_set1_max_min_ratio=1.227
adapter_rf1_writers_4_set2_max_min_ratio=1.347
direct_walstore_writers_4_max_min_ratio=1.159
adapter_rf1_writers_4_max_min_ratio=1.443
rf1_direct_adapter_four_writer_ratio=0.912
rf1_local_stability_gate=hold
rf3_same_host_healthy_baseline=false
rf3_queue_saturation_row_count=10
rf3_same_host_admission_eligible=false
architecture_candidate_selected=false
product_mutation_present=false
phase174_fixed_work_pipeline_status=hold
store_residue_count=0
```

Direct WALStore is stable in both independent sets. Durable adapter RF1 is not:
set 2 ranges from 284.094 to 382.759 MiB/s at four writers. This remains above
the unchanged `1.25x` limit after the direct control is stable, so it is a real
adapter-or-above attribution target rather than permission to tune the gate.

## Decision

D1 does not advance to a performance candidate. Phase 174 may continue into
D2 attribution because that adds evidence rather than product behavior. No D4
or D5 implementation is allowed unless D2/D3 later establish a stable,
semantically comparable cause and candidate.

Artifact:

```text
/mnt/smb/work/share/g15d-k8s/20260801T093246Z-phase174-d1-local-reversed.tar.gz
sha256=d9a96068a693adeb852787678283981a4f7f0b99bd450a927186cca3ddadabc4
```

m02 k3s remained inactive, matching its pre-run state.
