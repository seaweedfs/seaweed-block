# Phase 174 D2 RF1 Boundary Attribution QA Sign-Off

Verdict: **PASS for the RF1 attribution slice** at `4034f37`. D1 remains
**HOLD**, and no implementation candidate is authorized.

## Counter Closure

The exact D1 matrix completed 90 rows on m02's dedicated NVMe filesystem. All
ten adapter four-writer rows reconciled 16,384 requests, successful writes,
storage calls, and storage blocks plus 64 MiB at both adapter byte boundaries.

```text
rf1_attribution_counter_reconciliation=true
rf1_attribution_status=ok
direct_walstore_writers_4_max_min_ratio=1.211
adapter_rf1_writers_4_max_min_ratio=1.344
rf1_local_stability_gate=hold
architecture_candidate_selected=false
product_mutation_present=false
```

## Attribution

Four-writer adapter medians and their correlation with foreground wall time:

```text
boundary                         ns/op       correlation
adapter envelope                42,912.380  0.994
storage accounted               16,167.086  0.165
global commit-lock wait         13,154.722  0.192
WAL encode                       1,329.172  0.042
WAL append                       1,324.057  0.021
dirty map                          221.377 -0.153
adapter envelope unaccounted    27,561.795  0.960
foreground flusher               7,887.893  0.952
```

The envelope-unaccounted value is not asserted to be adapter CPU. It includes
off-CPU scheduling and storage work not represented by the non-overlapping
phase counters. Merged profiles from five runs per layer show:

- adapter CPU has no new dominant function; `StorageBackend.Write` is 16.57%
  cumulative and immediately enters `writeBytes`/`WALStore.Write`;
- block delay attributed to `StorageBackend.writeBytes` occurs at the
  `storage.Write` call, not its 256-stripe LBA lock;
- direct and adapter foreground time correlate with flusher cycle time at
  `0.825` and `0.952`, respectively.

The short 64 MiB sample sits near the periodic flusher boundary. Background
WAL-to-extent work therefore changes scheduler/device overlap and affects the
adapter envelope, while the accounted commit/encode/append phases do not
explain the variance. Phase 173 already rejected flusher controls that did not
produce a stable `1.30x` gain. This evidence does not reopen that backend
decision or justify a new product change.

## Decision

RF1 D2 attribution is complete. Continue only with diagnostic distinct-node
RF3 and frontend/mounted attribution. D4/D5 remain ineligible while D1 is HOLD
and D3 has no stable candidate.

Artifact:

```text
/mnt/smb/work/share/g15d-k8s/20260801T094609Z-phase174-d2-rf1-attribution.tar.gz
sha256=97ed6f8584822eac7a023a5203704add12349b56a1a5fbc744f86da9e8fbad2d
store_residue_count=0
```

m02 k3s remained inactive, matching its pre-run state.
