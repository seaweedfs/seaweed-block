# Phase 169 D1 Segment Format And Recovery QA

Validate the internal segmented WAL codec and committed-prefix recovery proof.
This is a local engine gate: it does not install Kubernetes resources, build
images, or select the candidate in the product.

## Source

Use the exact Phase 169 D1 commit on branch
`phase169-segmented-wal-group-commit`.

## Run

On a Linux host with Go and CGO enabled:

```bash
cd /tmp/seaweed_block
bash scripts/run-phase169-segment-format-gate.sh /tmp/seaweed_block
cat results/phase169-segment-format-gate/phase169-segment-format-summary.txt
```

If the shared source directory is stale, sync an exact clean archive of the D1
commit first. Do not run against uncommitted files or a moving shared tree.

## Required Evidence

The gate must exit zero and the summary must contain:

```text
clean_encode_decode=pass
invalid_geometry_and_order=pass
corruption_and_bounds=pass
uncommitted_tail_rule=pass
committed_corruption_fail_closed=pass
cross_segment_sequence_and_lsn=pass
trusted_manifest_anchors=pass
same_lba_recovery_order=pass
frozen_format_vector=pass
storage_regression=pass
parallelwal_race=pass
windows_compile=pass
product_selector_added=false
segment_version=1
segment_max_entries=256
segment_max_payload_bytes=1048576
committed_corruption_policy=fail_closed
uncommitted_tail_policy=ignore_after_trusted_boundary
phase169_segment_format_status=ok
```

Inspect the individual logs rather than relying only on the summary. Confirm
that each line follows a real test/build command and that the corruption tests
mutate encoded bytes before resealing only the outer checksum needed to reach
the intended inner validation.

## Boundary

- Same-LBA writes in one segment are valid and retain LSN order.
- Duplicate or non-contiguous LSNs are invalid.
- The decoder must reject size/count/payload bounds before allocating from
  untrusted fields.
- A malformed committed segment fails closed.
- A physical torn tail is ignored only when it is beyond the trusted committed
  byte boundary.
- The trusted manifest anchors the first segment sequence, first LSN, final
  durable LSN, and exact segment count.
- Recovery streams bounded segments and later same-LBA writes win only after
  the full manifest validates.
- No product selector or existing Store format changes are allowed in D1.

Write the result to
`internal/docs/qa-assignments/phase169-d1-segment-format-recovery-qa-signoff.md`.
