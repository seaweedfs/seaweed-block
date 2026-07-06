# Phase 147 WAL Multi-Block Record Design Gate

Status: design gate, no on-disk format change in this phase.

## Why This Exists

The NVMe/TCP write path now has enough evidence to stop guessing:

- Phase 140 traced the old 32KiB write shape to the NVMe/TCP target
  `MaxH2CDataLength`.
- Phase 141 added a 64KiB H2C opt-in and proved Linux host compatibility in the
  lab.
- Phase 142 showed the remaining product-owned cost moved to WAL append/encode.
- Phase 143/144 showed append and encode are tied under the 64KiB shape.
- Phase 145 removed one safe per-record allocation seam.
- Phase 146 measured that local change as visible, but still not a public
  performance/SLO claim.

The current WAL path already coalesces many independently encoded records into
one adjacent `WriteAt` through `walWriter.appendBatch`. That means the next
material improvement is unlikely to come from another small append wrapper. The
remaining high-leverage target is the number of WAL records and per-record
encode/checksum work.

## Current WAL Format

`core/storage/wal_entry.go` defines the current record layout:

```text
prefix:
  LSN      uint64
  Reserved uint64
  Type     uint8
  Flags    uint8
  LBA      uint64
  Length   uint32
data:
  Length bytes for walEntryWrite / walEntryPadding
trailer:
  CRC32    uint32
  EntrySize uint32
```

Current fixed overhead:

```text
walEntryPrefixSize = 30
walEntryHeaderSize = 38
```

Current write semantics:

- `walEntryWrite` represents exactly one block write.
- `LSN` is the LSN for that one block.
- `LBA` is the block address for that one block.
- `Length` must equal `BlockSize` for normal writes.
- `Data` is copied into the encoded WAL record.
- CRC covers prefix + data and the trailer stores CRC + total entry size.

Current batch execution:

- `WALStore.WriteBatch(startLBA, blocks)` allocates consecutive LSNs.
- It creates one `walEntry` per block.
- `walWriter.appendBatch` validates each record, serializes each record, and
  appends adjacent encoded bytes into a pending buffer.
- The pending buffer is written with fewer `WriteAt` calls, but recovery still
  sees independent single-block records.

Current recovery and flush assumptions:

- `recoverWAL` scans one record at a time.
- `dirtyMap` records one WAL offset per LBA.
- `readFromWAL` reads one record and returns one block.
- `flusher.flushOnce` reads each dirty entry's header, checks the on-disk LSN
  matches the dirty-map LSN, reads one block, writes one extent slot, then
  advances checkpoint to the highest verified flushed LSN.

## Design Choice

Candidate selected by this gate:

```text
candidate_design=multi_block_record
candidate_reduces_record_count=true
candidate_reduces_write_calls=false
```

Rationale:

- A vectored `pwritev` path could reduce some temporary buffer copying, but the
  current append path already coalesces adjacent encoded records into one
  `WriteAt` per pending region.
- A vectored path does not reduce `wal_encode_ops`, per-record CRC/trailer work,
  recovery record count, or dirty-map entries.
- A multi-block WAL record can reduce record count and per-record encode/checksum
  overhead for the common full-block contiguous WriteBatch path.
- A multi-block record changes WAL format semantics and therefore must be
  versioned and gated separately.

## Proposed Multi-Block Record

Add a new WAL entry type in a future phase:

```text
walEntryWriteBatch = 0x04
```

Proposed encoded meaning:

```text
LSN      = first LSN in the batch
Reserved = block count
Type     = walEntryWriteBatch
Flags    = 0 initially
LBA      = first LBA in the batch
Length   = blockCount * blockSize
Data     = blockCount contiguous block payloads
CRC32    = checksum(prefix + full Data)
EntrySize = walEntryHeaderSize + Length
```

Per-block logical identity:

```text
block i:
  lsn = firstLSN + i
  lba = firstLBA + i
  data = Data[i*blockSize : (i+1)*blockSize]
```

This design only applies to contiguous full-block `WriteBatch` calls. Single
`Write` calls, non-contiguous future writes, trim, barrier, padding, direct
extent writes, and recovery apply paths keep their current semantics until
explicitly redesigned.

## Required Invariants

`INV-WAL-BATCH-FORMAT-VERSION`

A multi-block record requires a WALStore format/version gate. Do not silently
write `walEntryWriteBatch` into a store that advertises the current
`WALStoreImplVersion=1`. A future prototype must either bump the implementation
version or add an explicit feature bit that old readers reject safely.

`INV-WAL-BATCH-CRC-ALL-OR-NOTHING`

The batch record CRC covers the entire batch. Recovery must not partially replay
a CRC-failed multi-block record. A torn or corrupted multi-block record is a
record-level failure, not a per-block best-effort replay.

`INV-WAL-BATCH-LSN-CONTIGUOUS`

The record's `LSN` is the first LSN. Per-block LSNs are contiguous
`firstLSN+i`. `WriteBatch` already allocates this shape today; a future
prototype must keep the same allocation before WAL append.

`INV-WAL-BATCH-LBA-CONTIGUOUS`

The record's `LBA` is the first LBA. Per-block LBAs are contiguous
`firstLBA+i`. The optimization must not be used for non-contiguous writes unless
the record format is extended to carry an explicit LBA table.

`INV-WAL-BATCH-DIRTYMAP-OFFSET`

The dirty map can no longer store only `(LBA -> WALOffset, LSN, Length)` if many
LBAs point to one WAL record. It must also know the in-record block index or
data offset, or `readFromWAL` must decode the batch and select the requested
block. Without this, reads of dirty data would return the first block for every
LBA in the batch.

`INV-WAL-BATCH-RECOVERY-SPLIT`

Recovery must split one batch record into per-LBA dirty-map entries and
per-block recovery entries:

```text
for i in [0, blockCount):
  dirtyMap.put(firstLBA+i, walOffset, firstLSN+i, blockSize, dataOffset=i*blockSize)
```

The recovered frontier is the highest per-block LSN in the batch.

`INV-WAL-BATCH-FLUSH-SPLIT`

The flusher must write each block to its own extent slot and compare/delete
dirty-map entries by their per-block LSN. It must not treat one batch record as
one extent write.

`INV-WAL-BATCH-CHECKPOINT-HIGHEST-LSN`

Checkpoint advancement remains based on the highest verified per-block LSN that
was flushed. For a fully flushed batch, that is `firstLSN + blockCount - 1`.

`INV-WAL-BATCH-READ-ONE-BLOCK`

`Read(lba)` must continue returning exactly one block. If the dirty-map entry
points into a batch, `readFromWAL` must return only the selected block's slice.

`INV-WAL-BATCH-FALLBACK-CURRENT-FORMAT`

The current single-block `walEntryWrite` path must remain available as fallback.
The prototype must be disableable, and current format recovery tests must pass
with the feature disabled.

## Prototype Checklist

Before enabling any multi-block record in a live gate:

1. Add the version/feature gate and make unsupported readers fail closed.
2. Add encode/decode tests for a batch record, including CRC mismatch and torn
   tail.
3. Extend dirty-map metadata to include in-record block offset.
4. Extend recovery scanning to split batch records.
5. Extend dirty read path to return the selected block from a batch.
6. Extend flusher to split batch records into extent writes and checkpoint by
   highest per-block LSN.
7. Prove compatibility: existing single-block WAL stores still recover.
8. Prove fallback: feature disabled keeps current format and current tests pass.
9. Add instrumentation: record count, batch block count, batch bytes, and
   fallback count.
10. Only then run a mounted NVMe/TCP profile gate.

## Decision

```text
phase147_decision=prototype_next
next_recommendation=phase148_wal_multiblock_record_local_prototype
```

The next phase should prototype the format behind an explicit local feature
gate, not turn it on in Kubernetes or change defaults.
