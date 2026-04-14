# smartwal — experimental persistence backend

Experimental implementation of `core/storage.LogicalStorage` using a
metadata-only WAL with extent-direct writes.

**Status: experimental.** Not the default backend, not advertised in
the sparrow scope statement, not exercised by `--persist-demo`. The
default crash-safe backend is `core/storage.WALStore`. SmartWAL lives
in this subpackage so the contract surface (LogicalStorage) is shown
to admit more than one implementation, and so this code can be
removed or extracted without touching the main storage API.

## When this design wins

| Compared to the default `WALStore` | smartwal benefit |
|---|---|
| WAL space per record | 32 bytes vs ~4138 bytes — **128× denser** |
| Read path | extent direct vs dirty-map → WAL or extent — fewer hops |
| In-memory state | no dirty map needed for reads — lower steady-state RAM |
| Compaction pressure | extent IS the destination — no draining required |

Trade-offs:

- Slightly more recovery work (extent CRC verification per surviving
  record, vs a flat WAL replay)
- Slightly more per-write IOPS (extent pwrite + ring append vs one
  WAL append)
- Failure-atomicity rollback runs on WAL append errors so an
  unsynced extent write cannot become "untracked durable" via a
  later Sync (atomic 4KB-on-4KB pwrites cover the device-level
  torn-write case)

## How it differs from `WALStore`

| Concern | WALStore | smartwal |
|---|---|---|
| Where data lives at write time | WAL | Extent |
| Where data lives at read time | WAL via dirty map, fall back to extent | Always extent |
| WAL record contents | full data + header + trailer (variable size) | 32-byte metadata |
| Recovery | replay WAL records into dirty map | scan ring, last-writer-wins per LBA, verify extent CRC |
| Single fsync covers both | yes (one file) | yes (one file) |
| Bounded WAL size | yes (circular) | yes (slot-based ring) |

Both implementations satisfy the same `LogicalStorage` contract.
The 11 contract assertions in `core/storage/contract_test.go` are
mirrored here in `store_test.go` so this subpackage is
self-contained.

## On-disk layout

```
[0          .. headerSize)                4KB header (magic, geometry)
[headerSize .. extentBase)                ring buffer (WALSlots × 32 bytes)
[extentBase .. extentBase + numBlocks*BS) extent (block-indexed)
```

Slot index = LSN % WALSlots. After WALSlots distinct LSNs, older
records are overwritten. The extent retains the data; only the
metadata pointer is lost. Recovery handles this gracefully: LBAs
whose metadata is gone read as whatever extent currently holds.

## How to use

```go
import "github.com/seaweedfs/seaweed-block/core/storage/smartwal"

// Create
s, err := smartwal.CreateStore("/tmp/store.bin", 1024, 4096)

// Open existing
s, err := smartwal.OpenStore("/tmp/store.bin")
_, err = s.Recover()

// Use as LogicalStorage
var ls storage.LogicalStorage = s
```

`smartwal.Store` satisfies `storage.LogicalStorage` (compile-time
assertion in `store.go`). Anything that takes a `LogicalStorage`
takes a smartwal.Store too.

## Tests

```bash
go test ./core/storage/smartwal/ -v
```

Coverage:

- LogicalStorage contract: 11 round-trip / boundary / lifecycle tests
- SmartWAL-specific:
  - `TestSmartWAL_WriteSyncCloseReopenRead` — clean round trip
  - `TestSmartWAL_AckedWritesSurviveSimulatedCrash` — drops the
    file handle without going through Close; acked data still
    recoverable
  - `TestSmartWAL_RecoverIsIdempotent`
  - `TestSmartWAL_OpenRejectsBadMagic`
  - `TestSmartWAL_RingWrapPreservesLatestPerLBA` — proves that
    extent retains all data even when ring slots are recycled

## What this implementation does NOT do

- **Compaction / checkpointing.** No background flusher trims old
  ring records (other than via natural wrap). Long-running stores
  with sparse working sets may need it later.
- **Per-block extent CRC.** CRC is in the WAL record only. If a ring
  record is overwritten before recovery, the extent block has no
  external CRC to check against.
- **Multi-block writes.** Each Write is one block. Larger writes
  must be split by the caller.
- **Coexistence with `WALStore`.** Different magic byte; the two
  cannot read each other's files. This is intentional — they are
  alternative implementations, not interchangeable on the same
  bytes.
