# Phase 34 D4-0 SmartWAL Injection Precheck

Date: 2026-05-29

Status: PASS for layout + mutation-tool contract; D4 dirty-failure scenario
remains pending.

## Why this precheck exists

The existing external TestRunner `corrupt_wal` primitive is V2-shaped. It uses
a fixed offset model:

```text
walOffset = 4096
walEnd    = min(fileSize, 4096 + 64MiB)
seekPos   = walOffset + (walEnd - walOffset) / 3
```

That is not a valid contract for V3 SmartWAL. SmartWAL stores its geometry in
the file header:

```text
[0 .. headerSize)                    header
[headerSize .. headerSize+slots*32)  WAL ring
[extentStart ..]                     block extents
```

Any WAL corruption test that does not parse the SmartWAL header can produce a
false green by writing to an empty slot or to extent data rather than to a real
WAL record.

## Delivered precheck

Added a read-only SmartWAL layout inspector:

```text
core/storage/smartwal/layout.go
core/storage/smartwal/layout_test.go
```

Added an explicit TestOps-only mutation utility:

```text
cmd/sw-block-testutil smartwal-corrupt-latest-record --path <store.bin> --out <dir>
```

This utility is deliberately not part of `sw-block ops`; the user-facing ops
surface stays read-only. The utility:

- parses the SmartWAL layout from the header,
- selects the highest-LSN valid WAL record,
- flips the last byte of that record's CRC,
- fsyncs the file,
- writes `smartwal-corruption-evidence.txt`.

The inspector returns:

```text
header_size
record_size
wal_offset
wal_length
wal_end
extent_start
extent_bytes
file_size
block_size
num_blocks
wal_slots
impl_kind
impl_version
```

It also exposes helpers to compute the slot offset for an LSN and to check
whether an offset is inside the WAL ring or extent region.

## Verification

Command:

```text
go test ./core/storage/smartwal ./cmd/sw-block-testutil
```

Result:

```text
ok github.com/seaweedfs/seaweed-block/core/storage/smartwal
ok github.com/seaweedfs/seaweed-block/cmd/sw-block-testutil
```

Tests prove:

- SmartWAL geometry is read from the on-disk header.
- WAL ring bounds are computed as `headerSize + walSlots*recordSize`.
- Record offset uses `slot = lsn % walSlots`.
- Non-SmartWAL files are rejected.
- Truncated SmartWAL files are rejected.
- The mutation utility writes evidence, flips a real record CRC byte, and the
  corrupted record no longer decodes as valid.

## D4 gate rule

D4 must not use the legacy `corrupt_wal` primitive directly.

A valid D4 corruption gate must first capture:

```text
smartwal_path=<path>
wal_offset=<parsed>
wal_length=<parsed>
target_lsn=<lsn>
target_record_offset=<computed>
target_offset_inside_wal=true
target_offset_inside_extent=false
before_bytes=<sample>
after_bytes=<sample>
mutation=flip_last_record_crc_byte
restart_persistence=hostpath
```

If those fields are not present, the D4 scenario must fail closed or be marked
blocked. A green result without those fields is considered self-proof, not
storage fault evidence.

## Remaining work

D4 dirty-failure testing still needs a live hostPath scenario that:

1. Locates the SmartWAL file for the target PVC.
2. Selects a real committed WAL record, not an arbitrary byte offset.
3. Mutates that record or its CRC.
4. Restarts the product.
5. Verifies the product refuses false `Ready=True` and emits a stable recovery
   reason.

This precheck only closes the layout-discovery prerequisite.
