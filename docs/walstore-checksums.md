# WALStore data checksums (C12)

New WALStore and walstore-chunk files use implementation version 2. The
shared superblock format is unchanged. A CRC32C region is appended after
WAL and extent data in the same file descriptor, with one little-endian
32-bit checksum per 4 KiB unit of each logical block. Short logical blocks
have their own checksum. Checksums are XOR-normalized against an equal-size
zero unit: the initial sparse region validates zeros, never arbitrary data.

Existing implementation-version-1 files remain usable without C12. There
is no implicit retrofit or migration. `blockvolume --require-data-checksums`
requires durable walstore or walstore-chunk and explicitly refuses opening
v1. Offline scrub-and-upgrade is a follow-up. The local durable status exposes
`StorageFormatVersion` and `DataChecksums`; VOLUME records expose
`storage_format_version` and `data_checksums`. New snapshot/fork records
preserve the source declaration. Missing legacy metadata means unknown
protection, not protected storage.

Every local extent read verifies the CRC before returning bytes. Mismatch
returns EIO and a typed `storage.ErrDataChecksum`, never successful wrong
bytes. Recent WAL reads keep the existing record CRC validation. A retained
WAL record repairs an incomplete extent/checksum pair on recovery and flush;
without a retained record the mismatch is surfaced rather than guessed.
Lazy remote fetches validate length and the manifest SHA-256 before installing
any block, and classify a mismatch as a data-checksum error.

The backend refuses subsequent IO after a runtime read or read-modify-write
checksum fault. Its optional host-report callback runs after releasing the
backend IO lock. Host heartbeat classification is pending integration with
the typed-recovery-reason change; this Draft does not claim that wiring yet.

## Durability and lock order

The existing extent stripe protects the data write and its CRC write, and
protects checked reads. No checksum mutex or scheduler is added. Write paths
retain `captureMu -> lifecycleMu -> extent stripe`; flusher validation retains
its existing extent-stripe/dirty-shard order. The stripe is released before
file Sync and before checkpoint persistence. The flusher sequence is:

1. Write extent bytes and CRC under the existing stripe.
2. Sync their shared file descriptor successfully.
3. Under checkpoint metadata locking, save/sync the C11 dirty bitmap through
   `BeforeCheckpointAdvance`.
4. Write/sync checkpoint metadata, then advance the WAL tail.

A failed data/CRC write, shared Sync, or bitmap save cannot advance the tail.
The test hook at the data/CRC gap is nil in production. Backend fault callbacks
run after the IO read lock is released, avoiding an IO-drain callback cycle.

## Controls

```sh
go test ./core/storage -run C12 -count=1
go test ./core/frontend/durable -run C12 -count=1
go test ./core/frontend/chunkstore -run C12 -count=1
go test ./cmd/blockvolume -run C12 -count=1
go test ./... -count=1
```

Controls cover bit flips in extent/checksum, sparse zero blocks, truncated
regions, v1 refusal without file mutation, a CRC-write failure retaining WAL,
C11 bitmap/CRC/shared-Sync ordering, runtime IO refusal and callback lock order,
remote chunk corruption, and protection metadata across snapshot/fork. The
Linux subprocess control is killed after syncing extent bytes but before the
CRC write, then recovers exact bytes from retained WAL. This proves process
crash recovery, not power-loss behavior.

## File-backed proxy measurement

`C12_PROXY=1 go test ./core/storage -run '^TestC12FileBackedProxy$' -v -count=1`
runs three serial repetitions of 2,048 operations on a 16 MiB file, fixed
random seed, 4 KiB operations. Reads use checkpointed extents; writes use the
WAL and drain the flusher every 64 operations. Sync mode calls WALStore.Sync
after each operation (including reads). Throughput includes fixed-cadence
flushes; p50/p99 measure foreground operations. Initialization is excluded.

The exact same harness on unmodified v1 main (233b2ba, test-only successor)
and this v2 implementation produced these medians across three repetitions:

| Mode | v1 p50 / p99 (us) | v2 p50 / p99 (us) | v1 / v2 ops/s | Throughput change |
| --- | --- | --- | --- | --- |
| Random read, async | 2.130 / 13.485 | 2.371 / 10.708 | 328805 / 360292 | +9.58% |
| Random read, Sync after read | 1330.659 / 1836.910 | 1328.933 / 1588.246 | 730.69 / 742.97 | +1.68% |
| Random write, async | 6.407 / 39.182 | 6.542 / 44.346 | 16340.96 / 15630.43 | -4.35% |
| Random write, Sync after write | 2326.499 / 3450.286 | 2313.883 / 2796.417 | 406.43 / 416.43 | +2.46% |

This short, warm file-backed proxy excludes the block frontend, mounted
filesystem, fio, and device qualification. Read p50 increased 11.3% despite
no measured throughput drop; variability and tail latency remain visible.
The mounted fio <5% target is not established by this proxy and remains a
separate lab measurement.
