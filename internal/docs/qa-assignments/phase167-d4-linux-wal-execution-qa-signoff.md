# Phase 167 D4 Linux WAL Execution QA Sign-off

Status: PASS for correctness and evidence; NOT PROMOTED for performance.

Source commit: `a5b687f`

Host: m02, Linux amd64, Go 1.25, 13th Gen Intel Core i5-13400.

## Scope

This gate validates the opt-in `parallel-walstore` execution changes:

- bounded checkpoint writes;
- ordered same-lane WAL append batching;
- ring-wrap split and failure behavior;
- bounded WAL recycle reads with per-record CRC validation;
- same-run candidate and legacy performance evidence.

It does not change the default backend and does not make an RF3 or mounted
performance claim.

## Correctness

The exact-commit Linux race run passed:

```text
core/storage/parallelwal  PASS
core/storage              PASS
core/recovery             PASS
core/transport            PASS
```

The product gate passed affected packages, Helm lint, and the 50x focused
matrix. The focused matrix includes:

- contiguous global LSN publication across lanes;
- Sync admission fencing;
- lower-LSN failure and terminal append drain;
- same-lane append coalescing;
- ring-wrap batch crash/reopen;
- second ring chunk failure remaining non-durable;
- recycle-time CRC corruption rejection;
- dual-header fallback and recycled-slot fallback;
- rebuild COW extent commit and rollback.

Independent review verdict: ACCEPT, no ordering, lock, close/recovery, or ring
accounting blocker.

## Independent Syscall Evidence

The gate compiles a test binary and runs deterministic tests under Linux
`strace`:

```text
external_syscall_validation=strace
external_append_pwrite_calls=5
external_recycle_pread_calls=3
```

The append probe submits eight concurrent same-lane writes. The recycle probe
recycles 224 records while still decoding and CRC-checking each record. These
counts independently corroborate the internal `wal_write_ops` and
`recycle_read_ops` diagnostics.

An additional 48000-record batch profile observed about 7900 `pwrite64` calls
and 190 `pread64` calls. Before recycle-read coalescing, the same shape issued
about 47746 `pread64` calls.

## Same-run Performance

The exact 3000-iteration release gate reported:

| Workload | Candidate | Legacy | Ratio |
|---|---:|---:|---:|
| 4 KiB, 1 writer | 49.79 MiB/s | 107.85 MiB/s | 0.462 |
| 4 KiB, 4 writers | 39.25 MiB/s | 104.08 MiB/s | 0.377 |
| 16-block batch, 4 writers | 116.95 MiB/s | 80.00 MiB/s | 1.462 |

Candidate four-writer scaling for 4 KiB writes was `0.788x`. The batch run
combined 48000 records into 8281 WAL writes and 188 recycle reads.

## Verdict

The execution changes are correct and materially reduce checkpoint, append,
and recycle syscall amplification for batched writes. They do not establish a
general throughput improvement: the 4 KiB path fails both the single-writer
regression limit and the four-writer scaling requirement.

`parallel-walstore` remains opt-in. `walstore` remains the default. RF3 and
mounted promotion claims are withheld. The next eligible experiment is a
Linux native asynchronous submission backend with a tested positioned-I/O
fallback; it must beat this exact same-run control before retention.

The gate did not deploy Kubernetes resources or modify the k3s lab.
