# Parallel WAL Candidate

`parallelwal` is the explicit Phase 167 `LogicalStorage` candidate selected
with:

```text
--durable-impl=parallel-walstore
```

It is not the default backend. `smartwal` and `walstore` keep their existing
formats and behavior.

## Problem

The legacy WAL path assigns a global LSN and appends every write through one
whole-volume critical section. That gives simple ordering, but concurrent
independent-LBA writers still contend on one append owner.

This candidate separates physical append ownership from logical publication:

- an LBA maps deterministically to one lane;
- each lane owns a non-overlapping WAL region;
- lanes may execute positioned writes concurrently;
- a global completion ledger publishes only the contiguous LSN prefix.

Physical completion out of order is allowed. Logical success over an LSN hole
is not.

## File Format

One preallocated file contains:

```text
[4 KiB header A]
[4 KiB header B]
[lane 0 WAL ring]
...
[lane N WAL ring]
[block extent A]
[block extent B]
```

Each WAL record contains a 32-byte header followed by one complete block. The
record stores its global LSN, LBA, flags, payload length, data CRC, and record
CRC. Each lane's logical head and tail are monotonic; physical slot selection
uses `sequence % slotsPerLane`.

The alternating headers are CRC protected and include:

- generation;
- block and volume geometry;
- lane count, stripe mapping, record size, and ring capacity;
- global durable and checkpoint LSNs;
- global retained-WAL floor;
- durable head and tail for every lane.
- the currently authoritative extent.

Open chooses the highest valid generation. A damaged latest header may fall
back to the prior valid generation; a torn or CRC-invalid committed record
fails with typed `storage.ErrWALIntegrityFault`.

## Write And Publication

The store allocates global LSN and lane sequence under one short metadata lock.
Execution then moves to the LBA's lane:

1. same-lane requests enter one bounded, sequence-ordered queue;
2. one drainer combines adjacent physical ring slots into positioned writes
   no larger than 1 MiB, splitting at ring wrap;
3. completion enters the global ledger;
4. the ledger publishes consecutive completed LSNs from `H+1`;
5. the caller returns only after its LSN is published.

If LSN `N` fails, the store enters a terminal error state. A physically
completed `N+1` cannot return success or advance `H`.

`WriteBatch` reserves one contiguous global LSN range and queues requests by
lane. Same-LBA writes always map to the same lane and remain ordered. A failed
later ring chunk terminal-faults the store: an earlier physical chunk may be
reported as a partial in-process result, but it cannot become durable because
the covering `Sync` fails and the durable header does not advance.

## Sync And Checkpoint

`Sync` first fences the highest LSN admitted when the call acquires the store
lock. It waits until that prefix is contiguously published or a terminal append
failure is known, then snapshots `H` and the per-lane published heads. It then:

1. fsyncs WAL writes;
2. writes the alternate header with `R=H`;
3. fsyncs the header;
4. returns without checkpointing when every lane remains below pressure.

When a lane exceeds its retained-slot threshold, `Sync` additionally:

1. writes only latest blocks with `LSN <= R` to the shared extent, coalescing
   contiguous LBAs into bounded 1 MiB positioned writes;
2. fsyncs the extent;
3. advances checkpoint and lane tails in the alternate header;
4. fsyncs that header;
5. advances `S` to the first LSN still honestly scannable.

Before advancing a lane tail, recycle verification reads adjacent WAL records
in bounded 1 MiB chunks. Every record is still decoded and CRC checked; the
optimization removes one-`pread`-per-record amplification, not integrity
validation.

No unstable record is copied into the checkpoint. No slot is reused before
the extent is durable and both header slots have been sealed with the recycled
tails, so fallback cannot scan a physical slot after it has been reused.

Rebuild BASE installation uses the inactive extent as a copy-on-write stage.
Before reusing that extent, a guard generation makes both valid headers point
at the current active extent, preserving header fallback. The stage is then
cleared, BASE blocks are written there, and any session-live WAL blocks are
overlaid before durability. The final alternate header switches
`ActiveExtent` only after the staged extent is fsynced. A crash or final-header
failure therefore leaves the prior header and prior acknowledged extent
authoritative. Normal WAL-pressure checkpoints keep using the active extent
because the WAL-first header preserves replay data.

`WriteExtentDirect` remains immediately readable as required by the shared
`LogicalStorage` contract; COW controls which extent recovery treats as
durable, not in-process visibility inside a rebuilding replica. The recovery
and authority layers must keep that replica non-ready until the terminal
barrier succeeds. A failed session is never evidence that the staged extent is
serviceable, and the next session resets it before reuse.

## Recovery

Recovery reads only records inside the durable lane head/tail ranges from the
selected header, validates every retained record, and merges lanes by global
LSN. It rejects:

- record CRC or data CRC mismatch;
- invalid LBA-to-lane mapping;
- duplicate committed LSN;
- record LSN above `R`;
- a hole in the required contiguous interval
  `[max(checkpoint + 1, S), R]`.

Extent data supplies checkpointed blocks. Recovery validates all retained WAL
records, but records at or below the checkpoint remain catch-up history only;
they cannot replace newer BASE bytes installed directly into the extent.
Retained records above the checkpoint rebuild the live overlay and all
retained records remain eligible for `ScanLBAs`. Requests below `S` return
typed `storage.ErrWALRecycled`.

`ApplyEntry` honors the source LSN from `LogicalStorage`, including a first
entry above LSN 1. Such a jump is published only after the entry append
succeeds. It advances the retained-WAL floor `S`, not the checkpoint: recovery
still replays every physically retained record, while the unrepresented LSN
interval is not falsely advertised as catch-up history. `AdvanceFrontier` is
reserved for the rebuild path after direct extent installation and moves `S`
beyond that BASE frontier.

## Frontend Concurrency

Removing whole-volume serialization exposed an existing partial-write hazard:
two `Read -> overlay -> Write` operations against different byte ranges of one
block could lose one update. `StorageBackend` therefore serializes every write
to an LBA through striped locks: partial RMW, single full-block writes, and
full-block batches all share the same exclusion boundary. Full-block writes
still skip the read.

## Current Evidence And Limit

Run:

```bash
scripts/run-phase167-parallel-wal-candidate-gate.sh
```

The gate stress-tests header fallback, Sync admission fencing, out-of-order
completion, terminal lower-LSN failure and append drain, source-frontier
jumps, unsynced-tail discard, CRC failure, ring wrap, recovery merge,
BASE-versus-retained-WAL precedence, failed BASE commit rollback, abandoned
BASE-stage reset, direct-frontier retention, and adapter integration. It also
runs the canonical `LogicalStorage` contract and compares 1/2/4/8-writer
throughput against legacy `walstore`.

The exact-commit Linux gate at `a5b687f` passed race, recovery stress, and
external syscall validation. The deterministic syscall probes observed five
`pwrite64` calls for eight concurrent same-lane writes and three `pread64`
calls while recycling 224 records. In the same 3000-iteration run, four-writer
16-block batches reached `116.95 MiB/s` versus legacy `80.00 MiB/s`, but 4 KiB
writes reached only `49.79 MiB/s` for one writer and `39.25 MiB/s` for four,
versus legacy `107.85 MiB/s` and `104.08 MiB/s`.

The batch path therefore improved, but the candidate did not meet the
single-writer or four-writer scaling thresholds:
`performance_claim_allowed=false`. `parallel-walstore` remains opt-in and must
not be described as a default-backend or general performance improvement.
