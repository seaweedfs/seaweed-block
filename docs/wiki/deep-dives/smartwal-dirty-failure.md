# SmartWAL Dirty-Failure Path

The SmartWAL corruption gate is the best example of why live dirty-failure
tests matter. It found a real false-ready bug that unit tests and clean failure
tests missed.

## Problem

A corrupted WAL record is not the same as a missing pod or a clean restart. If
the product detects corruption but still reports `Ready=True`, users get a
false safety signal.

The required behavior is:

```text
detect integrity fault
-> fail closed
-> block local readiness
-> projection refuses Ready=True without positive readiness
```

## Failure Chain

```mermaid
sequenceDiagram
  participant QA as TestOps corrupt gate
  participant WAL as SmartWAL
  participant BV as blockvolume
  participant BM as blockmaster
  participant OPS as operator-status/report

  QA->>WAL: corrupt real WAL record
  WAL-->>BV: recovery CRC mismatch / WAL integrity fault
  BV->>BV: keep status endpoint diagnosable
  BV->>BM: do not publish local primary readiness
  BM->>OPS: evidence lacks positive primary readiness
  OPS-->>QA: Ready is Unknown/Blocked, never Ready=True
```

## What The Gate Caught

The gate forced fixes through multiple layers:

1. Storage layer: do not skip mid-history CRC mismatch as a harmless torn tail.
2. blockvolume process: do not continue publishing healthy after durable
   recovery fault.
3. blockmaster projection: heartbeat/reachability is not enough for Ready.
   Primary readiness must be positively confirmed.

This sequence matters because each local fix was necessary but insufficient
until the surface stopped claiming false readiness.

## Main Code Areas

| Layer | Code area |
|---|---|
| SmartWAL recovery | `core/storage/smartwal/` |
| durable frontend readiness | `core/frontend/durable/` |
| blockvolume process readiness | `cmd/blockvolume/main.go` |
| blockmaster projection | `core/host/master/observation_snapshot.go`, `core/host/volume/projection_bridge.go` |
| status projection | `core/ops` |

## QA Evidence

| Artifact | Purpose |
|---|---|
| `helm-smartwal-corrupt-restart-chain.yaml` | live dirty-failure scenario |
| Phase 34 D4 finding | proved false Ready after real WAL corruption |
| Phase 34 D4 verify reports | tracked storage fix, process fix, projection fix |
| Phase 34 D4 PASS | confirmed no false Ready after corruption |

## Design Rule

Do not let "process is reachable" imply "volume is ready".

```text
reachable endpoint + assigned primary != positive readiness evidence
```

This same rule should be reused for returned-replica rebuild, partial recovery,
and future dirty shutdown scenarios.

## Non-Claims

- The gate does not prove all WAL corruption modes are recoverable.
- It does not prove no data loss under every power-loss pattern.
- It proves the product does not falsely report Ready when known dirty evidence
  is present.

