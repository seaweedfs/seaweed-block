# QA Verification - Phase 34 D4 SmartWAL Fix (#51)

Verdict: **STILL RED — fix is half-wired.** Commit `85d9375` correctly makes
SmartWAL recovery fail closed at the storage layer, but the blockvolume process
swallows the durable-recovery failure and proceeds to publish healthy anyway,
so the user-visible symptom is unchanged: `Ready=True reason=first_volume_verified`
after corruption. The gate is correctly still red. One precise wiring fix
remains.

Date: 2026-05-30

Source commit under test: `85d9375 storage: fail closed on smartwal integrity mismatch`
QA run: `20260530-000104-b168`
Scenario: `testops/scenarios/helm-smartwal-corrupt-restart-chain.yaml`
Tracking issue: #51

## What the Fix Got Right (verified live)

The storage-layer fix works. blockvolume recovery log
(`corrupt/blockvolume-pods.log`):

```text
smartwal: recovery CRC mismatch LSN=45 LBA=0 expected=11c18052 actual=0f934a52 — failing closed
blockvolume: durable recovery failed: recover failed: storage: WALIntegrity: CRC mismatch LSN=45 LBA=0 ...: storage: WAL integrity fault
```

- SmartWAL now **fails closed** instead of "skipping" (compare the prior run
  `20260529-232752-b23c` which said "skipping ... recovered LSN=59").
- The typed `WALIntegrity` fault is produced.
- No more silent skip of a mid-history corrupted committed record — the Layer 2
  durability concern from the prior finding is addressed at the storage layer.

Unit tests for the fix pass:
`go test ./core/storage/smartwal ./core/frontend/durable ./core/ops` PASS.

## What Is Still Broken (the half-wire)

Despite durable recovery failing, the volume still reports Ready:

`after-corrupt/corruption-status-summary.txt`:

```text
ready_true_after_corruption=true
blocked_true_after_corruption=false
reason_after_corruption=first_volume_verified
```

operator-snapshot.json for the volume:

```json
"status": "ready",
"reason_code": "first_volume_verified",
"conditions": [{"type": "Ready", "status": "True", ...}]
```

The string `wal_integrity_fault` appears **nowhere** in the live status surface
(report summary, operator-snapshot). The pod is `1/1 Running`, 0 restarts.

### Why: blockvolume logs the recovery failure but does not act on it

`cmd/blockvolume/main.go:493-507`:

```go
report, recErr := dp.RecoverVolume(recCtx, f.volumeID)
recCancel()
if recErr != nil {
    fmt.Fprintln(os.Stderr, "blockvolume: durable recovery failed:", report.Evidence)
} else {
    fmt.Fprintln(os.Stderr, "blockvolume: durable recovered:", report.Evidence)
}
// ...
startReadyAssignmentLoop(readyCh, f, durableProv, os.Stdout, os.Stderr)
```

When `recErr != nil` (the WALIntegrity case), the code **only logs to stderr**.
It does not return, does not mark the volume faulted, and does not stop
`startReadyAssignmentLoop` from running. So the process continues startup and
publishes healthy.

The adapter trace confirms PublishHealthy fires AFTER the recovery failure:

```text
blockvolume: durable recovery failed: ... WAL integrity fault
adapter[rid=r1] ... cmds=[FenceAtEpoch]
adapter[rid=r1] ... cmds=[PublishHealthy]
```

Compare line 490-491 just above: there IS a fail-closed process path
(`_ = h.Close(); return 1`) for an earlier error. The durable recovery failure
at line 496 simply does not take it.

### Consequence chain

```text
SmartWAL fails closed (correct)
-> recErr != nil, but main.go only logs it
-> startReadyAssignmentLoop runs anyway
-> adapter PublishHealthy
-> blockmaster ManagedVolume facts show healthy
-> projection mapping (correctly: wal_integrity_fault -> Blocked) never
   receives a wal_integrity_fault fact, because the volume-side never emits one
-> operator-snapshot: Ready=True reason=first_volume_verified
```

The ManagedVolume projection mapping the dev added (wal_integrity_fault ->
Blocked) is correct and unit-tested, but it is dead code in the live path
because the fact never arrives. The break is upstream, at the
volume-process-to-engine boundary.

## Precise Fix For Dev

When `dp.RecoverVolume` returns a `WALIntegrity` (or any typed durable
recovery) failure, the blockvolume must propagate it instead of swallowing it.
Two viable shapes:

1. **Fail closed at the process** (simplest, matches the existing
   line-490 pattern): on typed durable recovery failure, `h.Close(); return 1`.
   The pod crashloops -> not Ready -> blockmaster sees the volume down -> status
   is not Ready. Combined with a pod readiness probe, the operator-snapshot
   would show the volume as down/blocked, not Ready.

2. **Publish faulted** (richer, preferred for the status story): proceed but
   feed the `WALIntegrity` fault into the published observation so the engine
   emits a faulted/degraded fact carrying `wal_integrity_fault`. Then the
   existing projection mapping fires and the surface shows
   `Blocked=True reason=wal_integrity_fault`. This gives the user a precise
   reason instead of an opaque crashloop.

Option 2 is the better end-state because it makes the status surface
self-explaining (the whole point of Phase 32). Option 1 is acceptable as a
first step if it is faster, but then a pod readiness probe should also reflect
the durable recovery failure so the volume does not appear `1/1 Ready`.

Secondary defense-in-depth: the pod readiness probe should fail when durable
recovery failed; right now the pod is `1/1 Running` despite the failure.

## Gate Status

The D4 gate is **functioning and correctly red.** It moved the product one
layer forward (storage now fails closed) but the user-visible assertion
(`no Ready=True after corruption`) is still violated because of the unwired
volume-process branch. Keep D4 red until the recovery failure reaches the
status surface as a non-Ready, `wal_integrity_fault`-reasoned state.

## Lab State

Clean after the always-run cleanup: no helm release, no iSCSI sessions, no
multipath, no sw-block pods, no testops hostPath residue.

## Bottom Line

- Storage fail-closed (#51 core): **done and verified live.** Real progress.
- Status surface after corruption: **still Ready=True** — the recovery failure
  is logged but not propagated past `cmd/blockvolume/main.go:496`.
- Fix is one branch: turn the typed durable recovery failure into either a
  process fail-closed or a published `wal_integrity_fault` observation; the
  projection mapping is already in place to do the rest.
- Do not mark D4 passed. One more dev cycle.
