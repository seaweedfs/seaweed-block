# QA Verification #2 - Phase 34 D4 SmartWAL Fix (#51 follow-up)

Verdict: **STILL RED — the false Ready has been chased up one more layer.**
Commit `954083a` correctly stops the volume process from publishing healthy on
a recovery fault (verified live). But blockmaster's ManagedVolume projection
still reports `Ready=True reason=first_volume_verified`, because the volume's
"local readiness blocked / WALIntegrity" state is not carried through the
heartbeat/status channel that the projection consumes. This is now a
contract-level gap, not another local patch.

Date: 2026-05-30

Source commit under test: `954083a blockvolume: block healthy publication on recovery fault`
QA run: `20260530-225116-66a3`
Tracking issue: #51

## What `954083a` Got Right (verified live)

blockvolume recovery log (`corrupt/blockvolume-pods.log`):

```text
smartwal: recovery CRC mismatch LSN=45 ... — failing closed
blockvolume: durable recovery failed: ... storage: WAL integrity fault
blockvolume: durable recovery faulted; status endpoint remains available and local readiness remains blocked: ... WAL integrity fault
blockvolume: volume pvc-8c5d9111-... local readiness blocked (...WALIntegrity...); NOT applying primary assignment r1@1 to adapter
```

- The volume now **blocks local readiness** and **does not enter the adapter**.
- The adapter `PublishHealthy` trace from the prior run (`85d9375`) is **gone** —
  grep for PublishHealthy in this run's blockvolume log returns nothing.

So the `cmd/blockvolume/main.go:496` gap from the previous QA report is closed:
the recovery fault no longer drives a healthy publish. Real progress, third
layer of the onion peeled.

## What Is Still Broken — one layer up (blockmaster projection)

operator-snapshot.json for the volume still reports:

```json
"status": "ready",
"reason_code": "first_volume_verified",
"conditions": [{"type": "Ready", "status": "True", ...}]
```

`after-corrupt/corruption-status-summary.txt`:

```text
ready_true_after_corruption=true
blocked_true_after_corruption=false
reason_after_corruption=first_volume_verified
```

And the gate is fed by blockmaster's ClusterEvidenceService, whose
`cluster-evidence.json` shows:

```text
status: ok
desired_replicas: 1
observed_replicas: 1
primary_replica: r1
replicas: [{ replica_id: r1, replication_role: primary }]
```

### Root cause: heartbeat says "present", projection infers "Ready"

The restarted pod is `1/1 Running`, 0 restarts, and — by the dev's deliberate
design — keeps its status endpoint / heartbeat available so it stays
diagnosable. blockmaster therefore observes:

```text
replica r1 present + reachable + assigned primary
-> observed_replicas=1, primary_replica=r1, status=ok
-> ManagedVolume projection: Ready=True reason=first_volume_verified
```

But the volume's actual state is "local readiness blocked due to WALIntegrity
fault; primary assignment NOT applied to adapter." That fact never reaches the
projection. blockmaster equates "heartbeating + assigned primary" with Ready,
without requiring a positive local-readiness confirmation from the volume.

This is fresh-but-wrong, not merely a stale cache: blockmaster IS seeing the new
post-restart pod (observed_replicas reflects it). The pod heartbeats as
present; the readiness-blocked nuance is simply not in the channel the
projection reads.

### Note: the wal_integrity_fault -> Blocked mapping is still dead code live

Dev added (in `85d9375`) a ManagedVolume projection rule mapping
`wal_integrity_fault -> Blocked`. It is unit-tested and correct. But it still
never fires in the live path, because no live fact carrying
`wal_integrity_fault` reaches blockmaster — the string appears nowhere in the
live operator-snapshot or cluster-evidence. The fact is produced at the volume
(in its log) and consumed by nothing.

## The Pattern (worth naming)

This is the third consecutive cycle where a correct, verified fix pushed the
false-Ready up exactly one layer:

```text
1. storage:        skip corrupt record        -> fail closed         (85d9375, done)
2. volume process: publish healthy anyway      -> block readiness     (954083a, done)
3. blockmaster:    infer Ready from heartbeat  -> ??? (this finding)
```

Each patch was real and necessary, but the symptom did not move because the
root question was never answered at the contract level:

**What does `Ready=True` require, end to end?**

Right now it requires only "replica present + assigned primary + reachable."
It should require "the volume has positively confirmed local readiness." Until
the ManagedVolume projection demands a positive readiness signal (and treats
its absence as Pending/Unknown/Blocked), any volume-side fault that still lets
the pod heartbeat will surface as a false Ready.

## Recommended Fix (contract-level, not another local patch)

Pick one, but make it a contract change:

1. **Carry the fault on the heartbeat/status channel.** The volume's status
   endpoint / heartbeat must report `local_readiness=blocked,
   reason=wal_integrity_fault` when recovery is faulted. blockmaster's
   ManagedVolume projection then receives the fact and the existing
   `wal_integrity_fault -> Blocked` mapping fires. This is the smallest change
   that makes the dead-code mapping live and gives the user a precise reason.

2. **Make the projection require positive readiness.** blockmaster must not
   project `Ready=True` for an assigned primary that has not confirmed local
   readiness. An assigned-but-not-ready-confirmed primary becomes
   `Ready=Unknown` / `Pending` / `EvidenceStale` until the volume confirms
   readiness. This is the more robust fix because it closes the whole class
   (any future volume-side fault that blocks readiness while heartbeating),
   not just WALIntegrity.

Option 2 is the durable answer; option 1 is the targeted one. Ideally both:
the projection should require positive readiness AND the volume should carry a
reason so the user sees `wal_integrity_fault` rather than a bare Unknown.

This connects directly to the Phase 32 D7 stale/negative-first work: a primary
that is reachable-but-not-ready is the same shape as stale evidence — the
surface must not claim Ready.

## Gate Status

D4 remains **functional and correctly red.** It has now driven three real
product fixes and surfaced a fourth, deeper one. Keep it red until the
operator-snapshot shows the corrupted volume as NOT Ready (Blocked with
`wal_integrity_fault`, or Unknown/Pending), never `Ready=True
reason=first_volume_verified`.

## Lab State

Clean after the always-run cleanup: no helm release, no iSCSI sessions, no
multipath, no sw-block pods, no testops hostPath residue.

## Bottom Line

- `954083a`: **volume no longer publishes healthy on recovery fault — verified.**
- Still false Ready: **blockmaster projects Ready from heartbeat-presence, not
  from confirmed local readiness.** The `wal_integrity_fault -> Blocked` mapping
  is correct but starves because the fact never reaches it.
- Next fix should be **contract-level**: require positive local-readiness for
  Ready (and/or carry `wal_integrity_fault` on the heartbeat channel), not
  another local patch one layer down.
- Do not mark D4 passed.
