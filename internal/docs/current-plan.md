# Current Plan: Phase 60 Rebuild/Catch-Up Data-Path Gate

Status: complete.

Branch target: `phase54-returned-replica-reintegration-executor`

## Goal

Phases 56-59 closed the returned-replica rebuild planning path:

```text
rebuild contract
  -> target-owner creates SwBlockReplicaRebuild
  -> authority-executor writes planned rebuild status
```

That path intentionally did not move data. Phase 60 closes the next narrow
question:

```text
Do the existing rebuild/catch-up data paths actually move bytes, close sessions,
and converge replicas under the engine/adapter/transport/recovery stack?
```

This phase turns the existing component/transport evidence into a repeatable
TestRunner gate with terminal evidence.

## Scope

In scope:

- Run engine-driven catch-up evidence.
- Run transport catch-up evidence with barrier confirmation.
- Run engine-driven dual-lane rebuild evidence.
- Run post-close durable-ack publication ordering evidence.
- Run live-write-during-rebuild evidence.
- Run same-LBA arbitration evidence.
- Emit terminal key/value evidence that names the observed data-path events.
- Keep the claim honest: this is data-path proof, not Kubernetes executor
  call-site wiring.

Out of scope:

- No Kubernetes `authority-executor` trigger into a live blockvolume process.
- No `SwBlockReplicaRebuild.status.state=running/completed` driven by real bytes.
- No frontend publication.
- No failback.
- No ACK eligibility mutation.
- No cross-volume or RF=3 Kubernetes rebuild orchestration.

## Deliverables

### D1: Data-Path Gate Script

Status: complete.

Added:

```text
scripts/run-phase60-rebuild-catchup-datapath-gate.sh
```

The script runs targeted Go component/transport tests and records evidence such
as:

```text
start_catchup_observed=true
catchup_session_completed_observed=true
start_rebuild_observed=true
dual_lane_rebuild_observed=true
session_closed_completed_observed=true
durable_ack_observed=true
barrier_handshake_observed=true
byte_equal_assertions_passed=true
same_lba_last_write_wins_asserted=true
authority_executor_datapath_callsite=false
```

### D2: TestRunner Scenario

Status: complete.

Added:

```text
testops/scenarios/rebuild-catchup-datapath-chain.yaml
```

The scenario runs the gate on `m02` against `/tmp/seaweed_block` and asserts the
terminal evidence.

### D3: Local Validation

Status: PASS.

Run:

```text
go test ./core/replication/component ./core/transport -run "<Phase60 patterns>" -count=1 -v
C:\work\swblock.exe validate testops\scenarios\rebuild-catchup-datapath-chain.yaml
```

### D4: Live QA Gate

Status: QA PASS.

Sync the current tree to `m02:/tmp/seaweed_block`, then run:

```text
swblock run testops/scenarios/rebuild-catchup-datapath-chain.yaml
```

Expected terminal evidence:

```text
phase60_rebuild_catchup_datapath_status=ok
rebuild_traffic_started=true
catchup_traffic_started=true
byte_equal_assertions_passed=true
authority_executor_datapath_callsite=false
frontend_publication_allowed=false
failback_allowed=false
```

### D5: Close Docs

Status: complete.

On PASS, write:

```text
internal/docs/qa-assignments/phase60-rebuild-catchup-datapath-qa-signoff.md
internal/docs/finished-plans/phase60_finishedplan_rebuild_catchup_datapath_gate.md
```

Updated `docs/roadmap.md` to state that Phase 60 proves the existing
rebuild/catch-up data path, while Phase 61 remains the executor-to-runtime
call-site milestone.

## Exit

Phase 60 closed when the live TestRunner gate proved both catch-up and rebuild
traffic paths, byte-equality convergence, session close, durable ack ordering,
and explicitly states that executor/runtime Kubernetes wiring is still out of
scope.

Result:

```text
20260623-194022-f4ea rebuild-catchup-datapath-chain PASS 34/34
```

Sign-off:

```text
internal/docs/qa-assignments/phase60-rebuild-catchup-datapath-qa-signoff.md
```
