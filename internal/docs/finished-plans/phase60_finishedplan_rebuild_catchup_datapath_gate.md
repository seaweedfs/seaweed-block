# Phase 60 Finished Plan: Rebuild/Catch-Up Data-Path Gate

Status: complete.

QA: PASS.

## Goal

Close the gap between returned-replica rebuild planning and actual data-path
evidence. Phases 56-59 proved that the product can surface a rebuild contract,
create the target CR, and write planned status. Phase 60 proves that the
underlying catch-up/rebuild stack can actually move data and converge.

## Delivered

Added:

```text
scripts/run-phase60-rebuild-catchup-datapath-gate.sh
testops/scenarios/rebuild-catchup-datapath-chain.yaml
internal/docs/qa-assignments/phase60-rebuild-catchup-datapath-qa-signoff.md
```

Updated:

```text
internal/docs/current-plan.md
docs/roadmap.md
```

## Validated Path

The gate runs targeted Go tests for:

- engine-driven catch-up command emission;
- transport catch-up session completion;
- engine-driven dual-lane rebuild;
- recovery session close;
- durable ack observed after close;
- live WAL during rebuild;
- same-LBA arbitration;
- byte-equality convergence assertions.

Terminal evidence:

```text
phase60_rebuild_catchup_datapath_status=ok
start_catchup_observed=true
catchup_session_completed_observed=true
start_rebuild_observed=true
dual_lane_rebuild_observed=true
session_closed_completed_observed=true
durable_ack_observed=true
barrier_handshake_observed=true
live_wal_during_rebuild_observed=true
byte_equal_assertions_passed=true
same_lba_last_write_wins_asserted=true
rebuild_traffic_started=true
catchup_traffic_started=true
authority_executor_datapath_callsite=false
```

## Boundary

This phase proves the data path. It does not claim:

- live Kubernetes executor-triggered rebuild;
- `SwBlockReplicaRebuild.status=running/completed` from real traffic;
- frontend publication;
- failback;
- ACK eligibility mutation;
- RF=3 Kubernetes orchestration.

Those are intentionally left for Phase 61+.

## Verification

Local:

```text
go test ./core/replication/component ./core/transport -run "<Phase60 patterns>" -count=1 -v
C:\work\swblock.exe validate testops\scenarios\rebuild-catchup-datapath-chain.yaml
```

Live:

```text
20260623-194022-f4ea rebuild-catchup-datapath-chain PASS 34/34
```

Sign-off:

```text
internal/docs/qa-assignments/phase60-rebuild-catchup-datapath-qa-signoff.md
```

## Next

Phase 61 should wire the bounded authority executor/runtime call-site so the
same rebuild/catch-up traffic can be triggered from the product path and update
`SwBlockReplicaRebuild.status` from real terminal evidence.
