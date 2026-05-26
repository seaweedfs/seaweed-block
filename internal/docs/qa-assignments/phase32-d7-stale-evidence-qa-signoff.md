# QA Sign-off - Phase 32 D7 Stale Evidence

Verdict: **PASS**

Date: 2026-05-25

Validated source commit: `dc8d400 ops: prefer fresh restart evidence in bundle replay`

## Scope

Validates that `sw-block ops report --from-bundle` and the dashboard prefer
the freshest cluster evidence (by `captured_at`) when a bundle contains
both pre-restart `cluster-evidence.json` snapshots AND a post-restart
`cluster-after-restart.json`. This fix resolves N1 from the Phase 32
D5/D6 sign-off.

## Source Bundle

Fresh D5 scenario rerun:

- Run: `20260525-172250-bf28`
- Scenario: `helm-rf3-promotion-restart-persistence-chain.yaml`
- Result: 34/34 PASS
- Bundle root:
  `/v/share/g15d-k8s/20260525-172250-bf28-helm-rf3-promotion-restart/`

Bundle's cluster snapshots:

| Path | Role |
|---|---|
| `recovery/setup/status/cluster-evidence.json` | post-create, pre-promotion |
| `recovery/setup/status/report/cluster-evidence.json` | duplicate |
| `recovery/status/cluster-before.json` | pre-restart |
| `restart/cluster-after-restart.json` | post-restart (newest) |

## Scoped Unit Tests

```text
go test ./core/ops ./cmd/sw-block
```

Result on synced D7 tree: **PASS** (both packages).

## Before / After Evidence

### Phase 31 in-bundle truth (`restart/restart-promotion-summary.txt`)

```text
volume_id=pvc-9724a9c6-a9d0-4969-a870-7e77dfb0e9e4
before_restart_primary=r2
after_restart_primary=r2
before_restart_publish_target=192.168.1.184:3260
after_restart_publish_target=192.168.1.184:3260
post_restart_primary_count=1
reason=authority_persisted
```

### Regenerated report `summary.txt` (with D7 fix)

```text
volume=pvc-9724a9c6-... primary=r2@m02 frontend=192.168.1.184:3260 rf=3 ack=-
managed_volume=pvc-9724a9c6-... status=unknown reason=-
managed_volume_condition=Ready status=Unknown reason=unknown severity=info
managed_volume_action=observe.collect_bundle mode=read_only side_effect=observe executor=ops
```

The regenerated summary now names the post-restart primary `r2@m02` and
publish target `192.168.1.184:3260`. **Stale `r1@m01 frontend=192.168.1.181:3260`
is gone.**

The managed_volume `status=unknown` + `Ready=Unknown` reflects the
truthful post-restart snapshot taken while the system was still
reconverging - this is the correct negative-first behavior: the freshest
evidence is preferred, and when that evidence isn't sufficient to claim
Ready, the surface says Unknown instead of inventing readiness.

### Operator-snapshot

```json
"cluster": {
  "status": "blocked",
  "ready_volume_count": 0,        ← no false Ready
  "blocked_volume_count": 0,
  "stale_volume_count": 0
},
"volumes": [{
  "volume_id": "pvc-9724a9c6-a9d0-4969-a870-7e77dfb0e9e4",
  "pvc_name": "sw-block-multi-pvc-1",
  "status": "unknown",
  "conditions": [{"type":"Ready", "status":"Unknown", ...}]
}]
```

### Dashboard `/operator-snapshot.json`

```text
GET /operator-snapshot.json -> HTTP 200
  cluster.status=blocked
  ready_volume_count=0
  per-volume status=unknown, Ready=Unknown

POST/PUT/PATCH/DELETE -> 405
```

## Hard-Gate Compliance

| Requirement | Result |
|---|---|
| Scoped unit tests PASS | PASS |
| Scenario PASS | PASS (34/34) |
| `summary.txt` names post-restart primary | PASS (`r2@m02`) |
| `summary.txt` names post-restart publish target | PASS (`192.168.1.184:3260`) |
| No stale `primary=r1@m01 frontend=192.168.1.181:3260` in regenerated report | PASS (removed; fix landed) |
| operator-snapshot agrees with summary | PASS (same per-volume identity + Ready=Unknown) |
| Dashboard `/operator-snapshot.json` agrees | PASS (HTTP 200 + same fields) |
| No surface publishes `Ready=True` for older primary against contradictory newer evidence | PASS (Ready=Unknown emitted; no Ready=True) |
| POST/PUT/PATCH/DELETE to dashboard return 405 | PASS |

## Why Status Is `Unknown`, Not `Ready=True`

The post-restart cluster snapshot was captured during the reconvergence
window. At that moment:
- cluster aggregate `status=blocked` (transient)
- per-volume `status=unknown` (insufficient facts to claim Ready)
- Condition `Ready=Unknown` instead of `True`

This is the **correct** Phase 32 negative-first behavior:

- Phase 31's hard claim (authority persists across restart) is still
  PASS — `restart-promotion-summary.txt` measures `before==after primary`,
  `epoch monotonic`, `post_restart_primary_count=1`, and reader checksum
  OK. The fact that the captured post-restart snapshot shows a transient
  reconverging state does not contradict authority persistence; it only
  means the snapshot caught a moment when the read-model didn't yet
  agree.
- Phase 32's status surface correctly refuses to claim `Ready=True`
  against that fresh-but-insufficient evidence. This is exactly the
  failure-mode the negative-first plan defends against
  (`liveness misjudgment` / `stale evidence`).

If we wanted the regenerated report to claim `Ready=True` post-restart,
the scenario would need either:

1. A small settle interval between k3s rollout-ready and the
   `cluster-after-restart.json` capture, OR
2. A bounded `EvidenceStale` probe to refresh the snapshot once
   reconvergence completes.

Both are scenario-level polish, not D7 product gaps.

## Final Residue Audit

```text
helm release sw-block: none
iscsiadm sessions:     No active sessions
multipath -ll:         empty
dmsetup ls:            No devices found
sw-block pods:         none
```

## Blocking Findings

**None.**

## Non-Blocking Findings

### N1: Post-restart snapshot timing produces transient `unknown` status

The D7 fix correctly surfaces the freshest snapshot, but the D5 scenario
takes that snapshot ~1 second after k3s rollout-ready, which can catch
the system mid-reconverge. The post-restart snapshot legitimately reports
`status=unknown` for the volume because per-replica facts haven't refreshed
yet.

Two ways to resolve in a future cycle:

1. Add a bounded settle wait in the D5 scenario between the rollout-status
   confirmation and the `cluster-after-restart.json` capture (5-30s).
2. Implement the D7-class "bounded probe" referenced in the Phase 32 D7
   plan: when the freshest snapshot is `EvidenceStale`, the report layer
   can issue a refresh probe with a documented timeout.

Either approach lets the regenerated report show `Ready=True reason=first_volume_verified`
post-restart while still preserving the negative-first rule when evidence
truly is stale.

Not blocking because:
- The negative-first contract is held (no false Ready).
- Phase 31's authority-persistence claims still PASS through measured
  fields.
- The regenerated report now names the correct post-restart primary +
  publish target, resolving the N1 from D5/D6 sign-off.

## Verdict

Phase 32 D7 stale-evidence sign-off **PASS**.

The bundle-replay precedence fix (`dc8d400`) correctly resolves the
Phase 32 D5/D6 N1 finding: `sw-block ops report --from-bundle` now
prefers `restart/cluster-after-restart.json` over older snapshots based
on `captured_at`. The regenerated summary, operator-snapshot, and
dashboard all surface the post-restart primary + publish target, and
correctly refuse to project `Ready=True` against the transient
reconverging snapshot.

D7 closes the stale-evidence projection contract. **D8 close-gate** is
the final remaining sign-off for Phase 32.
