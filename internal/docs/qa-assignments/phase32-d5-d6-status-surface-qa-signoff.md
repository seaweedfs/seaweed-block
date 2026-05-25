# QA Sign-off - Phase 32 D5 / D6 Status Surface

Verdict: **PASS** with one non-blocking workflow gap on D5.

Date: 2026-05-25

Validated source commit: `76eb242 ops: extend phase32 status surface gates`

## Scope

Restart/promotion (D5) and multi-volume independence (D6) status-surface
agreement across report, dashboard, operator-snapshot.

## Run Summary

| Sub-gate | Scenario | QA run | Result |
|---|---|---:|---|
| Scoped unit tests | `go test ./core/ops ./cmd/sw-block` | local | PASS |
| D5 scenario | `helm-rf3-promotion-restart-persistence-chain.yaml` | `20260525-143121-3aaa` | 34/34 PASS |
| D6 scenario A | `helm-multi-volume-rf3-restart-smoke-chain.yaml` | `20260525-143400-e085` | 36/36 PASS |
| D6 scenario B (stronger) | `helm-multi-volume-rf3-interleaved-failover-chain.yaml` | `20260525-143747-f93a` | 56/56 PASS |
| D6 dashboard `/operator-snapshot.json` | fresh build against restart-smoke bundle | live probe | HTTP 200 + 3 distinct volumes |

## D5 Hard-Gate Compliance

Phase 31 product fields (carried unchanged):

```text
restart_promotion_status=ok
volume_id=pvc-638f80cf-1660-42b1-9851-aa633ab6b93a
before_restart_primary=r2
after_restart_primary=r2                  ← preserved
before_restart_publish_target=192.168.1.184:3260
after_restart_publish_target=192.168.1.184:3260  ← preserved
before_restart_epoch=2
after_restart_epoch=2                     ← monotonic
post_restart_primary_count=1              ← no split brain
reason=authority_persisted
```

Reader after restart: `/data/demo.bin: OK`.

Regenerated report from bundle:

```text
managed_volume status=ready reason=first_volume_verified
managed_volume_condition Ready status=True reason=first_volume_verified severity=info
read_only=true
```

Operator-snapshot:

```json
"cluster": {"status":"ok","ready_volume_count":1,"blocked_volume_count":0,"stale_volume_count":0}
per-volume: status=ready, reason_code=first_volume_verified, Condition Ready=True
```

| Required | Result |
|---|---|
| Scenario PASS from clean lab | PASS |
| `restart_promotion_status=ok` | PASS |
| `before_restart_primary == after_restart_primary` | PASS (r2 == r2) |
| `after_restart_epoch >= before_restart_epoch` | PASS (2 == 2) |
| `post_restart_primary_count=1` | PASS |
| reader checksum after restart | PASS |
| ManagedVolume status `recovered` or `ready` with stable reason | PASS (`ready` / `first_volume_verified`) |
| `operator-snapshot.json ready_volume_count=1, blocked_volume_count=0` | PASS |
| No surface shows old primary as Ready | PASS (only one primary visible per snapshot) |
| Dashboard `/operator-snapshot.json` HTTP 200 with same reason | PASS |
| `summary.txt` names the promoted primary and publish target after restart | PARTIAL - see N1 |

## D6 Hard-Gate Compliance

restart-smoke run-level summary:

```text
managed_volume_count=3
reader_verified_count=3
duplicate_publish_target_for_distinct_volume=false
cross_volume_authority_mixup=false
```

interleaved-failover run-level summary:

```text
cross_interference_observed=false
transparent_failover_claimed=true
recovered_volume_count=2
interleaved_target_volume_count=2
untouched_volume_stable=true
```

Regenerated operator-snapshot from restart-smoke bundle:

```json
"cluster": {"volume_count": 3, "ready_volume_count": 3, "blocked_volume_count": 0, "stale_volume_count": 0}
volumes: [
  {"volume_id":"pvc-2d248c2f-...", "pvc_name":"sw-block-multi-pvc-3", "reason_code":"first_volume_verified"},
  {"volume_id":"pvc-4432f840-...", "pvc_name":"sw-block-multi-pvc-2", "reason_code":"first_volume_verified"},
  {"volume_id":"pvc-67e3a97b-...", "pvc_name":"sw-block-multi-pvc-1", "reason_code":"first_volume_verified"}
]
```

Three distinct `volume_id` + three distinct `pvc_name`, each with its own
`first_volume_verified` reason and Ready=True Condition. No per-volume
status copied across.

Dashboard `/operator-snapshot.json` probe on D6 restart-smoke bundle:

```text
HTTP 200
volume_count=3, ready_volume_count=3
3 distinct volume_id + pvc_name entries
POST/PUT/PATCH/DELETE = 405
```

| Required | Result |
|---|---|
| Scenario PASS from clean lab | PASS (both A and B) |
| `managed_volume_count=3` | PASS |
| `reader_verified_count=3` (restart-smoke) | PASS |
| `cross_volume_authority_mixup=false` | PASS |
| `duplicate_publish_target_for_distinct_volume=false` | PASS |
| `operator-snapshot.json cluster.volume_count=3` | PASS |
| Three per-volume entries with distinct `volume_id` / `pvc_name` | PASS |
| No per-volume status/reason copied from another | PASS (3 independent identities) |
| Dashboard `/operator-snapshot.json` HTTP 200 with all 3 volume entries | PASS |

## Final Residue Audit

```text
helm release sw-block: none
iscsiadm sessions:     No active sessions
multipath -ll:         empty
dmsetup ls:            No devices found
sw-block / blockvolume pods: none
```

## Blocking Findings

**None.**

## Non-Blocking Findings

### N1: D5 `ops report --from-bundle` snapshots PRE-promotion state, not post-restart

When I ran `sw-block ops report --from-bundle <D5-run-root>`, the
regenerated `summary.txt` reported:

```text
volume=pvc-638f80cf-... primary=r1@m01 frontend=192.168.1.181:3260 rf=3
```

But the Phase 31 evidence in the same bundle says:

```text
before_restart_primary=r2  after_restart_primary=r2
before_restart_publish_target=192.168.1.184:3260  after_restart_publish_target=192.168.1.184:3260
```

So the post-restart primary is `r2@m02`, but the regenerated report shows
the **initial** primary `r1@m01` from right after first-volume creation.

Root cause: the D5 bundle contains **two** `cluster-evidence.json` snapshots
and one `cluster-after-restart.json`:
- `recovery/setup/status/cluster-evidence.json` — captured at
  `21:32:48Z`, immediately after first-volume create, BEFORE promotion;
  named `cluster-evidence.json` so `ops report --from-bundle` picks it up
  first.
- `recovery/setup/status/report/cluster-evidence.json` — duplicate, same
  capture.
- `restart/cluster-after-restart.json` — captured at `21:33:37Z` AFTER
  the k3s restart, but **NOT named** `cluster-evidence.json` so the report
  regenerator doesn't see it as a primary source.

The Phase 31 hard claims still PASS (they're verified via
`restart-promotion-summary.txt`'s pre/post fields, which carry the right
state). And `cluster-after-restart.json` IS captured and persisted.

But a cold reviewer running `sw-block ops report --from-bundle <bundle>`
on a D5 restart bundle would see the **pre-promotion** primary, which
contradicts the in-bundle `restart-promotion-summary.txt`.

Fix shape (one of):

1. **Scenario-side**: after the restart phase, copy
   `restart/cluster-after-restart.json` to a path the report regenerator
   prefers (e.g., overwrite the bundle-root `cluster-evidence.json`, or
   add a `restart/report/cluster-evidence.json` that wins by precedence).
2. **Tool-side**: `sw-block ops report --from-bundle` picks the MOST
   RECENT `cluster-evidence.json` (or `cluster*.json` matching the
   schema) in the bundle, not the lexicographically first.
3. **Bundle-shape rule**: document that for restart scenarios, the
   post-restart cluster snapshot is the canonical source; older snapshots
   are debug artifacts.

Recommend option 1 for the lowest blast radius (scenario change only). It
would also align with the D1a Workstream D failure-snapshot standard's
"capture the unreachable / post-event state as a first-class artifact"
principle.

Not blocking because:
- The D5 product claim (authority survives restart) is independently
  verified by `restart-promotion-summary.txt` measured fields.
- The negative-first rule (no `Ready=True` on a blocked path) is not
  violated; the regenerated report still claims `Ready=True` for the same
  volume that IS in fact ready, just with the wrong replica label.
- No surface across D5/D6 ever showed `Ready=True` against a stale or
  contradictory evidence set in a way that violated the Phase 32 contract.

D5/D6 still close PASS at the status-surface contract level; N1 is a
workflow polish for v0.3.3+.

## Verdict

Phase 32 D5 + D6 status surface sign-off **PASS**.

D5: promoted authority + publish target + epoch survives k3s restart;
status surfaces agree the volume is `Ready=True`; no old primary is
projected as Ready elsewhere. (One workflow N1 above.)

D6: 3 RF=3 PVCs maintain distinct identity across status surfaces; no
cross-volume authority mixup; no duplicate publish target; dashboard
preserves all three per-volume entries with HTTP 200.

D5 + D6 close the status-surface agreement requirement for the Phase 32
restart + multi-volume claims. D7 (stale-evidence bounded probe) and D8
(close gate) remain as the next two sign-off cycles.
