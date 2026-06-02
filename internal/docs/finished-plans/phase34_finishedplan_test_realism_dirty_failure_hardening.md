# Finished Plan: Phase 34 - Test Realism And Dirty-Failure Hardening

Closed: 2026-06-02

Verdict: PASS.

## Delivered Claim

Phase 34 hardens the existing Helm/PVC/read-only-ops alpha product by replacing
selected replay-only or self-proving checks with live evidence and one dirty
storage failure:

```text
unreachable status endpoint
-> no false Ready=True

k3s restart reconvergence
-> transient Unknown allowed
-> final stable Ready required

real SmartWAL corruption
-> storage fails closed
-> blockvolume withholds local readiness
-> master projection refuses Ready without positive primary readiness
```

This phase is not a new feature release. It is a correctness and release-trust
pass over negative-first status behavior.

## Evidence

- D1 self-proof audit:
  - `internal/docs/qa-assignments/phase34-self-proof-audit.md`
  - `internal/docs/qa-assignments/phase34-test-realism-plan.md`
- D2 live status endpoint unreachable:
  - `internal/docs/qa-assignments/phase34-d2-live-status-endpoint-unreachable-signoff.md`
  - PASS
  - pure status-path failure becomes non-ready/unknown evidence, not false
    ready and not blocked data-path failure.
- D3 restart convergence:
  - `internal/docs/qa-assignments/phase34-d3-restart-convergence-signoff.md`
  - PASS
  - restart may show transient Unknown but must converge to stable Ready.
- D4 SmartWAL dirty-failure:
  - run `20260601-020747-5a1f`
  - PASS, 30/30 actions
  - `target_offset_inside_wal=true`
  - `target_offset_inside_extent=false`
  - no `Ready=True` after corruption
  - cleanup residue zero
  - final report:
    `internal/docs/qa-assignments/phase34-d4-smartwal-corrupt-verify3-PASS.md`

Key code-chain evidence:

- `85d9375` storage fails closed on SmartWAL CRC mismatch.
- `954083a` blockvolume blocks local readiness after durable recovery fault
  while keeping status service diagnosable.
- `09aa6fe` master requires positive primary readiness evidence before
  projecting the published primary as healthy.
- `7fa34a7` prevents ManagedVolume Ready projection without primary readiness
  evidence.
- `bad6d7f` closes D4 scenario determinism by pinning blockmaster scheduling for
  the SmartWAL corruption gate.

## User-Facing Impact

- A status endpoint that cannot be reached no longer allows a stale or guessed
  `Ready=True`.
- Restart evidence must converge back to stable `Ready=True`; a single flicker
  is not enough.
- SmartWAL corruption no longer silently recovers through the status surface as
  a healthy volume.
- The product now follows a stronger rule:

```text
Ready=True requires positive primary readiness evidence.
Reachable process heartbeat alone is not enough.
```

## Important Non-Claims

- Not production-ready.
- No new HA claim.
- No rebuild, reintegration, or failback implementation.
- No backup/snapshot/restore.
- No mutating operator/admin/dashboard action.
- No NVMe ANA parity expansion.
- No broad chaos matrix, performance, RTO, RPO, or SLO claim.
- SmartWAL corruption currently surfaces as non-ready generic `unknown`; the
  more specific `wal_integrity_fault` reason is a follow-up.

## Followups

- Surface `reason=wal_integrity_fault` through product status so cold operators
  do not need blockvolume logs to understand the corruption reason.
- Continue replacing helper-summary self-proof checks with independent
  Kubernetes/product cross-checks only where release value is high.
- Consider one carefully scoped netem slow-replica gate after the
  Kubernetes-native operator status foundation exists.
- Start Phase 35: Kubernetes-native read-only operator foundation.
