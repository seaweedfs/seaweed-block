# Current Plan: Phase 34 - Test Realism And Dirty-Failure Hardening

Status: active, 85% complete. Started on 2026-05-29.

Branch: `phase33-testops-failure-hardening`

Base release: PR #50 / merge `8102cf3` (`v0.3.4-alpha` release baseline).
Phase 33 is closed in
`internal/docs/finished-plans/phase33_finishedplan_testops_failure_hardening.md`;
this file tracks the next realism pass on the same hardening branch.

## Product Goal

Raise release confidence by replacing self-proving or replay-only checks with
independent live evidence for the failure modes that most affect user trust.

User-facing rule:

```text
If Seaweed Block cannot positively prove a volume is safe and ready, it must not
claim Ready=True. Dirty storage faults, stale evidence, and unreachable status
paths must become explicit non-ready states with useful evidence.
```

## Scope Contract

| In | Out |
|---|---|
| live negative-status injection | new HA claims |
| restart convergence checks | rebuild/failback implementation |
| dirty SmartWAL corruption gate | NVMe ANA expansion |
| cross-checks between helper summaries and product/K8s facts | mutating operator/admin actions |
| cleanup after dirty-failure runs | production SLO/performance claims |
| narrow product fixes exposed by realism gates | broad control-plane rewrite |

Small product fixes are allowed only when a realism gate exposes a release-risk
bug. Avoid broad refactors.

## D1: Self-Proof Audit

Goal: identify checks that only grep helper-written summary fields and do not
independently prove product behavior.

Status: PASS.

Artifacts:

- `internal/docs/qa-assignments/phase34-self-proof-audit.md`
- `internal/docs/qa-assignments/phase34-test-realism-plan.md`

Acceptance:

```text
high-risk self-proof patterns are listed
each selected upgrade names the live signal that replaces or cross-checks it
scope avoids broad chaos matrix work
```

## D2: Live Status-Endpoint-Unreachable Gate

Goal: prove a live status collection failure becomes Unknown/EvidenceStale, not
Ready and not Blocked.

Status: PASS.

Artifact:

- `internal/docs/qa-assignments/phase34-d2-live-status-endpoint-unreachable-signoff.md`

Acceptance:

```text
status endpoint is blocked without killing the data path
Ready=True is absent
Blocked=True is absent for pure unreachable evidence
reason=status_endpoint_unreachable is consistent across surfaces
cleanup_status=ok
```

## D3: Restart Convergence Gate

Goal: prove restart observations do not stop at a transient Unknown state; the
status surface must eventually return to stable Ready when the volume is healthy.

Status: PASS.

Artifact:

- `internal/docs/qa-assignments/phase34-d3-restart-convergence-signoff.md`

Acceptance:

```text
restart may show transient Unknown
final status reaches Ready=True reason=first_volume_verified
Ready must be stable for consecutive polls, not a single flicker
no false Blocked=True
cleanup_status=ok
```

## D4: SmartWAL Dirty-Failure Gate

Goal: prove a real SmartWAL corruption is detected through the product surface
and never becomes false Ready=True.

Status: active; product fix chain landed, strict QA rerun pending.

Current code chain:

- `85d9375` storage fails closed on SmartWAL CRC mismatch.
- `954083a` blockvolume blocks local readiness after recovery fault while
  keeping status service diagnosable.
- `09aa6fe` master requires positive primary readiness evidence before
  projecting the published primary as healthy.
- `7fa34a7` ops projection test prevents ManagedVolume Ready projection without
  primary readiness evidence.

Artifacts:

- `internal/docs/qa-assignments/phase34-d4-smartwal-injection-precheck.md`
- `internal/docs/qa-assignments/phase34-d4-smartwal-corrupt-finding.md`
- `internal/docs/qa-assignments/phase34-d4-smartwal-corrupt-verify.md`
- `internal/docs/qa-assignments/phase34-d4-smartwal-corrupt-verify2.md`

Required QA rerun:

```text
testops/scenarios/helm-smartwal-corrupt-restart-chain.yaml
source commit: 7fa34a7 or newer on phase33-testops-failure-hardening
```

Acceptance:

```text
corruption evidence proves target_offset_inside_wal=true
corruption evidence proves target_offset_inside_extent=false
blockvolume logs a WAL integrity or durable recovery fault
operator-snapshot/report/dashboard do not show Ready=True after corruption
preferred: status is Blocked or Unknown with reason=wal_integrity_fault
acceptable for this slice: non-ready generic reason, if no surface lies Ready
cleanup_status=ok and all residue counters are zero
```

## D5: Cross-Validation And Noise Follow-Ups

Goal: reduce remaining weak evidence patterns without expanding into broad
chaos testing.

Status: planned after D4.

Candidates:

- Cross-check selected helper summary counts against independent Kubernetes or
  product evidence.
- Add timeline noise sanity for repeated identical `placement_verified` events.
- Decide whether one netem slow-replica gate is worth doing after D4 closes.

Acceptance:

```text
only high-value weak checks are upgraded
no RF/node-count permutation matrix
no broad chaos primitive sweep
```

## D6: Close And Release Claim Alignment

Goal: close Phase 34 and decide whether the result is part of `v0.3.5-alpha` or
the next hardening release.

Required inputs:

- D1-D4 sign-offs.
- D4 strict QA result after `7fa34a7`.
- Cleanup residue proof for dirty-failure run.
- Release wording that distinguishes:
  - live negative-status proof,
  - restart convergence proof,
  - SmartWAL dirty-failure proof,
  - remaining non-claims.

Acceptance:

```text
no false Ready=True in D2/D3/D4 surfaces
dirty-failure gate either passes or has an explicit product blocker
roadmap and release notes do not overclaim
finished plan moved under internal/docs/finished-plans/
```

## Current Progress

- 10%: self-proof audit and realism plan drafted.
- 25%: live status-endpoint-unreachable gate implemented and validated.
- 40%: restart convergence gate implemented and validated.
- 55%: V3-aware SmartWAL corruption injection precheck completed; old V2-style
  corruption primitive rejected as unsafe/self-proving.
- 65%: D4 live gate reached the real product assertion and found false
  Ready=True after SmartWAL corruption.
- 72%: storage layer changed from skip-on-CRC-mismatch to fail-closed.
- 78%: blockvolume now blocks local readiness after durable recovery fault.
- 83%: master projection now requires positive primary readiness evidence.
- 85%: ManagedVolume projection regression test added; strict D4 QA rerun is
  pending.

## Next Step

Ask QA to rerun:

```text
testops/scenarios/helm-smartwal-corrupt-restart-chain.yaml
```

against commit `7fa34a7` or newer. If it passes, write the Phase 34 D4 sign-off
and close D6. If it still reports Ready=True, treat the new evidence as the next
control-plane/status-surface blocker, not as a test flake.
