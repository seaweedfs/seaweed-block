# Current Plan: Phase 34 - Test Realism And Dirty-Failure Hardening

Status: active, 95% complete. Started on 2026-05-29.

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

Status: PASS on 2026-06-01.

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
- `internal/docs/qa-assignments/phase34-d4-smartwal-corrupt-verify3-PASS.md`

Passing QA rerun:

```text
testops/scenarios/helm-smartwal-corrupt-restart-chain.yaml
run 20260601-020747-5a1f, 30/30 PASS
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

- Surface the specific `wal_integrity_fault` reason through status evidence so
  cold operators do not need blockvolume logs to understand why a corrupted
  volume is Unknown.
- Preserve deterministic scheduling in future single-node dirty-failure gates:
  the D4 SmartWAL scenario now pins blockmaster to the scenario's `single_node`
  so stale local images on other lab nodes cannot affect the run.
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
- 92%: D4 strict rerun `20260601-020747-5a1f` passed 30/30. SmartWAL
  corruption now projects `Ready=Unknown` instead of false `Ready=True`.
- 95%: D4 scenario non-determinism fixed by pinning blockmaster to the
  scenario's `single_node`; D6 close report remains.

## Next Step

Write the Phase 34 D6 close report and release wording. The D4 product gate is
closed, with one product follow-up:

```text
surface reason=wal_integrity_fault instead of generic unknown
```

## Next Major Plan: Phase 35 - Kubernetes-Native Read-Only Operator Foundation

Phase 35 should start after Phase 34 D6 closes. It is the next productized
operations milestone, not an NVMe feature phase.

Goal:

```text
Make Seaweed Block look and behave like a normal Kubernetes storage product for
status and diagnostics: CRDs, Conditions, Events, and read-only reconciliation,
without adding mutating admin actions.
```

P0 scope:

- `SwBlockCluster` and `SwBlockVolume` CRDs.
- A status-only controller that writes `.status` and does not mutate storage.
- Projection of existing ManagedVolume statuses into Kubernetes Conditions:
  `Ready`, `Blocked`, `Recovering`, `Recovered`, `EvidenceStale`, and
  `CleanupRequired` where applicable.
- Kubernetes Events for the most important operator-visible transitions:
  `VolumeReady`, `CsiNodeImagePullFailed`, `AuthorityPromoted`,
  `EvidenceStale`, and cleanup-required warnings.
- Read-only boundary tests proving no promote, repair, rebuild, failback,
  delete-storage, or live cleanup action is executed by the controller.

P1 follow-up inside or immediately after Phase 35:

- Node readiness/preflight status in `SwBlockCluster.status.nodes[]`:
  iSCSI, multipath, image readiness, hostPath readiness, and observed version.
- Support-bundle pointers from status so `Blocked=True` gives a concrete next
  diagnostic command/evidence reference.
- Product-owned cleanup visibility: `CleanupRequired=True`, residue type, and
  safe next step. Automatic cleanup remains out of scope.

Out of Phase 35:

- NVMe ANA parity.
- Rebuild, reintegration, failback, and backup/restore.
- Finalizers and delete mutation.
- Upgrade execution.
- Production operator lifecycle claims.

Reasoning:

```text
NVMe should wait until the Kubernetes-native status/control foundation exists.
New protocol facts should plug into the same CRD/Condition/Event model instead
of creating another script-only or dashboard-only status path.
```
