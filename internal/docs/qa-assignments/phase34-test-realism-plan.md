# Phase 34 Plan - Test Realism And Anti Self-Proving Gates

Status: proposed on 2026-05-29.

Purpose: raise the product test bar from "the scenario can grep the helper's
summary" to "independent evidence proves the product behavior." Phase 33 remains
valid as failure-path hardening; Phase 34 targets the remaining realism gaps.

## Problem Statement

The largest remaining testing risk is not raw coverage count. It is tests that
are too close to self-proof:

```text
helper writes summary value X
scenario greps summary value X
test passes
```

That pattern can be useful as a smoke check, but it is weak release evidence
unless another independent source verifies the same claim.

## Test Realism Levels

| Level | Name | What It Proves | Release Value |
|---|---|---|---|
| L0 | Self-proof | A helper wrote the string the scenario greps | Low; avoid for hard claims |
| L1 | Replay / component | Given evidence, product surfaces explain it correctly | Good for status/report/dashboard contracts |
| L2 | Live injection | Real fault triggers product observation and classification | Required for P0 failure claims |
| L3 | Adversarial timing | Fault plus concurrency, restart, partial failure, or dirty state does not violate invariants | Required before broad reliability claims |

Phase 34 should reduce L0 hard assertions, keep L1 where appropriate, and
upgrade selected high-value L1/L2 gates to L2/L3.

## Scope

In:

- TestOps scenario changes.
- Runner/helper assertions that add independent evidence.
- Live status endpoint unreachable injection.
- Dirty-failure tests using existing chaos primitives where available.
- Restart convergence assertions.

Out:

- New storage features.
- New public HA claims.
- Operator mutation/admin actions.
- Full chaos matrix for every primitive.
- Large RF/node-count permutation testing.

## D1: Self-Proof Audit And Cross-Validation Plan

Goal: identify summary-only assertions that support hard claims and classify
them by risk.

Deliverable:

- `internal/docs/qa-assignments/phase34-self-proof-audit.md`

Acceptance:

- List at least 10 representative summary-grep assertions.
- Mark each as:
  - acceptable smoke,
  - needs independent cross-check,
  - should be replaced.
- Pick 3-5 high-value assertions to harden first.

Examples to audit:

- `managed_volume_count=3`
- `reader_verified_count=3`
- `cleanup_status=ok`
- `old_primary_stale_io_success_count=0`
- `cross_volume_authority_mixup=false`

Rule:

```text
If a field gates a product claim, validate it against an independent source:
kubectl, operator-snapshot JSON, product event stream, host probe, or direct IO.
```

## D2: F2b Live Status-Endpoint-Unreachable Gate

Goal: upgrade Phase 33 F2 from L1 replay to L2 live injection.

Scenario shape:

```text
helm install
-> create first PVC
-> verify Ready=True first
-> discover replica status_addr
-> block only the status port, not iSCSI data
-> collect report/explain/dashboard
-> assert Ready=Unknown + EvidenceStale/status_endpoint_unreachable
-> assert not Blocked unless another concrete blocker exists
-> remove network rule
-> cleanup zero residue
```

Important boundary:

- Do not kill `blockvolume`.
- Do not block iSCSI data path (`3260`).
- Block only status endpoint traffic, e.g. status port around `23260`.

Expected status:

```text
status=unknown
Ready=Unknown
reason=status_endpoint_unreachable
EvidenceStale=True or equivalent stale/unreachable condition
Blocked=True must be absent
Ready=True must be absent
mutation_allowed=false
```

Acceptance:

- TestRunner scenario PASS.
- Product surfaces agree across summary, operator-snapshot, explain, and
  dashboard.
- Cleanup restores network rules and leaves zero residue.

## D3: Restart Convergence Gate

Goal: distinguish "safe transient Unknown" from "eventual recovery."

Current gap:

- Existing restart replay can correctly show `Ready=Unknown` during early
  reconvergence.
- It does not always prove the product eventually returns to `Ready=True`.

Scenario addition:

```text
after k3s/product restart
-> capture immediate status; Unknown is allowed
-> poll report/operator-snapshot for up to 90s
-> require final Ready=True reason=first_volume_verified or equivalent
   for 3 consecutive polls
-> verify primary/epoch/publish target did not roll back
```

Acceptance:

- Immediate Unknown is not failure if evidence is still stale.
- Final non-convergence is failure.
- A single transient Ready=True is not enough; the status must remain Ready for
  3 consecutive polls to avoid passing on Ready/Unknown flicker.
- No old primary is surfaced as Ready during the window.

## D4: Corrupt WAL Dirty-Failure Gate

Goal: cover a storage-native dirty failure, not just clean Kubernetes failures.

Hard prerequisite - D4-0:

The existing `corrupt_wal` TestRunner primitive must not be used blindly for
V3. It was originally shaped around a V2 durable-file layout with a fixed
superblock plus large WAL region. V3 `smartwal` uses a different layout:

```text
[header][walSlots * recordSize][block data extents]
```

Before any D4 assertion is allowed:

- Locate the real V3 smartwal store file under the Helm hostPath durable root.
- Read or derive the V3 smartwal header/record layout.
- Prove the injected bytes land inside the WAL ring, not inside data extents.
- Capture an artifact that records file path, WAL offset, WAL length, injected
  offset, and before/after byte sample.
- Run with `restartPersistence: hostpath`; `emptyDir` mode is invalid for this
  gate because the durable file disappears across pod restart.

If D4-0 cannot prove V3-aware WAL corruption, D4 must be marked blocked rather
than producing a false green test.

Scenario shape:

```text
create durable volume
write data
stop blockvolume cleanly or at controlled point
use TestRunner corrupt_wal on that volume's durable path
restart product
assert recovery refuses unsafe replay or surfaces explicit recovery failure
assert no silent Ready=True on corrupted evidence
assert cleanup zero residue
```

Expected outcome:

- The exact reason code may need implementation alignment, but it must be
  stable and explicit, e.g. `wal_corrupt`, `recovery_evidence_invalid`, or
  `durable_recovery_failed`.
- The product must not silently mount or report Ready if the WAL cannot be
  trusted.

Acceptance:

- Direct evidence of corruption injection is captured.
- Product emits a stable failure reason.
- No false Ready=True.
- Support bundle/replay explains the failure.

## D5: Optional Netem Slow-Replica Gate

Goal: test partial network degradation without exploding the matrix.

Scenario shape:

```text
RF3 sync-quorum
-> inject_netem latency/loss on one replica status/data path
-> write workload
-> assert foreground behavior is bounded and explainable
-> assert no indefinite hang
-> cleanup netem and residue
```

Priority:

- P1 for Phase 34 unless D2-D4 finish quickly.
- Use one scenario only. Do not create a RF/node-count matrix.
- Do not start D5 until D4-0 has closed. The same realism rule applies: the
  fault must affect the intended path, not merely run a chaos primitive.

## D6: Event Noise Sanity

Goal: prevent dashboard/timeline from being drowned by repeated identical
events.

Check:

```text
For a short first-volume run, repeated identical (volume_id, event_type,
reason_code) events should remain below a documented threshold.
```

Acceptance:

- Establish a threshold rather than zero duplicates.
- If exceeded, classify as product/event-emission debt, not test flake.

## D7: Close Gate

Minimum close requirements:

- D1 audit complete.
- D2 F2b live status-unreachable PASS.
- D3 restart convergence PASS.
- D4 corrupt WAL dirty-failure PASS, or explicitly blocked with a concrete
  implementation issue because:
  - the product lacks a stable dirty-recovery reason code, or
  - the injection primitive is not yet proven against the V3 smartwal layout.
- Existing v0.3.5 user path still PASS.

Close report must classify each hard claim by realism level:

```text
L1 replay
L2 live injection
L3 adversarial timing / dirty failure
```

## Release Impact

Phase 34 should not block `v0.3.5-alpha` if Phase 33 is already accepted. It is
the right next hardening phase before any broader reliability or production-like
claims.

If Phase 34 closes cleanly, a later release may claim stronger language:

```text
Failure evidence is tested through live injection and selected dirty-failure
paths, not only bundle replay.
```

Still do not claim production readiness, rebuild/failback, backup/restore, or
broad SLOs.
