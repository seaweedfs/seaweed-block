# Current Plan: Phase 29 - Lifecycle/Cleanup Reliability Hardening

Status: active, 40% complete. Started on 2026-05-24 after Phase 28 D13
release packaging closed with immutable image publication.

## Product Goal

Turn cleanup and lifecycle handling from "helper-script behavior" into a stable
product-owned contract that is repeatable under multi-volume HA scenarios.

Phase 29 focus:

```text
install -> run multi-volume HA loops -> cleanup
-> residue check is deterministic
-> evidence vocabulary is stable
-> no helper TOCTOU race masks real state
```

This phase does not add new protocol or HA semantics. It hardens reliability of
the existing product loop.

## Scope Contract

| In | Out |
|---|---|
| product-owned cleanup contract and residue semantics | new NVMe ANA product claim |
| helper TOCTOU cleanup fixes (`run-multi-volume-*`) | rebuild/failback implementation |
| cleanup/report/dashboard field alignment for lifecycle outcomes | backup/snapshot/restore implementation |
| deterministic cleanup gates for RF3 multi-volume paths | broad production HA claim |
| small code fixes required to pass reliability gates | operator mutating lifecycle actions |

Principle: only small, release-reliability fixes are allowed. No broad model
refactor in this phase.

## Claim Boundary

Allowed after Phase 29:

```text
For the documented alpha loops, cleanup outcome is deterministic and auditable:
the same run either proves clean residue with stable evidence fields or fails
closed with explicit reason/evidence.
```

Still not allowed:

```text
Broad upgrade safety.
Production HA.
Mutating operator lifecycle.
Rebuild/failback or backup claims.
```

## D1: Cleanup Ownership Inventory

Goal: explicitly map which cleanup steps are product-owned vs helper-owned,
then define the migration boundary.

Acceptance:

- Add ownership matrix covering:
  - Helm resources,
  - Kubernetes workload objects,
  - iSCSI sessions/nodes,
  - dm-multipath/dmsetup residue,
  - support artifacts.
- For each row define:
  - executor,
  - evidence file,
  - failure reason code,
  - retry/idempotence rule.
- Mark temporary helper-owned rows that must move later.

Expected output:

- `internal/docs/ref/phase29-cleanup-ownership-matrix.md`

Status: PASS on 2026-05-24.

Output:

- `internal/docs/ref/phase29-cleanup-ownership-matrix.md`

Coverage:

- Helm release and chart-scoped Kubernetes resources.
- Demo pods, PVC/PV objects, generated blockvolume Deployments.
- iSCSI sessions and node records.
- dm-multipath / dmsetup residue.
- run-scoped hostPath residue, product processes, and support artifacts.

The matrix assigns truth owner, current executor, evidence artifact, failure
reason code, retry/idempotence rule, and migration target for each resource
class.

## D2: Helper TOCTOU Cleanup Fixes

Goal: remove known helper races where cleanup success/failure can be reported
inconsistently.

Primary target:

- `scripts/run-multi-volume-example.sh` post-loop cleanup TOCTOU follow-up from
  Phase 28.

Secondary targets (only if surfaced by D3 gates):

- `scripts/run-multi-volume-reattach-recovery.sh`
- `scripts/run-multi-volume-mounted-failover.sh`

Acceptance:

- Replace race-prone immediate checks with bounded wait + terminal flag
  semantics.
- `cleanup_status` and `deployments_gone` (or equivalent) are consistent.
- Failures keep diagnostics instead of timing out silently.

Status: PASS on 2026-05-24 for the primary target.

Implementation:

- `scripts/run-multi-volume-example.sh` now normalizes generated
  blockvolume Deployment listing through `list_blockvolume_deployments`.
- Cleanup success is recorded by a `deployments_gone=true` terminal flag
  observed inside the wait loop.
- The helper no longer performs the race-prone second `kubectl | grep`
  observation after success.
- The poll uses a deadline plus one final normalized observation before
  timeout diagnostics.

Evidence:

- Initial green run: `20260524-140609-c204`, PASS, 29/29 actions.
- N=3 regression:
  - `20260524-141408-35e3`, PASS, 29/29 actions.
  - `20260524-141615-7be6`, PASS, 29/29 actions.
  - `20260524-141814-83f6`, PASS, 29/29 actions.

Secondary targets remain gated by D4 only if those scenarios surface the same
pattern.

## D3: Lifecycle Evidence Contract Tightening

Goal: ensure lifecycle outcomes are represented by one stable vocabulary across
summary/report/dashboard.

Acceptance:

- Define required lifecycle fields:
  - `cleanup_status`
  - `k8s_residue_count`
  - `iscsi_residue_count`
  - `multipath_residue_count`
  - `process_residue_count`
  - `failure_count`
  - `failed_phase` (when non-green)
- Verify field parity across:
  - scenario summary text,
  - `sw-block ops report` summary,
  - dashboard artifacts from bundle replay.
- Missing required fields fail gate.

Expected output:

- `internal/docs/ref/phase29-lifecycle-evidence-contract.md`

## D4: Deterministic Cleanup Gates (QA-facing)

Goal: prove cleanup determinism for both normal and stressed multi-volume HA
paths.

Required reruns:

- `testops/scenarios/helm-multi-volume-rf3-readiness-chain.yaml`
- `testops/scenarios/helm-multi-volume-rf3-reattach-recovery-chain.yaml`
- `testops/scenarios/helm-multi-volume-rf3-mounted-failover-chain.yaml`
- `testops/scenarios/helm-multi-volume-rf3-interleaved-failover-chain.yaml`
- `testops/scenarios/cleanup-residue-chain.yaml`

Acceptance:

- All pass with explicit cleanup evidence.
- No active iSCSI sessions.
- No Seaweed residue multipath/dmsetup devices.
- No sw-block pods/process residue after uninstall path.
- Any failure case produces explicit reason/evidence (not timeout ambiguity).

## D5: Close Gate

Goal: close only when reliability is verified by independent QA replay.

Acceptance:

- D1-D4 outputs complete.
- QA rerun report confirms deterministic cleanup evidence.
- `internal/docs/qa-assignments/phase29-...-close-report.md` written.
- `internal/docs/finished-plans/phase29_finishedplan_...md` written.

## Next-Phase Entry Criteria

Phase 30 (model/control-plane hardening) or rebuild/failback planning can start
only when:

- Phase 29 cleanup determinism is green in independent QA reruns.
- No known helper TOCTOU cleanup bug remains open for active alpha loops.
- Lifecycle evidence vocabulary is stable across summary/report/dashboard.

## Progress

- D1: PASS - cleanup ownership matrix written
- D2: PASS - primary multi-volume cleanup TOCTOU fixed, N=3 regression green
- D3: pending
- D4: pending
- D5: pending
