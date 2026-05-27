# Current Plan: Phase 33 - TestOps Failure Hardening

Status: active, 10% complete. Started on 2026-05-27.

Branch: `phase33-testops-failure-hardening`

Base release: PR #50 / merge `8102cf3` (`v0.3.4-alpha` release baseline).

## Product Goal

Increase release confidence by proving Seaweed Block behaves correctly when
things fail, not only when the happy path works.

The user-facing rule for this phase:

```text
If the product cannot prove a volume is ready, it must not claim Ready=True.
It must surface a stable reason, collect useful evidence, and clean up
deterministically.
```

## Scope Contract

| In | Out |
|---|---|
| negative/failure TestOps scenarios | new HA feature claims |
| failure snapshot and support-bundle quality | rebuild/failback implementation |
| runner/helper primitives that reduce shell flake | NVMe ANA expansion |
| cleanup and residue assertions after failed runs | mutating operator/admin actions |
| stale/blocked/unreachable status-surface agreement | protocol/model rewrite |
| release note / roadmap claim alignment | production SLO/performance claims |

Small product fixes are allowed only when a failure gate exposes a release-risk
bug. Avoid broad refactors.

## D1: Failure Matrix And Plan

Goal: convert existing failure ideas into a small executable matrix.

Deliverables:

- Inventory 5-8 high-value failure classes from existing design docs,
  TestOps scenarios, and recent QA blockers.
- For each class, define:
  - trigger,
  - expected status,
  - stable reason code,
  - evidence files,
  - cleanup assertions,
  - whether it is release-blocking or exploratory.

Initial candidates:

- CSI image pull failure.
- blockmaster/API unreachable.
- stale evidence / stale bundle replay.
- corrupt or partial support-bundle evidence.
- cleanup residue after failed run.
- multi-volume cross-interference.
- restart during recovery or shortly after promotion.
- loopback publish target in a cross-node mount path.

Verify:

```text
matrix doc exists
each scenario has explicit pass/fail assertions
no broad HA claim added
```

## D2: Runner And Helper Hardening

Goal: reduce shell gymnastics and make failed runs self-explaining.

P0 items:

- Add or standardize failure snapshot capture.
- Add runner/helper wait for JSONPath-style conditions where existing actions
  cannot express PVC/Job/phase checks.
- Add deterministic Helm install/uninstall wrappers or scenario helpers.
- Add no-residue checks for iSCSI sessions, iSCSI node DB, multipath, dmsetup,
  Kubernetes resources, and product processes.

Verify:

```text
new/updated helper tests pass
existing happy-path gates still parse and run
failure artifacts are produced even when the trigger blocks progress
```

## D3: Negative Status Gates

Goal: prove blocked states do not become false Ready states.

Required surfaces for each negative gate:

- `summary.txt`
- `cluster-evidence.json`
- `operator-snapshot.json`
- dashboard `/operator-snapshot.json`
- `sw-block ops explain`

Required assertions:

- `Ready=True` is absent unless evidence proves readiness.
- blocked/stale/unavailable condition is present.
- reason code is stable and identical across surfaces.
- suggested actions are `read_only` or `dry_run`; no mutation is implied.

Verify:

```text
negative scenarios PASS
surface agreement table is produced
no false Ready=True in blocked/stale paths
```

## D4: Cleanup And Replay Gates

Goal: failed runs must still clean up and replay correctly.

Required assertions:

- Cleanup verifier returns `cleanup_status=ok` after scenario cleanup.
- Residue counters are zero across k8s, iSCSI session, iSCSI node DB,
  multipath, dmsetup, process, and test hostPath dimensions.
- Bundle replay prefers newest valid evidence and skips corrupt candidates.
- If evidence is stale or insufficient, replay returns `Ready=Unknown`, not
  `Ready=True`.

Verify:

```text
cleanup-residue gate PASS after negative scenarios
bundle replay gate PASS for corrupt/partial evidence
dashboard/report/explain agree after replay
```

## D5: Release Close

Goal: produce a compact release-hardening addendum.

Required inputs:

- Minimal new-user validation from `main` release baseline or this phase branch.
- Negative failure matrix PASS.
- Cleanup/replay gates PASS.
- No docs claim new product features beyond tested scope.

Close artifacts:

- QA sign-off under `internal/docs/qa-assignments/`.
- Finished plan under `internal/docs/finished-plans/`.
- Release note addendum if this becomes `v0.3.5-alpha`.

Acceptance:

```text
Phase 33 negative matrix PASS
minimal new-user validation PASS
cleanup/replay gates PASS
release claims updated or explicitly unchanged
```

## Current Progress

- 5%: branch created from merged `main`.
- 5%: `AGENTS.md` added to guide future agent behavior.
- 10%: Phase 33 scope, D1-D5 gates, and roadmap pointers drafted.

Next step: D1 failure matrix.
