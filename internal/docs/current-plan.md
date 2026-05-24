# Current Plan: Phase 28 - Operational Reliability And TestOps Hardening

Status: active, 30% complete. Started on 2026-05-23 after Phase 27 D5/D6/D8
independent QA reruns passed.

## Product Goal

Make the v0.3.x block product easier to validate, diagnose, clean up, and
extend before the next functional expansion. Phase 28 deliberately prioritizes
operational reliability and structure review over new user-facing storage
features.

The working thesis:

```text
simple stable observable block
-> reliable gates and cleanup
-> clearer TestOps/product interface
-> structure/model review
-> then operator, NVMe ANA, rebuild/failback, backup
```

## Scope Contract

| In | Out |
|---|---|
| cleanup residue detection and verifier hardening | new NVMe ANA product claim |
| TestOps runner action backlog and reference scenarios | backup/snapshot/restore implementation |
| flake matrix / repeatability gates | operator/CRD implementation |
| support-bundle/report consistency for multi-volume failures | broad production HA claim |
| structure review for control-plane/model tightening | large refactor without gates |
| small fixes that unblock reliability gates | mutating repair/promote/rebuild workflows |

## Priority

P0:

- D1 multipath stale-map cleanup verifier.
- D2 Phase 27 flake matrix repeatability gate.
- D3 TestOps runner action backlog from real scenario pain.
- D4 support-bundle/report consistency for multi-volume HA failures.

P1:

- D5 structure review: where control-plane logic is currently spread across
  launcher, CSI, ops, scripts, and helper scenarios.
- D6 model dependency map: what the operator/dashboard/report must consume
  from a stable ManagedVolume / control model.

P2:

- D7 model-tightening design proposal for the next major release.
- D8 next-feature readiness review for NVMe ANA, rebuild/failback, and backup.

## Claim Boundary

Allowed after Phase 28:

```text
The v0.3.x lab gates have stronger cleanup, repeatability, and diagnostic
coverage. TestOps gaps are documented with concrete action candidates. The
next operator/model work has an explicit dependency map.
```

Still not allowed:

```text
Production HA.
Arbitrary scale/SLO.
Operator-managed lifecycle.
NVMe ANA parity.
Backup/snapshot/restore.
Automatic rebuild/failback.
```

## D1: Multipath Cleanup Verifier

Goal: fail cleanup when orphan dm-multipath maps remain after iSCSI sessions
and Kubernetes resources are gone.

Reason: Phase 27 QA repeatedly found stale maps such as:

```text
mpathbp (363141082d2f7caa1) dm-2 ##,##
size=1.0M features='0' hwhandler='0' wp=rw
```

The old verifier caught normal Seaweed maps (`SeaweedF`, `BlockVol`,
`io.seaweedfs`) but missed orphan maps that lost vendor/IQN identity.

Acceptance:

- `scripts/verify-helm-cleanup.sh` captures `multipath.after-cleanup.txt`.
- It writes `multipath-residue.after-cleanup.txt`.
- It fails on normal Seaweed maps or orphan `mpath... ##,##` maps.
- `cleanup-summary.txt` includes `multipath_residue_count`.
- `cleanup-residue-chain.yaml` runs the verifier directly.

Status: PASS on 2026-05-23.

Evidence:

- Red check: direct verifier run against the orphan `mpathbp ... ##,##` map
  failed with `cleanup_status=failed`, `multipath_residue_count=1`, and
  `failure_count=1`.
- Green check: after removing the stale map, verifier passed with
  `cleanup_status=ok`, `multipath_residue_count=0`, and `failure_count=0`.
- Scenario: `testops/scenarios/cleanup-residue-chain.yaml`
- Run: `20260523-182000-41ee`
- Result: PASS, 4/4 phases, 13/13 actions

Fix included:

- `scripts/verify-helm-cleanup.sh` now writes
  `multipath-residue.after-cleanup.txt`.
- It fails on normal Seaweed maps and orphan `mpath... ##,##` maps.
- `cleanup-summary.txt` now includes `multipath_residue_count`.
- `cleanup-residue-chain.yaml` invokes the cleanup verifier directly and no
  longer requires a `.git` checkout when run from a tar-synced tree.

## D2: Phase 27 Flake Matrix

Goal: convert D7 from dev-pass to QA/nightly evidence.

Acceptance:

- Run D3 and D4 at least `N=5` each using
  `scripts/run-phase27-flake-matrix.ps1`.
- Artifact includes `flake-summary.txt` and `flake-summary.json`.
- Required release-grade result: `flake_rate_percent=0`.
- Failures preserve run IDs and bundle paths.

Status: pending.

## D3: TestOps Runner Product Interface

Goal: turn the runner-native PVC spike into a concrete TestOps backlog instead
of rewriting mature scenarios prematurely.

Inputs:

- `internal/docs/qa-assignments/testrunner-product-interface-audit.md`
- `testops/scenarios/experimental-runner-native-pvc-loop.yaml`

Acceptance:

- Decide which runner actions should be first-class:
  - `helm_install` / `helm_uninstall`
  - `kubectl_wait_jsonpath`
  - `kubectl_wait_completed`
  - `assert_no_multipath_maps`
  - ALUA AAS transition assertion
  - stale-path read/write rejection assertion
- Keep current helper-script gates where they carry complex orchestration.
- Add one runner-native smoke gate only where it improves clarity.

Status: pending.

## D4: Multi-Volume Support Bundle Consistency

Goal: make support/report artifacts explain multi-volume HA failures as clearly
as the Phase 27 summaries do.

Acceptance:

- `sw-block ops report` / dashboard / explain output carries consistent names
  for:
  - target volume
  - non-target volume stability
  - primary count
  - stale I/O probe result
  - RTPG/host-path transition
  - recovery method: CSI reattach vs mounted multipath
- Failed bundles preserve the same fields where possible.
- No mutating action is introduced.

Status: pending.

## D5: Structure Review

Goal: identify where product control-plane logic is spread too widely and what
should become explicit entities or state machines.

Review targets:

- Helm / install lifecycle.
- Launcher / BlockVolume lifecycle.
- CSI publish/stage observation.
- Recovery and promotion orchestration.
- Host-path/multipath evidence.
- ManagedVolume model projection.
- TestOps helper scripts.

Output:

- A short structure review with:
  - current owner,
  - desired truth owner,
  - executor,
  - evidence source,
  - risk if left mixed.

Status: pending.

## D6: Model Dependency Map

Goal: define which operations depend on a stable model before operator-grade
work starts.

Acceptance:

- Map report/dashboard/explain/operator dependencies onto the ManagedVolume
  model.
- Mark fields as stable, provisional, or test-only.
- Define how overlapping automata should be represented when node loss affects
  authority, CSI, host path, cleanup, and support evidence simultaneously.

Status: pending.

## D7: Model Tightening Proposal

Goal: produce the next-release design proposal for tighter state ownership.

Principles:

- Truth owners publish facts.
- Orchestration entities make global decisions.
- Executors perform allowed actions.
- Evidence records why an action was allowed or refused.
- Small automata are useful only if their interaction with global context is
  explicit.

Status: pending.

## D8: Next Feature Readiness Review

Goal: decide when to start NVMe ANA, rebuild/failback, and backup without
destabilizing v0.3.x.

Acceptance:

- NVMe ANA starts only after iSCSI cleanup/repeatability is stable.
- Rebuild/failback starts only after stale-path and multi-volume isolation
  evidence is repeatable.
- Backup starts only after volume identity/model boundaries are stable.

Status: pending.

## Progress

- D1: PASS - multipath cleanup verifier catches orphan maps `20260523-182000-41ee`
- D2: pending - Phase 27 N>=5 flake matrix
- D3: pending - TestOps action backlog
- D4: pending - support/report consistency
- D5: pending - structure review
- D6: pending - model dependency map
- D7: pending - model tightening proposal
- D8: pending - next feature readiness review
