# Current Plan: Phase 28 - Operational Reliability And TestOps Hardening

Status: active, 95% complete. Started on 2026-05-23 after Phase 27 D5/D6/D8
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
- Regression rerun after dmsetup residue coverage:
  `20260523-195058-3d4d`, PASS, 4/4 phases, 13/13 actions

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

Status: PASS on 2026-05-23.

Evidence:

- D3 mounted failover matrix:
  `results/phase28-d2-flake-d3-mounted-n5/flake-summary.txt`
  - `target_runs=5`
  - `pass_runs=5`
  - `fail_runs=0`
  - `flake_rate_percent=0`
  - Run IDs:
    `20260523-192731-dc0f`, `20260523-193114-03d0`,
    `20260523-193501-d275`, `20260523-193845-90bc`,
    `20260523-194232-8ea4`
- D4 interleaved failover matrix:
  `results/phase28-d2-flake-d4-interleaved-n5-r3/flake-summary.txt`
  - `target_runs=5`
  - `pass_runs=5`
  - `fail_runs=0`
  - `flake_rate_percent=0`
  - Run IDs:
    `20260523-191350-ae51`, `20260523-191644-fcb3`,
    `20260523-191915-0f86`, `20260523-192150-4a49`,
    `20260523-192433-362e`

Notes:

- The first D2 attempt surfaced a real cleanup gap: orphan multipath maps could
  remain mounted under stale kubelet CSI paths.
- Fix: cleanup verification can now opt into stale kubelet unmount +
  multipath/dmsetup flush via `SW_BLOCK_CLEANUP_MULTIPATH_FLUSH=1`.
- The mounted writer setup now waits for per-writer checksum evidence instead
  of treating Pod `Ready=True` as proof that the write completed.

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

Status: PASS on 2026-05-23.

Output:

- Backlog doc: `internal/docs/ref/testops-runner-action-backlog.md`
- Reference spike: `testops/scenarios/experimental-runner-native-pvc-loop.yaml`
- Spike run: `20260523-145417-4f50`, PASS, 22/22 actions

Decisions:

- Do not rewrite the scenario DSL now.
- Keep helper-script gates for complex orchestration where they produce better
  summaries and diagnostics.
- Promote repeated shell assertions into runner actions first:
  `assert_no_multipath_maps`, `kubectl_wait_jsonpath`,
  `kubectl_wait_completed`, `helm_install`, `helm_uninstall`,
  `assert_alua_aas_transition`, and `iscsi_assert_io_rejected`.

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

Status: PASS as a support-evidence contract on 2026-05-23.

Output:

- Contract doc:
  `internal/docs/ref/multi-volume-ha-support-evidence-contract.md`

Evidence audited:

- Mounted representative run:
  `20260523-194232-8ea4-helm-multi-volume-rf3-mounted-failover`
- Interleaved representative run:
  `20260523-192433-362e-helm-multi-volume-rf3-interleaved-failover`

Boundary:

- Current bundles already preserve stable field names for target volume,
  non-target stability, primary count, measured stale I/O probe, RTPG
  transition, and recovery method.
- Product dashboard/report integration should consume the same vocabulary, but
  this D4 slice does not claim a new dashboard feature.

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

Status: PASS as a structure review on 2026-05-23.

Output:

- `internal/docs/ref/phase28-structure-model-readiness-review.md`

## D6: Model Dependency Map

Goal: define which operations depend on a stable model before operator-grade
work starts.

Acceptance:

- Map report/dashboard/explain/operator dependencies onto the ManagedVolume
  model.
- Mark fields as stable, provisional, or test-only.
- Define how overlapping automata should be represented when node loss affects
  authority, CSI, host path, cleanup, and support evidence simultaneously.

Status: PASS as a model dependency map on 2026-05-23.

Output:

- `internal/docs/ref/phase28-structure-model-readiness-review.md`

## D7: Model Tightening Proposal

Goal: produce the next-release design proposal for tighter state ownership.

Principles:

- Truth owners publish facts.
- Orchestration entities make global decisions.
- Executors perform allowed actions.
- Evidence records why an action was allowed or refused.
- Small automata are useful only if their interaction with global context is
  explicit.

Status: PASS as a tightening proposal on 2026-05-23.

Output:

- `internal/docs/ref/phase28-structure-model-readiness-review.md`

## D8: Next Feature Readiness Review

Goal: decide when to start NVMe ANA, rebuild/failback, and backup without
destabilizing v0.3.x.

Acceptance:

- NVMe ANA starts only after iSCSI cleanup/repeatability is stable.
- Rebuild/failback starts only after stale-path and multi-volume isolation
  evidence is repeatable.
- Backup starts only after volume identity/model boundaries are stable.

Status: PASS as a readiness review on 2026-05-23.

Output:

- `internal/docs/ref/phase28-structure-model-readiness-review.md`

## Progress

- D1: PASS - multipath cleanup verifier catches orphan maps `20260523-182000-41ee`
- D2: PASS - D3/D4 Phase 27 N=5 matrices both `flake_rate_percent=0`
- D3: PASS - TestOps action backlog written
- D4: PASS - multi-volume support evidence field contract written
- D5: PASS - structure review written
- D6: PASS - model dependency map written
- D7: PASS - model tightening proposal written
- D8: PASS - next feature readiness review written
