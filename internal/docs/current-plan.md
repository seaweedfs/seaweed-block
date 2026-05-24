# Current Plan: Phase 28 - Productized Operations And Operator Foundation

Status: active, 85% complete. Started on 2026-05-23 after Phase 27 D5/D6/D8
independent QA reruns passed. Expanded on 2026-05-23 from a narrow
operational-hardening slice into the productized operations / operator
foundation plan.

## Product Goal

Turn the v0.3.x block product from "Helm plus scripts plus strong lab gates"
into a clearer Kubernetes product loop:

```text
install -> first PVCs -> multi-volume HA -> observe/explain -> support bundle
-> cleanup -> stable model -> operator foundation
```

Phase 28 deliberately treats operational reliability, state-model tightening,
and operator foundation as one plan. The reason is practical: an operator or
dashboard cannot be credible if cleanup, evidence, ManagedVolume state, and
reason codes are still scattered across scripts, launcher loops, CSI logs, and
TestOps helpers.

The working thesis:

```text
simple stable observable block
-> reliable gates and cleanup
-> one product evidence vocabulary
-> stable ManagedVolume / Condition model
-> read-only operator foundation
-> then mutating lifecycle, NVMe ANA, rebuild/failback, backup
```

## Scope Contract

| In | Out |
|---|---|
| cleanup residue detection and verifier hardening | new NVMe ANA product claim |
| TestOps runner action backlog and reference scenarios | backup/snapshot/restore implementation |
| flake matrix / repeatability gates | full operator implementation beyond read-only foundation |
| support-bundle/report consistency for multi-volume failures | broad production HA claim |
| structure review for control-plane/model tightening | large refactor without gates |
| small fixes that unblock reliability gates | mutating repair/promote/rebuild workflows |
| ManagedVolume / Condition field contract for operations | automatic rebuild/failback implementation |
| read-only operator foundation design and gated skeleton | physical-host-loss product claim expansion |
| ops/report/dashboard/CRD evidence vocabulary alignment | production SLO or broad scale claim |

## Priority

P0:

- D1 multipath stale-map cleanup verifier.
- D2 Phase 27 flake matrix repeatability gate.
- D3 TestOps runner action backlog from real scenario pain.
- D4 support-bundle/report consistency for multi-volume HA failures.
- D9 ManagedVolume operational model contract.
- D10 Kubernetes CRD / Condition / Event contract.
- D11 read-only operator foundation gate.
- D12 productized operations close gate.

P1:

- D5 structure review: where control-plane logic is currently spread across
  launcher, CSI, ops, scripts, and helper scenarios.
- D6 model dependency map: what the operator/dashboard/report must consume
  from a stable ManagedVolume / control model.
- D13 release packaging and docs alignment for the operator-foundation boundary.

P2:

- D7 model-tightening design proposal for the next major release.
- D8 next-feature readiness review for NVMe ANA, rebuild/failback, and backup.

## Claim Boundary

Allowed after Phase 28:

```text
The v0.3.x product has a clearer operations foundation: repeatable cleanup and
HA gates, product-owned support evidence, a stable ManagedVolume/Condition
contract, and a read-only operator foundation that can report install,
readiness, volume health, recovery evidence, and cleanup status without
inventing new truth.
```

Still not allowed:

```text
Production HA.
Arbitrary scale/SLO.
Mutating operator-managed lifecycle.
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
- Independent QA validation:
  `internal/docs/qa-assignments/phase28-operational-reliability-qa-validation.md`
  confirms D3 mounted N=5 and D4 interleaved N=5, both `flake_rate_percent=0`,
  with 510 failover-scenario actions and no failures.

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
  - current participant,
  - desired Fact Authority,
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

- Participants emit observations and Fact Authorities publish authoritative facts.
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

## Operational Foundation QA Addendum

Status: PASS on 2026-05-24 UTC.

Evidence:

- `internal/docs/qa-assignments/phase28-operational-reliability-qa-validation.md`

Scope:

- D1 cleanup-residue chain: `20260523-210004-bed1`, 13/13 PASS.
- D2 D3 mounted flake matrix: N=5, 5/5 PASS, `flake_rate_percent=0`.
- D2 D4 interleaved flake matrix: N=5, 5/5 PASS,
  `flake_rate_percent=0`.
- Direct host audit after QA: no multipath maps, no dmsetup devices, no iSCSI
  sessions, no sw-block pods/processes.
- PM-shape review of D3-D6 reference docs accepted with no blocking findings.

Boundary:

- This closes the operational reliability foundation of Phase 28.
- It does not close D9-D13, which are the expanded ManagedVolume / CRD /
  operator-foundation gates.

## D9: ManagedVolume Operational Model Contract

Goal: make ManagedVolume the stable read model for productized operations.

Why this belongs in Phase 28: the product already has strong PVC, failover,
report, and dashboard evidence, but some truth is still assembled from
launcher state, CSI observations, inventory snapshots, and helper summaries.
Operator-grade operations need one semantic projection that every surface can
consume.

Acceptance:

- Define the stable fields for a ManagedVolume:
  - identity: PVC/PV/volume ID, namespace, StorageClass,
  - placement: replicas, nodes, frontends, ports, protocol,
  - authority: primary, epoch, endpoint_version, primary_count,
  - data safety: RF, ack_profile, required frontier, candidate frontier,
  - host path: CSI reattach vs mounted multipath, RTPG/AAS evidence,
  - health: Ready/Degraded/Recovering/Blocked conditions,
  - support: reason_code, evidence_ref, timeline cursor.
- Mark fields as `stable`, `provisional`, or `test_only`.
- Map current `sw-block ops cluster`, `ops inventory`, `ops report`,
  dashboard, support bundle, and future CRD status to the same field names.
- Define dual-mode fact aggregation:
  - passive fact streams for steady-state observation,
  - bounded active probes at high-impact decision boundaries.
- Add a small regression test or golden fixture that catches renamed or missing
  core fields before docs/UI drift.

Status: dev complete, scoped tests PASS on 2026-05-23; QA validation pending.

Required output:

- `internal/docs/protocol/layered-participant-authority-master-executor-model.md`
- `internal/docs/ref/managed-volume-operational-model-contract.md`
- TDD/golden coverage for at least one healthy RF3 multi-volume case and one
  blocked/recovery case.

Implemented:

- `core/ops/managed_volume_contract.go`
- `core/ops/managed_volume_contract_test.go`
- stable field contract with Participant, Fact Authority, Master,
  aggregation mode, probe boundary, condition surface, and evidence
  requirement.

## D10: Kubernetes CRD / Condition / Event Contract

Goal: define the Kubernetes-native surface before writing a real operator.

This does not claim a production operator. It defines the API shape and the
status semantics that a read-only operator must expose.

Acceptance:

- Draft CRD status shape for the first operator-owned resources, likely:
  - `SwBlockCluster`
  - `SwBlockVolume` or `SwBlockManagedVolume`
- Define Conditions with stable reasons:
  - `Ready`
  - `Degraded`
  - `Recovering`
  - `Blocked`
  - `CleanupRequired`
- Define Kubernetes Events emitted from product-owned evidence, not from log
  scraping.
- Prove the CRD/Condition vocabulary round-trips from ManagedVolume fields.
- Keep the CRD scope read-only: no promote, repair, rebuild, delete, or cleanup
  actions in this phase.

Status: dev complete, scoped tests PASS on 2026-05-23; QA validation pending.

Required output:

- `internal/docs/ref/operator-crd-condition-event-contract.md`
- Example YAML snippets for one ready volume and one blocked volume.

Implemented:

- `core/ops/managed_volume_crd_contract.go`
- `core/ops/managed_volume_crd_contract_test.go`
- CRD resource contract for `SwBlockCluster` and `SwBlockVolume`.
- Condition vocabulary and Kubernetes Event severity mapping.
- Read-only/non-mutating action boundary.

## D11: Read-Only Operator Foundation Gate

Goal: create the first operator foundation without adding dangerous actions.

The operator foundation may start as a skeleton or prototype, but it must prove
the correct boundary: observe Kubernetes and sw-block, project status, emit
conditions/events, and never mutate storage authority.

Acceptance:

- Operator can be installed by Helm behind an explicit alpha flag, or a clear
  prototype path if code is not ready for chart inclusion.
- It reports cluster and volume status from the ManagedVolume model.
- It exposes Conditions/Events consistent with D10.
- It does not call mutating sw-block APIs.
- It is covered by a TestOps gate that validates:
  - install,
  - status projection,
  - blocked-bundle projection,
  - uninstall/cleanup.

Status: dev complete as a read-only operator snapshot gate on 2026-05-23;
QA validation pending.

Required output:

- `internal/docs/ref/read-only-operator-foundation-contract.md`
- `core/ops/operator_snapshot.go`
- `core/ops/operator_snapshot_test.go`
- `operator-snapshot.json` generated by `sw-block ops report`
- `/operator-snapshot.json` served by `sw-block ops dashboard`
- Internal review proving no mutating action path exists.

## D12: Productized Operations Close Gate

Goal: prove a user-facing operational loop, not just individual mechanisms.

Acceptance:

- Fresh cluster, documented install path.
- First volume and multi-volume smoke pass.
- `sw-block ops report` and dashboard show the same status vocabulary.
- ManagedVolume model and CRD/Condition contract describe the same volume
  state.
- Support bundle explains one healthy case and one blocked case.
- Cleanup verifier proves no Kubernetes, iSCSI, dm-multipath, or product-process
  residue.
- QA reruns the gate from clean state.
- PM review confirms the user-facing claim matrix is understandable and narrow.

Status: QA assignment written on 2026-05-23; QA/PM validation pending.

Required output:

- `internal/docs/qa-assignments/phase28-productized-operations-close-gate-assignment.md`
- Phase 28 close report after QA/PM validation.
- Updated README / quickstart / release note language for the operator
  foundation boundary.

## D13: Release Packaging And Claim Alignment

Goal: avoid shipping a strong internal plan with weak external packaging.

Acceptance:

- Publish immutable GHCR images for the Phase 28 consumable SHA.
- Update README, quickstart, and release note pins.
- State the boundary clearly:
  - Helm is the supported alpha install path.
  - Operator foundation is read-only/status-first unless D11 proves otherwise.
  - Mutating repair/rebuild/failback remains future work.
- Do not let `:alpha` mutable-tag evidence stand in for release evidence.

Status: docs prepared, image publication and final pin update pending.

Prepared output:

- `docs/releases/v0.3.3-alpha.md` draft release note.
- `docs/releases/README.md` updated with v0.3.2 and v0.3.3 boundaries.
- `README.md` and `docs/quickstart-kubernetes.md` mention the read-only
  operator snapshot without claiming a mutating operator.

Still required:

- D12 QA/PM close.
- Publish immutable GHCR images for the final Phase 28 close commit.
- Replace `<phase28-close-commit>` / prior pins with final SHA tags and
  digests.

## Progress

- D1: PASS - multipath cleanup verifier catches orphan maps `20260523-182000-41ee`
- D2: PASS - D3/D4 Phase 27 N=5 matrices both `flake_rate_percent=0`
- D3: PASS - TestOps action backlog written
- D4: PASS - multi-volume support evidence field contract written
- D5: PASS - structure review written
- D6: PASS - model dependency map written
- D7: PASS - model tightening proposal written
- D8: PASS - next feature readiness review written
- Operational foundation QA: PASS - `phase28-operational-reliability-qa-validation.md`
- D9: dev complete - layered model, ManagedVolume field contract, and contract
  tests added; `go test ./core/ops` PASS; QA validation pending
- D10: dev complete - CRD/Condition/Event contract and tests added;
  `go test ./core/ops` PASS; QA validation pending
- D11: dev complete - read-only operator snapshot artifact/API added;
  `go test ./core/ops ./cmd/sw-block` PASS; QA validation pending
- D12: QA assigned - productized operations close gate
- D13: docs prepared - image publication and final pin alignment pending
