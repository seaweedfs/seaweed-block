# Current Plan: Beta Hardening, V2 Parity, And TestOps Scale

Status: active.

This plan starts after the iSCSI/NVMe protocol-readiness slice closed on
2026-05-09. The project now has release-gated protocol frontends; the next work
is to turn that into a credible beta-quality block product.

## Decision

- Treat iSCSI and NVMe-oF as guarded product capabilities, not active bring-up.
- Use V2 as a coverage and behavior inventory, not as code to blindly copy.
- Move expensive Linux/K8s/kernel validation into runner-native TestOps suites.
- Keep default `go test` focused on unit and component seams.
- Split storage-engine responsibilities before pursuing hardware acceleration
  or application-semantic protocols.

## Reference Inputs

- `product-roadmap.md`
- `docs/roadmap.md`
- `finished-plans/phase1_finishedplan_frontend_protocol_readiness.md`
- `ref/v2-frontend-protocol-gap-audit.md`
- `ref/iscsi-v2-coverage-gap-audit.md`
- `ref/nvme-v2-coverage-gap-audit.md`
- `ref/storage-layer-architecture-learning.md`
- `ref/production-readiness-plan.md`

## Current Baseline

Closed protocol evidence:

- `iscsi-p6-alua-failover-chain`: PASS.
- `nvme-p4-multipath-failover-chain`: PASS.
- `nvme-p5-csi-protocol-chain`: PASS.
- `iscsi-p8-compat-soak-chain`: PASS.
- Runner-native `protocol-release-gate` suite: PASS and bundle-valid.

Known product constraints:

- Evidence is still primarily single-node lab evidence.
- Generated `blockvolume` workloads still depend on alpha/harness paths in
  several flows.
- `walstore` is still the MVP backend.
- Durable state layout, restart semantics, returned-replica lifecycle, and
  operator ownership are not yet beta-complete.
- Protocol gates prove frontend correctness; they do not prove broad distro
  compatibility, long soak, performance, upgrade safety, or production HA.

## Operating Insights

### Integration tests are expensive by design

Linux initiators, mounts, K8s, `iscsiadm`, `nvme-cli`, cleanup state, and
multi-process timing belong in TestOps. They are necessary, but they should be
release gates, not the default developer loop.

Default development loop:

```text
unit/component go test -> local subprocess tag when needed -> runner suite
```

Rules:

- default `go test`: pure unit/component coverage,
- `go test -tags subprocess`: local binary wiring on loopback,
- `swblock suite`: real Linux/K8s/lab integration.

When a runner failure happens, first ask whether the assertion can move down to
a component seam: authority projection, lifecycle rendering, protocol adapter,
ready assignment, frontend state, or storage-engine contract.

### V2 parity means behavior parity

V2 is useful because it encodes years of protocol and lifecycle edge cases.
V3 should reuse the inventory and expectations, while preserving V3 boundaries:

```text
placement intent != authority
authority movement != data continuity
frontend ready != replica ready
heartbeat observation != rebuild completion
best-effort ACK != full durability
```

### Storage architecture is the next leverage point

Lakebase/Neon-style systems win by moving the application/storage boundary, not
by making a generic block device magically understand database WAL. V3 should
not claim transparent Postgres WAL reduction through iSCSI/NVMe.

What does apply:

- block-delta foreground writes,
- background block-image materialization,
- bounded replay chains,
- compaction and checkpointing independent of frontend protocol state,
- clean interfaces between frontend, replica controller, and storage engine.

## Workstream A: Test Layering And Release Gates

Goal: keep the developer loop fast while retaining real product evidence.

Tasks:

- Maintain `protocol-release-gate` as the top-level product readiness gate for
  iSCSI + NVMe + CSI + soak.
- Keep `swblock validate-bundle --profile protocol-release-gate` as the
  post-run trust check.
- Convert expensive Go subprocess tests to either:
  - focused component tests, or
  - explicit `subprocess` tests only when binary wiring is the actual subject.
- Add component-level tests for each runner-discovered regression before
  accepting long-term runner-only coverage.
- Keep runner suites responsible for:
  - Linux kernel initiators,
  - K8s PVC lifecycle,
  - mounted filesystem workloads,
  - process cleanup,
  - artifact bundle provenance.

Near-term steps:

1. Audit current `subprocess` tests and tag each as keep / componentize /
   replace-by-runner.
2. Add a short scenario-to-component map for P4, P5, P6, and P8.
3. Make slow gates opt-in locally and required only in release/QA workflows.

Close bar:

- default targeted Go packages stay under a few seconds,
- every release-gate failure has a lower-level follow-up test or an explicit
  reason it cannot be lowered,
- QA can run one suite and validate one bundle for protocol readiness.

## Workstream B: V2 Parity Closure

Goal: close the remaining behavior/test gaps that matter for beta.

Focus areas:

- CSI node lifecycle:
  - NodeStage / NodeUnstage idempotency,
  - failed login cleanup,
  - mkfs failure cleanup,
  - node-plugin restart cleanup,
  - wrong-volume-at-staging-path guards,
  - concurrent stage/unstage.
- Durable backend pressure:
  - repeated larger writes,
  - slow backend behavior,
  - WAL retention pressure,
  - explicit error policy under full/slow storage.
- Frontend regression breadth:
  - iSCSI session stress,
  - NVMe queue and namespace identity consistency,
  - protocol-neutral CSI dispatch invariants.
- Host compatibility:
  - Ubuntu remains primary,
  - add at least one second Linux distro or kernel profile before beta claim,
  - Windows initiator remains optional unless explicitly product-scoped.

Near-term steps:

1. Refresh the V2 gap audits now that iSCSI/NVMe protocol readiness is closed.
2. Mark each gap as closed / component-test needed / runner-gate needed /
   intentionally deferred.
3. Pull the highest-value CSI lifecycle gaps into a focused PR.
4. Pull backend pressure gaps into storage-engine tests before adding new
   protocol features.

Close bar:

- no active P0/P1 V2 parity gaps without an owner or deferral reason,
- CSI lifecycle has adversarial component coverage,
- backend pressure behavior is named and test-pinned.

## Workstream C: Kubernetes Productization

Goal: stop depending on harness behavior for product lifecycle.

Tasks:

- Define durable root layout for generated `blockvolume` workloads.
- Replace default `emptyDir` with explicit durable-state configuration for
  non-throwaway scenarios.
- Add product-owned controller/operator behavior for generated workloads.
- Keep owner-reference cleanup, but do not rely on smoke scripts as the
  lifecycle controller.
- Make install and uninstall behavior repeatable:
  - image names,
  - required host modules,
  - privileged mounts,
  - cleanup expectations,
  - result diagnostics.

Near-term steps:

1. Write the durable root layout contract.
2. Add a lab scenario proving blockvolume pod restart preserves data.
3. Define the minimum operator/controller loop before implementing it.
4. Update alpha manifests only after the lifecycle contract is explicit.

Close bar:

- dynamic PVC data survives `blockvolume` pod restart,
- generated workloads are applied/removed by product-owned logic,
- install/cleanup docs match actual behavior.

## Workstream D: Availability, Recovery, And Reintegration

Goal: make failover and returned-replica behavior explicit enough for beta.

Tasks:

- Define returned-replica states:
  - observed,
  - candidate,
  - syncing/rebuilding,
  - ready,
  - fenced/stale.
- Define ACK profiles:
  - best-effort,
  - quorum,
  - full-ack,
  - unavailable/degraded policy.
- Pin stale-primary fencing behavior:
  - old primary must not accept writes after losing authority,
  - standby metadata sessions remain allowed where protocol requires them.
- Add rebuild/reintegration tests below the K8s surface first.
- Add mounted workload tests only after state-machine facts are component-pinned.

Near-term steps:

1. Write the returned-replica state-machine note.
2. Add component tests for promotion, stale primary, and returned replica.
3. Add a runner scenario only after component tests define the expected facts.

Close bar:

- state-machine facts are visible in status/artifacts,
- mounted failover remains green,
- returned replica can rejoin without unsafe writes or ambiguous readiness.

## Workstream E: Storage Engine Boundary

Goal: prepare for better backend behavior without turning frontend protocols
into storage engines.

Tasks:

- Separate interfaces conceptually, then in code:
  - frontend target,
  - replica controller,
  - storage engine,
  - compactor/checkpointer.
- Define block-delta write path:
  - foreground append,
  - background materialization,
  - bounded read replay,
  - compaction safety.
- Keep database-semantic protocols out of the block frontend scope until the
  block storage core is mature.
- Do not claim Postgres WAL reduction from generic block storage.

Near-term steps:

1. Document current `blockvolume` coupling points.
2. Add storage-engine contract tests around write, flush, image, replay, and
   compaction boundaries.
3. Prototype delta/image behavior behind an explicit backend gate only after
   contract tests exist.

Close bar:

- frontend protocol code no longer needs to know backend compaction details,
- storage pressure behavior is test-pinned,
- backend experiments do not change iSCSI/NVMe semantics silently.

## Workstream F: TestOps Platform Direction

Goal: make the product family testable by scenario contract, not manual lab
memory.

Tasks:

- Keep the open/basic runner surface useful:
  - YAML scenarios,
  - SSH execution,
  - result bundles,
  - run control,
  - bundle validation.
- Keep advanced agent/fleet capabilities as future optional scope:
  - remote agents,
  - shared KV/FUSE control plane,
  - binary distribution cache,
  - elastic AWS-scale test clusters,
  - long-running scenario corpus.
- Keep product-specific scenario corpus separate from platform primitives where
  possible.

Near-term steps:

1. Move repeated patterns into platform primitives:
   - `pin_build`,
   - `consume_pin`,
   - `assert_revision_matches`,
   - `collect_remote_bundle`,
   - `assert_protocol_shape`,
   - `assert_no_residue`.
2. Keep the protocol release suite as the reference scenario.
3. Add dashboard-friendly run summaries only after the schema is stable.

Close bar:

- one command can run and validate product readiness,
- result bundles are self-contained enough for QA and developer triage,
- stale image / wrong commit / missing child evidence fails before debugging
  product code.

## Immediate Sequence

1. Refresh V2 gap audits against the now-closed protocol baseline.
   - status: started 2026-05-09.
   - first pass updates `ref/v2-frontend-protocol-gap-audit.md`,
     `ref/iscsi-v2-coverage-gap-audit.md`, and
     `ref/nvme-v2-coverage-gap-audit.md` so closed protocol-readiness work is
     not re-opened as active beta work.
2. Pick one high-value CSI lifecycle gap and implement it as component-first.
   - status: started 2026-05-09.
   - first component seam: conflicting CSI protocol parameters now fail closed
     instead of silently preferring one key.
3. Write the durable root layout contract for generated `blockvolume`
   workloads.
4. Define returned-replica state-machine facts and tests.
5. Add a storage-engine coupling note and contract-test outline.
6. Keep `protocol-release-gate` as a periodic/release gate, not a default
   developer test.

## QA Assignments To Prepare Next

- Durable restart / reattach validation:
  - prove data survives `blockvolume` pod restart under dynamic PVC.
- Returned-replica reintegration validation:
  - prove stale primary fencing and safe returned-replica state.
- Multi-node attach validation:
  - prove pod on a non-host-local node can attach through advertised frontend
    address.
- Protocol release gate repeatability:
  - keep as regression evidence after significant frontend/CSI changes.

## Non-Claims

- This plan does not claim production HA.
- This plan does not claim broad distro compatibility.
- This plan does not claim performance readiness.
- This plan does not claim transparent database WAL reduction.
- This plan does not require an immediate process split.
- This plan does not replace the public roadmap; it is the internal execution
  driver.
