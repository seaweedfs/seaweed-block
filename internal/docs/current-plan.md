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
- `ref/durable-root-layout-contract.md`
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

## Whole-Plan Delivery Gate

The plan is delivered when Seaweed Block moves from protocol-release-gated to
beta-hardening-release-gated.

Final suite:

```text
beta-hardening-gate
```

Required child gates:

1. `protocol-release-gate`
   - iSCSI P6,
   - NVMe P4,
   - NVMe P5,
   - iSCSI P8.
2. `csi-lifecycle-component-gate`
   - adversarial CSI controller/node component tests.
3. `durable-restart-reattach-chain`
   - dynamic PVC data survives `blockvolume` restart / reattach.
4. `returned-replica-reintegration-chain`
   - returned replica state and stale-primary fencing are explicit.
5. `operations-status-diagnostics-chain`
   - status, diagnostics, and cleanup evidence exist.
6. `cleanup-residue-chain`
   - no iSCSI sessions, NVMe subsystems, V3 processes, or K8s residue.

Pass criteria:

- all child gates PASS,
- suite-level `status.json` and `result.json` are consistent,
- `swblock validate-bundle --profile beta-hardening` returns VALID,
- shared product commit and runner commit are pinned,
- artifacts include status and diagnostic evidence,
- suite passes twice back-to-back in a clean lab,
- docs include claims and non-claims.

## TDD / Anti-Drift Rule

Every workstream must map to executable proof.

Rules:

- If behavior can be checked in unit/component tests, do not start with m01/m02.
- If Linux/K8s/kernel behavior is required, create or update a TestOps scenario
  before relying on manual QA.
- Every QA-found regression gets a component test unless the behavior is only
  observable through an OS/kernel/lab surface.
- Every runner suite must assert fields and artifacts, not only a PASS line.
- Every plan item maps to at least one of:
  - component test,
  - `subprocess` test,
  - runner scenario,
  - bundle validator check,
  - documented non-claim.

Gate matrix:

| Plan area | Local proof | Release proof |
|---|---|---|
| CSI lifecycle | `go test ./core/csi` adversarial cases | CSI lifecycle runner chain |
| Protocol readiness | protocol component + field tests | `protocol-release-gate` |
| Durable root/restart | storage contract tests | restart/reattach runner chain |
| Returned replica | state-machine component tests | reintegration runner chain |
| Operations layer | status/diagnostic schema tests | operations diagnostics chain |
| TestOps scale | runner unit tests + validate-bundle | suite status/result bundle |
| RDMA later | transport contract tests | perf/soak comparison gate |

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

## Workstream G: Operations Layer

Goal: make the system operable, not only testable.

Why this is separate:

- Data-path gates prove that I/O can work.
- Operations decides whether a user can install, observe, upgrade, repair, and
  safely clean up the system.
- Without this layer, the product can pass protocol tests and still be unsafe
  to run outside our lab.

Scope:

- install / upgrade / uninstall,
- generated `blockvolume` ownership,
- durable root management,
- volume and replica status,
- frontend path status,
- rebuild / reintegration progress,
- degraded/healthy conditions,
- diagnostics bundle,
- admin controls such as drain, force-detach, replace replica, and cleanup stale
  attachment.

Near-term steps:

1. Define the minimum operator-visible status model.
2. Define the generated-workload ownership model before writing a controller.
3. Add diagnostic bundle requirements to runner artifacts and product docs.
4. Keep admin actions explicit and conservative until fencing/reintegration
   facts are test-pinned.

Close bar:

- users can tell what is healthy, degraded, rebuilding, or stuck,
- cleanup and uninstall are product-owned, not harness-owned,
- a failed lab run produces enough status/artifacts for diagnosis without
  source-level debugging.

## Workstream H: Iterative Release And Feedback Loop

Goal: avoid building an enterprise block product in isolation.

Principle:

- Every hardening milestone should produce a narrow, usable release slice.
- Public docs must say what works, what is gated, and what is not claimed.
- TestOps evidence is necessary, but it is not a substitute for real users,
  issues, installs, and external feedback.

Release slices:

1. Alpha public slice:
   - single-node k3s,
   - dynamic PVC,
   - iSCSI default,
   - app write/read,
   - clean teardown,
   - visible non-claims.
2. Protocol preview slice:
   - optional NVMe,
   - iSCSI/NVMe release-gate evidence,
   - multipath/failover documented as lab-gated, not production HA.
3. Beta lab slice:
   - durable root layout,
   - restart/reattach survives,
   - CSI lifecycle hardening,
   - one-command TestOps validation.
4. Operations preview slice:
   - Helm/operator direction,
   - status conditions,
   - diagnostics bundle,
   - upgrade/uninstall path.
5. HA preview slice:
   - multi-node attach,
   - mounted failover,
   - stale-primary fencing,
   - returned replica state.
6. Performance preview slice:
   - baseline perf matrix,
   - backend pressure behavior,
   - optional RDMA/KV-backed data-plane experiment.

Open-source boundary idea:

- Keep the basic product and SSH/YAML runner open enough to earn trust.
- Keep advanced fleet/agent TestOps, large private scenario corpus, hosted
  validation, enterprise operations, advanced HA policy, and cloud-scale
  automation as possible enterprise layers.

Close bar:

- every roadmap phase has a user-visible slice,
- release notes include non-claims,
- feedback from real installs influences the next hardening step.

## Later Track: RDMA / Data Plane Acceleration

Goal: use RDMA only after the storage/control boundary is clear.

RDMA can help:

- lower CPU and latency for remote storage I/O,
- remote replica writes,
- rebuild / catch-up transfer,
- remote reads from storage nodes,
- shared RDMA KV-backed storage engine experiments.

RDMA does not fix:

- ambiguous authority,
- stale-primary fencing,
- CSI lifecycle bugs,
- unbounded WAL/replay behavior,
- unclear operator status,
- inefficient storage-engine write amplification.

Roadmap placement:

1. Current plan: define durable state, storage-engine contracts, operations, and
   recovery facts.
2. Next storage phase: split frontend/controller/storage responsibilities and
   define backend pressure/delta/image behavior.
3. Later acceleration phase: integrate RDMA/KV as an optional data plane and
   compare against TCP under the same correctness gates.

Non-claim:

- Do not sell RDMA as a substitute for storage-engine or operations hardening.

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
   - status: started 2026-05-09.
   - first contract defines `emptyDir` as throwaway smoke mode and hostPath
     state as durable lab mode.
4. Define returned-replica state-machine facts and tests.
5. Add a storage-engine coupling note and contract-test outline.
6. Add the operations-layer status model and release feedback loop to the
   public/internal roadmap.
7. Keep `protocol-release-gate` as a periodic/release gate, not a default
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
