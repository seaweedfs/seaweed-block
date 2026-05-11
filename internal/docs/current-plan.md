# Current Plan: Beta Seed Stabilization And Cost Reduction

Status: active. Started after the first full `beta-hardening-gate` PASS on
2026-05-11.

The previous beta-hardening seed plan is archived in
`finished-plans/phase2_finishedplan_beta_hardening_seed.md`.

## Goal

Turn the first green beta seed suite into a stable, repeatable, lower-cost
release gate.

This plan is about proof quality and iteration speed, not adding another large
feature surface.

## Current Baseline

Closed evidence:

- `protocol-release-gate`: PASS and bundle-valid.
- `beta-hardening-gate`: first full PASS.
- Product commit for first beta green:
  `8822f20e91c2b88727ead9e49f9bf75eec28c791`.
- Runner commit for first beta green:
  `cf65daaf2ce5cf500e1efa48b411f7cb66dbac0b`.
- QA run id: `20260511-031605-8258`.
- `swblock validate-bundle --profile beta-hardening --expect-commit 8822f20`:
  VALID.

## Delivery Gate

This plan is complete when:

1. `beta-hardening-gate` passes twice back-to-back without manual cleanup.
2. The second bundle validates with `--profile beta-hardening`.
3. The second run records the same expected product commit and runner commit.
4. The final `cleanup-residue` child passes.
5. Any repeatability failure is either fixed or documented as a scoped product
   follow-up with a lower-level test.
6. The beta suite cost map is documented with at least one concrete
   componentization target for the next plan.

If the second run fails and a fix changes the product or runner commit, the
repeatability clock restarts at the new commit pair. The close claim must always
be two consecutive PASS runs at the same product commit and runner commit.

## Immediate Next Step

Assign QA one repeatability run:

```text
swblock suite --results-dir <beta-root> \
  --env product_root=/tmp/seaweed-block-plan-roadmap-refresh-devrun \
  --env ssh_key=C:/work/dev_server/testdev_key \
  testops/suites/beta-hardening-gate.yaml

swblock validate-bundle --profile beta-hardening \
  --expect-commit 8822f20 \
  <suite-run-dir>
```

Acceptance:

- suite status: PASS,
- 10/10 children PASS,
- bundle validator: VALID,
- `product_commit=8822f20e91c2b88727ead9e49f9bf75eec28c791`,
- `runner_commit=cf65daaf2ce5cf500e1efa48b411f7cb66dbac0b`,
- no manual cleanup between the prior green run and this run,
- final cleanup residue child passes.

## Workstream A: Repeatability Stamp

Purpose: prove the suite is stable enough to be a milestone gate.

Tasks:

- Wait for QA's second back-to-back beta run.
- If PASS:
  - record the second run id and wall clock here,
  - mark the seed suite stable,
  - keep `beta-hardening-gate` as the milestone suite.
- If FAIL:
  - classify as product, scenario, runner, or lab residue,
  - reproduce with the smallest child chain possible,
  - add a component test if the failure is not inherently Linux/K8s/kernel-only,
  - rerun only the failed child before asking QA for the full suite again.

## Workstream B: Suite Cost Map

Purpose: prevent the release gate from becoming the default developer loop.

Current beta suite cost is about 20-22 minutes.

Reference evidence:

- Cost map: `ref/beta-hardening-suite-cost-map.md`.
- Source run: `20260511-031605-8258`.
- Top cost drivers:
  - `iscsi-p8-compat-soak`: `659s` (`50.5%`),
  - `nvme-p5-csi-protocol`: `273s` (`20.9%`),
  - `csi-rf1-durable-restart`: `165s` (`12.6%`),
  - `nvme-p4-multipath-failover`: `138s` (`10.6%`).

Tasks:

- Record per-child wall clock from the first full green run.
- Classify each child:
  - keep integration,
  - componentize next,
  - split into smoke + deep gate,
  - periodic only.
- Identify the top 1-2 expensive children where assertions can move lower.

Top-candidate summary. The full ten-child classification is recorded in
`ref/beta-hardening-suite-cost-map.md`.

- `iscsi-p8-compat-soak`: keep as release/periodic; add smaller component gates
  for any specific failures it discovers.
- `nvme-p5-csi-protocol`: keep K8s path for protocol propagation; lower manifest
  rendering and lifecycle persistence checks into component tests.
- `nvme-p4-multipath-failover`: keep kernel multipath path; keep CMIC/NMIC/ANA
  field checks in component/assert helpers.

First componentization target for the next plan:

- `nvme-p5-csi-protocol` protocol propagation contract:
  - CSI protocol parameter extraction,
  - lifecycle `protocol` persistence,
  - launcher NVMe/iSCSI argument render shape,
  - stale-image/version gate behavior.
  - initial fast gate: `testops/scenarios/nvme-p5-protocol-component-gate.yaml`.

Second target:

- `csi-rf1-durable-restart` restart-state contracts around durable identity,
  status/projection refresh, and safe reattach.
  - initial fast gate:
    `testops/scenarios/csi-rf1-durable-restart-component-gate.yaml`.

## Workstream C: Operations Layer Prep

Scope for this plan: context and next-plan input only.

Purpose: make the product operable, not only testable.

Near-term scope:

- status model:
  - volume,
  - replica,
  - frontend,
  - peer/rebuild,
  - durable lineage,
  - cleanup residue.
- diagnostics bundle:
  - product version,
  - scenario provenance,
  - status endpoint snapshots,
  - host initiator state,
  - K8s resources,
  - cleanup actions.
- admin lifecycle:
  - install,
  - uninstall,
  - cleanup stale attachment,
  - force detach only after fencing semantics are pinned.

Deliverable for this plan:

- identify the first operations-layer contract to pull into the next active
  plan after repeatability closes.

This work should start as docs and component contracts before operator code, but
operator implementation is not in this plan's close gate.

## Workstream D: Mini-Protocol Stabilization

Scope for this plan: context and next-plan input only.

Purpose: keep lifecycle protocols explicit instead of hiding state transitions
inside logs or incidental readiness flags.

Mini-protocols to keep visible:

- authority: identity, epoch, assignment, stale-owner fencing,
- replication: peer probe, degraded/catching-up/healthy, returned replica,
- CSI: provision, publish, stage, mount, unstage, delete,
- iSCSI: login/session, ALUA metadata, SCSI I/O,
- NVMe: Connect, Identify, ANA, controller/namespace identity,
- TestOps: run lifecycle, provenance, cleanup, validation.

Rule:

```text
make each mini-protocol explicit, observable, and component-pinned before
extracting shared abstractions.
```

Near-term target:

- write the returned-replica state-machine note:
  - old primary returns fenced/superseded,
  - local durable recovery is not the same as authority readiness,
  - primary-side peer status owns rejoin evidence,
  - promotion eligibility remains gated by epoch/frontier rules.

Deliverable for this plan:

- decide whether returned-replica state-machine notes are the next active plan
  after repeatability, or whether suite cost reduction comes first.

## Workstream E: Future Storage And Performance Direction

Scope for this plan: context only.

Purpose: keep RDMA/delta/backend work grounded in correctness gates.

Current position:

- RDMA can improve data-plane latency/CPU once storage/control boundaries are
  clean.
- RDMA does not fix authority, fencing, CSI lifecycle, cleanup, or operations.
- Delta/image storage can improve write efficiency, but it needs explicit
  storage-engine contracts before frontend protocols depend on it.

Next storage work is not in this immediate plan unless a beta gate exposes a
backend-pressure bug.

## Dev / QA Split

Developer handles:

- component tests,
- scenario syntax,
- single-child runner checks,
- reviewer-assisted changes,
- small runner/product fixes.

QA handles by default:

- second full beta suite run,
- long soak,
- independent milestone validation,
- ambiguous lab behavior,
- Windows/Linux controller trust checks.

Only the second full beta suite run is an active QA assignment for this plan.
The other items are default ownership rules for later work.

Default rule:

```text
single child or fast proof -> developer runs
full suite / repeatability / trust-critical proof -> QA runs
```

## Non-Claims

- This plan does not claim production HA.
- This plan does not claim broad distro compatibility.
- This plan does not claim performance readiness.
- This plan does not deliver an operator.
- This plan does not replace user feedback or iterative release planning.
