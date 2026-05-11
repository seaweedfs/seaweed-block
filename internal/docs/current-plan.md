# Current Plan: Fast Gates And Operations Contract Prep

Status: active. Started after the repeatable `beta-hardening-gate` claim on
2026-05-11.

The previous plan is archived in
`finished-plans/phase3_finishedplan_beta_seed_stabilization.md`.

## Goal

Turn the repeatable beta milestone suite into a practical development workflow:

- keep the 20-23 minute `beta-hardening-gate` as a milestone/release gate,
- use fast component gates for deterministic protocol and lifecycle shape bugs,
- prepare the first operations-layer contracts without starting operator work.

This plan is about iteration speed and proof structure. It does not expand the
product feature surface.

## Current Baseline

Closed milestone evidence:

- `beta-hardening-gate` run 1:
  `20260511-031605-8258`, PASS, 10/10, `21m46s`, bundle VALID.
- `beta-hardening-gate` run 2:
  `20260511-040412-ac38`, PASS, 10/10, `22m55s`, bundle VALID.
- Product commit for the repeatability claim:
  `8822f20e91c2b88727ead9e49f9bf75eec28c791`.
- Runner commit for the repeatability claim:
  `cf65daaf2ce5cf500e1efa48b411f7cb66dbac0b`.
- No manual cleanup between runs; no residue after run 2.

Forward-work commits after that baseline added fast gates:

- `testops/scenarios/nvme-p5-protocol-component-gate.yaml`
- `testops/scenarios/csi-rf1-durable-restart-component-gate.yaml`
- `testops/scenarios/operations-volume-status-snapshot-component-gate.yaml`

These gates are for developer iteration. They do not replace the milestone
suite until they have their own validation history.

Fast gate validation evidence at product commit
`b926b7e50c522665b66a81a2990a3fe925364365`:

| Gate | Run ID | Result | Wall Clock | Bundle |
| --- | --- | --- | ---: | --- |
| `nvme-p5-protocol-component-gate` | `20260510-214940-d726` | PASS | `1.486s` | collected |
| `csi-rf1-durable-restart-component-gate` | `20260510-214947-206b` | PASS | `1.034s` | collected |
| `operations-volume-status-snapshot-component-gate` | `20260510-222618-0452` | PASS | `1.113s` | collected |

The first two fast gate runs recorded
`pin_build/git.sha=b926b7e50c522665b66a81a2990a3fe925364365` and empty
`pin_build/git.status`.

The operations snapshot gate recorded
`pin_build/git.sha=c4426ca1d0ad46d47773c1cd1185edd9f944cf4f` and empty
`pin_build/git.status`.

Static validation:

- `swblock validate testops/scenarios/nvme-p5-protocol-component-gate.yaml`:
  VALID.
- `swblock validate testops/scenarios/csi-rf1-durable-restart-component-gate.yaml`:
  VALID.
- `swblock validate
  testops/scenarios/operations-volume-status-snapshot-component-gate.yaml`:
  VALID.

## Delivery Gate

This plan is complete when:

1. The fast component gates run successfully through `swblock run` on m02 from
   a clean product checkout. Done for the first two gates at `b926b7e`; done
   for the operations snapshot gate at `c4426ca`.
2. Each fast gate validates with `swblock validate` and collects a complete run
   bundle. Done for run IDs `20260510-214940-d726`,
   `20260510-214947-206b`, and `20260510-222618-0452`.
3. The current plan documents when to run:
   - unit/component tests,
   - fast runner-native component gates,
   - child integration chains,
   - full beta milestone suite.
4. The beta suite remains unchanged unless there is a deliberate decision to add
   one or both fast gates.
5. The first operations-layer contract is selected for the next plan, with
   required fields and non-claims documented.

If either fast gate fails on m02, fix the gate or the product contract before
considering full-suite changes.

## Workstream A: Fast Gate Validation

Purpose: prove the fast gates are usable by developers without QA acting as a
manual command runner.

Tasks:

- Validate locally:
  - `go test -count=1 ./core/csi ./core/host/master ./core/lifecycle ./core/launcher`
  - `go test -count=1 -run <rf1-fast-regex> ./internal/testops ./core/host/master ./core/host/volume ./core/launcher`
  - `swblock validate` for fast gate YAML files.
- After the branch is present on m02, run:
  - `swblock run testops/scenarios/nvme-p5-protocol-component-gate.yaml`
  - `swblock run testops/scenarios/csi-rf1-durable-restart-component-gate.yaml`
  - `swblock run
    testops/scenarios/operations-volume-status-snapshot-component-gate.yaml`
- Record run IDs, wall clocks, and whether the collected bundles include
  `pin_build/git.sha`, `git.status`, and the Go test JSON/log artifact.

Developer owns these runs by default. QA is not needed unless the run exposes a
lab-only or ambiguous failure.

## Workstream B: Developer Loop Contract

Purpose: make test selection explicit so engineers do not default to the full
suite.

Target shape:

- Unit/component test:
  - run before every code change commit when the touched package has coverage,
  - no m02 dependency,
  - expected runtime: seconds.
- Fast runner-native component gate:
  - run before pushing changes that affect CSI/lifecycle/launcher/restart
    contracts,
  - uses m02 only for runner parity and shared artifact shape,
  - expected runtime: under a few minutes.
- Child integration chain:
  - run when changing Linux/K8s/kernel/initiator behavior,
  - examples: `nvme-p5-csi-protocol-chain`,
    `csi-rf1-durable-restart-chain`.
- Full beta suite:
  - run for milestone readiness, release candidates, or after multiple child
    chains changed,
  - not the default debug loop.

Deliverable:

- Add or update docs so this selection rule is visible from `testops/README.md`
  and this plan.

Implementation:

- `testops/README.md` now includes a "Which Gate To Run" table that maps
  package-local, fast component, child integration, and milestone/release
  changes to the smallest appropriate gate.
- The README also records the beta-hardening repeatability evidence so the
  full suite is clearly a milestone gate, not the default developer loop.

## Workstream C: Operations Contract Selection

Purpose: start the operations layer from observable facts, not operator code.

Selected first contract:

- `volume status snapshot`

Reference: `ref/operations-volume-status-snapshot-contract.md`.

Required fields:

- product version / git revision,
- volume id,
- replica id,
- protocol frontends,
- authority role,
- replication role,
- durable lineage,
- frontend readiness,
- peer health,
- cleanup residue hints.

Non-claims:

- no force-detach semantics until fencing is pinned,
- no HA operator promise,
- no performance dashboard,
- no cloud-scale scheduler.

Deliverable:

- choose the first operations contract and write it as a reference note for the
  next active plan.

Implementation seed:

- `core/ops.BuildVolumeStatusSnapshot` assembles the selected contract from:
  - master `StatusResponse` frontend facts,
  - local volume `StatusProjection`,
  - replication peer status,
  - durable volume status,
  - synthetic residue inputs.
- Missing role/revision inputs are represented as explicit `unavailable`
  strings rather than silent zero-value success.
- Component validation:
  - `go test -count=1 ./core/ops`: PASS.
- Runner-native fast gate:
  - `testops/scenarios/operations-volume-status-snapshot-component-gate.yaml`
    added; static `swblock validate` PASS, m02 run
    `20260510-222618-0452` PASS in `1.113s`.

## Workstream D: Mini-Protocol Notes

Purpose: keep the product's concurrent protocols explicit.

Protocols currently visible:

- authority: identity, epoch, assignment, stale-owner fencing,
- replication: peer probe, degraded/catching-up/healthy, returned replica,
- CSI: provision, publish, stage, mount, unstage, delete,
- iSCSI: login/session, ALUA metadata, SCSI I/O,
- NVMe: Connect, Identify, ANA, controller/namespace identity,
- TestOps: run lifecycle, provenance, cleanup, validation.

Near-term note:

- returned-replica state machine:
  - old primary returns fenced/superseded,
  - local durable recovery is not authority readiness,
  - primary-side peer status owns rejoin evidence,
  - promotion eligibility remains gated by epoch/frontier rules.

This is documentation/component-contract work only in this plan.

## Dev / QA Split

Developer handles:

- fast component gates,
- single-child runner checks,
- scenario syntax,
- reviewer-assisted changes,
- small runner/product fixes.

QA handles:

- full beta suite repeatability,
- long soak,
- independent milestone validation,
- ambiguous lab behavior,
- trust checks across Windows/Linux controller environments.

Default rule:

```text
single child or fast proof -> developer runs
full suite / repeatability / trust-critical proof -> QA runs
```

## Non-Claims

- This plan does not claim production HA.
- This plan does not claim broad distro or kernel compatibility.
- This plan does not claim performance readiness.
- This plan does not deliver an operator.
- This plan does not change the beta milestone suite unless explicitly decided.
- This plan does not replace user feedback or iterative release planning.
