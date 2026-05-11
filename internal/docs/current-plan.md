# Current Plan: Read-Only Operations Status Report Integration

Status: active. Started after closing
`finished-plans/phase4_finishedplan_fast_gates_operations_contract_prep.md` on
2026-05-11.

## Goal

Turn the component-level volume status report contract into useful read-only
operational evidence.

This is an observability report. It is not a V2-style block/data snapshot,
clone, backup, rollback point, or restore feature.

The goal is not to build an operator yet. The goal is to make one stable report
answer:

```text
What volume/replica is this host serving, what role does it believe it has,
what frontend facts are exposed, what durable lineage is latched, what peers
are known, and what residue would make a cleanup or failover decision unsafe?
```

This plan contributes to the product roadmap's operations layer by converting
facts currently scattered across status endpoints, TestOps artifacts, and host
residue checks into a single schema-controlled evidence object.

## Baseline

Closed phase 4 evidence:

- `core/ops.BuildVolumeStatusReport`: implemented.
- `go test -count=1 ./core/ops`: PASS.
- `operations-volume-status-report-component-gate`:
  - run id:
    `20260510-232649-03fd`,
  - product commit:
    `c8a27ac4ca35e4686420ce068bb67811b9a95fd9`,
  - result: PASS,
  - wall clock: `1.081s`,
  - bundle collected.
- `swblock validate` passes for all scenario YAMLs.

Relevant references:

- `ref/operations-volume-status-report-contract.md`
- `ref/beta-hardening-suite-cost-map.md`
- `product-roadmap.md`, Track F: Operations Layer

## Delivery Gate

This plan is complete when:

1. A product-side read-only collector can produce a `VolumeStatusReport` from
   existing status inputs without starting new authority paths.
2. The collector has component tests for:
   - happy-path primary with protocol frontends,
   - returned/non-primary replica with durable lineage but frontend-fenced,
   - missing peer/durable inputs represented explicitly,
   - residue arrays emitted as empty arrays instead of `null`.
3. A runner-native fast gate validates the collector path and collects a sample
   status report artifact.
4. No endpoint or command added in this plan mutates authority, lifecycle,
   storage, iSCSI/NVMe sessions, or Kubernetes resources.
5. Documentation states what an operator may inspect from the report and what
   it must not infer.

## Workstream A: Status Report Collector

Purpose: make the report useful outside pure unit tests while preserving the
read-only boundary.

Target shape:

- Define a small collector seam that accepts already-collected inputs:
  - master status response,
  - local status projection,
  - peer status,
  - durable status,
  - residue facts,
  - product/runner revision metadata.
- Keep collection and assembly separate:
  - assembly is `core/ops.BuildVolumeStatusReport`,
  - collection is a thin caller-owned layer,
  - no authority decisions inside ops.
- Prefer component tests with fake inputs before adding any live endpoint.

Non-goal:

- Do not add force-detach, cleanup, promote, demote, or restart controls.

## Workstream B: Runner Evidence Artifact

Purpose: make TestOps bundles carry the same operational evidence a human would
ask for during a failure.

Target shape:

- Extend the operations status report component gate or add a sibling gate that:
  - emits one JSON report artifact,
  - validates `schema_version`,
  - validates frontend identity fields including zero-valued `lun`,
  - validates explicit `unavailable` markers,
  - archives the artifact under the run bundle.

This remains a fast gate. It should stay seconds-scale and should not require
k3s, kernel initiators, or product process startup.

## Workstream C: Schema Boundary Hardening

Purpose: keep the operations schema stable as product DTOs evolve.

Known risk from review:

- `core/ops` currently imports host/volume, durable, replication, and control
  DTO packages directly. This is acceptable for the seed but can couple the ops
  schema too tightly to implementation DTOs if the package grows.

Tasks:

- Add JSON-shape tests for the fields operators care about.
- Decide whether to keep direct DTO imports or introduce a smaller
  `ReportInput` DTO layer owned by `core/ops`.
- Keep all schema changes append-only.

## Workstream D: Operations Non-Claims

Purpose: prevent the report from becoming an unsafe pseudo-operator.

Rules:

- Report is evidence only.
- Report can block unsafe action; it cannot authorize destructive action by
  itself.
- Any future admin control must have a separate protocol with fencing and
  authority semantics.
- A stale report must be treated as stale evidence, not current truth.

## Dev / QA Split

Developer handles:

- component tests,
- fast runner-native status report gate,
- schema and docs,
- reviewer-assisted changes.

QA handles:

- independent validation if the report is added to a full integration chain,
- ambiguous differences between report facts and live lab state,
- milestone evidence if this becomes part of a release gate.

Default rule:

```text
status report schema/component proof -> developer runs
status report in long integration/milestone suite -> QA validates
```

## Non-Claims

- This plan does not deliver a block/data snapshot feature.
- This plan does not deliver an operator.
- This plan does not claim HA.
- This plan does not define force detach.
- This plan does not change `beta-hardening-gate`.
- This plan does not introduce a public unauthenticated operations API.
