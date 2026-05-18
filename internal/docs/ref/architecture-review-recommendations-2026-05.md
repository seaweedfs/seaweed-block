# Seaweed Block Architecture Review Recommendations (2026-05)

Scope: current `sw-block` codebase, with emphasis on Kubernetes product path,
RF=2 recovery Stage 1, and near-term multi-node readiness.

## Executive Summary

The module layering is directionally correct. The main risks are not "wrong
architecture style" but missing product-critical closure in three seams:

1. publish-target selection is not node-aware/deterministic enough,
2. recovery/runtime failure surfaces still rely on panic in key paths,
3. Stage 1 recovery proof is not yet fully wired through attach selection and
   host-path evidence.

If these are fixed, the stack can support a credible Stage 1 product claim and
prepare cleanly for Stage 2 multipath.

## P0 Recommendations (Do First)

### R1. Make CSI publish target selection node-aware and deterministic

Problem:
- `LookupPublishTarget(ctx, volumeID, nodeID)` currently does not use `nodeID`
  in selection.
- `publishTargetFromStatus()` returns the first matching frontend instead of a
  policy-selected target.

Why this matters:
- Multi-node attach behavior will become ambiguous.
- Wrong target selection can break recovery even when authority is correct.

Suggested implementation:
- Introduce a `TargetSelectionPolicy` in `core/csi`:
  - input: `volume status`, `nodeID`, optional preferred protocol,
  - output: one selected target + reason.
- Selection order for Stage 1:
  1) assigned primary target for the requested node (or node-reachable alias),
  2) requested protocol match,
  3) highest generation `(epoch, endpoint_version)`,
  4) fail closed with typed reason if ambiguous.
- Keep authority-shaped fields out of `publish_context`, but include selection
  reason in logs/artifacts.

Acceptance:
- Unit tests covering:
  - node mismatch,
  - mixed iSCSI/NVMe frontends,
  - two candidates with different generation,
  - no valid target -> `ErrPublishTargetNotFound`.

### R2. Replace panic-based runtime failures with fail-closed typed errors

Problem:
- Some runtime code paths panic on invalid wiring or state.

Why this matters:
- Panic is acceptable in deep invariant tests, but dangerous in product loops.
- We need predictable refusal semantics and support-bundle evidence.

Suggested implementation:
- Keep constructor-time invariant checks, but convert operational panics to:
  - typed error,
  - explicit status degradation,
  - blocker reason exported to ops/inventory.
- Apply first to:
  - recovery sender sink wiring path,
  - authority store load/runtime failure path.

Acceptance:
- No panic in normal product control/data loop for expected misconfig/failure.
- Errors map to stable blocker reasons visible in inventory/support bundle.

### R3. Wire Stage 1 recovery gate to attach-target generation and required frontier

Problem:
- Candidate durable frontier is now visible, but writer required frontier is
  not yet fully compared in D4 claim path.

Why this matters:
- Without this check, recovery can look successful while violating durability
  semantics.

Suggested implementation:
- D4 runner captures:
  - pre-failure publish target generation `(replica, epoch, endpoint_version)`,
  - writer required frontier LSN,
  - candidate durable frontier LSN.
- Recovery claim only if:
  - candidate covers required frontier,
  - authority moved generation correctly,
  - recreated pod staged against new target generation,
  - post-failure checksum passes.

Acceptance:
- Gate emits explicit PASS/REFUSE with one blocker class:
  - `required_frontier_missing`,
  - `candidate_frontier_behind_required`,
  - `authority_promotion_missing`,
  - `host_path_recovery_not_verified`.

## P1 Recommendations (Next)

### R4. Introduce a lightweight capability matrix with code-owner linkage

Goal:
- Prevent doc drift between roadmap claims and runtime behavior.

Minimal shape:
- For each feature claim: `status`, `gate`, `owner`, `evidence path`,
  `non-claim sentence`.

### R5. Harden CSI node stage identity contract

Goal:
- Reduce ambiguity from local staging markers.

Plan:
- Bind stage identity to selected target generation evidence in node-local
  metadata and validate on restage/republish.

## What Not To Do Now

- Do not fold Stage 2 multipath (ALUA/ANA transparent switching) into Stage 1
  closure criteria.
- Do not expose manual promote as primary user workflow.
- Do not claim RF=2 recovery from authority move alone.

## Proposed 2-Week Execution Slice

Week 1:
- R1 target selection policy + tests.
- R2 panic-to-error conversion (first two hotspots) + blocker mapping.

Week 2:
- R3 D4 runner frontier/generation assertions.
- Ops artifact schema update and one QA dry run assignment.

Expected outcome:
- Stage 1 claim can be evaluated by a single gate with deterministic
  publish-target behavior and explicit refusal semantics.
