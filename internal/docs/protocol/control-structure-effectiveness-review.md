# Control Structure Effectiveness Review

Date: 2026-06-06

Status: working review for post-Phase 36 planning.

## Purpose

This review answers a product risk raised after the Phase 35/36 operations push:

```text
Are we improving the real control loop, or are we creating semantic loops that
sound coherent but do not add product capability?
```

The concern is valid. Seaweed Block spent significant effort building engine and
model vocabulary. Some of that effort became real gates and user-visible
behavior; some remains a design frame that must not be mistaken for shipped
capability.

## Review Standard

A control/model change is effective only if it connects all five layers:

```text
1. Live fact source
2. Product judgment
3. User-visible status or action
4. Independent test / TestOps gate
5. Failure evidence when the judgment is wrong or incomplete
```

If it only improves vocabulary or diagrams, it is useful design context but not
product capability.

## What Became Real Capability

### ManagedVolume Read Model

Real capability:

- `ManagedVolume` facts now feed report, dashboard, `operator-snapshot.json`,
  `ops explain`, CRD status, and Events.
- Blocked, stale, cleanup-required, and healthy paths share Condition/reason
  vocabulary.
- Phase 32, 35, and 36 gates proved no false `Ready=True` across multiple
  surfaces.

Why it is not purely abstract:

- It changed code under `core/ops`.
- It changed Helm/operator-status surfaces.
- It produced QA-visible failures when fields disagreed or when Events aborted
  reconciliation.
- It made support bundles replayable without SSH/log spelunking.

Remaining limitation:

- It is still mostly a read model. It does not yet own mutating lifecycle
  decisions.

### Negative-First Status

Real capability:

- `status_endpoint_unreachable` becomes `Ready=Unknown` /
  `EvidenceStale=True`.
- `csi_node_image_pull_failed` becomes `Ready=False` / `Blocked=True`.
- SmartWAL corruption no longer leaks as false `Ready=True`.

Why it is real:

- Dirty/live gates failed before fixes and passed after fixes.
- D4 SmartWAL corruption pushed fixes through storage, blockvolume readiness,
  and blockmaster projection.

Remaining limitation:

- Some specific reasons still terminate as generic `unknown` unless the fault
  is carried through all layers.

### Status-Only Operator Foundation

Real capability:

- CRDs exist.
- Optional controller writes `.status` and Events.
- RBAC prevents workload/storage/spec mutation.
- Event identity is bounded.
- Phase 36 adds node readiness, cleanup visibility, support refs, and safe next
  steps.

Why it is real:

- It runs in Kubernetes.
- Live QA validated `/status` writes and forbidden mutation verbs.
- QA caught and revalidated schema casing and Event conflict bugs.

Remaining limitation:

- It does not create CR objects.
- It does not own finalizers.
- It does not execute cleanup, repair, rebuild, failback, or upgrade.

## Where The Model Is Still Too Semantic

### Engine Vocabulary Without Product Closure

The engine docs define facts, context, invariants, actions, executors, and
evidence. That vocabulary is useful, but it becomes too abstract when a feature
does not prove:

- which live component owns each fact,
- which code computes the product judgment,
- which executor is allowed to act,
- which user-visible status proves the result,
- which TestOps gate fails if the model is bypassed.

Risk:

```text
logical closure in docs
but no capability closure in product
```

### Action Model Is Not Yet Real

Read-only action hints exist, but mutating actions are not implemented as a
complete loop:

```text
facts -> preconditions -> allowed action -> executor -> result -> evidence
```

Current state:

- `allowed_actions[]` and safe next steps are useful for explanation.
- They do not yet protect a real executor from unsafe mutation.

Therefore, any next mutating phase must make the action model concrete before
adding broad lifecycle features.

### Live Node Evidence Gap

Phase 36 projects node readiness, but live negative node facts are incomplete:

- missing CSI image / unregistered CSI driver can be masked,
- some negative node paths are replay-only,
- loopback/cross-node risk is not fully represented live.

This is not a philosophical issue. It is the next practical blocker because
future actions will depend on node truth.

## Product Direction

The direction is not "continue operations fixes forever".

The direction is:

```text
make the read-only control loop truthful
-> define the action loop
-> implement one bounded mutating lifecycle slice
-> then add larger protocol/data features
```

This explains why NVMe ANA, rebuild/failback, and backup/restore should not be
next. They would add more state and more failure modes before the existing
control loop can reliably say what is true and what action is safe.

## Recommended Next Phases

### Phase 37: Live Node Evidence Hardening

Goal:

Make node readiness blockers real, not replay-only, without expanding into a
general node-operations phase.

Required live facts:

- Kubernetes Node Ready / SchedulingDisabled.
- CSI node pod readiness.
- CSIDriver and node-plugin registration.
- Required image presence or image-pull status.
- iSCSI and multipath readiness.
- loopback publish-target cross-node blocker.

Why first:

- Read-only, low mutation risk.
- Directly improves status trust.
- Prevents future operator actions from executing on false node assumptions.

Estimated effort: small/medium.

### Phase 38: Lifecycle Action Model Review

Goal:

Turn the semantic action model into a concrete, executable contract before
mutation.

Required output:

- action types,
- fact requirements,
- preconditions,
- invariants,
- allowed executor,
- idempotency/retry behavior,
- failure status,
- evidence emitted,
- TestOps or unit/component gate for at least one no-op/dry-run action and one
  rejected action.

Why second:

- It prevents finalizer/delete safety from becoming another local script with
  nicer names.
- It prevents the review from becoming another semantic-only document.

Estimated effort: medium.

### Phase 39: Finalizer / Delete Safety

Goal:

Implement the first bounded mutating operator slice.

Scope:

- protect PVC/CRD deletion when residue would remain,
- remove or block deletion with explicit status,
- prove idempotency and cleanup evidence,
- keep repair/rebuild/failback out.

Why third:

- It is Kubernetes-product critical.
- It is smaller and safer than rebuild/failback.
- It validates the action model against a real executor.

Estimated effort: medium/high.

## Review Gate For Future Model Work

Before approving a new "model" phase, require a one-page answer:

```text
What live failure or user confusion does this model remove?
Which current code paths will change?
Which product status/action becomes more correct?
Which gate would fail before the model change and pass after?
What is explicitly still not claimed?
```

If those cannot be answered, the work is probably semantic cleanup rather than
product hardening.

## Bottom Line

The recent control-model work was not wasted: it produced real read-only
operations capability and caught real bugs. But the model is only half closed.
The next value is not more vocabulary. The next value is making the fact/action
loop concrete enough that the first mutating lifecycle behavior can be safely
implemented.
