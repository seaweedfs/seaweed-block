# Protocol Design And Hardening

This directory is the working protocol-design entry point for Seaweed Block.

The goal is not to create a large abstract architecture library. The goal is to
keep product behavior explainable and testable as the Kubernetes, CSI, host
path, failover, repair, and dashboard surfaces grow.

## Source Lineage

This directory distills the V3 design references under:

- `C:\work\seaweedfs\sw-block\design\v3-architecture.md`
- `C:\work\seaweedfs\sw-block\design\v3-runtime-state-machines-overview.md`
- `C:\work\seaweedfs\sw-block\design\v3-invariant-ledger.md`
- `C:\work\seaweedfs\sw-block\design\protocol-anti-patterns.md`
- `C:\work\seaweedfs\sw-block\design\v3-recovery-algorithm-consensus.md`

Those documents remain the deeper historical source. The files here are the
current-product operating copy for v0.3+ planning.

## Design Thesis

Seaweed Block should use:

```text
one product read model centered on ManagedVolume
+ many local projection controllers / small automata
+ invariant ledger
+ anti-pattern guardrails
+ cross-controller model tests
```

Do not build one giant state machine. Do not scatter unrelated local state
machines. The product needs a shared volume-centered fact model and small
engines that project from it.

## Documents

- [`control-model-principles.md`](./control-model-principles.md)
  defines the truth-domain model and the rules for large context plus small
  automata.
- [`managed-volume-operations-model.md`](./managed-volume-operations-model.md)
  defines the Phase 22 center of gravity: a PVC-backed `ManagedVolume` read
  model that composes K8s, CSI, authority, replica, host-path, workload, and
  evidence facts.
- [`layered-participant-authority-master-executor-model.md`](./layered-participant-authority-master-executor-model.md)
  defines the reusable Participant / Fact Authority / Master / Executor
  hierarchy: participants emit observations, fact authorities publish
  authoritative facts, masters compute collective state and allowed actions,
  executors perform side effects, and a domain master may become a fact
  authority for the next higher layer.
- [`engine-design-guidelines.md`](./engine-design-guidelines.md)
  defines the reusable engine/control-plane method: facts, context,
  multi-state projections, invariant checks, action contracts, executor
  boundaries, and evidence.
- [`invariant-ledger.md`](./invariant-ledger.md)
  starts the current-project invariant ledger. Any new protocol claim should
  add or update a row here.
- [`protocol-anti-patterns.md`](./protocol-anti-patterns.md)
  copies the important anti-patterns into the current project vocabulary.
- [`phase22-control-context-plan.md`](./phase22-control-context-plan.md)
  proposes the next hardening phase: ManagedVolume Operations Model.
- [`operations-state-dependency-review.md`](./operations-state-dependency-review.md)
  defines the Phase 22 scope decision: operations v1 over a real product read
  model, not a pure dashboard pass or narrow protocol refactor.

## Review Rule

Any behavior change that affects authority, recovery, CSI attach, host path,
Kubernetes node behavior, or support evidence should answer:

1. Which Fact Authority publishes the new fact?
2. Which invariant proves the behavior?
3. Which anti-pattern did we check against?
4. Is this a projection, or does it create authority?
5. Does a cross-controller model test cover the composed state?
6. Does the behavior update the `ManagedVolume` story for PVC, replica,
   frontend, CSI, host path, recovery, or evidence?

If the answers are unclear, update the protocol docs before shipping the
behavior.
