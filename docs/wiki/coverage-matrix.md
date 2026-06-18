# Engineering Coverage Matrix

This matrix is the review checklist for whether the wiki covers the last
several months of Seaweed Block work. It is domain-based rather than phase-based
because developers need to understand subsystems, ownership, and invariants, not
just the order in which phases happened.

Coverage levels:

| Level | Meaning |
|---|---|
| `linked` | source docs are linked, but wiki explanation is thin |
| `mapped` | wiki explains the domain, ownership, code area, and source evidence |
| `deep` | wiki includes background, contract, ownership, state transitions, code entry points, evidence, failure taxonomy, implementation checklist, and QA gates |

## Domain Coverage

| Domain | Why it matters | Source evidence | Current wiki coverage | Next doc work |
|---|---|---|---|---|
| Product positioning and claims | Prevents over-claiming and release confusion | `docs/releases/`, `docs/roadmap.md`, `docs/user-capabilities.md` | mapped | claim-boundary page by release |
| Day-1 Kubernetes install and first PVC | User entry path and release smoke | Phase 20, 25, 40, 44 finished plans; quickstart | mapped | install flow diagram and troubleshooting |
| CSI lifecycle | PVC/PV path, CreateVolume/DeleteVolume, node stage/publish | `core/csi`, `docs/runtime-state-machines.md`, Phase 44 D2 | deep | expand DeleteVolume details later |
| SwBlockVolume CR identity | Bridge from CSI to operation layer | Phase 44 D2 sign-off, `core/csi/kubernetes_metadata.go` | deep | add field-by-field CRD contract later |
| operator-status | CRD status, Events, node evidence, cleanup/delete-safety | Phases 35-40, `core/ops` | deep initial | add status writer/schema conformance page |
| lifecycle-owner | bounded finalizer add/release | Phases 41-44, lifecycle-owner refs | deep initial | add admission policy field matrix |
| Admission/RBAC boundary | Prevents finalizer owner from becoming broad operator | Phase 42 sign-off, Helm templates | linked | VAP/RBAC decision diagram |
| Delete-safety | Holds unsafe deletion, releases on clean evidence | Phase 39, 42, 44; finalizer-delete-safety contract | deep initial | field-by-field status contract |
| Cleanup verifier | Zero-residue authority and safety evidence | cleanup scripts, Phase 29, 36, 44 | mapped | residue taxonomy |
| ManagedVolume readiness | Shared ready/blocked/unknown model | Phase 22, 32, 35-36 | deep initial | add reason-code taxonomy |
| Node readiness / CSI evidence | Avoids false node-ready status | Phase 37 | deep | keep live K8s evidence current as CRD evolves |
| SmartWAL dirty failure | Proves dirty storage evidence cannot become false Ready | Phase 34 | deep | add exact test log excerpts later |
| WAL/frontier/barrier/recovery | Persistence and recovery vocabulary behind dirty-failure and rebuild | methodology docs, recovery tutorial, Phase 34 | deep | add exact SmartWAL layout diagrams later |
| Authority / epoch / promotion | One-primary, fencing, restart authority persistence | Phases 13-18, 31, invariant ledger | deep initial | add code-level product-loop walkthrough |
| Returned-replica rebuild/reintegration | Next major storage lifecycle feature | recovery refs, roadmap | deep initial | add concrete future gate plan |
| iSCSI frontend / ALUA / multipath | Current default frontend and transparent failover path | iSCSI refs, Phase 17, 27, 37 | deep | keep artifact examples current as scenarios evolve |
| NVMe ANA parity | Planned protocol parity path | NVMe refs, roadmap | linked | future protocol page |
| TestOps runner and scenarios | Product evidence, not just tests | `testops/`, Phase 33-34 | deep initial | add runner action reference |
| Support bundle/replay/report/dashboard | Cold-reader diagnosis and support path | Phases 23-24, 28, 32, 36, 44 | deep | keep artifact list aligned with collect script |
| Release engineering | Image/chart skew prevention | Phases 40, 44, release notes | mapped | release checklist page |
| Docker volume plugin idea | Future non-K8s adapter | roadmap | linked | investigation page later |

## Coverage Decision

The wiki is useful when each major domain has:

```text
problem statement
industry/product pattern
Seaweed Block ownership model
state machine or protocol diagram
publish/API shape
main code entry points
evidence contract
failure taxonomy
implementation checklist
QA gates and failure evidence
explicit non-claims
```

## Priority For Deep Pages

1. CSI lifecycle and `SwBlockVolume` ownership.
2. Operation layer v0.5: operator-status + lifecycle-owner + admission.
3. ManagedVolume readiness and negative-first priority rules.
4. SmartWAL dirty-failure path.
5. TestOps authoring and evidence realism.
6. Authority/promotion/restart persistence.
7. iSCSI ALUA/multipath frontend.
8. WAL/frontier/barrier/recovery.
9. Live node/CSI evidence.
10. Support bundle cold-reader path.
11. Returned-replica rebuild readiness.

These pages cover most of the hard-earned knowledge from the last
few months. The rest can remain linked until those are reviewed.
