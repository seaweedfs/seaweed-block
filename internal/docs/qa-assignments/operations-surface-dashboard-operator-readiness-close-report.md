# QA Close - Phase 23 Operations Surface / Dashboard / Operator-Readiness

Verdict: PASS for Phase 23 scope. Recommended close after review.

Progress at close: about 85%.

This close covers the read-only operations surface built on Phase 22's
ManagedVolume model. It does not claim a hosted dashboard, CRD/operator
reconciliation, mutating admin actions, repair/rebuild, backup/restore, or
production SLOs.

## Product Claim

Seaweed Block now exposes ManagedVolume readiness, blockers, recovery state,
dry-run next actions, evidence refs, and future-operator status contract through
the same product-owned model:

```text
ManagedVolumeProjection
-> Conditions
-> report summary / HTML
-> ops explain text
-> operator-readiness contract
-> replayable support-bundle evidence
```

## Hard Gate Table

| Gate | Requirement | Result |
|---|---|---|
| HG-0 | Phase 23 plan defines read-only operations/dashboard/operator-readiness scope | PASS |
| HG-1 | ManagedVolume projection emits stable Conditions | PASS |
| HG-2 | Conditions use Kubernetes-style `True` / `False` / `Unknown` status values | PASS |
| HG-3 | Blocked states emit `Ready=False` and blocker Conditions | PASS |
| HG-4 | Recovered states emit `Ready=True` and `Recovered=True` | PASS |
| HG-5 | Condition evidence refs are additive and propagated | PASS |
| HG-6 | `ops report` summary includes ManagedVolume Conditions | PASS |
| HG-7 | `ops report` HTML includes a Managed Volume Conditions table | PASS |
| HG-8 | `ops explain` includes Conditions, actions, preconditions, invariant refs, evidence refs, and non-claims | PASS |
| HG-9 | Future operator contract maps status/conditions/events/actions without enabling mutation | PASS |
| HG-10 | First-volume / blocked / recovery bundle replay all produce report + explain + operator contract | PASS |
| HG-11 | Scope regression passes for CLI/ops/CSI/launcher/master | PASS |

## TDD Evidence

Tests added or extended:

- `core/ops/managed_volume_conditions_test.go`
- `core/ops/managed_volume_operator_contract_test.go`
- `core/ops/observation_report_test.go`
- `core/ops/observation_bundle_test.go`

Coverage:

- ready first-volume condition,
- blocked loopback cross-node condition,
- recovered transparent failover condition,
- blocked condition evidence propagation,
- report summary condition output,
- report HTML condition table,
- explain text condition/action/precondition/invariant output,
- operator contract blocked and recovered event mapping,
- first-volume / blocked / recovery replay gate.

## Internal Review

Read-only boundary:

- No current CLI/report path performs Kubernetes, host, CSI, authority, or
  repair mutation.
- Future operator contract sets `mutation_allowed=false` for every emitted
  action.

Truth boundary:

- Conditions derive from `ManagedVolumeProjection`.
- Report, explain, and operator contract consume the same projection instead of
  recomputing state from logs.
- Evidence refs are pointers to artifacts; they are not new truth owners.

Operator boundary:

- `ManagedVolumeOperatorContractFromProjection` is a contract shape, not a CRD.
- It can feed future CRD status and Kubernetes Events, but it does not install a
  controller or reconcile resources.
- Future mutations require separate RBAC, audit, policy, and product gates.

## Regression

Passed:

```text
go test ./cmd/sw-block ./core/ops ./core/csi ./core/launcher ./core/host/master -count=1
```

Known repository-wide caveat inherited from Phase 22:

- `go test ./... -count=1` has unrelated failures in `cmd/sparrow` and
  `core/frontend/iscsi`.
- Those packages are outside Phase 23's touched surface and should be tracked
  separately before claiming full-repo release green.

## Non-Claims

Phase 23 does not deliver:

- hosted dashboard service,
- CRDs,
- operator reconciliation,
- mutating admin actions,
- promote/repair/rebuild/failback,
- backup/snapshot/restore,
- production SLOs,
- NVMe ANA parity.

## Verdict

PASS for Phase 23 scope.

The product now has a read-only operations surface that is dashboard-ready and
operator-contract-ready while preserving the model-first, no-mutation boundary.

Recommended next action: choose Phase 24 as either hosted read-only dashboard
or operator scaffolding. Do not start mutating admin workflows until their
action contracts, RBAC/audit, and product gates are separately specified.
