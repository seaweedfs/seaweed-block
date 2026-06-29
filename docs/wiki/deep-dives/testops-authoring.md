# TestOps Authoring Guide

TestOps scenarios are product evidence. They should not merely prove that a
helper script printed the expected text.

## Realism Levels

| Level | Pattern | Product value |
|---|---|---|
| L0 self-proof | helper writes `x=1`, scenario greps `x=1` | weak; avoid for claims |
| L1 replay | saved bundle is replayed through report/status logic | good for projection |
| L2 live injection | real failure is induced and observed | good for status/failure claims |
| L3 adversarial | live failure plus timing/concurrency/isolation | release-grade evidence |

## Scenario Shape

A good scenario has:

```text
pre-run cleanup
-> install/provision
-> induce or observe product state
-> collect independent evidence
-> assert status/action/surface behavior
-> cleanup
-> residue verifier
```

## Anti-Pattern: Self-Proof

Bad:

```text
script echoes managed_volume_count=3
scenario greps managed_volume_count=3
```

Better:

```text
script reports managed_volume_count=3
scenario independently queries Kubernetes or operator-snapshot and compares
```

## Gate Design Checklist

Before adding a scenario, answer:

```text
What product claim does this gate defend?
What independent evidence source checks the helper output?
What false-positive would make this test dangerous?
What residue must be cleaned?
Does failure produce a cold-reader bundle?
Is the gate L1, L2, or L3?
```

## Evidence Surfaces

Prefer gates that compare at least two surfaces:

- CRD status,
- operator-snapshot.json,
- report summary,
- dashboard JSON,
- explain output,
- Kubernetes Events,
- host cleanup verifier,
- direct workload read/write.

## Scenario Families

| Family | Purpose |
|---|---|
| first-volume | user install and PVC loop |
| multi-volume RF=3 | identity isolation and frontend collision prevention |
| restart persistence | data/authority persistence after k3s restart |
| negative status | blocked/unknown reasons and no false Ready |
| dirty failure | WAL/status endpoint corruption-style failures |
| lifecycle-owner | admission and finalizer boundaries |
| cleanup | zero-residue and delete-safety evidence |

## Main Files

| Area | Path |
|---|---|
| scenario definitions | `testops/scenarios/` |
| suites | `testops/suites/` |
| runner contracts | `internal/docs/ref/testops-runner-binary-contract.md` |
| control/data contract | `internal/docs/ref/testops-control-data-contract.md` |
| QA sign-offs | `internal/docs/qa-assignments/` |

## Release Rule

Do not call a feature product-ready from unit tests alone. A release claim needs
at least one replay or live gate that exercises the user-visible surface.

