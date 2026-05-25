# Current Plan: Phase 30 - Control Model / ManagedVolume Hardening

Status: active, 80% complete. Started on 2026-05-24 after Phase 29 lifecycle
cleanup reliability closed.

## Product Goal

Make the Kubernetes block product easier to operate and safer to extend by
stabilizing the control-plane read model before adding mutating operator,
rebuild/failback, NVMe ANA, or backup workflows.

Phase 30 focus:

```text
PVC/PV + ManagedVolume + Launcher + CSI + Authority + HostPath + Cleanup
-> one explicit state dependency model
-> each fact has an authority
-> each action has an executor
-> each status/condition has evidence
-> report/dashboard/operator consume the same model
```

This phase is model/control-plane hardening. It does not add a new user-visible
HA claim.

## Scope Contract

| In | Out |
|---|---|
| state dependency review across PVC, launcher, CSI, authority, host path, cleanup | rebuild/failback implementation |
| stable/provisional/test-only field classification | mutating operator actions |
| model-backed condition/action contract tightening | NVMe ANA parity implementation |
| small code refactors that remove duplicated projection logic | backup/snapshot/restore |
| regression gates proving no product-loop breakage | broad production SLO |

Principle: no new mutating workflows until the read model can explain current
state from owned facts.

## D1: Control-State Dependency Review

Goal: document where each user-visible state currently comes from and which
component owns it.

Acceptance:

- Cover PVC/PV, StorageClass, Helm values, launcher placement, generated
  blockvolume Deployments, authority epoch/primary, CSI publish/stage,
  host-path facts, workload checks, cleanup facts, and support/report artifacts.
- For each domain define:
  - fact authority,
  - passive source,
  - allowed bounded probe,
  - consuming master/projection,
  - executor for future actions,
  - current risk if logic remains scattered.
- Identify which facts are stable, provisional, or test-only.

Status: PASS on 2026-05-24.

Artifact:

- `internal/docs/ref/phase30-control-state-dependency-review.md`

## D2: ManagedVolume Field Contract Tightening

Goal: update the model contract so report/dashboard/operator surfaces know
which fields are stable API-like facts and which remain internal or test-only.

Acceptance:

- Update `internal/docs/ref/managed-volume-operational-model-contract.md` or
  add a Phase 30 supplement.
- Add/adjust tests that assert stable field names for:
  - identity,
  - desired replication/ack/profile,
  - authority,
  - CSI target,
  - host path,
  - cleanup,
  - allowed actions.
- No mutating action is exposed.

Status: PASS on 2026-05-25.

Artifacts:

- `core/ops/managed_volume_contract.go`
- `core/ops/managed_volume_contract_test.go`
- `internal/docs/ref/managed-volume-operational-model-contract.md`

## D3: Projection Ownership Cleanup

Goal: remove at least one duplicated or ambiguous projection path from code.

Candidate targets:

- cleanup evidence and lifecycle status,
- blocked first-volume diagnostics,
- CSI reattach vs host-path transparent recovery classification,
- action hint generation.

Acceptance:

- One selected status chain has a single projection owner.
- Tests prove summary/report/dashboard/operator snapshot agree.
- No scenario helper becomes the source of truth for a product status.

Status: PASS on 2026-05-25.

Selected chain:

```text
cleanup-summary.txt -> CleanupEvidence -> report summary/dashboard/operator snapshot
```

Artifacts:

- `core/ops/cleanup_evidence.go`
- `core/ops/cleanup_evidence_test.go`
- `internal/docs/ref/phase30-cleanup-projection-ownership.md`

## D4: Regression Gates

Goal: prove model tightening did not break the product loops.

Required gates:

- `go test ./core/ops ./cmd/sw-block`
- Helm first-volume via sw-block CLI
- Helm multi-volume Day-1
- One RF3 recovery gate selected by risk
- `cleanup-residue-chain.yaml`

Status: PASS on 2026-05-25.

Evidence:

- `go test ./core/ops ./cmd/sw-block`: PASS
- `helm-first-volume-via-sw-block-cli-chain.yaml`: PASS, run
  `20260525-004305-a872`, 34/34 actions
- `helm-multi-volume-day1-chain.yaml`: PASS, run
  `20260525-004405-c28a`, 29/29 actions
- `helm-multi-volume-rf3-reattach-recovery-chain.yaml`: PASS, run
  `20260525-004612-d656`, 29/29 actions
- `cleanup-residue-chain.yaml`: PASS, run `20260525-005026-fe98`,
  13/13 actions

## D5: Close Gate

Goal: close only when the dependency review, field contract, projection cleanup,
and regression gates are complete.

Acceptance:

- D1-D4 complete.
- QA or independent rerun validates the selected gates.
- Close report and finished plan are written.

## Progress

- D1: PASS - control-state dependency review written
- D2: PASS - field/action contract tightened; stable facts and read-only/dry-run actions tested
- D3: PASS - cleanup projection ownership moved into CleanupEvidence helper
- D4: PASS - regression gates passed
- D5: pending
