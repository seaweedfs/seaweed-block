# Phase 30 Control Model / ManagedVolume Hardening Close Report

Date: 2026-05-25

Verdict: **PASS**.

Phase 30 closes the control-model hardening loop. It does not add a new HA,
operator, rebuild, NVMe, or backup claim. It stabilizes the read model and
projection ownership that future operator-grade operations depend on.

## Source Commits

- `fc2b74f` - start Phase 30 control model hardening
- `ff320e6` - tighten ManagedVolume fact/action contract
- `75790f4` - centralize cleanup evidence projection
- `9f85334` - record regression gates

## Delivered Claim

The Kubernetes block product now has an explicit read-model contract:

```text
participants publish observations
fact authorities own facts
ManagedVolume / Engine masters compose state
actions carry executor + policy boundary
evidence proves state and next-step recommendations
```

The delivered state is read-only/dry-run only. No mutating operator workflow is
authorized by Phase 30.

## Gates

| Gate | Result | Evidence |
|---|---|---|
| D1 control-state dependency review | PASS | `internal/docs/ref/phase30-control-state-dependency-review.md` |
| D2 ManagedVolume field/action contract | PASS | `core/ops/managed_volume_contract.go`, `core/ops/managed_volume_contract_test.go` |
| D3 cleanup projection ownership cleanup | PASS | `core/ops/cleanup_evidence.go`, `core/ops/cleanup_evidence_test.go` |
| D4 regression gates | PASS | selected TestOps gates below |
| D5 close gate | PASS | this report plus finished plan |

## D1 Evidence

The dependency review covers:

- Helm install intent,
- Kubernetes PVC/PV,
- StorageClass parameters,
- launcher placement,
- generated blockvolume runtime,
- authority identity,
- promotion readiness,
- CSI publish/stage,
- host path,
- workload evidence,
- cleanup evidence,
- support/report artifacts.

Artifact:

- `internal/docs/ref/phase30-control-state-dependency-review.md`

## D2 Evidence

The ManagedVolume contract now separates fields from actions:

- fields name participant, Fact Authority, Master, aggregation mode, condition
  surface, and evidence;
- actions name deciding Master, executor, mode, side-effect class, policy gate,
  required facts, invariants, and evidence;
- all Phase 30 actions remain `read_only`, `dry_run`, or disabled until future
  operator policy.

Validation:

```text
go test ./core/ops ./cmd/sw-block
```

Result: PASS.

## D3 Evidence

Selected projection chain:

```text
cleanup-summary.txt
  -> CleanupEvidenceFromSummary
      -> CleanupEvidence
          -> ReportSummaryLines
          -> ReportRow
          -> operator-snapshot Cluster.Cleanup
```

No report surface should independently invent cleanup field names or status
classification.

Artifacts:

- `core/ops/cleanup_evidence.go`
- `internal/docs/ref/phase30-cleanup-projection-ownership.md`

Validation:

```text
go test ./core/ops ./cmd/sw-block
```

Result: PASS.

## D4 Regression Evidence

All required regression gates passed on 2026-05-25:

| Gate | Run ID | Result |
|---|---|---|
| `go test ./core/ops ./cmd/sw-block` | local | PASS |
| `helm-first-volume-via-sw-block-cli-chain.yaml` | `20260525-004305-a872` | 34/34 PASS |
| `helm-multi-volume-day1-chain.yaml` | `20260525-004405-c28a` | 29/29 PASS |
| `helm-multi-volume-rf3-reattach-recovery-chain.yaml` | `20260525-004612-d656` | 29/29 PASS |
| `cleanup-residue-chain.yaml` | `20260525-005026-fe98` | 13/13 PASS |

Total TestOps regression actions: **105/105 PASS**.

## Product Surfaces

The following surfaces consume the same ManagedVolume / CleanupEvidence model:

- `sw-block ops report` JSON,
- report `summary.txt`,
- report `index.html`,
- `operator-snapshot.json`,
- `sw-block ops explain` text.

Stable cleanup fields:

- `cleanup_status`
- `k8s_residue_count`
- `iscsi_residue_count`
- `multipath_residue_count`
- `process_residue_count`
- `hostpath_residue_count`
- `failure_count`
- `failed_phase`
- `cleanup_evidence`

## Non-Claims

- No mutating operator action.
- No rebuild/reintegration/failback implementation.
- No NVMe ANA parity implementation.
- No backup/snapshot/restore implementation.
- No new production SLO.
- No broad compatibility or scale claim.

## Followups

- Use the field/action contract as the input boundary for the next read-only
  operator step.
- Extend the same ownership pattern to CSI reattach vs transparent host-path
  recovery classification.
- Before mutating cleanup/repair/rebuild actions, add executor policy gates and
  tests that prove allow/refuse/pending behavior.

