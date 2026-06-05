# Phase 36 D1 Operations Model Contract Review

Status: PASS.

Date: 2026-06-05.

## Scope Reviewed

Phase 36 D1 defines the read-only operations actionability contract. It does
not wire live node readiness or cleanup projection yet.

Reviewed artifacts:

- `internal/docs/current-plan.md`
- `internal/docs/product-roadmap.md`
- `internal/docs/protocol/operator-readiness-contract.md`
- `core/ops/managed_volume_crd_contract.go`
- `core/ops/operator_status_controller.go`
- `charts/seaweed-block/crds/swblockclusters.block.seaweedfs.com.yaml`

## Contract Additions

`SwBlockCluster.status` now has typed/schema-backed places for:

- `nodes[]`: node readiness/preflight facts and node-level conditions,
- `supportBundleRefs[]`: support evidence pointers,
- `cleanup`: cleanup verifier status and residue counters,
- `safeNextSteps[]`: read-only, dry-run, or user-scripted next-step hints.

## Boundary Review

The fields are observation and advice only.

They do not imply:

- CR object ownership,
- finalizers,
- automatic cleanup,
- storage mutation,
- workload mutation,
- PVC/PV mutation,
- iSCSI/multipath/hostPath mutation,
- promote/repair/rebuild/failback/delete/backup/restore execution.

`safeNextSteps[].mutationAllowed` is required in the CRD schema and must remain
`false` for Phase 36.

## TDD Evidence

Focused tests were added first and failed on the missing contract/schema:

```text
TestManagedVolumeCRDContract_Phase36ActionabilityFields
TestPhase36D1SwBlockClusterActionabilitySchema
```

After implementing the structural contract, the focused tests passed.

## Regression Checks

```text
go test ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
helm template sw-block charts/seaweed-block --namespace kube-system --include-crds \
  --set operatorStatus.create=true --set operatorStatus.dryRun=false
```

All passed.

## Findings

No blocking findings.

Non-blocking:

- D1 is structural only. D2 must prove real node evidence populates
  `SwBlockCluster.status.nodes[]`.
- D3 must prove support-bundle refs are not just schema fields but usable
  cold-reader evidence pointers.
- D4 must prove cleanup visibility does not become automatic cleanup.
