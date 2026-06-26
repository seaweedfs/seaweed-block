# Phase 89 QA Sign-off: SwBlockVolume Authority Facts

Status: pending QA.

## Scope

Validate that observed authority facts are projected into read/status surfaces
without enabling failback execution.

## Gate

Run:

```text
bash scripts/run-phase89-swblockvolume-authority-facts-gate.sh .
swblock validate testops/scenarios/swblockvolume-authority-facts-chain.yaml
```

## Required Evidence

The summary must contain:

```text
phase89_swblockvolume_authority_facts_status=ok
go_test_core_ops_authority_projection=pass
crd_status_primary_replica_id=true
crd_status_publish_target=true
crd_status_authority_epoch=true
crd_status_authority_endpoint_version=true
operator_snapshot_snake_authority=true
crd_status_camel_authority=true
report_summary_authority_line=true
failback_activation_inputs_visible=true
expected_current_replica_source=swblockvolume.status.primaryReplicaID
expected_current_epoch_source=swblockvolume.status.authorityEpoch
failback_activation_attempted=false
failback_target_created=false
storage_mutation_allowed=false
```

## Pass Criteria

- `ManagedVolumeProjection` preserves primary replica, publish target, epoch,
  and endpoint version.
- `operator-snapshot.json` uses snake_case authority fields.
- `SwBlockVolume.status` uses camelCase authority fields.
- The CRD schema contains the new status fields and does not leak snake_case.
- `summary.txt` includes the authority line.
- No failback target is created and no failback activation is attempted.

## Non-Claims

Phase 89 does not prove live failback. It only exposes the status evidence that
the next activation phase must consume.
