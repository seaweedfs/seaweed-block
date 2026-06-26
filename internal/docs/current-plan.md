# Current Plan: Phase 89 SwBlockVolume Authority Facts

Status: complete.

## Goal

Phase 89 closes the status-evidence gap found after Phase 88. The failback
target owner can only safely activate a returned-replica failback target when
it has current authority inputs:

```text
expectedCurrentReplicaID
expectedCurrentEpoch
```

Those facts already exist in internal volume evidence, but they were not
visible on `SwBlockVolume.status`. This phase projects them through the
read-only/status path so later failback activation can consume observed state
instead of hardcoded or manual inputs.

## Deliverables

### D1: Projection Contract

`ManagedVolumeProjection` now preserves:

```text
primary_replica_id
publish_target
authority_epoch
authority_endpoint_version
```

`ManagedVolumeOperatorStatus` exposes the same fields in snake_case for
`operator-snapshot.json`.

### D2: Kubernetes Status Contract

`SwBlockVolume.status` now exposes the same facts as camelCase:

```text
primaryReplicaID
publishTarget
authorityEpoch
authorityEndpointVersion
```

The CRD schema includes those fields with integer `int64` authority counters.

### D3: Report Surface

`summary.txt` now includes a compact authority line:

```text
managed_volume_authority=<volume> primary=<replica> publish_target=<addr> epoch=<n> endpoint_version=<n>
```

### D4: Gate

Added:

```text
scripts/run-phase89-swblockvolume-authority-facts-gate.sh
testops/scenarios/swblockvolume-authority-facts-chain.yaml
```

The gate proves:

```text
projection preserves observed authority facts
operator-snapshot uses snake_case authority fields
SwBlockVolume.status uses camelCase authority fields
CRD schema contains the new status fields
report summary includes authority facts
no failback target is created
no failback activation is attempted
```

## Verification

```text
bash scripts/run-phase89-swblockvolume-authority-facts-gate.sh .
swblock validate testops/scenarios/swblockvolume-authority-facts-chain.yaml
```

Expected terminal evidence:

```text
phase89_swblockvolume_authority_facts_status=ok
crd_status_primary_replica_id=true
crd_status_authority_epoch=true
operator_snapshot_snake_authority=true
crd_status_camel_authority=true
report_summary_authority_line=true
failback_activation_attempted=false
failback_target_created=false
```

## Next

Phase 90 should use these status facts to make failback target activation
explicitly evidence-gated:

```text
only activate when SwBlockVolume.status has current primary + epoch
write expectedCurrentReplicaID and expectedCurrentEpoch onto the target
still do not claim frontend publication after failback
```
