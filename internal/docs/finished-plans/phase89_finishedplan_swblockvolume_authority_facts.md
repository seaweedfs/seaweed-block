# Phase 89 Finished Plan: SwBlockVolume Authority Facts

Status: complete.

Date: 2026-06-26.

## What Changed

Phase 89 projects current authority facts into the status/read surfaces used by
the operation layer:

```text
primary replica
publish target
authority epoch
authority endpoint version
```

These fields are now visible in:

```text
ManagedVolumeProjection
operator-snapshot.json
SwBlockVolume.status
summary.txt
```

## Why

Failback activation needs a positive current-authority input before it can set:

```text
expectedCurrentReplicaID
expectedCurrentEpoch
```

Before this phase, the data existed in evidence but was not exposed on the
canonical Kubernetes status object. That would force a future activation path
to use manual values or duplicate evidence parsing.

## Boundary

This phase does not:

```text
create SwBlockReplicaFailback targets
activate failback
call the failback executor
publish a frontend after failback
mutate storage
```

It is a status projection phase only.

## Verification

```text
scripts/run-phase89-swblockvolume-authority-facts-gate.sh .
swblock validate testops/scenarios/swblockvolume-authority-facts-chain.yaml
```

Expected result:

```text
phase89_swblockvolume_authority_facts_status=ok
```
