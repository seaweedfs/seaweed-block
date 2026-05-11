# Finished Plan: Read-Only Operations Status Report Integration

Status: historical reference. Closed on 2026-05-11 after the read-only
operations status report collector, schema gate, artifact gate, returned-replica
case, and operator-facing guide were validated.

This phase turned the first operations-layer status report from a seed builder
into a small read-only evidence surface. It is observability evidence only. It
is not a V2-style block/data snapshot, clone, backup, rollback point, restore
feature, operator, force-detach protocol, or cleanup authorization.

Current work is tracked in `../current-plan.md`.

## Goal

Make one stable report answer:

```text
What volume/replica is this host serving, what role does it believe it has,
what frontend facts are exposed, what durable lineage is latched, what peers
are known, and what residue would make a cleanup or failover decision unsafe?
```

## Close Evidence

| Gate | Run ID | Product Commit | Result | Wall Clock | Notes |
| --- | --- | --- | --- | ---: | --- |
| `operations-volume-status-report-component-gate` | `20260510-232649-03fd` | `c8a27ac4ca35e4686420ce068bb67811b9a95fd9` | PASS | `1.081s` | baseline builder gate |
| same | `20260510-233106-cfb9` | `569855f` | PASS | `1.201s` | collected `volume-status-report.json` |
| same | `20260511-000155-b27d` | `171d872` | PASS | `1.226s` | schema-shape pinning |
| same | `20260511-001715-9921` | `c18489e` | PASS | `1.288s` | read-only collector seam |
| same | `20260511-002427-09ff` | `be45263` | PASS | `1.368s` | returned replica durable-ready/frontend-fenced case |

All 13 runner scenario YAML files validated after the schema/collector changes.

## What Closed

1. `core/ops.BuildVolumeStatusReport` assembles schema version `1.0` from
   existing status facts.
2. `VolumeStatusReportCollector` provides an injectable read-only seam for:
   - master status,
   - local status projection,
   - peer status,
   - durable status,
   - residue,
   - product/runner provenance.
3. Collector errors return a partial report plus joined source errors, so useful
   evidence is preserved without hiding failed source collection.
4. The fast runner-native gate writes and collects `volume-status-report.json`.
5. JSON shape tests pin operator-facing keys and keep valid zero-valued frontend
   identity fields such as `lun:0`.
6. Returned/non-primary replica status is covered: durable lineage can be
   latched and operational while the replica remains frontend-fenced.
7. `ref/operations-volume-status-report-operator-guide.md` documents what an
   operator may inspect and what must not be inferred.

## Important Non-Claims

- This phase does not deliver a block/data snapshot.
- This phase does not deliver an operator.
- This phase does not add force-detach, cleanup, promote, demote, or restart
  controls.
- This phase does not claim HA.
- This phase does not change `beta-hardening-gate`.
- This phase does not introduce a public unauthenticated operations API.
- A stale report is stale evidence, not current truth.

## Follow-Up Decisions

- Keep `core/ops` direct imports of product DTOs acceptable for this seed.
  Revisit a thinner ops-owned input DTO only if the package grows or DTO churn
  starts leaking into the schema.
- Future admin controls must be separate mini-protocols with fencing and
  authority semantics. They must not reuse this report as authorization.
- Continue moving concrete expensive-suite assertions into fast component gates
  when the failure can be isolated without a live lab.

