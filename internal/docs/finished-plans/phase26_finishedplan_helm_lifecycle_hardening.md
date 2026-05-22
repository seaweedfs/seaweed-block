# Finished Plan - Phase 26 Helm Lifecycle Hardening

Closed: 2026-05-22

## Result

PASS. Phase 26 turned the v0.3 Helm-first alpha loop into a more release-shaped
Kubernetes lifecycle slice:

```text
chart hygiene
-> Helm install
-> first PVC
-> narrow upgrade / rollback smoke
-> multi-PVC Day-1 smoke
-> cold support-bundle replay
-> Helm uninstall and host cleanup
```

## Evidence

| Gate | Run | Result |
|---|---:|---|
| D1 chart release hygiene | `20260522-131641-7a61` | PASS, 15/15 |
| D2 Helm lifecycle smoke | `20260522-131951-a6d4` | PASS, 27/27 |
| D3 multi-volume Day-1 | `20260522-152903-1116` | PASS, 29/29 |
| D4 support bundle diagnostics | `20260522-153929-93a3` | PASS, 38/38 |

Close report:

- `internal/docs/qa-assignments/phase26-helm-lifecycle-hardening-close-report.md`

Release note:

- `docs/releases/v0.3.1-alpha.md`

## Product Fixes

- Materialized workload endpoint ports are persisted in placement intent, so a
  later volume cannot reshuffle an existing blockvolume Deployment's ports.
- Placement verification now preserves materialized data/control addresses.
- Observation facts from multiple same-node blockvolume processes merge by
  `(volume, replica)` with independent freshness.
- `scripts/collect-helm-support-bundle.sh` collects Helm/K8s/log/iSCSI evidence
  and proves cold `sw-block ops report/explain/timeline --from-bundle` replay.

## Non-Claims

This phase does not claim production readiness, broad upgrade safety, operator
or CRD lifecycle, mutating admin actions, backup/snapshot/restore, rebuild,
failback, production hosted dashboard, or new recovery semantics.

