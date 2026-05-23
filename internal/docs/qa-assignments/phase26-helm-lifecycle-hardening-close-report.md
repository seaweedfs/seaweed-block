# QA Close - Phase 26 Helm Lifecycle Hardening

Date: 2026-05-22

Verdict: PASS (strict). Phase 26 is ready to close as a Helm lifecycle
hardening release slice.

## Scope

This close validates the Phase 26 claim:

```text
Helm chart hygiene
-> Helm install
-> first PVC
-> narrow upgrade / rollback smoke
-> multi-PVC Day-1 smoke
-> read-only support bundle replay
-> Helm uninstall and host cleanup
```

It does not validate production readiness, CRD/operator lifecycle, mutating
admin workflows, broad upgrade safety, backup/snapshot/restore, performance
SLOs, or new recovery semantics.

## Gate Evidence

| Gate | Run | Result | Notes |
|---|---:|---|---|
| D1 chart release hygiene | `20260522-131641-7a61` | PASS, 15/15 | `helm lint`, `helm template`, chart package, metadata summary |
| D2 Helm lifecycle smoke | `20260522-131951-a6d4` | PASS, 27/27 | existing PV preserved through upgrade and rollback |
| D3 multi-volume Day-1 | `20260522-152903-1116` | PASS, 29/29 | 3 PVCs, 3 writers, 3 readers, 3 ManagedVolume rows |
| D4 support bundle diagnostics | `20260522-153929-93a3` | PASS, 38/38 | cold `report/explain/timeline --from-bundle`, blocked bundle explains ImagePullBackOff |

Independent QA replay:

| Gate | QA Run | Result |
|---|---:|---|
| D1 chart release hygiene | `20260522-155827-ce94` | PASS, 15/15 |
| D2 Helm lifecycle smoke | `20260522-155835-2057` | PASS, 27/27 |
| D3 multi-volume Day-1 | `20260522-155944-27b3` | PASS, 29/29 |
| D4 support bundle diagnostics | `20260522-160203-227d` | PASS, 38/38 |

QA validation report:

- `internal/docs/qa-assignments/phase26-helm-lifecycle-hardening-qa-validation.md`

## Acceptance

| Requirement | Result |
|---|---|
| Chart renders and packages cleanly | PASS |
| Helm install reaches ready state | PASS |
| Existing PVC survives the gated upgrade / rollback smoke | PASS |
| Multiple PVCs bind through the Helm-installed StorageClass | PASS |
| Writer and reader checksum pass for each PVC in the D3 smoke | PASS |
| `sw-block ops report` lists all D3 ManagedVolumes | PASS |
| Support bundle contains Helm metadata, K8s snapshots, logs, iSCSI state, report, timeline, and explain output | PASS |
| Cold `sw-block ops report --from-bundle` works | PASS |
| Cold `sw-block ops explain volume --from-bundle` works | PASS |
| Cold `sw-block ops timeline volume --from-bundle -o jsonl` works | PASS |
| Synthetic ImagePullBackOff bundle emits `reason=csi_node_image_pull_failed` | PASS |
| Helm uninstall plus host cleanup leaves no sessions or processes | PASS |

## Product Fixes During Phase

- Persist materialized workload endpoint ports in placement intent so later
  volume IDs cannot reshuffle existing blockvolume Deployment ports.
- Preserve materialized data/control addresses in placement verification.
- Merge observation slots from the same Kubernetes node by `(volume, replica)`
  with independent freshness so multiple same-node blockvolume processes do not
  overwrite each other's publish-target facts.
- Added `scripts/collect-helm-support-bundle.sh` as a reusable read-only Helm
  support-bundle collector.

## Regression

Passed:

```text
go test ./core/authority ./core/host/master -count=1
go test ./core/lifecycle ./core/launcher ./core/csi ./cmd/blockmaster ./cmd/blockcsi ./cmd/sw-block -count=1
bash -n scripts/run-multi-volume-example.sh
bash -n scripts/collect-helm-support-bundle.sh
swblock validate testops/scenarios/helm-multi-volume-day1-chain.yaml
swblock validate testops/scenarios/helm-support-bundle-diagnostics-chain.yaml
git diff --check
```

`git diff --check` produced only Windows CRLF warnings, no whitespace errors.

## Non-Claims

Phase 26 does not deliver:

- production-grade Helm lifecycle,
- general upgrade safety,
- CRDs or operator reconciliation,
- mutating admin actions,
- backup/snapshot/restore,
- rebuild/failback,
- new HA or recovery semantics,
- production hosted dashboard,
- performance/RTO/SLO claims.

## Release Image Note

D3 and D4 validated Phase 26 code through local TestOps images. Publish a new
immutable GHCR SHA and update README / quickstart pins before presenting
v0.3.1 as an externally consumable image release.

## Verdict

PASS for Phase 26 scope.

Recommended close: prepare the v0.3.1 alpha release note / PR using this report
as evidence, and carry operator packaging plus broader lifecycle management into
the next phase.
