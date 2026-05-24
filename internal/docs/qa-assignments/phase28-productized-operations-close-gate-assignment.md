# QA Assignment - Phase 28 Productized Operations Close Gate

Date: 2026-05-23

Goal: validate that Phase 28 is a productized operations loop, not just a set
of isolated mechanisms.

This gate covers D9-D12. It does not cover D13 release packaging because D13
requires published immutable GHCR images and doc pins.

## Scope

In:

- Helm install path.
- First-volume user loop.
- Multi-volume smoke.
- Healthy and blocked support evidence.
- Read-only report/dashboard/operator snapshot.
- Cleanup residue verification.
- ManagedVolume / CRD / Condition / Event vocabulary alignment.

Out:

- Mutating operator actions.
- CRD install or controller-manager lifecycle.
- Repair, rebuild, failback, delete, or cleanup mutation.
- NVMe ANA, backup/restore, upgrade SLO, broad performance SLO.

## Required Source Contracts

QA should inspect these files before running scenarios:

- `internal/docs/ref/managed-volume-operational-model-contract.md`
- `internal/docs/ref/operator-crd-condition-event-contract.md`
- `internal/docs/ref/read-only-operator-foundation-contract.md`
- `internal/docs/ref/multi-volume-ha-support-evidence-contract.md`
- `internal/docs/protocol/layered-participant-authority-master-executor-model.md`

Acceptance:

- All docs agree that ManagedVolume is the operations read model.
- Operator foundation is status/read-only only.
- Reason codes and Conditions are stable vocabulary, not UI-specific wording.

## Gate Runs

Run from clean lab state.

Controller-side helper:

```powershell
.\scripts\run-phase28-productized-ops-close-gate.ps1 `
  -Runner C:\work\swblock.exe `
  -ResultsDir results\phase28-productized-ops-close `
  -ArtifactShareRoot V:\share\g15d-k8s
```

The helper runs G1/G2/G3/G5 and validates the D11
`operator-snapshot.json` contract from G1 artifacts. G1 also serves the
dashboard with its freshly built `sw-block` binary and saves the routed
`/operator-snapshot.json` response under
`basic-app/status/report/dashboard-route/`. QA may still inspect artifacts
manually, but the helper provides a consistent summary:

```text
phase28-productized-ops-close-summary.txt
phase28-productized-ops-close-summary.json
```

### G1 Helm First Volume Via CLI

Scenario:

```text
testops/scenarios/helm-first-volume-via-sw-block-cli-chain.yaml
```

Required evidence:

- Helm install succeeds.
- PVC binds.
- Writer verifies.
- Reader verifies.
- `first-volume-summary.txt` reports:
  - `first_volume_status=ok`
  - `writer_verified=true`
  - `reader_verified=true`
  - `inventory_status=ok`
  - `cleanup_status=ok`
- Report artifacts exist:
  - `status/report/index.html`
  - `status/report/cluster-evidence.json`
  - `status/report/timeline.jsonl`
  - `status/report/summary.txt`
  - `status/report/operator-snapshot.json`

### G2 Multi-Volume Day-1

Scenario:

```text
testops/scenarios/helm-multi-volume-day1-chain.yaml
```

Required evidence:

- `multi_volume_status=ok`
- `requested_volume_count=3`
- `writer_verified_count=3`
- `reader_verified_count=3`
- `managed_volume_count=3`
- `cleanup_status=ok`

### G3 Support Bundle Healthy + Blocked

Scenario:

```text
testops/scenarios/helm-support-bundle-diagnostics-chain.yaml
```

Required evidence:

- Healthy bundle is self-explaining.
- Blocked bundle is self-explaining.
- Blocked case includes stable reason code, not only pod logs.
- `sw-block ops explain` can produce a cold-reader explanation.

### G4 Read-Only Operator Snapshot

This is a D11-specific artifact check. It may be verified from G1 report output
or by running `sw-block ops report` directly against a preserved bundle.

Required evidence in `operator-snapshot.json`:

- `"api_version": "block.seaweedfs.com/v1alpha1"`
- `"kind": "ReadOnlyOperatorFoundationSnapshot"`
- `"read_only": true`
- `"mutation_allowed": false`
- `crd_contract.group="block.seaweedfs.com"`
- one volume entry per ManagedVolume.
- allowed actions are only `read_only` or `dry_run`.

Required dashboard evidence:

- `/operator-snapshot.json` serves the same read-only boundary.
- POST/PUT/PATCH/DELETE to dashboard return method-not-allowed.

### G5 Cleanup Residue

Scenario:

```text
testops/scenarios/cleanup-residue-chain.yaml
```

Required evidence:

- `cleanup_status=ok`
- `multipath_residue_count=0`
- `dmsetup_residue_count=0`
- no active iSCSI sessions.
- no generated sw-block Deployments.
- no product processes on all participating hosts.

## Hard-Gate Clause Table

| Clause | Requirement |
|---|---|
| HG-0 | Source contracts are present and aligned. |
| HG-1 | Helm first-volume user loop passes from clean state. |
| HG-2 | Multi-volume day-1 loop passes and reports 3 ManagedVolumes. |
| HG-3 | Healthy support evidence is self-explaining. |
| HG-4 | Blocked support evidence is self-explaining with stable reason codes. |
| HG-5 | `sw-block ops report` includes the five report artifacts including `operator-snapshot.json`. |
| HG-6 | Dashboard serves read-only HTML, JSON, JSONL, summary, and operator snapshot. |
| HG-7 | Operator snapshot has read-only mutation boundary. |
| HG-8 | ManagedVolume model and CRD/Condition contract use the same status vocabulary. |
| HG-9 | Cleanup verifier proves Kubernetes/iSCSI/multipath/dmsetup/process residue is zero. |
| HG-10 | User-facing non-claims remain narrow and visible. |

## PM Review Questions

PM should be able to answer from artifacts without engineering help:

- What is installed?
- How many volumes are ready?
- Which PVC maps to which sw-block volume?
- Why is a volume blocked?
- Is this report read-only?
- What is not claimed yet?
- What cleanup evidence proves the lab is clean?

## Close Criteria

Phase 28 D12 can close only when:

- all HG clauses pass,
- QA report names run IDs and bundle paths,
- PM review accepts the claim boundary,
- D13 remains explicitly open if immutable images are not yet published.

Use this close-report template to keep the result comparable across reruns:

```text
internal/docs/qa-assignments/phase28-productized-operations-close-report-template.md
```
