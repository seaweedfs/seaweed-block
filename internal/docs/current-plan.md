# Current Plan: Phase 25 - v0.3 Helm + Observable First-Volume Release

Status: closed, 100% complete. Plan simplified and closed on 2026-05-22.

Reference:

- `internal/docs/ref/product-delivery-review-simple-stable-observable-block.md`

## Product Goal

Deliver a simple, stable, observable Kubernetes block alpha:

```text
Helm install
-> first PVC
-> writer/reader data check
-> read-only report/dashboard
-> clean uninstall
-> docs and release note match the exact claim
```

This phase was release reconciliation and hardening. It had two steps only:

```text
D1: docs + release claim alignment - PASS
D2: gate replay + close evidence - PASS
```

## Scope Contract

| In | Out |
|---|---|
| Helm release hardening | CRD/operator implementation |
| README / quickstart / release note alignment | new mutating admin action |
| single-node and multi-node Helm gate replay | new protocol capability |
| dashboard/report evidence consistency | model or protocol refactor |
| immutable image / digest documentation | backup/snapshot/restore |
| cleanup and host residue verification | rebuild/reintegration/failback |

Principle: Phase 25 may take small bug fixes only when they block the v0.3 user
path. No model rewrite, no operator controller, no broad architecture refactor.

## Current Closed Inputs

As of 2026-05-22:

- Phase 20 Day-1 activation: closed.
- Phase 22 ManagedVolume model: closed for scope.
- Phase 23 operations surface/operator-readiness contract: closed for scope.
- Phase 24 hosted read-only dashboard: closed for scope.

## D1: Docs + Release Claim Alignment

Goal: make the user-facing product story match the current code and gates.

Status: PASS on 2026-05-22.

Artifacts:

- `internal/docs/product-roadmap.md`
- `README.md`
- `docs/quickstart-kubernetes.md`
- `docs/releases/v0.3-alpha.md`
- `docs/releases/README.md`

Required content:

- v0.3 is Helm alpha install + first PVC + read-only report/dashboard +
  cleanup.
- Script activation remains alpha/dev fallback, not the preferred v0.3 story.
- `sw-block ops generate-helm-values` input/output is explained.
- Single-node behavior is clear: one selected node, loopback mode.
- Multi-node behavior is clear: Ready schedulable nodes, non-loopback
  InternalIP, external iSCSI/status, CHAP.
- `sw-block ops report` vs `sw-block ops dashboard` is clear:
  - report writes static artifacts,
  - dashboard serves the same read-only evidence locally.
- Immutable `sha-<commit>` images are recommended for QA/PM/release proof.
- Mutable `:alpha` is documented as smoke/demo only.
- Non-claims are explicit:
  - not production-ready,
  - no operator lifecycle,
  - no mutating admin UI/actions,
  - no backup/snapshot/restore,
  - no upgrade/rollback safety,
  - no broad performance/RTO/SLO claim,
  - no new recovery scope beyond already gated evidence.

Acceptance:

```text
README + quickstart + release note describe the same v0.3 claim.
No doc claims a capability without a gate.
Roadmap marks Phase 22/23/24 closed and Phase 25/v0.3 closed.
```

## D2: Gate Replay + Close Evidence

Goal: prove the documented v0.3 path is runnable and self-explaining.

Status: PASS on 2026-05-22.

Evidence:

- Single-node Helm gate: `20260522-031019-ef25`, PASS, 34/34 actions.
- Multi-node Helm gate: `20260522-031124-0a44`, PASS, 51/51 actions.
- Documented Go CLI generator gate: `20260522-091642-b9a7`, PASS, 31/31
  actions.
- Both runs record immutable image tags and digests.
- Both runs produce `status/report/index.html`, `cluster-evidence.json`,
  `timeline.jsonl`, and `summary.txt`.
- Both runs finish with `cleanup_status=ok`, zero k8s residue, zero process
  residue, and zero hostPath residue.

Required gates:

- Helm single-node first-volume gate:
  - `testops/scenarios/helm-single-node-first-volume-chain.yaml`
  - expected: PASS
- Helm multi-node first-volume gate:
  - `testops/scenarios/helm-first-volume-chain.yaml`
  - expected: PASS
- Report/dashboard consistency:
  - `summary.txt`
  - `index.html`
  - `cluster-evidence.json`
  - `timeline.jsonl`
  - dashboard endpoint serving same evidence when applicable
- Image identity:
  - image tag recorded,
  - digest recorded,
  - release validation uses immutable tag.
- Cleanup:
  - Helm release removed,
  - StorageClass/demo PVC/pods removed,
  - no active iSCSI sessions,
  - no sw-block processes,
  - no test-scoped residue.

Acceptance:

```text
single-node Helm gate PASS
multi-node Helm gate PASS
report/dashboard reason codes agree
image tag/digest evidence present
cleanup clean
close report written
finished plan written
```

Close artifacts:

- `internal/docs/qa-assignments/v0.3-helm-observable-first-volume-close-report.md`
- `internal/docs/finished-plans/phase25_finishedplan_v0.3_helm_observable_first_volume.md`

## Claim Matrix

| Area | Can Claim For v0.3 | Cannot Claim |
|---|---|---|
| Install | Helm alpha install on supported k3s/Kubernetes labs | production installer, broad distro support |
| First Volume | PVC create, writer/reader data check, clean report | performance/SLO, upgrade safety |
| Recovery | Existing gated RF3 recovery evidence remains valid | new recovery scope beyond prior gates |
| Dashboard | local read-only dashboard/report over product evidence | production hosted UI, mutating admin UI |
| Cleanup | documented uninstall + host cleanup verification | fully automated operator lifecycle |
| Images | immutable tag/digest release validation | mutable `:alpha` as release proof |

## Risks

| Risk | Mitigation | Fallback |
|---|---|---|
| `:alpha` image drift | release docs require immutable `sha-<commit>` tags for QA/PM | use local/internal images for dev gates |
| single-node vs three-node behavior confusion | quickstart explains loopback vs external iSCSI/CHAP values | provide separate single-node and multi-node commands |
| docs drift from implementation | close gate checks README, quickstart, release note against gate artifacts | block release note until docs are corrected |
| dashboard/report reason mismatch | compare summary, HTML, JSON, explain/dashboard reason codes | fix projection consistency only; no model rewrite |
| cleanup residue after Helm uninstall | host cleanup verification is required | document manual cleanup command and keep release blocked until clean |

## Dependency Order

```text
D1 docs alignment
-> D2 gate replay
-> close report
```

Do not start operator/CRD implementation until this phase closes.
