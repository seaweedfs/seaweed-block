# Finished Plan: Phase 24 - Hosted Read-Only Dashboard

Status: closed for Phase 24 scope.

Close report:

- `internal/docs/qa-assignments/hosted-read-only-dashboard-mvp-close-report.md`

## What Closed

Phase 24 made the operations surface hostable without changing the underlying
truth model.

Delivered:

- `NewObservationDashboardHandler(cluster ClusterEvidence) http.Handler`
- HTTP endpoints:
  - `/` and `/index.html`
  - `/cluster-evidence.json`
  - `/timeline.jsonl`
  - `/summary.txt`
  - `/healthz`
- `sw-block ops dashboard`
  - `--from-bundle`
  - `--master-api`
  - live inventory fallback
  - default `--listen 127.0.0.1:9334`
- HTML report now includes a first-class `Managed Volumes` section.
- Text output now uses normalized `cluster.ManagedVolumes`, preserving bundle
  artifact hints for recovery and blocked cases.
- Dashboard replay gate covers first-volume, blocked image-pull, and Stage 2
  recovery evidence.

## Product Boundary

The dashboard is read-only.

It can explain:

- cluster/volume status,
- ManagedVolume readiness/recovery/blocker conditions,
- safe read-only or dry-run action contracts,
- evidence refs,
- recent timeline,
- machine-readable artifacts.

It cannot:

- promote,
- repair,
- rebuild,
- fail back,
- delete,
- clean up,
- mutate Kubernetes,
- mutate CSI,
- mutate authority,
- mutate host state.

## Validation

Targeted tests:

```text
go test ./core/ops -run ObservationDashboard -count=1
go test ./cmd/sw-block -run OpsDashboard -count=1
go test ./core/ops -run "ObservationBundle_DashboardReplayGate|ObservationBundle_D6ReplayGate" -count=1
```

Scoped regression:

```text
go test ./cmd/sw-block ./core/ops ./core/csi ./core/launcher ./core/host/master -count=1
```

Result: PASS.

## Internal Review

- Truth owner: master/inventory/bundle evidence remains the source of truth.
- Dashboard role: presentation adapter over normalized ClusterEvidence.
- Safety boundary: unsafe HTTP methods return 405.
- Operator boundary: this does not introduce CRDs or reconciliation.
- Product boundary: local/read-only alpha dashboard, not production UI.

## Carry Forward

- Add live TestOps dashboard capture.
- Add event folding to reduce noisy repeated placement events.
- Decide whether the next operations phase should be:
  - operator/CRD scaffolding, or
  - dashboard UX hardening and browser smoke.
- Keep mutating admin actions behind separate safety, RBAC, audit, and QA
  gates.
