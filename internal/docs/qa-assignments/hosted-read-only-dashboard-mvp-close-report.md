# QA Close: Hosted Read-Only Dashboard MVP

Verdict: PASS for Phase 24 scope.

Release confidence: about 90%. The scoped regression is green. Full repository
release confidence is still limited by known unrelated failures outside this
phase (`cmd/sparrow` and `core/frontend/iscsi` from earlier full-repo checks).

## Product Claim

Seaweed Block can expose a local read-only dashboard/API surface over the same
product-owned `ClusterEvidence` and ManagedVolume model used by `sw-block ops
report`.

The dashboard can be served from:

- an offline support/test bundle,
- a live master ClusterEvidence API snapshot,
- live Kubernetes inventory fallback for alpha inspection.

The dashboard is observation-only. It does not promote, repair, delete, rebuild,
clean up, or mutate Kubernetes, CSI, authority, host, or volume state.

## Hard Gate Clauses

| Clause | Result | Evidence |
| --- | --- | --- |
| HG-0: shared evidence source | PASS | Handler consumes normalized `ClusterEvidence`; no second dashboard state model. |
| HG-1: HTML status page | PASS | `GET /` and `/index.html` serve `RenderObservationReportHTML`. |
| HG-2: machine JSON | PASS | `GET /cluster-evidence.json` serves normalized ClusterEvidence including ManagedVolumes. |
| HG-3: timeline JSONL | PASS | `GET /timeline.jsonl` serves product event JSONL. |
| HG-4: text summary | PASS | `GET /summary.txt` serves the same status text used by report artifacts. |
| HG-5: health check | PASS | `GET /healthz` returns `ok`. |
| HG-6: mutation rejection | PASS | POST returns 405 with `read-only dashboard`. |
| HG-7: CLI bundle mode | PASS | `sw-block ops dashboard --from-bundle ...` serves support artifacts. |
| HG-8: CLI master-api mode | PASS | `sw-block ops dashboard --master-api ...` serves live master evidence. |
| HG-9: loopback default | PASS | CLI default listen address is `127.0.0.1:9334`. |
| HG-10: ManagedVolume-first UX | PASS | HTML includes a first-class Managed Volumes section with status, reason, conditions, and safe actions. |
| HG-11: bundle replay coverage | PASS | First-volume, blocked image-pull, and Stage 2 recovery bundles replay through dashboard handler. |
| HG-12: text/HTML/JSON consistency | PASS | Summary/explain now use normalized ManagedVolume projections so artifact hints are not lost. |

## Test Evidence

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

## Non-Claims

- Not a production hosted control plane.
- Not a mutating admin UI.
- Not an operator or CRD controller.
- No authn/authz model beyond local alpha binding.
- No backup, snapshot, restore, repair, rebuild, promote, failback, or cleanup
  workflow.
- No broad browser compatibility or production UI polish claim.

## Review Notes

- The dashboard is an adapter over observation data, not a truth owner.
- The CLI serves local loopback by default to avoid accidentally exposing an
  unauthenticated alpha dashboard.
- Bundle mode is important: failed tests and support cases can be inspected
  without a live cluster.
- The text consistency fix is substantive: `summary.txt` and `ops explain`
  now render normalized `cluster.ManagedVolumes`, preserving recovery/blocker
  hints from bundle artifacts.

## Natural Next Work

- Add a TestOps live scenario that starts `sw-block ops dashboard` during
  first-volume or Helm smoke and captures screenshots or HTTP artifacts.
- Add optional browser smoke once the UI stabilizes.
- Add event folding/deduplication for repeated placement events.
- Add future authn/authz and RBAC only when dashboard scope moves beyond local
  alpha.
