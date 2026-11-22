# `docs/` overview

An index of this directory. Internal planning and historical design notes
belong under `internal/docs/`, not here.

## Design and evidence

| File | Purpose |
|---|---|
| `design/block-design.md` | Core design: frontends, walstore and smartwal, durability, authority, replication engine, recovery; verification and known issues |
| `design/agent-workspace-design.md` | Agent workspace disks: chunk backend, cuts, snapshots, forks, GC, host moves, security model |
| `design/kubernetes-block-design.md` | Kubernetes block volumes: CSI, Helm and operator, replication and failover, multipath, readiness |
| `spec/` | TLA+ specifications of the protocols where a bug would lose data |
| `workspace-measurements.md` | Measured workspace results and how each was measured |
| `kubernetes-status.md` | Kubernetes feature status, claims, non-claims and release images |
| `releases/` | Release notes: what each release claimed and the run evidence behind it |

## Operations

| File | Purpose |
|---|---|
| `standalone-deployment.md` | Run `blockmaster` and `blockvolume` with systemd, outside Kubernetes |
| `quickstart-kubernetes.md` | Kubernetes first-volume path with Helm |
| `user-capabilities.md` | Kubernetes capabilities, read-only operations commands, status vocabulary |
| `kubernetes-app-demo.md` | Presentation demo: an app pod writes and a replacement pod reads one PVC |
| `chunk-manifests.md` | Chunk upload settings and the HEAD/segment manifest format |
| `workspace-lease.md` | Workspace host lease, clean and forced moves, clock skew |
| `volume-lifecycle.md` | Workspace volume hand-over, same-host return, crash recovery and symptoms (standalone) |
| `fsync-latency.md` | Reading the per-stage `fsync` histograms under `/status/durable` |

## Engineering references (Kubernetes line)

| File | Purpose |
|---|---|
| `architecture.md` | Short Kubernetes product architecture |
| `developer-architecture.md` | Module-by-module boundaries for contributors |
| `runtime-state-machines.md` | Control-plane, CSI and frontend loops |
| `operations-v1.md` | Legacy V1 alpha operations manual; use the quickstart and release notes for current claims |
| `wiki/` | Engineering wiki (MkDocs): code map, state machines, deep dives |

## Calibration evidence

`calibration/` contains evidence documents still referenced by calibration
code (`core/calibration`, `cmd/sparrow`).

## Documentation rule

If a document is meant for external users, avoid internal phase or gate labels.
