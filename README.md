# seaweed-block 🚧 Alpha

Block Storage for Kubernetes

`seaweed-block` is a lightweight Kubernetes block-storage project. The current
alpha MVP can run a dynamic PVC through CSI, iSCSI, a generated
`blockvolume` daemon, and a pod checksum write/read on a single-node k3s lab.

The project is not production-ready yet. It is now far enough along for
contributors to run the MVP path, inspect the architecture, and help harden the
Kubernetes packaging, persistence, recovery, and TestOps workflows.

## Quick Start: Kubernetes Alpha MVP

Read the MVP guide first:

- [deploy/k8s/alpha/README.md](deploy/k8s/alpha/README.md)

Build local images:

```bash
bash scripts/build-g15b-images.sh "$PWD"
```

For k3s, import them:

```bash
docker save sw-block:local | sudo k3s ctr images import -
docker save sw-block-csi:local | sudo k3s ctr images import -
```

Run the verified dynamic PVC create/delete smoke:

```bash
bash scripts/run-k8s-alpha.sh "$PWD"
```

Expected output:

```text
PASS: dynamic PVC create/delete completed checksum write/read and cleanup
```

The same flow is documented as a manual `kubectl apply` sequence in
[deploy/k8s/alpha/README.md](deploy/k8s/alpha/README.md).

## Current MVP Status

What works in the current alpha:

- CSI dynamic PVC `CreateVolume`.
- Blockmaster lifecycle + placement + assignment publication.
- Launcher-generated `blockvolume` Deployment.
- iSCSI frontend path.
- Pod filesystem write/read checksum.
- CSI `DeleteVolume` cleanup path.
- TestOps registry and a minimal `cmd/sw-testops` CLI.

Current defaults:

- frontend protocol: iSCSI
- durable backend: `walstore`
- launcher-generated state: `emptyDir`
- Kubernetes evidence: single-node k3s lab

High-value references:

- [docs/architecture.md](docs/architecture.md)
- [docs/roadmap.md](docs/roadmap.md)
- [deploy/k8s/alpha/README.md](deploy/k8s/alpha/README.md)

## What This Repo Is Not Yet

This repository is not yet:

- production-ready storage
- a multi-node Kubernetes storage system
- a complete operator
- a durable K8s volume product across pod restart
- a failover-under-mounted-PVC product
- an NVMe-oF CSI product path

The current alpha is meant to be honest and runnable, not over-claimed.

## Semantic Core Background

The project started as a standalone block-storage experiment built around a
deterministic semantic core. That recovery/authority foundation still matters:

```text
facts -> engine decision -> adapter command -> runtime execution -> session close
```

## Product Vision

`seaweed-block` aims to make block storage for Kubernetes much lighter, easier,
and more flexible than traditional storage stacks.

The product direction is:

1. simpler to understand and operate than heavyweight systems such as Ceph
2. lighter to start and iterate on for developers and platform teams
3. flexible enough to grow from a small cluster service into a practical Kubernetes block platform
4. easier to reason about during failure and recovery, without a maze of hidden control-plane behavior

In short:

1. easier than heavyweight storage systems
2. more direct than control-plane-heavy designs
3. still structured enough to grow into serious replicated block storage

## Design Philosophy

The technical design is intentionally shaped around a few strict choices:

1. semantic core first: recovery meaning is defined in a deterministic engine before broad system growth
2. one route only: observation, decision, execution, and terminal close follow one explicit path
3. strict authority boundaries: engine decides, adapter normalizes, runtime executes
4. terminal truth is narrow: recovery is not "successful" until explicit session close says so
5. reviewable growth: the project is phase-driven so new features do not silently pollute the core
6. runtime must not silently redefine semantics, including widening engine-issued recovery targets

## Why This Approach

Many storage systems become difficult because recovery semantics, transport
mechanics, retries, and product features get mixed together.

`seaweed-block` is an attempt to separate those layers more cleanly:

1. facts determine semantics
2. transport does not silently redefine policy
3. the system can be tested and reviewed from the semantic contract outward
4. the same semantic model should later support broader runtime work without changing its meaning

Legacy semantic-core status:

1. semantic core: present
2. replay/conformance runtime: present
3. adapter-backed route: present
4. runnable block sparrow: present
5. early operations UX: present only for the sparrow demo
6. persistence / crash recovery: present in lower-level components, not yet a production Kubernetes durability claim

## Legacy Sparrow Demo

The older `sparrow` demo still exists as a semantic-core development entry
point. Most new users should start with the Kubernetes alpha MVP above.

## What Works In The Legacy Demo

The current repository can run one narrow block slice end-to-end through real
TCP transport.

Demonstrated paths:

1. healthy: replica is already caught up
2. catch-up: replica is behind within retained window
3. rebuild: replica is behind beyond retained window

The current route stays intentionally narrow:

1. one semantic route
2. one active session at a time
3. one terminal-close authority
4. executor honors the engine-issued `targetLSN`

## Legacy Demo Limitations

The sparrow demo is not the Kubernetes product path. Its limitations are kept
visible because it remains useful for semantic-core development:

1. narrow demo route
2. not a full Kubernetes product
3. not the CSI/iSCSI runtime path
4. not a replacement claim over the older V2 baseline

## Repo Layout

```text
cmd/
  sparrow/        runnable Phase 04 demo entry point

core/
  engine/         deterministic semantic core
  schema/         conformance case schema and conversion
  runtime/        replay runner
  conformance/    YAML semantic cases
  adapter/        single-route adapter boundary
  storage/        minimal in-memory block store
  transport/      minimal TCP transport for the runnable sparrow
```

`core/` is the public-facing semantic center path for this repository.

For a full taxonomy of events, commands, truth domains, and operator
enums — and why there is no single unified state diagram — see
[docs/surface.md](docs/surface.md).

## Quick Start

Requirements:

1. Go `1.23+`

Run the runnable sparrow:

```bash
go run ./cmd/sparrow
```

Expected outcome:

1. healthy demo passes
2. catch-up demo passes
3. rebuild demo passes

Run the test suite:

```bash
go test ./...
```

## Operations

The sparrow supports optional flags for repeatable validation and
read-only inspection. Defaults are unchanged from the Phase 04 demo:

```bash
go run ./cmd/sparrow                       # three demos, text output (default)
go run ./cmd/sparrow --help                # authoritative scope statement
go run ./cmd/sparrow --json                # machine-readable output for CI
go run ./cmd/sparrow --runs 10             # repeat the full demo N times
go run ./cmd/sparrow --http :9090          # add read-only HTTP inspection
go run ./cmd/sparrow --calibrate           # Phase 06 calibration pass (C1-C5)
go run ./cmd/sparrow --calibrate --json    # machine-readable calibration Report
go run ./cmd/sparrow --persist-demo --persist-dir DIR    # Phase 07 single-node persistence demo
```

HTTP endpoints (read-only): `GET /` returns the self-describing surface
map; `GET /status`, `GET /projection`, `GET /trace`, `GET /watchdog`,
`GET /diagnose` expose the bounded single-node inspection surface. Every
mutation verb returns 501 with an explicit read-only ops-surface body.

See [docs/single-node-surface.md](docs/single-node-surface.md) for the
bounded single-node product surface, or
[docs/bootstrap-validation.md](docs/bootstrap-validation.md) for the
full list of supported flags, endpoints, and exit codes.

This binary is a development and validation entry point only. The
production operations surface is `weed shell` after integration.

## Calibration

Phase 06 adds a small calibration set that drives the accepted route
through five scenario families (C1-C5) and records expected-versus-
observed evidence. Run it with:

```bash
go run ./cmd/sparrow --calibrate          # text report
go run ./cmd/sparrow --calibrate --json   # machine-readable Report
```

Evidence artifacts:

- [docs/calibration/scenario-map.md](docs/calibration/scenario-map.md)
- [docs/calibration/divergence-log.md](docs/calibration/divergence-log.md)

If a case diverges, record it in `divergence-log.md` before changing
the route or the expectations.

## Persistence and the local data process

A bounded local data process owns read, write, flush, checkpoint,
and recovery on one node, behind the `LogicalStorage` interface.
Acked writes survive abrupt process kill; recovery is deterministic;
a background flusher drains the WAL into the extent and advances
the on-disk checkpoint.

```bash
go run ./cmd/sparrow --persist-demo --persist-dir /tmp/sparrow-persist
```

What's proven (single-node):

- Acked writes survive process kill (verified by simulated-kill tests
  that bypass `Close()` and a crash family across four windows).
- Recovery is deterministic across reopens of the same on-disk state.
- Unacked writes may vanish but never corrupt acked data.

What's NOT in scope: distributed durability across nodes;
power-loss durability beyond what `fsync` guarantees at the
OS+device boundary; bit-rot detection in the extent.

For details:

- [docs/local-data-process.md](docs/local-data-process.md) — the institution, what it owns, the crash model, carry-forward
- [docs/persistence.md](docs/persistence.md) — backend implementation details, on-disk format, exit codes, NVMe/raw-device path

## Replication institutions

The current replicated path is documented as two bounded lower institutions:

- [docs/data-sync-institution.md](docs/data-sync-institution.md) — byte movement, wire protocol, lineage gate, achieved-frontier report
- [docs/recovery-execution-institution.md](docs/recovery-execution-institution.md) — command admission, real execution start, invalidation, close-path lifecycle truth

## Single-node product surface

Above the three lower institutions (local data, data sync,
recovery execution) sits one bounded single-node operator
surface — start / inspect / validate / diagnose — exposed as six
read-only HTTP endpoints plus the sparrow CLI. No cluster-shaped
wording; no mutation authority.

- [docs/single-node-surface.md](docs/single-node-surface.md) — surface map, workflow, honesty rules, carry-forward

## Replicated durable slice

The first bounded product capability beyond single-node operation:
one old-primary → new-primary → rejoin path that converges with
explicit fencing and stale-lineage rejection. Mechanism, not policy
— who becomes primary and when to fail over belong to later phases.

- [docs/replicated-slice.md](docs/replicated-slice.md) — the bounded route, authority boundary, durability claim, known limitations, carry-forward

## Design Rules

The current implementation is intentionally shaped around a few strict rules:

1. timers trigger observation; facts determine semantics
2. the engine owns semantic recovery decisions
3. the adapter/runtime may execute, but may not redefine policy
4. terminal truth comes only from explicit session close
5. session `targetLSN` is fixed by the engine and must not be silently widened by the executor

## Near-Term Direction

The next planned steps are:

1. freeze and stabilize the first public runnable shape under `core/`
2. add minimal operations and test interfaces
3. calibrate against selected high-value scenarios from the existing benchmark path
4. expand only after the semantic boundary stays clean

## Honesty Note

This repository should currently be read as:

1. a runnable semantic-core-first block prototype
2. a clean recovery-route reference
3. a base for future operations, calibration, and storage work

It should not yet be read as:

1. finished block product
2. production-ready replicated storage
3. complete protocol or deployment surface
