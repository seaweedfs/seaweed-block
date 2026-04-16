# Single-Node Product Surface

The single-node product surface is the bounded operator-facing
layer above the accepted lower institutions (local data process,
data sync, recovery execution). It makes one repeatable workflow —
start, inspect, validate, diagnose — explicit and testable without
claiming any cluster-shaped authority.

This doc names what the surface owns, what it does not, and what
each endpoint is for.

## One-sentence scope

**One bounded single-node operator surface — start / inspect /
validate / diagnose — exposed as six read-only HTTP endpoints plus
a small CLI, mirroring accepted lower-institution truth without
cluster-shaped wording or mutation authority.**

## Repeatable workflow

| Step | Command | What it proves |
|---|---|---|
| start | `go run ./cmd/sparrow` | The three canonical recovery paths (healthy, catch-up, rebuild) complete end-to-end on one node |
| inspect | `go run ./cmd/sparrow --http :9090` then `curl /` | The surface is self-describing; every endpoint names what it owns |
| validate | `go run ./cmd/sparrow --runs N --json` | Repeatable, deterministic outcome suitable for CI; exit code reflects pass/fail |
| diagnose | `curl /diagnose`, `/trace`, `/watchdog` | Bounded local evidence sufficient to answer "what is this node doing and why" without inventing new authority |

Optional extensions that stay inside the single-node boundary:

| Command | What it adds |
|---|---|
| `--calibrate` | Drives a wider scenario set through the accepted route; still single-node |
| `--persist-demo --persist-dir DIR` | Demonstrates the local data process through one clean stop-and-restart cycle |

## HTTP endpoints

| Path | Owns |
|---|---|
| `GET /` | Self-describing surface map: the scope statement, every sub-endpoint, and a note that the surface is read-only |
| `GET /status` | Version, uptime, scope, current demo label |
| `GET /projection` | The adapter's current `ReplicaProjection` (semantic truth derived from lower institutions) |
| `GET /trace` | The engine's decision trace for the current demo — full fidelity, untransformed |
| `GET /watchdog` | The adapter's start-timeout watchdog lifecycle evidence (arm / clear / supersede / fire / fire_noop) |
| `GET /diagnose` | A bounded summary over projection + last trace step + watchdog counters — useful for "is this node healthy, and if not, why" at a glance |

Every path responds with 501 to any non-GET/HEAD/OPTIONS method.
Unknown paths return 404 with a hint to `/`.

## What this surface owns

1. **Self-description** — one endpoint (`GET /`) lists every other
   endpoint; an operator can discover the capability without reading
   source or design docs.
2. **Bounded diagnosis** — `/diagnose` collapses projection + trace
   tail + watchdog counters into one small structured view. The full
   evidence stays at the dedicated endpoints.
3. **Honest empty states** — every endpoint returns a valid JSON
   response even before any demo has run. Empty arrays, not `null`;
   zero values, not fabricated fields.
4. **Mutation-closed enforcement** — the read-only boundary is
   enforced at the HTTP layer, not by omitting handlers.

## What this surface does NOT own

The following belong to later phases. P11's surface must not absorb
them:

| Concern | Owner | Why not here |
|---|---|---|
| Choosing `catch_up` vs `rebuild` | engine | semantic policy |
| Deciding to fail over or promote | P14 | topology authority |
| Epoch authority / endpoint minting | P14 | produced above the executor |
| Multi-replica coordination | P12 | replicated slice, not one node |
| Operator mutations (retire / repair / rebalance) | P15 | governance surface, needs P14 truth first |
| A cluster shell / CLI that implies collective control | P15 | weed shell, post-integration |

The surface is explicitly **read-only**. Any HTTP verb other than
GET / HEAD / OPTIONS returns 501. The CLI admits local workflow
actions (`--runs`, `--calibrate`, `--persist-demo`) which are
operator-initiated workflow invocations, not arbitrary mutations
against a running server.

## Honesty rules (tested)

These rules are enforced by `core/ops/surface_test.go`:

| Rule | Proof |
|---|---|
| Surface index lists exactly the endpoints wired into the mux | `TestSurfaceIndex_SelfDescribing` |
| Surface index declares itself single-node | `TestSurfaceIndex_WordingIsSingleNode` |
| Surface index contains no cluster-shaped vocabulary ("cluster", "failover", "promotion", "topology authority", "rf3") | `TestSurfaceIndex_WordingIsSingleNode` |
| `/diagnose` returns honest empty values when no demo has run | `TestDiagnose_EmptyStateIsHonest` |
| `/diagnose` exposes only fields derivable from lower truth | `TestDiagnose_OnlyReadsLowerTruth` |
| `/` and `/diagnose` reject every mutation verb | `TestSurfaceMutations_IncludesRootAndDiagnose` |
| Every read endpoint returns a valid empty-safe JSON response | existing `*_EmptyWhenNoDemo` tests |

## Reading order

For someone new to this surface:

1. `GET /` — the surface map (run the sparrow, hit the root)
2. `core/ops/server.go` — endpoint wiring
3. `core/ops/handlers.go` — handler bodies, state snapshot reads
4. `core/ops/state.go` — the read-only mirror over adapter state
5. `core/ops/surface_test.go` — honesty proofs

## Carry-forward

Items the single-node surface leaves explicitly to later phases:

- **P12 (bounded replicated durable slice)** — multi-replica status
  aggregation, rejoin visibility, multi-peer inspection.
- **P14 (topology / failover policy)** — any surface that exposes
  or accepts promotion / placement / epoch decisions.
- **P15 (operator-facing governance surface)** — a cluster-shaped
  shell (`weed shell block.*`), operator mutation verbs,
  production-grade authentication, and operator-visible retry /
  repair / rebalance actions.

P13 (hardening gate) will define the release criteria that this
surface must meet before P14 widens it into a replicated control
surface.
