# First-Launch Validation Workflow

The one canonical path a tester or operator runs to validate that
the bounded persistent RF2 slice is working. Every step is short
and directly actionable.

For the full list of what the slice does and doesn't support, see
[docs/p13-support-envelope.md](p13-support-envelope.md).

## TL;DR

```bash
# 1. Prove the persistent slice works — all routes, both backends.
#    THIS is what validates P13's supported claims.
go test ./core/adapter -run "TestPersistent|TestAbrupt" -count=1 -v

# 2. (Optional) Run the sparrow with the HTTP inspection surface.
#    Note: the sparrow's demos run on in-memory BlockStore; the
#    HTTP surface exposes that demo's state. It validates the
#    surface shape and the support_claim pointer — NOT the
#    persistent slice itself. For persistent-slice evidence,
#    step 1 is authoritative.
go run ./cmd/sparrow --http 127.0.0.1:9090

# 3. In another shell: read the diagnosis surface
curl -s localhost:9090/         | jq   # surface map
curl -s localhost:9090/diagnose | jq   # bounded summary + support_claim
```

If **step 1 is green**, the 10 frozen supported claims in
`docs/p13-support-envelope.md` hold. Step 2/3 prove the
self-describing surface is intact and point a reader at this doc
and at the envelope — they are **not** a substitute for step 1
when qualifying the persistent slice.

## Step 1 — Validate the persistent route suite

```bash
go test ./core/adapter -run "TestPersistent|TestAbrupt" -count=1 -v
```

This runs 9 scenarios × 2 persistent backends = 18 subtests:

| Scenario | What it proves | walstore | smartwal |
|---|---|---|---|
| Handoff_Convergence | epoch+1 handoff converges | ✓ | ✓ |
| Rejoin_AfterReopen | graceful reopen preserves new primary's truth | ✓ | ✓ |
| CatchUp_Branch | R ≥ S, R < H; engine traces `catch_up` | ✓ | ✓ |
| Rebuild_Branch | R < S; engine traces `rebuild` | ✓ | ✓ |
| StopAfterConvergence | abrupt stop → acked data survives | ✓ | ✓ |
| StopAfterCatchUp | abrupt stop after catch-up → all blocks intact | ✓ | ✓ |
| StopAfterRebuild | abrupt stop after rebuild → all blocks intact | ✓ | ✓ |
| RejoinAfterAbruptStop | reopen + new primary converges | ✓ | ✓ |
| StaleLineageAfterReopen | old-epoch ship rejected at data plane | ✓ | ✓ |

**Pass criterion:** all 18 subtests green.

**Failure handling:** the test output names exactly which backend
and scenario diverged. Check the trace in the failure message
against `docs/p13-support-envelope.md` — the supported set names
which route each scenario exercises.

## Step 2 — Start the sparrow with HTTP inspection (optional)

```bash
go run ./cmd/sparrow --http 127.0.0.1:9090
```

The sparrow runs its three canonical demos (healthy, catch-up,
rebuild) in-process, then keeps the HTTP server alive until
Ctrl+C.

**What this step actually validates:**

- The self-describing surface (`GET /`) returns the expected map
- `/diagnose` returns a bounded summary with a `support_claim`
  field pointing at this envelope
- The surface is read-only (mutation verbs return 501)

**What this step does NOT validate:**

- The **sparrow's demos run on in-memory `BlockStore`, not
  WALStore or smartwal.Store.** The `/diagnose` output reflects
  that in-memory demo state. It does not prove the current
  process is exercising the persistent RF2 slice.
- The persistent slice itself — that's what Step 1 proves.

Treat Step 2/3 as a surface sanity check. Read them together with
Step 1 for the full picture.

## Step 3 — Read the diagnosis surface

### 3a. Discover the surface

```bash
curl -s localhost:9090/ | jq
```

Returns the surface map: the scope statement, every sub-endpoint
with a one-line description, and a read-only note. This is the
self-describing P11 single-node product surface — the entry point
for any operator unfamiliar with the repo.

### 3b. Read /diagnose

```bash
curl -s localhost:9090/diagnose | jq
```

The bounded diagnosis summary. Fields in the order you should read
them:

| Field | What it tells you |
|---|---|
| `mode` | `healthy` → done. `degraded` → read further. |
| `decision` | Engine's most recent recovery classification: `none`, `catch_up`, `rebuild`, `unknown`. |
| `session_phase` | `none` / `starting` / `running` / `completed` / `failed`. |
| `frontiers.replica_flushed_lsn` (R) | Replica's durable frontier. |
| `frontiers.primary_tail_lsn` (S) | Primary's oldest retained WAL LSN. |
| `frontiers.primary_head_lsn` (H) | Primary's newest written LSN. |
| `last_session_kind` | Most recent recovery class that ran (`catch_up` / `rebuild` / empty). |
| `last_decision` | Literal string the engine last traced. |
| `watchdog_summary` | Counters for start-timeout timer events. |
| `support_claim` | **Points at the bounded support envelope doc.** |

### 3c. Interpret R/S/H

- R ≥ H → caught up (no recovery needed)
- R ≥ S, R < H → catch-up territory
- R < S → rebuild territory

This is the same classification the engine applies.

## Step 4 — Decide the operator action

| Projection state | Operator action |
|---|---|
| `mode == "healthy"` | None — replica is caught up. |
| `mode != "healthy"` and `session_phase == "running"` | Wait — recovery in progress. |
| `mode != "healthy"` and `session_phase == "failed"` | Consult `/trace` for the full decision history; reassign if needed. |
| `watchdog_summary.fires > 0` and `watchdog_summary.cleared_by_start == 0` | Real start-timeout failure — consult `/trace`. |
| Anything else | Consult `/trace` for context; the engine trace is the authoritative decision log. |

## Step 5 — Where to look when something fails

| Symptom | Read this |
|---|---|
| Test failed in step 1 | The failing subtest name; the engine trace assertion message |
| `/diagnose` shows degraded | `/trace` for the decision sequence; `/watchdog` for timer events |
| Something not in the supported table | [docs/p13-support-envelope.md](p13-support-envelope.md) "Not yet supported" — probably out of scope |
| Operator workflow question | [docs/single-node-surface.md](single-node-surface.md) — surface contract |
| Lower-institution question | `docs/data-sync-institution.md`, `docs/recovery-execution-institution.md`, `docs/replicated-slice.md` |

## What this workflow does not cover

Everything in the "Not yet supported" table of the envelope doc:
topology authority, automated failover, RF3, frontend protocols,
operator governance, in-flight abrupt stop, proactive R≥H fence.
If your question is about any of those, the answer is **"not in
this slice"** — those are P14 / P15 work.
