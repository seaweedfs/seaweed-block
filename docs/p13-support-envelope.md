# P13 First-Launch Support Envelope

**Status: closed (P13 S4 release-gate artifact)**

This is the **final P13 closure artifact** for the bounded
first-launch replicated slice. It names what the current persistent
RF2 slice supports, what it explicitly does not yet support, where
the evidence lives for each claim, and the one canonical validation
workflow a tester or operator runs to check the slice is working.

This is the bounded first-launch contract — **not a production
block-service completion statement**.

## Start here

1. **Run the validation workflow** — [docs/first-launch-validation.md](first-launch-validation.md)
2. **Read the supported / unsupported tables** below
3. **Hit `GET /diagnose`** on the running sparrow for live state — its `support_claim` field points back to this doc

Anything not on this page is out of scope for P13 first-launch.

## One-sentence scope

**Persistent replicated RF2 handoff/rejoin on `WALStore` and
`smartwal.Store`, qualified against graceful and abrupt-stop
replica faults, with a read-only diagnosis surface that mirrors
accepted lower truth. No new semantics, no policy, no topology
authority.**

## Supported now (frozen at P13 closure)

| # | Claim | Evidence |
|---|---|---|
| 1 | Graceful persistent handoff convergence (epoch+1) | `TestPersistent_Handoff_Convergence/{walstore,smartwal}` |
| 2 | Graceful reopen + `Recover()` after handoff | `TestPersistent_Rejoin_AfterReopen/{walstore,smartwal}` |
| 3 | Catch-up branch (R ≥ S, R < H) on persistent storage | `TestPersistent_CatchUp_Branch/{walstore,smartwal}` — trace asserted |
| 4 | Rebuild branch (R < S) on persistent storage | `TestPersistent_Rebuild_Branch/{walstore,smartwal}` — trace asserted |
| 5 | Abrupt stop after successful convergence — acked data survives | `TestAbrupt_StopAfterConvergence_AckedDataSurvives/{walstore,smartwal}` |
| 6 | Abrupt stop after catch-up branch — rebuild + catch-up blocks both survive | `TestAbrupt_StopAfterCatchUp_AckedDataSurvives/{walstore,smartwal}` |
| 7 | Abrupt stop after rebuild branch — rebuild blocks survive | `TestAbrupt_StopAfterRebuild_AckedDataSurvives/{walstore,smartwal}` |
| 8 | Rejoin after abrupt stop — new primary converges through recovered replica | `TestAbrupt_RejoinAfterAbruptStop_Converges/{walstore,smartwal}` |
| 9 | Stale old-lineage traffic rejected after reopen | `TestAbrupt_StaleLineageAfterReopen_Rejected/{walstore,smartwal}` |
| 10 | Read-only diagnosis surface (`GET /diagnose`) mirrors lower truth | `TestDiagnose_*` in `core/ops/surface_test.go` |

Every row is proven on **both** persistent backends
(`WALStore` WAL+extent and `smartwal.Store` extent-direct).

## Not yet supported / not yet claimed (frozen at P13 closure)

| # | Concern | Owner |
|---|---|---|
| 1 | Abrupt stop **during in-flight** rebuild/catch-up (interrupted mid-session) | Future P13 slice |
| 2 | Caught-up handoff proactive fence (R ≥ H lineage gap) | P14 |
| 3 | Automated failover trigger (who/when decides reassignment) | P14 |
| 4 | Topology authority / epoch minting | P14 |
| 5 | RF3 / quorum / multi-replica coordination | P14 |
| 6 | Frontend protocols (iSCSI, NVMe/TCP, CSI) | P15 |
| 7 | Operator governance (retire / repair / rebalance verbs) | P15 |
| 8 | Primary-side abrupt fault qualification (this slice covers replica-side only) | Future P13 slice |
| 9 | Cross-datacenter / cross-zone topology | Post-P14 |
| 10 | Online schema changes, resize, tiering | Post-P15 |

## Release gate — what each route claims, proves, exposes

| Route | Proof | Evidence surface | Support claim | Out-of-scope note |
|---|---|---|---|---|
| Handoff (epoch bump) | `TestPersistent_Handoff_Convergence` | `/projection` (Epoch, SessionKind), `/trace` (`decision` steps), `/diagnose` (last_decision, last_session_kind) | Old primary → new primary → replica byte-for-byte converges | Who picks the new primary, when to bump epoch → P14 |
| Catch-up | `TestPersistent_CatchUp_Branch` | `/diagnose` (decision="catch_up", frontiers.R ≥ frontiers.S) | R ≥ S, R < H path runs with no rebuild escalation | Streaming catch-up with live WAL window → later P13/P14 |
| Rebuild | `TestPersistent_Rebuild_Branch` | `/diagnose` (decision="rebuild", frontiers.R < frontiers.S) | R < S path runs with full base blocks + frontier advance | Resumable/partial rebuild → P14 |
| Graceful reopen | `TestPersistent_Rejoin_AfterReopen` | `/diagnose` frontier non-zero after reopen | Recovered replica holds new primary's truth | Crash timing variants → see "in-flight abrupt stop" |
| Abrupt stop + reopen | `TestAbrupt_*` | WALStore defensive-scan log / SmartWAL CRC-verified ring log | Acked data survives; recovered R and H do not regress below pre-stop values; stale old-epoch rejected | In-flight interruption → future P13 slice |

## Evidence surface

The single-node read-only HTTP surface is the operator-facing
entry point. It is a mirror over accepted lower truth — no
mutation verbs, no new authority.

| Endpoint | What it answers |
|---|---|
| `GET /` | What endpoints exist; what this surface does and does not do |
| `GET /status` | Version, uptime, current demo label |
| `GET /projection` | The engine's deterministic projection (Mode, RecoveryDecision, SessionKind, SessionPhase, Epoch, EndpointVersion, R, S, H, Reason) |
| `GET /trace` | The full decision trace for the current demo |
| `GET /watchdog` | Start-timeout watchdog lifecycle events |
| `GET /diagnose` | Bounded diagnosis: mode + session + **frontiers** + **last decision** + **last session kind** + watchdog summary + **support_claim** pointing back here |

The `support_claim` field on `/diagnose` is the operator's anchor
to this doc — it's the shortest honest statement of what this
slice supports.

## Diagnosis: "is this replica healthy / recovering / degraded?"

Use `GET /diagnose` and read in this order:

1. **`mode`** — `healthy`, `degraded`, etc.
2. **`decision`** — the engine's most recent recovery classification (`none`, `catch_up`, `rebuild`, `unknown`).
3. **`session_phase`** — lifecycle state (`none`, `starting`, `running`, `completed`, `failed`).
4. **`frontiers`** — R/S/H triple.
   - R ≥ H → caught up
   - R ≥ S, R < H → catch-up territory
   - R < S → rebuild territory
5. **`last_session_kind`** — what recovery class last ran (catch-up or rebuild).
6. **`last_decision`** — the literal string the engine last traced.
7. **`watchdog_summary`** — timer events. `fires > 0` without a matching `cleared_by_start` → real start-timeout failure.

If `mode == "healthy"`, the operator action is **none**.
If `mode != "healthy"` and `session_phase == "running"`, the
operator action is **wait** (recovery in progress).
Anything else: consult `/trace` for the full decision history.

## Porting notes (P13-appropriate)

This slice does **not** yet port:

- V2 topology / failover policy
- V2 operator shell
- V2 master / assignment authority
- V2 frontend (iSCSI / NVMe-oF)

This slice **does** reuse V2 patterns where they map directly:

- Durability contract (acked-on-Sync, lost-if-unacked) — matches
  V2's blockvol durability wording.
- Epoch-based lineage fencing — matches V2's `HandleAssignment`
  epoch bump semantics.

## Carry-forward to P14 / P15

- **P14** — proactive R ≥ H fence, automated failover, topology
  authority, epoch minting, RF3, placement.
- **P15** — operator governance surface, frontend protocols, full
  ecosystem integration.

## Honesty rule

Any claim in this doc that does not have a row in the "Supported
now" table or a linked test is **not** a supported claim. New
rows land only when a test backs them. Removed rows must carry
an explanatory note in git history.
