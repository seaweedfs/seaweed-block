// Package recovery is the V3 dual-lane rebuild/recover mechanism (G7 redo).
//
// This package contains the layer-1 receiver mechanism plus the
// primary-side feeder/session coordinator used by the transport
// integration. It still has no engine state and does not mint lineage.
//
// # Architecture (three layers, this package = layer 1)
//
//	Layer 1 receiver (this package, core/recovery/):
//	  - RebuildBitmap: per-LBA "WAL has won base" flag.
//	  - RebuildSession: receiver-side session that applies base + WAL
//	    lanes concurrently, arbitrates via the bitmap, emits base-batch
//	    ack facts, and reports completion via TryComplete().
//
//	Layer 2 feeder/session control (this package + core/transport/):
//	  - PeerShipCoordinator: per-replica single-flight session state,
//	    pin floor, barrier witness, and close authorization.
//	  - Sender: one session driver. It owns base frames and delegates
//	    all WAL feeding to a WalShipperSink, so recovery and live WAL do
//	    not have two independent emitters.
//	  - Primary-side pin_floor management (recycle gate; advances on
//	    replica BaseBatchAcked facts).
//	  - Session lifecycle, fanout of OnLocalWrite to active sessions.
//	  - Barrier witness + coordinator close predicate.
//
//	Layer 3 (engine / adapter integration):
//	  - Error classification, retry budgets, escalation rules.
//
// # POC assumptions (for first integration milestone)
//
//   - Base lane is dense: sender ships every LBA in [0, NumBlocks)
//     (no zero-block elision). When sparse mode lands, it becomes a
//     separate INV with explicit substrate basement-clearing — see
//     INV-BASE-SPARSE-REQUIRES-SUBSTRATE-CLOSURE.
//   - Base batch ack is an LBA prefix cursor: receiver reports "all
//     LBAs < lbaUpper have been installed via base lane". Range-list
//     acks are a later refinement.
//   - WAL lane reuses one wire message (frameWALEntry) for both
//     historical backlog (kind=Backlog) and session-live writes
//     (kind=SessionLive). Receiver-side dispatch is by SessionStart
//     activation + per-frame Kind byte, not by payload-byte heuristic.
//   - Bitmap: single sync.Mutex on RebuildSession protects bitmap +
//     walApplied + baseDone. Fine-grained locking is a perf milestone
//     after correctness is proved.
//   - TryComplete() is the receiver-side per-cut witness:
//     baseDone ∧ barrierWitnessed. The primary-side coordinator is the
//     authority that decides whether a session may close.
//   - Sender runs BASE and WAL concurrently. BASE frames are written by
//     Sender; WAL frames are owned by the injected WalShipperSink. This
//     preserves the single-feeder invariant: one ordered WAL egress
//     entity per peer, with Sender only orchestrating the session.
//
// # Forward-looking semantic refinement (not yet implemented)
//
//   - Bitmap as WAL-claim (anti-base-fill), not just WAL-applied:
//     when a stale/duplicate WAL LSN arrives that the substrate's
//     apply gate skips (per-LBA stale-skip), the bitmap may STILL
//     be marked to prevent base lane from refilling that LBA with
//     historical extent bytes. Today MarkApplied is called after a
//     successful ApplyEntry; the refined semantic decouples them:
//     "claim" the LBA on the WAL side regardless of whether the
//     specific entry's bytes were durably written. This handles the
//     overlap-history case where Replica is already ahead of the
//     Primary's chosen pin. Candidate INV: INV-REPL-OVERLAP-HISTORY-
//     NO-REGRESS (replica must not regress per-LBA bytes when a
//     Primary's chosen pin overlaps already-shipped history).
//
// # Layer 3 error taxonomy (this package)
//
// All sender / receiver error returns are wrapped in `Failure` (see
// failure.go) carrying Kind + Phase + Underlying. Callers (engine /
// integration shim) inspect via errors.As to make retry decisions.
// Kinds map onto core/engine/RecoveryFailureKind at the integration
// boundary; the mapping is in the integration shim, not here.
//
// # INV ledger (G7 redo)
//
// Layer 1 (this package):
//
//	INV-DUAL-LANE-WAL-WINS-BASE
//	  Same LBA: once WAL lane has applied (bitmap.MarkApplied), base
//	  lane MUST skip subsequent base blocks for that LBA.
//
//	INV-SESSION-COMPLETE-ON-CONJUNCTION-LAYER1
//	  TryComplete returns done=true iff baseDone ∧ barrierWitnessed.
//	  walApplied/AchievedLSN are observations, not a local target-band
//	  completion oracle.
//
//	INV-BITMAP-NO-INDEPENDENT-LOCK
//	  RebuildBitmap exposes no synchronization; callers (RebuildSession)
//	  serialize access via session mutex. V2-faithful design.
//
// Layer 2 (future, listed here for context):
//
//	INV-PIN-EXISTS-ONLY-DURING-SESSION
//	  Primary maintains no pin_floor in steady state; recycle proceeds
//	  per retention. pin_floor appears only during an active session.
//
//	INV-PIN-STABLE-WITHIN-SESSION
//	  Same session: pin_floor is monotonically non-decreasing. To pick
//	  a different anchor, invalidate session and start a new lineage.
//
//	INV-PIN-ADVANCES-ONLY-ON-REPLICA-ACK
//	  pin_floor advances iff the replica has emitted a BaseBatchAcked
//	  fact for an LBA range whose base data is now installed on the
//	  replica side. Primary cannot advance pin_floor on its own.
//	  See docs/recovery-pin-floor-wire.md §3 (cadence) + §4 (semantics).
//
//	INV-PIN-COMPATIBLE-WITH-RETENTION
//	  pin_floor ≥ retained(S) always holds. A session demanding a pin
//	  below S MUST fail-loud → invalidate → new lineage.
//	  See docs/recovery-pin-floor-wire.md §5 for the inequality and
//	  the proposed FailurePinUnderRetention kind.
//
//	INV-RECYCLE-GATED-BY-MIN-ACTIVE-PIN
//	  Primary's WAL recycle floor = min(pin_floor) over active sessions;
//	  no sessions ⇒ pure retention.
//
//	INV-SESSION-COMPLETE-CLOSURE
//	  System-level done = receiver witness ∧ primary coordinator close
//	  predicate. In the dual-lane path the close predicate consumes
//	  PrimaryWalLegOk recorded at BarrierReq emission; AchievedLSN is
//	  an observation.
//
//	INV-LIVE-CAUGHT-UP-IFF-FRONTIER-AT-BARRIER
//	  "Live caught up" is provable only via probe/barrier comparing
//	  R_repr against H at a frozen time, not via shipper liveness.
//
//	INV-SESSION-TEARDOWN-IS-EXPLICIT
//	  Convergence → InSync → teardown session → drop pin_floor →
//	  recycle resumes. All transitions are explicit events.
//
//	INV-SINGLE-FLIGHT-PER-REPLICA
//	  At most one active rebuild session per replica at any time.
//	  Concurrent attempts are rejected at session start.
package recovery
