// Package recovery is the V3 dual-lane rebuild/recover mechanism (G7 redo).
//
// This package contains ONLY the layer-1 mechanism — no engine state,
// no transport wire, no primary-side coordinator. It can be unit-tested
// in isolation against any LogicalStorage implementation.
//
// # Architecture (three layers, this package = layer 1)
//
//	Layer 1 (this package, core/recovery/):
//	  - RebuildBitmap: per-LBA "WAL has won base" flag.
//	  - RebuildSession: receiver-side session that applies base + WAL
//	    lanes concurrently, arbitrates via the bitmap, emits base-batch
//	    ack facts, and reports completion via TryComplete().
//
//	Layer 2 (TODO, will live in core/rebuild_coordinator/ or engine):
//	  - Primary-side pin_floor management (recycle gate; advances on
//	    replica BaseBatchAcked facts).
//	  - Session lifecycle, fanout of OnLocalWrite to active sessions.
//	  - Barrier-ack closure (system-level proof of catch-up).
//	  - Single-flight enforcement: at most one active rebuild session
//	    per replica.
//
//	Layer 3 (TODO):
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
//     historical backlog (kind=Backlog) and post-target session live
//     (kind=SessionLive). Receiver-side dispatch is by SessionStart
//     activation + per-frame Kind byte, not by payload-byte heuristic
//     (round-46 lane-purity preserved).
//   - Bitmap: single sync.Mutex on RebuildSession protects bitmap +
//     walApplied + baseDone. Fine-grained locking is a perf milestone
//     after correctness is proved.
//   - TryComplete() is layer-1's closure: baseDone ∧ walApplied ≥
//     targetLSN. barrier-ack is layer 2's system-level confirmation.
//   - Sender: SEQUENTIAL phases — base-lane, BaseDone, backlog scan,
//     wait Close(), drainAndSeal live queue, barrier. SessionLive
//     entries pushed via PushLiveWrite are buffered and flushed AFTER
//     backlog drain, NOT interleaved by LSN order in real time.
//     Spec §3.2 #3 ("one ordered outbound queue per peer mixing
//     recover-tagged and post-target traffic with explicit LSN order")
//     is the next milestone. Documented as an explicit POC gap so
//     "tests pass" is not misread as "spec §3.2 #3 satisfied".
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
//	  TryComplete returns done=true iff baseDone ∧ walApplied ≥
//	  targetLSN. NOT including the barrier-ack: that is layer-2's
//	  system-level confirmation, not layer-1's mechanical predicate.
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
//	  System-level done = layer-1 TryComplete ∧ barrier-ack(achieved
//	  == targetLSN). Both halves required before declaring InSync.
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
