package replication

import (
	"fmt"
	"log"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/storage"
	"github.com/seaweedfs/seaweed-block/core/transport"
)

// ReplicaApplyGate is the T4d-2 lane-aware replica recovery apply
// gate. Per architect round-43 lock + round-44 refinement + kickoff
// §9 (Q1/Q2/Q3) + T4d mini-plan v0.3 §2.2:
//
//	"T4d stale-entry safety belongs at the replica recovery apply
//	 gate, with substrate fixes as defense-in-depth. The invariant
//	 is about no per-LBA data regression, not just frontier
//	 monotonicity."
//
// Lane behavior (round-43 verbatim):
//
//	"Recovery-stream stale entries are valid duplicates and must be
//	 skipped as data writes while still counted as recovery-stream
//	 coverage. Live-lane stale entries are abnormal and must not
//	 mutate data; they should be skipped/rejected under lineage/
//	 order diagnostics and must not advance recovery coverage."
//
// Round-44 refinements:
//   - INV-REPL-RECOVERY-COVERAGE-ADVANCES-ON-SKIP — skip data write
//     but per-session recoveryCovered MUST advance; otherwise barrier
//     completion lies and live lane sees "recovery hasn't covered
//     this slot."
//   - INV-REPL-LIVE-LANE-STALE-FAILS-LOUD — live lane stale entries
//     return error (caller logs + closes conn); do NOT silently skip;
//     do NOT advance recoveryCovered.
//
// Lane discrimination (Q2 — no wire byte; implicit from session
// context): catch-up / rebuild sessions ship with
// `lineage.TargetLSN > liveShipTargetLSN` (=1); live ship sessions
// ship with `TargetLSN == liveShipTargetLSN`. The gate reads the
// lineage's TargetLSN signal — already on the wire from T4a/T4c —
// without adding a new field.
//
// Per-LBA applied LSN source (Option C hybrid, kickoff §2.5 #1):
// at session init, query `store.AppliedLSNs()` for substrate-
// reported per-LBA latest-applied-LSN seed; substrates returning
// `ErrAppliedLSNsNotTracked` (BlockStore) fall back to session-only
// tracking, seeded from live + recovery applies during the session.
//
// Pinned invariants (T4d mini-plan §4 #6):
//   - INV-REPL-NO-PER-LBA-DATA-REGRESSION (goal-level)
//   - INV-REPL-RECOVERY-STALE-ENTRY-SKIP-PER-LBA (mechanism)
//   - INV-REPL-RECOVERY-COVERAGE-ADVANCES-ON-SKIP (round-44)
//   - INV-REPL-LIVE-LANE-STALE-FAILS-LOUD (round-44)
//   - INV-REPL-RECOVERY-COVERAGE-RESTART-SAFE (Option C makes pinnable)
//   - INV-REPL-LANE-DERIVED-FROM-HANDLER-CONTEXT (Q2)
type ReplicaApplyGate struct {
	store storage.LogicalStorage

	mu       sync.Mutex
	sessions map[uint64]*applyGateSession // keyed by lineage.SessionID
}

// applyGateSession is the per-recovery-session state. Per round-44
// 2-map split: liveTouched + recoveryCovered are distinct because
// the same physical LBA can appear in both (live lane wrote it AND
// recovery lane saw it during the session). appliedLSN is the
// stale-skip discriminator.
type applyGateSession struct {
	// appliedLSN: highest LSN successfully applied to each LBA
	// (across both lanes). The stale-skip discriminator.
	appliedLSN map[uint32]uint64

	// liveTouched: LBAs the live lane wrote during this session.
	// Recovery must yield to these (live-wins via per-LBA LSN check).
	liveTouched map[uint32]bool

	// recoveryCovered: LBAs the recovery stream processed during
	// this session (whether data was applied or stale-skipped).
	// Drives barrier-completion accounting (round-44 #1).
	recoveryCovered map[uint32]bool
}

// NewReplicaApplyGate constructs a gate over the given substrate.
//
// Called by: replica-side wiring code that constructs the
// `transport.ReplicaListener` with an `ApplyHook` option.
// Owns: per-session state maps; substrate query at session init.
// Borrows: store handle (caller retains; gate calls ApplyEntry +
// AppliedLSNs through the interface).
func NewReplicaApplyGate(store storage.LogicalStorage) *ReplicaApplyGate {
	return &ReplicaApplyGate{
		store:    store,
		sessions: make(map[uint64]*applyGateSession),
	}
}

// ApplyRecovery routes the entry through the recovery-lane apply
// path. Per round-46 architect ruling: the gate is lane-PURE —
// CALLER decides lane (from connection/session handler context)
// and invokes the appropriate explicit method. The gate does NOT
// inspect the lineage for lane discrimination.
//
// Implements `transport.ApplyHook.ApplyRecovery`. Called by
// `transport.ReplicaListener`'s MsgShipEntry handler when the
// caller has determined this entry is on the recovery lane.
//
// Behavior (architect round-43/44):
//   - if entry.LSN <= appliedLSN[lba] → SKIP data write +
//     advance `recoveryCovered` (round-44 #1)
//   - else → substrate.ApplyEntry + update `appliedLSN` +
//     `recoveryCovered`
//
// Returns nil on success (including legitimate stale-skip);
// non-nil error on substrate ApplyEntry failure.
//
// Owns: per-session state mutation under g.mu.
// Borrows: data slice (substrate may copy; gate does not retain).
func (g *ReplicaApplyGate) ApplyRecovery(lineage transport.RecoveryLineage, lba uint32, data []byte, lsn uint64) error {
	g.mu.Lock()
	sess, ok := g.sessions[lineage.SessionID]
	if !ok {
		sess = g.initSessionLocked(lineage.SessionID)
		g.sessions[lineage.SessionID] = sess
	}
	g.mu.Unlock()
	return g.applyRecovery(sess, lba, data, lsn)
}

// ApplyLive routes the entry through the live-lane apply path.
// Per round-46 architect ruling: the gate is lane-PURE — CALLER
// decides lane from connection/session handler context.
//
// Implements `transport.ApplyHook.ApplyLive`.
//
// Behavior (architect round-43/44):
//   - if entry.LSN <= appliedLSN[lba] → return error
//     (round-44 #2 INV-REPL-LIVE-LANE-STALE-FAILS-LOUD)
//   - else → substrate.ApplyEntry + update `appliedLSN` +
//     `liveTouched` ONLY (do NOT touch `recoveryCovered`)
func (g *ReplicaApplyGate) ApplyLive(lineage transport.RecoveryLineage, lba uint32, data []byte, lsn uint64) error {
	g.mu.Lock()
	sess, ok := g.sessions[lineage.SessionID]
	if !ok {
		sess = g.initSessionLocked(lineage.SessionID)
		g.sessions[lineage.SessionID] = sess
	}
	g.mu.Unlock()
	return g.applyLive(sess, lba, data, lsn)
}

// applyRecovery handles the recovery-lane apply: stale-skip + always
// advance recoveryCovered. Pins:
//   - INV-REPL-RECOVERY-STALE-ENTRY-SKIP-PER-LBA (skip data on stale)
//   - INV-REPL-RECOVERY-COVERAGE-ADVANCES-ON-SKIP (coverage advances
//     even when data is skipped — round-44 #1)
//   - INV-REPL-NO-PER-LBA-DATA-REGRESSION (goal-level result)
func (g *ReplicaApplyGate) applyRecovery(sess *applyGateSession, lba uint32, data []byte, lsn uint64) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if existing, ok := sess.appliedLSN[lba]; ok && lsn <= existing {
		// Stale: skip data write but ADVANCE recoveryCovered.
		sess.recoveryCovered[lba] = true
		return nil
	}
	// Fresh: apply data + update both maps.
	if err := g.store.ApplyEntry(lba, data, lsn); err != nil {
		return fmt.Errorf("apply gate: recovery substrate.ApplyEntry lba=%d lsn=%d: %w", lba, lsn, err)
	}
	sess.appliedLSN[lba] = lsn
	sess.recoveryCovered[lba] = true
	return nil
}

// applyLive handles the live-lane apply: fail-loud on stale (round-44
// #2 — INV-REPL-LIVE-LANE-STALE-FAILS-LOUD). Live lane is
// authoritative on (lineage, session) grounds; stale entry on live
// = wire ordering violation. Reject + return error so caller
// (replica handler) logs + closes conn.
//
// Live lane MUST NOT advance recoveryCovered (Q1/round-44 split).
func (g *ReplicaApplyGate) applyLive(sess *applyGateSession, lba uint32, data []byte, lsn uint64) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if existing, ok := sess.appliedLSN[lba]; ok && lsn <= existing {
		// Stale: fail-loud. Caller logs + drops conn.
		return fmt.Errorf("apply gate: live-lane stale entry lba=%d lsn=%d <= applied=%d (INV-REPL-LIVE-LANE-STALE-FAILS-LOUD)",
			lba, lsn, existing)
	}
	// Fresh: apply data + update appliedLSN + liveTouched. NOT
	// recoveryCovered — that's recovery-lane only.
	if err := g.store.ApplyEntry(lba, data, lsn); err != nil {
		return fmt.Errorf("apply gate: live substrate.ApplyEntry lba=%d lsn=%d: %w", lba, lsn, err)
	}
	sess.appliedLSN[lba] = lsn
	sess.liveTouched[lba] = true
	return nil
}

// initSessionLocked builds a fresh session state. Called with g.mu
// held. Seeds appliedLSN from substrate AppliedLSNs() (Option C
// hybrid); falls back to empty map on `ErrAppliedLSNsNotTracked`.
func (g *ReplicaApplyGate) initSessionLocked(sessionID uint64) *applyGateSession {
	sess := &applyGateSession{
		appliedLSN:      make(map[uint32]uint64),
		liveTouched:     make(map[uint32]bool),
		recoveryCovered: make(map[uint32]bool),
	}
	seed, err := g.store.AppliedLSNs()
	if err != nil {
		// ErrAppliedLSNsNotTracked is the documented sentinel (Option
		// C hybrid). Anything else is an unexpected substrate error;
		// log and proceed with empty map (gate's session-only
		// tracking will fill in via subsequent applies).
		log.Printf("apply gate: session %d substrate AppliedLSNs unavailable (%v); falling back to session-only tracking", sessionID, err)
		return sess
	}
	for lba, lsn := range seed {
		sess.appliedLSN[lba] = lsn
	}
	return sess
}

// CloseSession clears per-session state. Called by replica-side
// wiring on session teardown (lineage advance to a new session,
// peer disconnect, etc.). Optional — leaving stale sessions in the
// map costs memory but doesn't affect correctness for fresh sessions.
//
// Called by: replica-side session-lifecycle hook (post-T4d-2: the
// existing acceptMutationLineage advance path is the natural close
// point — when activeLineage advances to a new SessionID, the prior
// SessionID's gate state can be released).
func (g *ReplicaApplyGate) CloseSession(sessionID uint64) {
	g.mu.Lock()
	delete(g.sessions, sessionID)
	g.mu.Unlock()
}

// SessionState exposes per-session state for assertion-only
// inspection in tests + diagnostics. Returns a snapshot; caller-
// owned (mutation does not affect gate state).
//
// Called by: tests; ops-inspection surfaces.
func (g *ReplicaApplyGate) SessionState(sessionID uint64) (appliedLSN, liveTouched, recoveryCovered int, found bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	sess, ok := g.sessions[sessionID]
	if !ok {
		return 0, 0, 0, false
	}
	return len(sess.appliedLSN), len(sess.liveTouched), len(sess.recoveryCovered), true
}

// SessionRecoveryCoverage returns true iff the given LBA was
// processed by the recovery stream during this session (data
// applied OR stale-skipped). Drives barrier-completion accounting.
//
// Called by: tests pinning INV-REPL-RECOVERY-COVERAGE-ADVANCES-ON-SKIP.
func (g *ReplicaApplyGate) SessionRecoveryCoverage(sessionID uint64, lba uint32) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	sess, ok := g.sessions[sessionID]
	if !ok {
		return false
	}
	return sess.recoveryCovered[lba]
}

// SessionAppliedLSN returns the gate's tracked applied LSN for an
// LBA in this session. Returns 0 + false if not tracked.
//
// Called by: tests pinning per-LBA stale-skip discrimination.
func (g *ReplicaApplyGate) SessionAppliedLSN(sessionID uint64, lba uint32) (uint64, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	sess, ok := g.sessions[sessionID]
	if !ok {
		return 0, false
	}
	lsn, present := sess.appliedLSN[lba]
	return lsn, present
}
