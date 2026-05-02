package recovery

import (
	"errors"
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// RebuildSession is the receiver-side mechanism for one rebuild lineage.
// Two lanes feed it concurrently:
//
//   - Base lane (ApplyBaseBlock): the snapshot data the primary's
//     base sender ships, representing extent contents at the session's
//     pin boundary. Each call consults the bitmap; if the WAL lane has
//     already won at this LBA, the base block is skipped.
//   - WAL lane (ApplyWALEntry): in-order, LSN-bearing entries from
//     the primary's live shipper for writes that landed at or above
//     the pin. Each apply marks the bitmap so subsequent base blocks
//     for the same LBA are skipped.
//
// Both lanes serialize through `mu`. Layer 1 design choice: single
// session mutex (V2-faithful). Fine-grained locking is a perf
// milestone after correctness is proved.
//
// This type knows nothing about pin_floor, primary recycle, wire
// protocol, or barrier ack. Those live in layer 2.
type RebuildSession struct {
	store     storage.LogicalStorage
	targetLSN uint64 // frozen at session creation; WAL lane must reach this for completion

	mu         sync.Mutex
	bitmap     *RebuildBitmap
	walApplied uint64 // highest LSN observed via WAL lane in this session; monotonic
	baseDone   bool   // base lane has signaled stream-complete via MarkBaseComplete
	completed  bool   // TryComplete has returned done=true at least once; latched

	// barrierWitnessed: latched true on the FIRST WitnessBarrier() call
	// (receiver invokes this at frameBarrierReq arrival, before TryComplete).
	//
	// §IV.2.1 / recover-semantics-adjustment-plan §1 — A-class wave:
	// the arrival of BarrierReq from the primary attests that the primary's
	// WalShipper observed PrimaryWalLegOk under serializer lock and chose to
	// emit. Layer-1 TryComplete adds this as an explicit conjunct so the
	// replica's "session done" signal cannot be claimed without the primary's
	// barrier witness — matching the §IV.2.1 coordinator-owned narrative.
	//
	// Pre-A-class behavior (sole `walApplied >= targetLSN`) was the
	// recover(a,b) replica-side independent verdict; per §I P8 / G0 closure
	// that authority moves to the primary (coordinator), and the replica
	// becomes a per-cut witness, not a closer.
	barrierWitnessed bool

	// Per-kind WAL apply counters (observability only — do NOT use
	// to infer convergence; see WALEntryKind doc). Useful for INV
	// pinning in tests and operator dashboards: "session shipped N
	// backlog entries, M session-live entries".
	backlogApplied     uint64
	sessionLiveApplied uint64

	// Base lane progress (POC: contiguous prefix cursor).
	// `baseAckedPrefix` advances on BaseBatchAcked(lbaUpper); layer 2
	// reads it to advance Primary-side pin_floor. Range-list ack is a
	// later refinement.
	baseAckedPrefix uint32
}

// NewRebuildSession constructs a session targeting the given LSN.
// numBlocks comes from the substrate (store.NumBlocks()) and sizes the
// bitmap. targetLSN is the frozen WAL completion target.
func NewRebuildSession(store storage.LogicalStorage, targetLSN uint64) *RebuildSession {
	return &RebuildSession{
		store:     store,
		targetLSN: targetLSN,
		bitmap:    NewRebuildBitmap(store.NumBlocks()),
	}
}

// ApplyBaseBlock installs one base-lane block at lba. Returns
// (skipped=true, nil) when the WAL lane has already won this LBA;
// (skipped=false, nil) when the substrate apply succeeded.
//
// Per v3-recovery-algorithm-consensus.md §6.10 (INV-RECV-BITMAP-CORE):
// BASE writes go through `WriteExtentDirect`, NOT the LSN-tracked
// `ApplyEntry` path. The bitmap is the sole arbiter of BASE-vs-WAL
// conflict at this LBA; the substrate's per-LBA stale-skip / WAL-
// record machinery does not interpret BASE bytes as a competing-LSN
// write (because BASE never enters the substrate's WAL).
//
// Frontier advancement is deferred to `MarkBaseComplete`, which calls
// `store.AdvanceFrontier(targetLSN)` once the BASE phase signals
// stream-complete. Pre-§6.10 code piggybacked on `ApplyEntry`'s
// implicit frontier-advance side effect; the new direct-extent path
// must opt in explicitly.
//
// INV-DUAL-LANE-WAL-WINS-BASE.
func (s *RebuildSession) ApplyBaseBlock(lba uint32, data []byte) (skipped bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.completed {
		return false, errors.New("recovery: ApplyBaseBlock after session completed")
	}
	if s.bitmap.IsApplied(lba) {
		return true, nil
	}
	if err := s.store.WriteExtentDirect(lba, data); err != nil {
		return false, fmt.Errorf("recovery: base extent-direct lba=%d: %w", lba, err)
	}
	return false, nil
}

// ApplyWALEntry installs one WAL-lane entry at lba with the entry's
// real LSN. Marks the bitmap so subsequent base blocks for the same
// LBA are skipped. walApplied advances to the new LSN if it is the
// highest seen so far.
//
// `kind` distinguishes historical-backlog replay from session-live
// writes for observability + test pinning. It does NOT affect apply
// behavior (both kinds win base lane via bitmap). See WALEntryKind
// docstring: kind is observability, not a convergence proof.
//
// Stale-skip is NOT done at this layer — it is the substrate's
// responsibility (or layer 2's apply gate). Layer 1 reflects whatever
// the substrate accepts. If a strict per-LBA stale-skip is required,
// it lives in the apply gate that calls into here.
func (s *RebuildSession) ApplyWALEntry(kind WALEntryKind, lba uint32, data []byte, lsn uint64) error {
	if kind != WALKindBacklog && kind != WALKindSessionLive {
		return fmt.Errorf("recovery: ApplyWALEntry kind=%d not in {Backlog, SessionLive}", kind)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.completed {
		return errors.New("recovery: ApplyWALEntry after session completed")
	}
	if err := s.store.ApplyEntry(lba, data, lsn); err != nil {
		return fmt.Errorf("recovery: wal apply lba=%d lsn=%d: %w", lba, lsn, err)
	}
	s.bitmap.MarkApplied(lba)
	if lsn > s.walApplied {
		s.walApplied = lsn
	}
	switch kind {
	case WALKindBacklog:
		s.backlogApplied++
	case WALKindSessionLive:
		s.sessionLiveApplied++
	}
	return nil
}

// BaseBatchAcked records that base lane has finished installing all
// LBAs in [0, lbaUpper). Called by the receiver-side handler after a
// chunk of base blocks has been applied. Layer 2 reads
// `BaseAckedPrefix` to advance Primary's pin_floor.
//
// POC: contiguous prefix only. Range-list acks are a later refinement.
func (s *RebuildSession) BaseBatchAcked(lbaUpper uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lbaUpper > s.baseAckedPrefix {
		s.baseAckedPrefix = lbaUpper
	}
}

// MarkBaseComplete signals that the base lane has shipped the last
// block. Combined with walApplied ≥ targetLSN, this is the layer-1
// completion predicate.
//
// Per §6.10 frontier-pairing: BASE writes go through
// `WriteExtentDirect` which does NOT advance the substrate's
// nextLSN / walHead / syncedLSN. The recovery layer compensates by
// advancing the frontier to `targetLSN` here — once BASE is done,
// the replica's R/H boundaries reflect "the snapshot at fromLSN
// is installed" without needing a competing WAL apply at
// targetLSN. Subsequent WAL frames at LSN > targetLSN advance
// further via `ApplyEntry`'s normal side effects.
func (s *RebuildSession) MarkBaseComplete() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.baseDone = true
	// §6.10 frontier reconciliation: BASE bytes are now installed
	// to extent; declare frontier at targetLSN so post-rebuild
	// Sync()/Boundaries() report a non-degenerate value even on
	// BASE-only sessions (no WAL frames in this session).
	s.store.AdvanceFrontier(s.targetLSN)
}

// SeedWalApplied seeds walApplied at session start with the receiver's
// fromLSN watermark. Called by the receiver after constructing the
// session in frameSessionStart, BEFORE any ApplyWALEntry.
//
// Rationale: for a "rebuild" session where the base lane covers the
// entire snapshot through targetLSN, no WAL frames will arrive (nothing
// to drain past the pin on the primary side). Without seeding, walApplied
// stays at 0 and TryComplete's `walApplied >= targetLSN` conjunct never
// holds — base-only rebuilds would never close. Seeding with fromLSN
// represents "the receiver starts the session already at or past
// fromLSN by virtue of base-lane coverage"; ApplyWALEntry monotonically
// advances it from there if WAL frames do arrive.
//
// Idempotent: only advances; lower seeds are silently ignored.
func (s *RebuildSession) SeedWalApplied(lsn uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if lsn > s.walApplied {
		s.walApplied = lsn
	}
}

// WitnessBarrier latches the barrier-arrival witness. Called by the
// receiver immediately on frameBarrierReq arrival, BEFORE TryComplete.
//
// §IV.2.1 / recover-semantics-adjustment-plan §1 — A-class wave:
// per §I P8, the "session semantically complete" claim is
// coordinator-owned (primary). On the replica side, the arrival of
// BarrierReq IS the witness that the primary's WalShipper observed
// PrimaryWalLegOk (DebtZero ∨ LiveTail) at the serializer-locked
// snapshot. Layer-1 TryComplete adds this as a required conjunct so
// the replica's local view cannot independently claim closure.
//
// Idempotent: latch-only, no error on repeat calls.
func (s *RebuildSession) WitnessBarrier() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.barrierWitnessed = true
}

// TryComplete reports whether the layer-1 completion predicate holds.
// Returns (achievedLSN, true) when baseDone ∧ walApplied ≥ targetLSN
// ∧ barrierWitnessed; achievedLSN is walApplied.
//
// §IV.2.1 / recover-semantics-adjustment-plan §1 — A-class wave:
// the historic recover(a,b) sole gate (`walApplied >= targetLSN`) is
// no longer dispositive. The realizable §IV.2.1 conjunct synthesized
// from existing observables is:
//
//	baseDone (replica MarkBaseComplete fired)
//	∧ walApplied ≥ targetLSN  (kept pre-C-class for Y-as-comparator;
//	                          C-class will replace with cut-witness)
//	∧ barrierWitnessed         (BarrierReq arrival = primary attests
//	                          PrimaryWalLegOk under shipMu)
//
// Note: layer-1 TryComplete is NOT the system-close authority.
// Per §IV.2.1, the terminal claim "session semantically complete"
// is coordinator-owned (primary's CanEmitSessionComplete after
// reading BarrierResp). TryComplete here is the replica's
// per-cut layer-1 witness check, not a verdict.
//
// Idempotent: once done=true, the session is latched completed and
// subsequent Apply* calls return errors.
func (s *RebuildSession) TryComplete() (achievedLSN uint64, done bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.completed {
		return s.walApplied, true
	}
	if !s.baseDone {
		return 0, false
	}
	if s.walApplied < s.targetLSN {
		return 0, false
	}
	if !s.barrierWitnessed {
		return 0, false
	}
	s.completed = true
	return s.walApplied, true
}

// Status returns a snapshot of session progress for layer 2 / monitoring.
type Status struct {
	TargetLSN          uint64
	WALApplied         uint64
	BaseDone           bool
	Completed          bool
	BarrierWitnessed   bool   // §IV.2.1 A-class: latched on frameBarrierReq arrival
	BaseAckedPrefix    uint32
	BitmapApplied      int    // count of LBAs the WAL lane has marked
	BacklogApplied     uint64 // count of WALKindBacklog entries applied
	SessionLiveApplied uint64 // count of WALKindSessionLive entries applied
}

func (s *RebuildSession) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Status{
		TargetLSN:          s.targetLSN,
		WALApplied:         s.walApplied,
		BaseDone:           s.baseDone,
		Completed:          s.completed,
		BarrierWitnessed:   s.barrierWitnessed,
		BaseAckedPrefix:    s.baseAckedPrefix,
		BitmapApplied:      s.bitmap.AppliedCount(),
		BacklogApplied:     s.backlogApplied,
		SessionLiveApplied: s.sessionLiveApplied,
	}
}
