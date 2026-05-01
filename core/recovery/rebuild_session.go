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

// TryComplete reports whether the layer-1 completion predicate holds.
// Returns (achievedLSN, true) when baseDone ∧ walApplied ≥ targetLSN;
// achievedLSN is walApplied (which by predicate is ≥ targetLSN).
//
// Idempotent: once it returns done=true, the session is latched
// completed and subsequent Apply* calls return errors.
//
// INV-SESSION-COMPLETE-ON-CONJUNCTION-LAYER1. Note: the system-level
// "rebuild done" closure additionally requires a barrier-ack from the
// replica matching achievedLSN — that is layer 2's responsibility.
//
// §IV.2.1 / FS-1 / Gate G0 — Tier 1 completion-authority site.
// The `walApplied < targetLSN` predicate below is the historic
// recover(a,b) gate; per consensus §I P8, this is NOT the recover(a)
// completion authority. The migration target (per
// `sw-block/design/recover-semantics-adjustment-plan.md` §1 row
// "TryComplete: walApplied ≥ Target 独断 → 新 conjunct" and
// `learn/2026-05-01-recover-target-audit.md` Tier 1 row) replaces
// this with `baseDone ∧ ReplicaWalWitness ∧ (coordinator-explicit
// PrimaryWalLegOk OR barrier state machine phase)`. NO behavior
// change pre-Gate G0; Tier-1 edits await T2 predicate name + witness
// channel pinning.
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
	s.completed = true
	return s.walApplied, true
}

// Status returns a snapshot of session progress for layer 2 / monitoring.
type Status struct {
	TargetLSN          uint64
	WALApplied         uint64
	BaseDone           bool
	Completed          bool
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
		BaseAckedPrefix:    s.baseAckedPrefix,
		BitmapApplied:      s.bitmap.AppliedCount(),
		BacklogApplied:     s.backlogApplied,
		SessionLiveApplied: s.sessionLiveApplied,
	}
}
