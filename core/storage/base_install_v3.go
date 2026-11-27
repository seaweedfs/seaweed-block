package storage

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// The v3 BASE install protocol, store part (format bump v3, DESIGN r17.3.1
// sections 8.1, 8.2, 8.4, 8.5 and 8.7; plan row 9). It is dark: only tests
// reach it until the seam and the callers move onto it (row 9b) and the
// format is activated (row 12b).
//
// installMu is the topmost lock (DESIGN 4, 8.7). Begin and complete take it
// exclusive, which drains every already-admitted mutation of the old ID; a
// handle mutation takes it shared and checks, under it, that the volume is
// still installing under the handle's ID. Below it the order is the store's
// optional chunk capMu -> captureMu -> lifecycleMu -> syncMu -> s.mu -> descMu -> fileSyncMu,
// and nothing below ever waits for installMu.

var (
	// ErrInstallBaseUncovered: a begin from none whose BaseLSN is below the
	// acknowledged W or the checkpoint (DESIGN 8.1, r17.2).
	ErrInstallBaseUncovered = errors.New("storage: v3 BASE install refused: BaseLSN below W or CheckpointLSN")
	// ErrInstallSupersedeUncovered: a superseding begin whose BaseLSN is
	// below the acknowledged W or the checkpoint (DESIGN 8.1, 8.5).
	ErrInstallSupersedeUncovered = errors.New("storage: v3 superseding BASE install refused: BaseLSN below W or CheckpointLSN")
	// ErrInstallSuperseded: a mutation or completion through a handle whose
	// ID is no longer the installing ID (DESIGN 8.7).
	ErrInstallSuperseded = errors.New("storage: v3 install handle superseded")
	// ErrBaseInstallNoSpace: an install-lane append found no WAL space; the
	// session fails and is retried, nothing waits (DESIGN 8.4).
	ErrBaseInstallNoSpace = errors.New("storage: v3 install-lane append found no WAL space")
	// ErrInstallLSNGap: a replicated or install-lane entry would leave an
	// unanchored LSN gap (DESIGN 8.1, ApplyEntry gap rule).
	ErrInstallLSNGap = errors.New("storage: v3 replicated entry leaves an unanchored LSN gap")
)

// installReopenV3 is an installing reopen's recovered bounds (DESIGN 8.3).
type installReopenV3 struct {
	id      [16]byte
	E, R    uint64
	damaged bool
	walSize uint64
	records []walScannedRecordV3 // the trusted write records <= R, for a resume
}

// resumeBaseInstallV3 resumes the installing ID after a reopen (DESIGN
// 8.3, "Resume (same ID)"). Under installMu exclusive it appends nothing
// until all three hold: the trusted-through-W check (the recovered suffix
// is intact, R >= W*, and E >= the persisted retainedHead), the 6.6 fence
// of [E, floor + WALSize) with fsync, and a publication of retainedHead = E
// (G3). Otherwise it refuses with ErrInstallSuffixDamaged, writing nothing;
// the volume stays installing and not ready. The writer continues at E,
// the live position after this open's recovery; the recovered install-lane
// records are replayed so the completed image serves them.
func (s *WALStore) resumeBaseInstallV3(id [16]byte) (*installHandleV3, error) {
	v := s.v3
	if v == nil {
		return nil, errors.New("storage: v3 BASE resume on a non-v3 store")
	}
	v.installMu.Lock()
	defer v.installMu.Unlock()
	s.captureMu.Lock()
	defer s.captureMu.Unlock()
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	v.syncMu.Lock()
	defer v.syncMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("storage: BASE resume after Close")
	}
	if err := s.stickyIOFault(); err != nil {
		return nil, err
	}
	d := v.pub.current()
	ro := v.installReopen
	if d.BaseState != BaseInstalling || d.BaseInstallID != id {
		return nil, ErrInstallSuperseded
	}
	if ro == nil || ro.id != id || v.w == nil {
		return nil, fmt.Errorf("%w: resume without this open's recovered bounds", errV3NotYet)
	}
	if ro.damaged || ro.E < d.RetainedHead {
		return nil, fmt.Errorf("%w: R %d, E %d, persisted retainedHead %d", ErrInstallSuffixDamaged, ro.R, ro.E, d.RetainedHead)
	}
	v.recoverGen.Add(1)
	defer v.recoverGen.Add(1)
	floor, sameNonce := v.pub.reuseFloor()
	if err := s.fenceV3(ro.E, floor, d.WALSize, sameNonce, d.WALOffset); err != nil {
		return nil, err
	}
	if _, err := v.pub.publish(nil, nil, func(n *descriptorV3) error {
		if n.BaseState != BaseInstalling || n.BaseInstallID != id {
			return ErrInstallSuperseded
		}
		n.RetainedHead = ro.E
		return nil
	}); err != nil {
		return nil, err
	}
	if err := v.w.advanceHeadV3(ro.E); err != nil {
		return nil, err
	}
	for _, r := range ro.records {
		s.replayRecordV3(r, ro.walSize, d.BlockSize)
		e := r.Entry
		count := uint64(1)
		if e.Type == walEntryWriteBatch {
			count = e.BatchCount
		}
		for i := uint64(0); i < count; i++ {
			s.recordMapWriteV3(e.LBA+i, e.LSN+i)
		}
	}
	v.installReopen = nil
	v.liveInstallID = id
	v.installEpoch++
	v.preBaseHighest = 0 // every recovered record is of this install
	if err := v.pub.invalidateMapProofV3(); err != nil {
		return nil, err
	}
	return &installHandleV3{s: s, id: id, epoch: v.installEpoch}, nil
}

// installHandleV3 is the only path for BASE extent writes and install-lane
// WAL applies on a v3 store (DESIGN 8.7).
type installHandleV3 struct {
	s     *WALStore
	id    [16]byte
	epoch uint64 // the store's install epoch at this handle's begin
	// completed is set once this handle's own MarkBaseComplete succeeded:
	// the session's WAL lane may still be sending (the Sender sends BaseDone
	// from its base lane), so install-lane applies stay admitted until the
	// next begin.
	completed atomic.Bool
}

// beginBaseInstallV3 is the DESIGN 8.1 begin. It refuses, before any
// descriptor or extent write, unless BaseLSN covers both the acknowledged W
// and the checkpoint. It then freezes P (the writer's head, a record
// boundary), publishes installing (BaseLSN, BaseCursor = P, the new ID) and
// republishes it into the other slot, so both fallback choices are
// installing before the first BASE mutation is admitted.
func (s *WALStore) beginBaseInstallV3(baseLSN uint64, id [16]byte) (*installHandleV3, error) {
	v := s.v3
	if v == nil {
		return nil, errors.New("storage: v3 BASE install on a non-v3 store")
	}
	if id == ([16]byte{}) {
		return nil, fmt.Errorf("%w: zero BaseInstallID", errDescriptorTransition)
	}
	v.installMu.Lock() // exclusive: drains the old ID's admitted mutations
	defer v.installMu.Unlock()
	hooks := s.chunk.Load()
	release, err := holdChunkBaseTransitionV3(hooks)
	if err != nil {
		return nil, err
	}
	defer release()
	s.captureMu.Lock() // quiesce the install/capture boundary (8.1 step 1)
	defer s.captureMu.Unlock()
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	v.syncMu.Lock()
	defer v.syncMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("storage: BASE install after Close")
	}
	if err := s.stickyIOFault(); err != nil {
		return nil, err
	}
	if v.w == nil {
		return nil, errWALBindingV3 // a half-published reset before its Recover
	}
	// Attachment takes captureMu too. A layer installed after the outer
	// snapshot was not drained by this transition: retry without mutation.
	if s.chunk.Load() != hooks {
		return nil, ErrMapCoverageStale
	}
	d := v.pub.current()
	switch d.BaseState {
	case BaseInstalled:
		// installed -> installing is not an 8.5 edge: retire first. A
		// replica re-added with its older store meets this (v1 limit).
		return nil, fmt.Errorf("%w: begin while installed (retire first): %w", errDescriptorTransition, ErrRebuildRetainedBase)
	case BaseInstalling:
		if d.BaseInstallID == id {
			return nil, fmt.Errorf("%w: begin with the installing ID", errDescriptorTransition)
		}
		if d.BaseInstallID != v.liveInstallID {
			// A superseding begin after an installing reopen (DESIGN 8.3)
			// needs that reopen's recovered bounds.
			if ro := v.installReopen; ro == nil || ro.id != d.BaseInstallID {
				return nil, fmt.Errorf("%w: superseding begin without the installing reopen's recovered bounds", errV3NotYet)
			}
		}
	}
	// W includes every acknowledged LSN, published or not yet (mode C).
	// BaseLSN must also cover every appended LSN: an unacknowledged record
	// above BaseLSN would lie behind BaseCursor, so recovery from
	// max(CheckpointLSN, BaseLSN) would meet a hole where the allocator
	// continued. Such a begin is refused, writing
	// nothing; the records are never rewound or dropped to make room.
	w := max(d.W, v.wack.Load())
	appended := v.appended.Load()
	if baseLSN < w || baseLSN < d.CheckpointLSN || baseLSN < appended {
		refusal := ErrInstallBaseUncovered
		if d.BaseState == BaseInstalling {
			refusal = ErrInstallSupersedeUncovered
		}
		return nil, fmt.Errorf("%w: BaseLSN %d, W %d, CheckpointLSN %d, appended %d",
			refusal, baseLSN, w, d.CheckpointLSN, appended)
	}
	P := v.w.logicalHeadV3() // 8.1 step 2: the serialized writer's head
	if ro := v.installReopen; ro != nil && d.BaseState == BaseInstalling && ro.id == d.BaseInstallID {
		// Covering supersession after an installing reopen (DESIGN 8.3, 6.6,
		// G3): fence [E, floor + WALSize) and fsync before the installing
		// publication, so no record of the old install beyond E re-chains;
		// anchor at P = max(E, the persisted retainedHead), never lowering
		// retainedHead; the writer restarts at P. Reads and the flusher see
		// a recovery-like boundary.
		v.recoverGen.Add(1)
		defer v.recoverGen.Add(1)
		floor, sameNonce := v.pub.reuseFloor()
		if err := s.fenceV3(ro.E, floor, d.WALSize, sameNonce, d.WALOffset); err != nil {
			return nil, err
		}
		P = max(ro.E, d.RetainedHead)
		if err := v.w.advanceHeadV3(P); err != nil {
			return nil, err
		}
		s.dm.clear()
	}
	// Step 3: installing into the fallback slot.
	if _, err := v.pub.publish(nil, nil, func(n *descriptorV3) error {
		n.BaseState, n.BaseLSN, n.BaseCursor, n.BaseInstallID = BaseInstalling, baseLSN, P, id
		n.RetainedHead = max(n.RetainedHead, P)
		return nil
	}); err != nil {
		return nil, err
	}
	// Step 4: republish it, metadata-only, into the other slot.
	if _, err := v.pub.publish(nil, nil, nil); err != nil {
		return nil, err
	}
	v.liveInstallID = id
	v.installEpoch++
	v.installReopen = nil
	// A new install re-arms a lifted retirement deferral: its own image
	// needs its own durable local presence before it may retire.
	v.pub.retireGate.CompareAndSwap(retireAllowedV3, retireDeferredV3)
	v.preBaseHighest = appended
	if err := v.pub.invalidateMapProofV3(); err != nil { // an install transition (9.1)
		return nil, err
	}
	return &installHandleV3{s: s, id: id, epoch: v.installEpoch}, nil
}

// BeginBaseInstall is the BASE install seam (DESIGN 8.7). A v3 store runs
// the section 8.1 begin; a v2 store keeps its pre-seam behaviour behind the
// legacy handle (it had no begin step).
func (s *WALStore) BeginBaseInstall(baseLSN uint64, installID [16]byte) (InstallHandle, error) {
	// A store awaiting its chunk layer takes no BASE: a BASE writes the
	// extent and its install drains through the held flusher. (A chunk
	// store takes no replication, the only BASE source.)
	if s.drainHeld.Load() {
		return nil, ErrDrainHeldForChunkLayer
	}
	if s.v3 == nil {
		return NewLegacyInstallHandle(s, baseLSN), nil
	}
	h, err := s.beginBaseInstallV3(baseLSN, installID)
	if err != nil {
		return nil, err
	}
	return h, nil
}

// MarkBaseComplete completes this handle's install (DESIGN 8.2).
func (h *installHandleV3) MarkBaseComplete() error {
	if err := h.s.markBaseCompleteV3(h.id); err != nil {
		return err
	}
	h.completed.Store(true)
	return nil
}

var _ BaseInstallPreparer = (*WALStore)(nil)

// admitV3 takes installMu shared and checks, under it, that the volume is
// installing under h's ID (DESIGN 8.7). The caller releases it only after
// its whole mutation.
func (h *installHandleV3) admitV3(baseWrite bool) (release func(), err error) {
	v := h.s.v3
	if hook := v.testInstallBeforeAdmit; hook != nil {
		hook()
	}
	v.installMu.RLock()
	if !h.admissibleV3(baseWrite) {
		v.installMu.RUnlock()
		return nil, ErrInstallSuperseded
	}
	if hook := v.testInstallAdmitted; hook != nil {
		hook()
	}
	return v.installMu.RUnlock, nil
}

// admissibleV3 is the admission predicate, under installMu shared: no begin
// has run since this handle's own, and either the volume is installing
// under the handle's ID, or (install-lane applies only) the handle has
// completed and the volume is installed under its ID or retired since.
func (h *installHandleV3) admissibleV3(baseWrite bool) bool {
	v := h.s.v3
	if v.installEpoch != h.epoch {
		return false
	}
	d := v.pub.current()
	if d.BaseState == BaseInstalling && d.BaseInstallID == h.id {
		return true
	}
	if baseWrite || !h.completed.Load() {
		return false
	}
	return (d.BaseState == BaseInstalled && d.BaseInstallID == h.id) || d.BaseState == BaseNone
}

// WriteBase installs one BASE block into the extent (the v3 replacement of
// WriteExtentDirect). No LSN is recorded; the chunk's map coverage becomes
// UNKNOWN, and a dirty entry for the LBA no longer flushes over it.
func (h *installHandleV3) WriteBase(lba uint32, data []byte) error {
	release, err := h.admitV3(true)
	if err != nil {
		return err
	}
	defer release()
	s := h.s
	s.captureMu.RLock()
	defer s.captureMu.RUnlock()
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	if err := s.checkInstallBlockV3(lba, data); err != nil {
		return err
	}
	// Proof invalidation and UNKNOWN precede the mutation (s.mu is released
	// before the extent lock, as in the legacy direct write).
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.New("storage: WriteBase after Close")
	}
	if p := s.v3.mapProof; p != nil {
		if err := p.mutatedV3(); err != nil {
			s.mu.Unlock()
			return err
		}
	}
	s.markMapUnknownV3(uint64(lba))
	s.mu.Unlock()
	lock := &s.extentMu[uint64(lba)%uint64(len(s.extentMu))]
	lock.Lock()
	defer lock.Unlock()
	_, _, previousLSN, _, hadDirty := s.dm.get(uint64(lba))
	if err := s.writeExtentUnlocked(lba, data); err != nil {
		return err
	}
	if c := s.chunk.Load(); c != nil {
		c.sink.OnExtentWrite(uint64(lba), 0)
	}
	if hadDirty {
		s.dm.compareAndDelete(uint64(lba), previousLSN)
	}
	return nil
}

// ApplyInstallEntry appends one install-lane record with the source's LSN
// (DESIGN 8.1 order and gap rules, 8.4 no space).
func (h *installHandleV3) ApplyInstallEntry(lba uint32, data []byte, lsn uint64) error {
	release, err := h.admitV3(false)
	if err != nil {
		return err
	}
	defer release()
	return h.s.applyEntryV3(lba, data, lsn, true)
}

// applyEntryV3 appends one replicated entry with the source's LSN on a v3
// store (DESIGN 8.1 and 6.7, r16/r16.1): through the v3 writer in LSN
// order; an LSN at or below the highest appended is a superseded no-op when
// the LBA already holds a higher LSN and ErrWALLSNOrder otherwise; an LSN
// that would leave an unanchored gap above max(CheckpointLSN, BaseLSN,
// highest) is refused (recovery needs a contiguous prefix). installLane is
// the install handle's apply (its admission ran already); an ordinary
// replicated apply is refused while installing (that lane is the handle's).
func (s *WALStore) applyEntryV3(lba uint32, data []byte, lsn uint64, installLane bool) error {
	v := s.v3
	s.captureMu.RLock()
	defer s.captureMu.RUnlock()
	if err := s.checkInstallBlockV3(lba, data); err != nil {
		return err
	}
	s.beforeChunkMutation(uint64(lba), 1)
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("storage: ApplyEntry after Close")
	}
	if err := s.stickyIOFault(); err != nil {
		return err
	}
	d := v.pub.current()
	if !installLane && d.BaseState == BaseInstalling {
		return fmt.Errorf("%w: an ordinary replicated apply while installing", ErrBaseInstalling)
	}
	highest := v.appended.Load()
	if lsn <= highest {
		if _, _, held, _, ok := s.dm.get(uint64(lba)); ok && held > lsn {
			return nil // superseded no-op: the LBA already holds a higher LSN
		}
		return fmt.Errorf("%w: replicated %d at or below %d", ErrWALLSNOrder, lsn, highest)
	}
	if anchor := max(d.CheckpointLSN, d.BaseLSN, highest); lsn > anchor+1 {
		return fmt.Errorf("%w: %d above %d", ErrInstallLSNGap, lsn, anchor)
	}
	dataCopy := append([]byte(nil), data...)
	phys, err := s.appendWriteV3(lba, dataCopy, lsn, nil)
	s.walMutation.Add(1)
	if err != nil {
		if installLane && errors.Is(err, errWALFull) {
			return fmt.Errorf("%w: %w", ErrBaseInstallNoSpace, err)
		}
		return err
	}
	s.dm.put(uint64(lba), phys, lsn, uint32(len(dataCopy)))
	s.noteChunkMutation(uint64(lba))
	if lsn >= s.nextLSN {
		s.nextLSN = lsn + 1
	}
	s.walHead = max(s.walHead, lsn)
	if s.walTail == 0 {
		s.walTail = lsn
	}
	return nil
}

// markBaseCompleteV3 is DESIGN 8.2. Under installMu exclusive (no handle
// mutation is admitted) it validates the ID, then settles step 2 exactly as
// a checkpoint does: one fsync for the BASE extent bytes and the
// install-lane suffix (they share the file), the optional metadata and
// chunk barriers, and a fresh owned-map candidate that carries the UNKNOWN
// coverage the BASE writes set. It then publishes
// installed with the same ID and that candidate in one publication
// (retainedTail = max(retainedTail, P), 8.4), stops serving the superseded
// pre-BASE records and moves the frontier to BaseLSN. A guest write that
// changes the map meanwhile makes the candidate stale and the completion
// fails. On any error the volume stays installing and the error propagates.
func (s *WALStore) markBaseCompleteV3(id [16]byte) error {
	v := s.v3
	if v == nil {
		return errors.New("storage: v3 BASE complete on a non-v3 store")
	}
	v.installMu.Lock()
	defer v.installMu.Unlock()
	hooks := s.chunk.Load()
	release, err := holdChunkBaseTransitionV3(hooks)
	if err != nil {
		return err
	}
	defer release()
	s.captureMu.Lock()
	defer s.captureMu.Unlock()
	if s.chunk.Load() != hooks {
		return ErrMapCoverageStale
	}
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	s.mu.RLock()
	closed := s.closed
	gen := v.recoverGen.Load()
	s.mu.RUnlock()
	if closed {
		return errors.New("storage: MarkBaseComplete after Close")
	}
	d := v.pub.current()
	if d.BaseState != BaseInstalling || d.BaseInstallID != id {
		return ErrInstallSuperseded
	}
	// Step 2.
	if err := s.syncFile(nil, s.fd.Sync); err != nil {
		return err
	}
	cp := d.CheckpointLSN // the checkpoint does not move here
	if s.metadata != nil {
		if err := s.metadata.BeforeCheckpointAdvance(cp); err != nil {
			return err
		}
	}
	if h := s.chunk.Load(); h != nil {
		if err := h.sink.BeforeCheckpointAdvance(cp); err != nil {
			return err
		}
	}
	var prepared *preparedMapPublicationV3
	var mutateMap func(*descriptorV3) error
	if v.ownedMap != nil {
		var err error
		if prepared, err = s.prepareOwnedMapCheckpointV3(cp); err != nil {
			return err
		}
	} else if v.mapCandidate != nil {
		var err error
		if mutateMap, err = v.mapCandidate(cp); err != nil {
			return err
		}
	}
	// Step 3: installed, same ID, with the candidate, under the leader role.
	v.syncMu.Lock()
	defer v.syncMu.Unlock()
	if !v.flushLifetimeValidV3(gen) {
		prepared.abandonV3()
		return ErrMapCoverageStale // a recovery or reset ran meanwhile
	}
	installed := func(n *descriptorV3) error {
		if n.BaseState != BaseInstalling || n.BaseInstallID != id {
			return ErrInstallSuperseded
		}
		n.BaseState = BaseInstalled
		n.RetainedTail = max(n.RetainedTail, d.BaseCursor)
		if mutateMap != nil {
			return mutateMap(n)
		}
		return nil
	}
	if prepared != nil {
		_, err = v.pub.publishMapV3(&s.mu, nil, prepared, installed)
	} else {
		_, err = v.pub.publish(&s.mu, nil, installed)
	}
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := v.pub.invalidateMapProofV3(); err != nil {
		return err
	}
	// Records before P (LSN <= the highest appended at begin, which the
	// begin bounded by BaseLSN) are behind the installed BaseCursor and
	// superseded by the BASE image: recovery never scans them, so the open
	// store stops serving or flushing them too.
	for _, e := range s.dm.snapshot() {
		if e.LSN <= v.preBaseHighest {
			s.dm.compareAndDelete(e.LBA, e.LSN)
		}
	}
	// The frontier move (8.7): the BASE image is durable through BaseLSN.
	if d.BaseLSN >= s.nextLSN {
		s.nextLSN = d.BaseLSN + 1
	}
	s.walHead = max(s.walHead, d.BaseLSN)
	if d.BaseLSN > v.appended.Load() {
		v.appended.Store(d.BaseLSN)
	}
	return nil
}

func (s *WALStore) checkInstallBlockV3(lba uint32, data []byte) error {
	if err := s.stickyIOFault(); err != nil {
		return err
	}
	if lba >= uint32(s.sb.VolumeSize/uint64(s.sb.BlockSize)) {
		return fmt.Errorf("storage: install LBA %d out of range", lba)
	}
	if len(data) != int(s.sb.BlockSize) {
		return fmt.Errorf("storage: install data size %d != block size %d", len(data), s.sb.BlockSize)
	}
	return nil
}

// DeferBaseRetirementV3 keeps a completed BASE installed (no retirement to
// none) until AllowBaseRetirementV3. A chunk-managed provider calls it at
// open, before Recover and before any publication. Not a v3 store: no-op.
func (s *WALStore) DeferBaseRetirementV3() {
	if s.v3 != nil {
		s.v3.ownerAttachDurable.Store(false)
		s.v3.pub.retireGate.CompareAndSwap(retireNoGateV3, retireDeferredV3)
	}
}

// CommitChunkAttachV3 is the provider's durable attach commit (row 12a
// retirement timing): under installMu exclusive it runs commit (the
// provider removes its fresh-from-HEAD marker and fsyncs the directory; it
// must not re-enter the store), and only when commit succeeds it records
// that this open's chunk attach is durable and, if the volume is installed,
// lifts the retirement deferral. On a commit error nothing changes. So a
// completed BASE never retires while the provider could still read the
// volume as fresh from HEAD. Not a v3 store: commit only.
func (s *WALStore) CommitChunkAttachV3(commit func() error) error {
	v := s.v3
	if v == nil {
		return commit()
	}
	v.installMu.Lock()
	defer v.installMu.Unlock()
	if err := commit(); err != nil {
		return err
	}
	v.ownerAttachDurable.Store(true)
	if v.pub.current().BaseState == BaseInstalled {
		v.pub.retireGate.Store(retireAllowedV3)
	}
	return nil
}

// OwnerChunkAttachDurableV3 reports whether this open's chunk attach was
// durably committed (CommitChunkAttachV3); it resets at every open.
func (s *WALStore) OwnerChunkAttachDurableV3() bool {
	return s.v3 != nil && s.v3.ownerAttachDurable.Load()
}

// AllowBaseRetirementV3 lifts the deferral once the chunk layer has durably
// recorded local presence for the installed image; retirement then happens
// on the next ordinary publication whose 8.4 conditions hold.
func (s *WALStore) AllowBaseRetirementV3() {
	if s.v3 != nil {
		s.v3.pub.retireGate.Store(retireAllowedV3)
	}
}

// markMapUnknownV3: an effective direct extent/BASE mutation makes its
// chunk's coverage UNKNOWN (DESIGN 9, r16.1). Caller holds WALStore.mu.
func (s *WALStore) markMapUnknownV3(lba uint64) {
	v := s.v3
	if v.ownedMap == nil {
		return
	}
	e := &v.mapEntries[lba/v.ownedMap.chunkBlocks]
	e.Unknown = true
	// A BASE byte has no WAL record: inherit its image frontier, not an
	// unrelated later write. Keep UNKNOWN conservative as before.
	birth := s.nextLSN - 1
	if d := v.pub.current(); d.BaseState == BaseInstalling {
		birth = d.BaseLSN
	}
	e.LSN = max(e.LSN, birth)
}

// InstalledBaseLSN reports a completed local BASE obligation, not a normal
// checkpoint. A caller must still revalidate under its attachment admission.
func (s *WALStore) InstalledBaseLSN() (uint64, bool) {
	if s.v3 == nil {
		return 0, false
	}
	d := s.v3.pub.current()
	return d.BaseLSN, d.BaseState == BaseInstalled
}

// Caller holds installMu exclusively and no lower lock. The returned release
// must be deferred before acquiring captureMu, so it runs after lower unlocks.
// ErrRebuildRetainedBase: a rebuild began on a store whose earlier BASE is
// still installed (a replica removed and added back with its older store).
// v1 does not retire it for a new rebuild; the operator wipes the replica's
// data directory and adds it back as a new replica.
var ErrRebuildRetainedBase = errors.New("re-add with a retained base is not supported in v1: wipe the replica's data directory and re-add")

func holdChunkBaseTransitionV3(h *chunkHooks) (func(), error) {
	if h == nil {
		return func() {}, nil
	}
	if h.baseTransitionV3 == nil {
		return nil, fmt.Errorf("%w: BASE transition with an unsupported attached chunk layer", errV3NotYet)
	}
	return h.baseTransitionV3()
}
