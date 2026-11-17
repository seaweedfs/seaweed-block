package storage

import (
	"errors"
	"fmt"
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
// usual captureMu -> lifecycleMu -> syncMu -> s.mu -> descMu -> fileSyncMu,
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
	// ErrInstallLSNGap: an install-lane entry would leave an unanchored LSN
	// gap (DESIGN 8.1, ApplyEntry gap rule).
	ErrInstallLSNGap = errors.New("storage: v3 install-lane entry leaves an LSN gap")
)

// installHandleV3 is the only path for BASE extent writes and install-lane
// WAL applies on a v3 store (DESIGN 8.7).
type installHandleV3 struct {
	s  *WALStore
	id [16]byte
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
	d := v.pub.current()
	switch d.BaseState {
	case BaseInstalled:
		// installed -> installing is not an 8.5 edge: retire first.
		return nil, fmt.Errorf("%w: begin while installed (retire first)", errDescriptorTransition)
	case BaseInstalling:
		if d.BaseInstallID != v.liveInstallID {
			// A superseding begin after an installing reopen needs the 6.6
			// fence and P = max(E, persisted head) first (plan row 11).
			return nil, fmt.Errorf("%w: superseding begin after an installing reopen", errV3NotYet)
		}
		if d.BaseInstallID == id {
			return nil, fmt.Errorf("%w: begin with the installing ID", errDescriptorTransition)
		}
	}
	// W includes every acknowledged LSN, published or not yet (mode C).
	w := max(d.W, v.wack.Load())
	if baseLSN < w || baseLSN < d.CheckpointLSN {
		refusal := ErrInstallBaseUncovered
		if d.BaseState == BaseInstalling {
			refusal = ErrInstallSupersedeUncovered
		}
		return nil, fmt.Errorf("%w: BaseLSN %d, W %d, CheckpointLSN %d", refusal, baseLSN, w, d.CheckpointLSN)
	}
	P := v.w.logicalHeadV3() // 8.1 step 2: the serialized writer's head
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
	v.preBaseHighest = v.appended.Load()
	if err := v.pub.invalidateMapProofV3(); err != nil { // an install transition (9.1)
		return nil, err
	}
	return &installHandleV3{s: s, id: id}, nil
}

// admitV3 takes installMu shared and checks, under it, that the volume is
// installing under h's ID (DESIGN 8.7). The caller releases it only after
// its whole mutation.
func (h *installHandleV3) admitV3() (release func(), err error) {
	v := h.s.v3
	if hook := v.testInstallBeforeAdmit; hook != nil {
		hook()
	}
	v.installMu.RLock()
	d := v.pub.current()
	if d.BaseState != BaseInstalling || d.BaseInstallID != h.id {
		v.installMu.RUnlock()
		return nil, ErrInstallSuperseded
	}
	if hook := v.testInstallAdmitted; hook != nil {
		hook()
	}
	return v.installMu.RUnlock, nil
}

// WriteBase installs one BASE block into the extent (the v3 replacement of
// WriteExtentDirect). No LSN is recorded; the chunk's map coverage becomes
// UNKNOWN, and a dirty entry for the LBA no longer flushes over it.
func (h *installHandleV3) WriteBase(lba uint32, data []byte) error {
	release, err := h.admitV3()
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
	release, err := h.admitV3()
	if err != nil {
		return err
	}
	defer release()
	s := h.s
	v := s.v3
	s.captureMu.RLock()
	defer s.captureMu.RUnlock()
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	if err := s.checkInstallBlockV3(lba, data); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("storage: ApplyInstallEntry after Close")
	}
	if err := s.stickyIOFault(); err != nil {
		return err
	}
	d := v.pub.current()
	highest := v.appended.Load()
	if lsn <= highest {
		if _, _, held, _, ok := s.dm.get(uint64(lba)); ok && held > lsn {
			return nil // superseded no-op: the LBA already holds a higher LSN
		}
		return fmt.Errorf("%w: install-lane %d at or below %d", ErrWALLSNOrder, lsn, highest)
	}
	if anchor := max(d.CheckpointLSN, d.BaseLSN, highest); lsn > anchor+1 {
		return fmt.Errorf("%w: %d above %d", ErrInstallLSNGap, lsn, anchor)
	}
	dataCopy := append([]byte(nil), data...)
	phys, err := s.appendWriteV3(lba, dataCopy, lsn)
	s.walMutation.Add(1)
	if err != nil {
		if errors.Is(err, errWALFull) {
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
	return nil
}

// markBaseCompleteV3 is DESIGN 8.2. Under installMu exclusive it validates
// the ID, makes the BASE extent bytes and the install-lane suffix durable,
// publishes installed with the same ID (retainedTail = max(retainedTail,
// P), 8.4) and then moves the in-memory frontier to BaseLSN. On any error
// the volume stays installing and the error propagates.
func (s *WALStore) markBaseCompleteV3(id [16]byte) error {
	v := s.v3
	if v == nil {
		return errors.New("storage: v3 BASE complete on a non-v3 store")
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
		return errors.New("storage: MarkBaseComplete after Close")
	}
	d := v.pub.current()
	if d.BaseState != BaseInstalling || d.BaseInstallID != id {
		return ErrInstallSuperseded
	}
	// Step 2: the BASE extent bytes and the install-lane WAL suffix share the
	// file; one fsync through the leaf makes both durable.
	if err := s.syncFile(nil, s.fd.Sync); err != nil {
		return err
	}
	// Step 3: installed, same ID.
	if _, err := v.pub.publish(nil, nil, func(n *descriptorV3) error {
		n.BaseState = BaseInstalled
		n.RetainedTail = max(n.RetainedTail, d.BaseCursor)
		return nil
	}); err != nil {
		return err
	}
	if err := v.pub.invalidateMapProofV3(); err != nil {
		return err
	}
	// Records before P (LSN <= the highest appended at begin) are behind the
	// installed BaseCursor and superseded by the BASE image: recovery never
	// scans them, so the open store stops serving or flushing them too.
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

// markMapUnknownV3: an effective direct extent/BASE mutation makes its
// chunk's coverage UNKNOWN (DESIGN 9, r16.1). Caller holds WALStore.mu.
func (s *WALStore) markMapUnknownV3(lba uint64) {
	v := s.v3
	if v.ownedMap == nil {
		return
	}
	v.mapEntries[lba/v.ownedMap.chunkBlocks].Unknown = true
}
