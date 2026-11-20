package storage

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// The single v3 descriptor publisher (format bump v3, DESIGN r15 sections
// 4, 5 and 5.1; plan row 3).
//
// Every v3 descriptor change goes through descriptorPublisherV3.publish:
// Sync W, checkpoint, metadata-only rollover, seed, BASE transitions and
// reset. It is dark until the v3 store is wired in (plan rows 4 and 5) and
// activated (row 12b); the v2 checkpoint path in walstore.go is unchanged.
//
// Lock graph (DESIGN 4), the only order:
//
//	metadata callbacks (run by the caller first, holding none of these)
//	  -> WALStore.mu (snapshot only, released before slot I/O)
//	  -> descMu
//	  -> fileSyncMu (leaf, inside syncFile)
//
// Nothing below descMu is ever held while acquiring WALStore.mu, and no
// callback runs under descMu or fileSyncMu.

var (
	// errDescriptorIdentityChange: a publication tried to change the
	// store identity or geometry, which no v3 publication may do.
	errDescriptorIdentityChange = errors.New("storage: v3 descriptor identity or geometry change refused")
	// errDescriptorTransition: a publication asked for a BASE, map, nonce
	// or bounds change that DESIGN 5/8.5 does not allow.
	errDescriptorTransition = errors.New("storage: v3 descriptor transition refused")
)

// descriptorIOV3 is the store I/O the publisher needs. writeSlot does one
// pwrite of a whole slot; syncFile runs the store's leaf (pwrite + fsync
// under fileSyncMu); sticky reports a latched store fault; latch records a
// fault in the store so every other store path (appends, Sync, reuse) is
// refused too. The publisher latches on a slot write error as well as on an
// fsync error (DESIGN 5): the v2 leaf does not latch pwrite errors, so the
// v3 publisher does it itself.
type descriptorIOV3 struct {
	writeSlot func(off int64, b []byte) error
	fsync     func() error
	syncFile  func(write, sync func() error) error
	sticky    func() error
	latch     func(err error)
}

// ErrDescriptorPublishUncertain: a descriptor slot write or fsync failed, so
// the slot on disk may hold the new generation, the old one, or a torn mix.
// The open store refuses every further publication and all reuse; only a
// reopen, which reads back and selects the actual slots, resolves it
// (DESIGN 5, failure rule).
var ErrDescriptorPublishUncertain = errors.New("storage: v3 descriptor publication outcome uncertain; reopen required")

// descriptorPublisherV3 holds the authoritative in-memory descriptor and
// the contents of the fallback slot, which pins reuse (DESIGN 5).
type descriptorPublisherV3 struct {
	descMu sync.Mutex
	// retireGate defers the installed -> none retirement (retireNoGateV3,
	// retireDeferredV3, retireAllowedV3); read without a lock.
	retireGate atomic.Uint32

	cur        descriptorV3 // selected slot: authoritative
	curSlot    slotIDV3
	other      descriptorV3 // fallback slot, when valid
	otherValid bool

	io     descriptorIOV3
	accept slotImplAcceptor

	// failed is set by the first slot write or fsync failure. uncertain is
	// the candidate that may be on disk: it stays pinned (its tail bounds
	// reuse, and its map dependencies stay referenced) until reopen.
	failed    error
	uncertain *descriptorV3

	// Immutable map dependencies and volatile proof domain, bound before serving.
	mapCurrent *chunkMapCandidateV3
	mapDomain  *mapProofEpochV3

	// trace, when set by a test, records lock acquisitions and releases
	// in order (the section 11 item 9 lock recorder).
	trace func(event string)
}

func newDescriptorPublisherV3(cur descriptorV3, curSlot slotIDV3, other *descriptorV3, io descriptorIOV3) *descriptorPublisherV3 {
	p := &descriptorPublisherV3{cur: cur, curSlot: curSlot, io: io}
	if other != nil {
		p.other, p.otherValid = *other, true
	}
	return p
}

func (p *descriptorPublisherV3) note(event string) {
	if p.trace != nil {
		p.trace(event)
	}
}

// current returns the authoritative descriptor.
func (p *descriptorPublisherV3) current() descriptorV3 {
	p.descMu.Lock()
	defer p.descMu.Unlock()
	return p.cur
}

// reuseFloor is the lowest logical WAL position still referenced by a valid
// slot (DESIGN 5): min of both tails when both carry the same nonce. With
// mixed nonces (a reset between its two publications, DESIGN 8.6) tails are
// not comparable and ok is false: nothing may be reused.
func (p *descriptorPublisherV3) reuseFloor() (floor uint64, ok bool) {
	p.descMu.Lock()
	defer p.descMu.Unlock()
	return p.reuseFloorLocked()
}

func (p *descriptorPublisherV3) reuseFloorLocked() (uint64, bool) {
	if p.failed != nil {
		return 0, false // an uncertain slot may reference anything: no reuse until reopen
	}
	if !p.otherValid {
		return p.cur.RetainedTail, true
	}
	if p.other.Nonce != p.cur.Nonce {
		return 0, false
	}
	return min(p.cur.RetainedTail, p.other.RetainedTail), true
}

// publish is publishDescriptor (DESIGN 5). Callers run their metadata
// callbacks before calling it. If storeMu is non-nil it is taken first,
// snap runs under it, then descMu is taken and storeMu released before any
// slot I/O. mutate edits a copy of the CURRENT authoritative descriptor,
// never a caller's snapshot; the publisher then applies the monotonic merge
// and the transition rules, validates the whole result and writes it into
// the fallback slot with slotGen+1.
func (p *descriptorPublisherV3) publish(storeMu sync.Locker, snap func(), mutate func(next *descriptorV3) error) (descriptorV3, error) {
	return p.publishWithMapV3(storeMu, snap, mutate, nil)
}

func (p *descriptorPublisherV3) publishWithMapV3(storeMu sync.Locker, snap func(), mutate func(next *descriptorV3) error, prepared *preparedMapPublicationV3) (descriptorV3, error) {
	if storeMu != nil {
		storeMu.Lock()
		p.note("lock WALStore.mu")
		if snap != nil {
			snap()
		}
		p.descMu.Lock()
		p.note("lock descMu")
		storeMu.Unlock()
		p.note("unlock WALStore.mu")
	} else {
		p.descMu.Lock()
		p.note("lock descMu")
	}
	defer func() {
		p.note("unlock descMu")
		p.descMu.Unlock()
	}()
	return p.publishLockedWithMapV3(mutate, prepared)
}

// uncertainCandidate returns the candidate whose publication failed, if
// any. Its dependencies (WAL bytes from its tail, its map root and pages)
// stay pinned until reopen.
func (p *descriptorPublisherV3) uncertainCandidate() (descriptorV3, bool) {
	p.descMu.Lock()
	defer p.descMu.Unlock()
	if p.uncertain == nil {
		return descriptorV3{}, false
	}
	return *p.uncertain, true
}

func (p *descriptorPublisherV3) publishLocked(mutate func(next *descriptorV3) error) (descriptorV3, error) {
	return p.publishLockedWithMapV3(mutate, nil)
}

func (p *descriptorPublisherV3) publishLockedWithMapV3(mutate func(next *descriptorV3) error, prepared *preparedMapPublicationV3) (descriptorV3, error) {
	return p.publishLockedCoreV3(mutate, prepared, false)
}

// publishLockedCoreV3 is the publication body; reset permits the nonce and
// cursor change of the reset transaction (DESIGN 8.6) and nothing else.
func (p *descriptorPublisherV3) publishLockedCoreV3(mutate func(next *descriptorV3) error, prepared *preparedMapPublicationV3, reset bool) (descriptorV3, error) {
	if p.failed != nil {
		return descriptorV3{}, p.failed
	}
	if err := p.io.sticky(); err != nil {
		return descriptorV3{}, err
	}
	next := p.cur
	if mutate != nil {
		if err := mutate(&next); err != nil {
			return descriptorV3{}, err
		}
	}
	if p.retireGate.Load() != retireDeferredV3 {
		retireBaseV3(&p.cur, &next)
	}
	if err := mergeDescriptorV3Mode(&p.cur, &next, reset); err != nil {
		return descriptorV3{}, err
	}
	if err := p.validateMapPublicationV3(next, prepared); err != nil {
		return descriptorV3{}, err
	}
	if err := checkCountersV3(&next); err != nil {
		return descriptorV3{}, err
	}
	gen, err := nextSlotGenV3(p.cur.SlotGen)
	if err != nil {
		return descriptorV3{}, err
	}
	next.SlotGen = gen
	next.MigrationPhase = MigrationNormal
	if err := next.validate(p.accept); err != nil {
		return descriptorV3{}, err
	}
	target := slotB
	off := int64(slotBOffsetV3)
	if p.curSlot == slotB {
		target, off = slotA, slotAOffsetV3
	}
	buf := next.encode()
	wrote := false
	err = p.io.syncFile(func() error {
		p.note("hold fileSyncMu")
		wrote = true
		werr := p.io.writeSlot(off, buf)
		if werr != nil && p.io.latch != nil {
			// Latch inside the leaf, before fileSyncMu is released: a store
			// Sync queued on fileSyncMu must not fsync and acknowledge
			// through this uncertain slot (codex01 row 3 round 2).
			p.io.latch(fmt.Errorf("%w: %w", ErrDescriptorPublishUncertain, werr))
		}
		return werr
	}, p.io.fsync)
	if err != nil {
		if wrote {
			// The slot write or its fsync failed: the fallback slot may hold
			// the new generation, the old one or a torn mix. Latch, keep the
			// candidate pinned, and refuse everything until reopen.
			uncertain := next
			p.uncertain = &uncertain
			p.failed = fmt.Errorf("%w: %w", ErrDescriptorPublishUncertain, err)
			// The store fault was latched inside the leaf (write error) or by
			// the leaf itself (fsync error); latching again is a no-op.
			if p.io.latch != nil {
				p.io.latch(p.failed)
			}
			return descriptorV3{}, p.failed
		}
		return descriptorV3{}, err // refused before any I/O (a latched fault)
	}
	// Success boundary: a fault latched by another path while this
	// publication was in flight still refuses it (#172). The slot write and
	// its fsync did happen, so the slot on disk may name this generation:
	// its dependencies stay pinned as an uncertain candidate until reopen.
	if err := p.io.sticky(); err != nil {
		uncertain := next
		p.uncertain = &uncertain
		return descriptorV3{}, err
	}
	p.other, p.otherValid = p.cur, true
	p.cur, p.curSlot = next, target
	if prepared != nil {
		p.mapCurrent = prepared.candidate
	}
	// Report the slot tuples to the owned map (descMu -> liveMu): roots
	// neither slot names any more, and superseded candidates, are released.
	// A publication before the map is bound reports nothing; that only
	// delays a release until the next report.
	if p.mapCurrent != nil && p.mapCurrent.owner != nil {
		p.mapCurrent.owner.slotsPublishedV3(p.cur, p.other, p.otherValid)
	}
	return next, nil
}

// checkCountersV3 refuses a publication whose counters would exceed 2^64-2
// (DESIGN 2.4) with ErrCounterExhausted, before any write. Slot decoding of
// such a value stays errSuperblockInvalid.
func checkCountersV3(d *descriptorV3) error {
	for _, c := range []struct {
		name string
		v    uint64
	}{
		{"W", d.W}, {"CheckpointLSN", d.CheckpointLSN}, {"retainedTail", d.RetainedTail},
		{"retainedHead", d.RetainedHead}, {"mapGeneration", d.MapGeneration},
		{"mapCoveredCheckpoint", d.MapCoveredCheckpoint}, {"BaseLSN", d.BaseLSN},
		{"BaseCursor", d.BaseCursor},
	} {
		if c.v > counterMaxV3 {
			return fmt.Errorf("%w: %s %d", ErrCounterExhausted, c.name, c.v)
		}
	}
	return nil
}

// mergeDescriptorV3 applies the monotonic merge and transition rules of
// DESIGN 5 and 8.5 to next, a mutated copy of cur.
func mergeDescriptorV3(cur, next *descriptorV3) error { return mergeDescriptorV3Mode(cur, next, false) }

func mergeDescriptorV3Mode(cur, next *descriptorV3, reset bool) error {
	if next.ImplKind != cur.ImplKind || next.ImplVersion != cur.ImplVersion || next.UUID != cur.UUID ||
		next.CreatedAt != cur.CreatedAt || !sameGeometryV3(*cur, *next) {
		return errDescriptorIdentityChange
	}
	// W and CheckpointLSN are max-merged: a stale request never lowers them.
	next.W = max(cur.W, next.W)
	next.CheckpointLSN = max(cur.CheckpointLSN, next.CheckpointLSN)
	// The nonce changes only through the reset transaction (DESIGN 8.6,
	// plan row 8); within one nonce the retained bounds never move back.
	if reset {
		// The reset publication (DESIGN 8.6): a new nonce and a fresh,
		// empty cursor space; everything else follows the ordinary rules.
		if next.Nonce == cur.Nonce || next.Nonce == ([16]byte{}) || next.RetainedTail != next.RetainedHead {
			return fmt.Errorf("%w: a reset needs a new nonzero nonce and an empty cursor space", errDescriptorTransition)
		}
	} else if next.Nonce != cur.Nonce {
		return fmt.Errorf("%w: nonce change outside a reset", errDescriptorTransition)
	} else if next.RetainedTail < cur.RetainedTail {
		return fmt.Errorf("%w: retainedTail %d below %d", errDescriptorTransition, next.RetainedTail, cur.RetainedTail)
	}
	// The map root changes only to a strictly higher generation; the full
	// coverage predicates against the root are added by plan row 7.
	if next.MapGeneration != cur.MapGeneration || next.MapRootLocator != cur.MapRootLocator ||
		next.MapRootHash != cur.MapRootHash || next.MapCoveredCheckpoint != cur.MapCoveredCheckpoint ||
		next.MapLineage != cur.MapLineage || next.MapFormatID != cur.MapFormatID {
		if next.MapGeneration <= cur.MapGeneration {
			return fmt.Errorf("%w: %w: map root change without a higher generation (%d -> %d)",
				mapCoverageRefusal("map_generation_not_increasing"), errDescriptorTransition, cur.MapGeneration, next.MapGeneration)
		}
		if next.MapLineage != cur.MapLineage {
			return fmt.Errorf("%w: map lineage change outside an install or reset", errDescriptorTransition)
		}
	}
	if next.WMode != cur.WMode && next.WMode != WModeLagged && next.WMode != WModeStrict {
		return fmt.Errorf("%w: WMode %d", errDescriptorTransition, next.WMode)
	}
	return baseTransitionAllowedV3(cur, next)
}

// The retirement gate (row 12a retired-BASE ruling): a chunk-managed store
// defers retirement until its chunk layer has durably recorded local
// presence for the installed image, so the installed descriptor, the
// provider's evidence that the local image is complete, is not erased
// first. Stores that never set it retire as before.
const (
	retireNoGateV3   uint32 = iota // retire when the 8.4 conditions hold
	retireDeferredV3               // keep installed even when they hold
	retireAllowedV3                // the deferral was lifted for this open
)

// retireBaseV3 is the installed -> none retirement (DESIGN 8.4, 8.5): a
// later normal publication of an installed descriptor zeroes the three BASE
// fields once the checkpoint covers BaseLSN and both slots (the current one,
// which becomes the fallback, and the new one) carry retainedTail >=
// BaseCursor. Before that, recovery still needs the BASE scan start.
func retireBaseV3(cur, next *descriptorV3) {
	if cur.BaseState != BaseInstalled || next.BaseState != BaseInstalled ||
		next.BaseInstallID != cur.BaseInstallID || next.CheckpointLSN < next.BaseLSN ||
		cur.RetainedTail < cur.BaseCursor || next.RetainedTail < cur.BaseCursor {
		return
	}
	next.BaseState, next.BaseLSN, next.BaseCursor, next.BaseInstallID = BaseNone, 0, 0, [16]byte{}
}

// baseTransitionAllowedV3 is the DESIGN 8.5 edge table. The conditions an
// edge needs (coverage, destructive mutation, drain) are enforced by the
// install callers (plan rows 9 to 11); this refuses every other edge.
func baseTransitionAllowedV3(cur, next *descriptorV3) error {
	same := next.BaseState == cur.BaseState && next.BaseLSN == cur.BaseLSN &&
		next.BaseCursor == cur.BaseCursor && next.BaseInstallID == cur.BaseInstallID
	if same {
		return nil
	}
	ok := false
	switch cur.BaseState {
	case BaseNone:
		ok = next.BaseState == BaseInstalling
	case BaseInstalling:
		switch next.BaseState {
		case BaseInstalling:
			ok = next.BaseInstallID != cur.BaseInstallID // superseding begin
		case BaseInstalled:
			ok = next.BaseInstallID == cur.BaseInstallID && next.BaseLSN == cur.BaseLSN
		case BaseNone:
			ok = false // never: there is no abort (DESIGN r17, 8.5)
		}
	case BaseInstalled:
		switch next.BaseState {
		case BaseInstalled:
			// Retirement may clear BaseCursor below the tail; the ID and
			// BaseLSN stay.
			ok = next.BaseInstallID == cur.BaseInstallID && next.BaseLSN == cur.BaseLSN
		case BaseNone:
			// Retirement (8.4, 8.5): the checkpoint covers the BASE and the
			// published tail is at or past BaseCursor; the publisher also
			// requires it of the other slot (retireBaseLockedV3).
			ok = next.BaseLSN == 0 && next.CheckpointLSN >= cur.BaseLSN &&
				cur.RetainedTail >= cur.BaseCursor && next.RetainedTail >= cur.BaseCursor
			// installed -> installing is not an 8.5 edge: a new install
			// begins only after installed -> none (retire, then begin).
		}
	}
	if !ok {
		return fmt.Errorf("%w: BASE %d -> %d", errDescriptorTransition, cur.BaseState, next.BaseState)
	}
	return nil
}

// rolloverIfPinned is the metadata-only rollover (DESIGN 5.1): when the
// fallback slot still pins reuse below the selected tail, republish the
// current descriptor unchanged into the fallback slot so both carry the new
// tail. It needs no record, Sync, guest write or checkpoint change. It
// reports whether it published.
func (p *descriptorPublisherV3) rolloverIfPinned() (bool, error) {
	p.note("lock descMu")
	p.descMu.Lock()
	defer func() {
		p.note("unlock descMu")
		p.descMu.Unlock()
	}()
	if p.failed != nil {
		return false, p.failed
	}
	if !p.otherValid || p.other.Nonce != p.cur.Nonce || p.other.RetainedTail >= p.cur.RetainedTail {
		return false, nil
	}
	if _, err := p.publishLocked(nil); err != nil {
		return false, err
	}
	return true, nil
}
