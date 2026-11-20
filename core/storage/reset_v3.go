package storage

import (
	"crypto/rand"
	"errors"
	"fmt"
)

// The v3 nonce/position reset transaction (format bump v3, DESIGN r16
// section 8.6; plan row 8), publisher part.
//
// Step 1 (checkpoint or preserve every old acknowledged dependency) is the
// store's: the publisher refuses the reset unless every acknowledged LSN is
// checkpointed (CheckpointLSN >= W). Retained old-incarnation WAL bytes are
// then all at or below the checkpoint, so they are preserved in the extent
// and no longer required; the map root is unchanged by a reset, so no map
// page is retired. Steps 2 and 3 publish the new incarnation into
// one slot and then the other. Between them the slots carry different
// nonces: no reuse floor exists (reuseFloor reports !ok) and no new-nonce
// record may be admitted, so a crash there opens the complete new image
// with the old dependencies untouched.

// ErrResetNotPreserved: a reset was asked for while old acknowledged data
// is still retained in the WAL or not yet checkpointed (DESIGN 8.6 step 1).
var ErrResetNotPreserved = errors.New("storage: v3 reset refused: old acknowledged data is not checkpointed")

// resetIncarnationV3 runs DESIGN 8.6 steps 2 and 3. nonce is the new
// incarnation's nonce, and cursor its first logical cursor. The caller
// quiesces the store and recreates the WAL writer afterwards.
func (p *descriptorPublisherV3) resetIncarnationV3(nonce [16]byte, cursor uint64) (descriptorV3, error) {
	p.descMu.Lock()
	defer p.descMu.Unlock()
	if p.failed != nil {
		return descriptorV3{}, p.failed
	}
	cur := p.cur
	if cur.CheckpointLSN < cur.W {
		return descriptorV3{}, fmt.Errorf("%w: checkpoint %d below W %d",
			ErrResetNotPreserved, cur.CheckpointLSN, cur.W)
	}
	if cur.BaseState != BaseNone {
		return descriptorV3{}, fmt.Errorf("%w: reset during a BASE install", errDescriptorTransition)
	}
	if p.otherValid && p.other.Nonce != cur.Nonce {
		// A previous reset stopped between its two publications; only a
		// reopen resolves it.
		return descriptorV3{}, fmt.Errorf("%w: a reset is already half published", errDescriptorTransition)
	}
	// Step 2: the new incarnation into the fallback slot.
	if _, err := p.publishLockedCoreV3(func(n *descriptorV3) error {
		n.Nonce = nonce
		n.RetainedTail, n.RetainedHead = cursor, cursor
		return nil
	}, nil, true); err != nil {
		return descriptorV3{}, err
	}
	// Step 3: republish it into the other slot. A failure here latches the
	// publisher (an uncertain slot) and holds: never a successful reset.
	return p.publishLockedCoreV3(nil, nil, false)
}

// resetIncarnationV3 is the store half of the reset transaction (DESIGN 8.6;
// plan row 8). It is dark: nothing in production calls it, and it must not
// be activated before the reset model (r4.5) passes.
//
// It quiesces the store (the capture barrier exclusive, then the leader
// role and the store lock: captureMu -> lifecycleMu -> syncMu -> s.mu), and
// refuses with ErrResetNotPreserved, writing nothing, unless the dirty map
// is empty and every appended LSN is checkpointed (CheckpointLSN >=
// appended; a v3 checkpoint never exceeds the acknowledged W, so this also
// means appended == acknowledged). The empty map also closes the window
// between a checkpoint publication and the flusher's release of the covered
// entries, which still name old-incarnation WAL positions. It then
// publishes the new incarnation into both slots under a fresh random nonce
// with the cursor space starting at 0, recreates the WAL writer on it, bumps
// the recovery generation (an outer Sync that straddles the reset is not
// acknowledged) and invalidates the map proof epoch. LSNs, W and the map
// root are unchanged.
func (s *WALStore) resetIncarnationV3() error {
	v := s.v3
	if v == nil {
		return errors.New("storage: reset on a non-v3 store")
	}
	release := s.holdChunkDestructive(true)
	defer release()
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	v.syncMu.Lock()
	defer v.syncMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("storage: reset after Close")
	}
	if err := s.stickyIOFault(); err != nil {
		return err
	}
	d := v.pub.current()
	appended := v.appended.Load()
	if n := s.dm.len(); n != 0 || d.CheckpointLSN < appended {
		return fmt.Errorf("%w: dirty %d, appended %d, checkpoint %d",
			ErrResetNotPreserved, n, appended, d.CheckpointLSN)
	}
	var nonce [16]byte
	for nonce == ([16]byte{}) || nonce == d.Nonce {
		if _, err := rand.Read(nonce[:]); err != nil {
			return err
		}
	}
	v.recoverGen.Add(1)       // odd while the reset runs (see recoverGen)
	defer v.recoverGen.Add(1) // even again at the end, on every path
	if _, err := v.pub.resetIncarnationV3(nonce, 0); err != nil {
		return err
	}
	w, err := newWALWriterV3(s.fd, v.pub)
	if err != nil {
		s.latchV3(err)
		return err
	}
	v.w = w
	return v.pub.invalidateMapProofV3()
}

// halfResetV3 reports a reset stopped between its two publications: the
// two valid slots carry different nonces.
func (p *descriptorPublisherV3) halfResetV3() bool {
	p.descMu.Lock()
	defer p.descMu.Unlock()
	return p.otherValid && p.other.Nonce != p.cur.Nonce
}
