package storage

import (
	"errors"
	"fmt"
)

// Acknowledgement while installing (format bump v3, DESIGN r17.4 section 7;
// plan row 10). Dark: only tests build a v3 store.

var (
	// ErrBaseInstalling: the logical Sync is gated while BaseState =
	// installing. It is always returned with frontier 0 (DESIGN 7).
	ErrBaseInstalling = errors.New("storage: v3 BASE install in progress; not ready")
	// ErrNotInstalling: SyncInstallLog outside an install.
	ErrNotInstalling = errors.New("storage: v3 install-log sync outside a BASE install")
	// ErrInstallIDMismatch: SyncInstallLog for an ID that is not installing.
	ErrInstallIDMismatch = errors.New("storage: v3 install-log sync for another install ID")
)

// InstallLogSyncer is the rebuild receiver's install-log durability ack
// (DESIGN 7, O1-a). It is deliberately not on LogicalStorage: only
// core/recovery type-asserts it.
type InstallLogSyncer interface {
	SyncInstallLog(installID [16]byte) (uint64, error)
}

var _ InstallLogSyncer = (*WALStore)(nil)

// SyncInstallLog makes the install lane durable and acknowledges it while
// installing (DESIGN 7). It revalidates the ID under installMu shared,
// freezes the cut H (every LSN pwritten so far), fsyncs the WAL and then
// publishes W = max(W, H) with BaseState still installing: always mode A,
// whatever the volume's WMode. It attests log durability only, never a
// readable image, and never changes BaseState. A v2 store keeps its
// pre-v3 behaviour: the receiver's ack is its ordinary Sync.
func (s *WALStore) SyncInstallLog(installID [16]byte) (uint64, error) {
	v := s.v3
	if v == nil {
		return s.Sync()
	}
	v.installMu.RLock()
	defer v.installMu.RUnlock()
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	if err := installLogTargetV3(v.pub.current(), installID); err != nil {
		return 0, err
	}
	v.syncMu.Lock() // the leader role: no committer batch or recovery meanwhile
	defer v.syncMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, errors.New("storage: SyncInstallLog after Close")
	}
	if err := s.stickyIOFault(); err != nil {
		s.mu.Unlock()
		return 0, err
	}
	H := v.appended.Load()
	s.mu.Unlock()
	// Mode A: the WAL fsync precedes the slot publication.
	if err := s.syncFile(nil, s.fd.Sync); err != nil {
		return 0, fmt.Errorf("storage: install-log fsync: %w", err)
	}
	if _, err := v.pub.publish(&s.mu, nil, func(n *descriptorV3) error {
		if err := installLogTargetV3(*n, installID); err != nil {
			return err
		}
		n.W = max(n.W, H)
		return nil
	}); err != nil {
		return 0, err
	}
	if hook := v.testInstallLogBeforeAck; hook != nil {
		hook()
	}
	// The #172 ack boundary: a fault latched after the publisher's final
	// check (a queued append of this ID failing under s.mu) refuses the ack.
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.stickyIOFault(); err != nil {
		return 0, err
	}
	if H > v.wack.Load() {
		v.wack.Store(H)
	}
	s.syncedLSN = max(s.syncedLSN, H)
	return H, nil
}

func installLogTargetV3(d descriptorV3, installID [16]byte) error {
	if d.BaseState != BaseInstalling {
		return ErrNotInstalling
	}
	if d.BaseInstallID != installID {
		return ErrInstallIDMismatch
	}
	return nil
}
