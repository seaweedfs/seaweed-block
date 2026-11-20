package storage

import "fmt"

// holdBaseTransitionV3 drains a whole cut, including pre-copy, remote CAS,
// ambiguous-outcome bookkeeping and map settlement. It runs under installMu
// exclusive, before captureMu or any lower store lock; no remote call or
// persistent bitmap I/O occurs here. Close joins the transition registration.
func (l *chunkLayerImpl) holdBaseTransitionV3() (func(), error) {
	done, err := l.register()
	if err != nil {
		return nil, err
	}
	if hook := l.testBaseTransitionBeforeLock; hook != nil {
		hook()
	}
	l.capMu.Lock()
	l.stMu.Lock()
	var refusal error
	switch {
	case l.closed:
		refusal = errChunkLayerClosed
	case l.fenced:
		refusal = errChunkLayerFenced
	case l.detached != nil:
		refusal = errChunkLayerDetached
	case l.pending != nil:
		refusal = errChunkCASOutcomeUnknown
	}
	l.stMu.Unlock()
	if refusal != nil {
		l.capMu.Unlock()
		done()
		return nil, fmt.Errorf("storage: BASE transition refused: %w", refusal)
	}
	return func() {
		// All lower store locks have been released. Descriptor truth wins even
		// after a refused or partly published transition; never infer completion.
		d := l.s.v3.pub.current()
		l.noteAllMutated()
		l.stMu.Lock()
		l.allDirty = true
		l.dirtyVer++
		switch d.BaseState {
		case BaseInstalling:
			l.hdr.Base = chunkBaseInstalling
		case BaseInstalled:
			l.hdr.Base = chunkBaseInstalledAwaitingCut
		default:
			l.hdr.Base = chunkBaseNone
		}
		// The barrier saved presence while installing; release sees the
		// installed publication only after all lower locks were released.
		presenceDurable := l.presenceVer == l.savedPresVer && len(l.absent) == 0 && len(l.localBlocks) == 0
		l.stMu.Unlock()
		if d.BaseState == BaseInstalled && presenceDurable && l.s.OwnerChunkAttachDurableV3() {
			l.s.AllowBaseRetirementV3()
		}
		l.capMu.Unlock()
		done()
	}, nil
}
