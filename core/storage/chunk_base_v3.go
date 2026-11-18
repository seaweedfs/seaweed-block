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
		done()
		return nil, fmt.Errorf("storage: BASE transition refused: %w", refusal)
	}
	return func() {
		// All lower store locks have been released. Descriptor truth wins even
		// after a refused or partly published transition; never infer completion.
		d := l.s.v3.pub.current()
		l.noteAllMutated()
		l.stMu.Lock()
		l.reconcileGen++
		if l.reconciled != nil {
			close(l.reconciled)
			l.reconciled = nil
		}
		l.reconcile = map[uint64]bool{}
		l.hashMode = false
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
		l.stMu.Unlock()
		done()
	}, nil
}
