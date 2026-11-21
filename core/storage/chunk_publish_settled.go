package storage

// headTransition is one confirmed publication: HEAD moved from prev to next.
type headTransition struct{ prev, next chunkHead }

// noteSettled records a confirmed publication for OnPublishSettled. l.stMu
// held; prev is the HEAD the layer held before next.
func (l *chunkLayerImpl) noteSettled(prev, next chunkHead) {
	if l.cfg.OnPublishSettled == nil || next.Gen == prev.Gen { // a no-op cut publishes nothing
		return
	}
	l.settled = append(l.settled, headTransition{prev: copyHead(prev), next: copyHead(next)})
}

// unlockCap releases the capture lock, then reports the publications
// confirmed under it.
func (l *chunkLayerImpl) unlockCap() {
	l.capMu.Unlock()
	l.runPublishSettled()
}

// runPublishSettled calls OnPublishSettled for every recorded publication,
// in publication order, outside the capture and state locks, registered with
// Close. A closing layer drops them.
func (l *chunkLayerImpl) runPublishSettled() {
	hook := l.cfg.OnPublishSettled
	if hook == nil {
		return
	}
	l.stMu.Lock()
	ts := l.settled
	l.settled = nil
	l.stMu.Unlock()
	if len(ts) == 0 {
		return
	}
	done, err := l.register()
	if err != nil {
		return
	}
	defer done()
	for _, t := range ts {
		hook(l.ctx, t.prev, t.next)
	}
}
