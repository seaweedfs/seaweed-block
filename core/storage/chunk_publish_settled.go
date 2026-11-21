package storage

// headTransition is one confirmed publication: HEAD moved from prev to next.
type headTransition struct{ prev, next chunkHead }

// noteSettled records a confirmed publication for OnPublishSettled, in
// publication order. l.stMu held; prev is the HEAD the layer held before
// next.
func (l *chunkLayerImpl) noteSettled(prev, next chunkHead) {
	if l.cfg.OnPublishSettled == nil || next.Gen == prev.Gen { // a no-op cut publishes nothing
		return
	}
	l.settled = append(l.settled, headTransition{prev: copyHead(prev), next: copyHead(next)})
}

// unlockCap releases the capture lock, then delivers the publications
// recorded so far.
func (l *chunkLayerImpl) unlockCap() {
	l.capMu.Unlock()
	l.runPublishSettled()
}

// runPublishSettled delivers recorded publications to OnPublishSettled one
// at a time, in publication order, outside the capture and state locks. One
// caller at a time drains the queue (any other caller only leaves its
// reports queued for it, so a hook that publishes again cannot deadlock);
// the drainer is registered with Close. Once Close has begun, no further
// report is delivered: the rest are dropped.
func (l *chunkLayerImpl) runPublishSettled() {
	hook := l.cfg.OnPublishSettled
	if hook == nil {
		return
	}
	l.stMu.Lock()
	if l.settledDraining || len(l.settled) == 0 {
		l.stMu.Unlock()
		return
	}
	if l.closed {
		l.settled = nil
		l.stMu.Unlock()
		return
	}
	l.settledDraining = true
	l.wg.Add(1) // under stMu while not closed: Close's wait covers this drain
	l.stMu.Unlock()
	defer l.wg.Done()
	for {
		l.stMu.Lock()
		if l.closed {
			l.settled = nil
		}
		if len(l.settled) == 0 {
			l.settledDraining = false
			l.stMu.Unlock()
			return
		}
		t := l.settled[0]
		l.settled = l.settled[1:]
		l.stMu.Unlock()
		hook(l.ctx, t.prev, t.next)
	}
}
