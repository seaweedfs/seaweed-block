package durable

// ChunkStartupUpdates wakes the process owner when a startup wait ends. It is
// not an event queue: the owner must read ChunkStartupOutcome for its volume.
// INV-WS-STARTUP-RETRY-001 keeps stale recoveries from terminating a newer one.
func (p *DurableProvider) ChunkStartupUpdates() <-chan struct{} { return p.startupUpdates }

// ChunkStartupOutcome is a latched, generation-bound startup failure. A new
// recovery or Close invalidates it; reading it does not consume it. It grants
// no assignment and makes no claim about whether this holder was superseded.
func (p *DurableProvider) ChunkStartupOutcome(volumeID string) (uint64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.volumes[volumeID]
	if p.closed || h == nil || h.startupEnded == 0 || h.startupEnded != h.recoverGen {
		return 0, false
	}
	return h.startupEnded, true
}

func (p *DurableProvider) endChunkStartup(h *volHandle, gen uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || gen != h.recoverGen || h.startupEnded != 0 {
		return
	}
	h.startupEnded = gen
	select {
	case p.startupUpdates <- struct{}{}:
	default:
	}
}
