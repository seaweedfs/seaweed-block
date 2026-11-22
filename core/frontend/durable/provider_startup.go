package durable

import (
	"errors"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

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

// ChunkStartupSuperseded reports only a proven foreign-holder claim refusal
// for this exact recovery. Selection still rechecks gen under the provider lock.
func (p *DurableProvider) ChunkStartupSuperseded(volumeID string, gen uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.volumes[volumeID]
	return !p.closed && h != nil && gen != 0 && h.recoverGen == gen && h.startupEnded == gen && h.startupSuperseded
}

// ChunkStartupStop is this exact recovery's no-retry stop, if its startup
// ended on a local root that never becomes valid: the reason
// (stale_local_root, local_root_rolled_back) and its operator line; "" for
// any other outcome (V1-SPEC r2.38).
func (p *DurableProvider) ChunkStartupStop(volumeID string, gen uint64) (reason, detail string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.volumes[volumeID]
	if p.closed || h == nil || gen == 0 || h.recoverGen != gen || h.startupEnded != gen {
		return "", ""
	}
	return h.startupStop, h.startupStopDetail
}

// stopDetailError carries the operator line of a no-retry stop.
type stopDetailError struct {
	err    error
	detail string
}

func (e *stopDetailError) Error() string { return e.err.Error() }
func (e *stopDetailError) Unwrap() error { return e.err }

// startupStopOf classifies a startup's ending cause into its no-retry stop.
func startupStopOf(cause error) (reason, detail string) {
	var d *stopDetailError
	if errors.As(cause, &d) {
		detail = d.detail
	}
	switch {
	case errors.Is(cause, errChunkRootRolledBack):
		return "local_root_rolled_back", detail
	case errors.Is(cause, errChunkStaleRootNoRetry):
		return "stale_local_root", detail
	}
	return "", ""
}

func (p *DurableProvider) endChunkStartup(h *volHandle, gen uint64, cause error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || gen != h.recoverGen || h.startupEnded != 0 {
		return
	}
	h.startupEnded = gen
	h.startupSuperseded = errors.Is(cause, errChunkForeignClaim)
	h.startupStop, h.startupStopDetail = startupStopOf(cause)
	select {
	case p.startupUpdates <- struct{}{}:
	default:
	}
}

// ChooseChunkStartupExit validates and selects an outcome in one provider-lock
// section, so recovery cannot change generation between observation and choice.
// choose must only latch the process outcome; it must not block or call back
// into the provider. No remote work or resource cleanup belongs in this call.
func (p *DurableProvider) ChooseChunkStartupExit(volumeID string, gen uint64, choose func()) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.volumes[volumeID]
	if p.closed || h == nil || h.startupEnded != gen || gen == 0 || h.recoverGen != gen {
		return false
	}
	choose()
	return true
}

// chunkFenceWatchInterval bounds how long after a runtime fence the backend
// reports not operational and the process owner is woken. I/O is refused
// from the fence itself (readyCheck reads the layer's latched flag).
const chunkFenceWatchInterval = 50 * time.Millisecond

// watchChunkFence waits for the attached layer to fence after startup (its
// publication found HEAD owned by another holder or epoch: the layer latches
// fenced only on supersede or owner change, never on a clean detach), then
// latches the backend not operational with that evidence and marks the
// volume runtime-fenced (INV-FE-PRIMARY-READY-OPERATIONAL-001). It ends when
// the provider closes or the layer is no longer this volume's; Close joins it
// through p.inflight (counted when it starts) after cancelling closeCtx.
func (p *DurableProvider) watchChunkFence(volumeID string, h *volHandle, layer storage.ChunkLayer) {
	defer p.inflight.Done()
	t := time.NewTicker(chunkFenceWatchInterval)
	defer t.Stop()
	for {
		select {
		case <-p.closeCtx.Done():
			return
		case <-t.C:
		}
		p.mu.Lock()
		current := !p.closed && p.volumes[volumeID] == h && h.chunk == layer
		p.mu.Unlock()
		if !current {
			return
		}
		if !layer.Fenced() {
			continue
		}
		h.backend.SetOperational(false, "chunk layer fenced after startup: HEAD has another publication owner")
		p.mu.Lock()
		if !p.closed && p.volumes[volumeID] == h && h.chunk == layer {
			h.runtimeFenced = true
			select {
			case p.fenceUpdates <- struct{}{}:
			default:
			}
		}
		p.mu.Unlock()
		return
	}
}

// ChunkRuntimeFenceUpdates wakes the process owner when a volume's attached
// layer fenced after startup; read ChunkRuntimeFenced for the volume.
func (p *DurableProvider) ChunkRuntimeFenceUpdates() <-chan struct{} { return p.fenceUpdates }

// ChunkRuntimeFenced reports a latched runtime fence of the volume's
// attached layer (HEAD has another publication owner).
func (p *DurableProvider) ChunkRuntimeFenced(volumeID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.volumes[volumeID]
	return !p.closed && h != nil && h.runtimeFenced
}
