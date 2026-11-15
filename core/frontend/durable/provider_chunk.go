package durable

// walstore-chunk: attaching the storage chunk layer to a provider volume.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// ErrNotChunkVolume is returned by the chunk operations for a volume that
// has no attached chunk layer (another impl, or not recovered yet).
var ErrNotChunkVolume = errors.New("durable: volume has no chunk layer")

// ErrChunkAwaitingAuthority is returned by the chunk operations for a
// recovered volume whose layer is not attached yet because the master's
// assignment is not visible in the projection.
var ErrChunkAwaitingAuthority = errors.New("durable: chunk layer awaiting master authority")

// ErrChunkNotPrimary and ErrChunkEpochStale are the causes of a cut the
// capture admission refused (wrapped with storage.ErrChunkCaptureRefused):
// the projection is not a healthy primary of the volume here (the caller
// goes elsewhere), or its lineage moved since the layer attached (a new
// attach is needed).
var (
	ErrChunkNotPrimary = errors.New("durable: not a healthy primary for the chunk layer")
	ErrChunkEpochStale = errors.New("durable: chunk layer lineage moved since attach")
)

var (
	errProviderClosed   = errors.New("durable: provider closed")
	errChunkNoAuthority = errors.New("durable: projection carries no master authority for the chunk layer")
)

const (
	// chunkAuthorityPoll is how often a volume awaiting authority checks
	// the projection.
	chunkAuthorityPoll = 20 * time.Millisecond
	// chunkAttachTimeout bounds one deferred attach attempt; a failed
	// attempt is retried after chunkAttachRetry.
	chunkAttachTimeout = 30 * time.Second
	chunkAttachRetry   = time.Second
)

// chunkLineage is the authority a chunk layer publishes under: the
// projection identity at attach time. Every capture admission requires the
// current projection to be healthy and to carry this same lineage; a moved
// lineage refuses the cut instead of publishing under the old epoch.
type chunkLineage struct {
	volumeID, replicaID string
	epoch, endpoint     uint64
}

// chunkAuthority: the projection names this volume, is healthy and carries
// a master-assigned epoch. Before the master's assignment is applied (the
// host blocks it until local recovery is done) the projection has epoch 0
// and is not healthy; a layer is never attached under that.
func chunkAuthority(proj frontend.Projection, volumeID string) bool {
	return proj.VolumeID == volumeID && proj.Healthy && proj.Epoch > 0
}

// recoverChunk runs after a successful local Recover, generation gen. It
// attaches the layer when the projection already carries master authority
// and reports true. Otherwise it starts an authority wait bound to gen
// (which attaches as soon as the assignment is applied) and reports
// false: the volume is recovered but stays non-operational until then.
// The caller holds h.attachMu and is registered with Close.
func (p *DurableProvider) recoverChunk(ctx context.Context, volumeID string, h *volHandle, gen uint64) (bool, error) {
	err := p.attachChunk(ctx, volumeID, h)
	if errors.Is(err, errChunkNoAuthority) {
		p.startChunkAuthorityWait(volumeID, h, gen)
		return false, nil
	}
	return err == nil, err
}

// startChunkAuthorityWait starts the authority wait of recovery gen,
// registered with Close like a recovery. A wait of an earlier recovery
// sees it is stale and exits on its own.
func (p *DurableProvider) startChunkAuthorityWait(volumeID string, h *volHandle, gen uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || h.waitGen == gen {
		return
	}
	h.waitGen = gen
	p.inflight.Add(1)
	go p.chunkAuthorityWait(volumeID, h, gen)
}

// current reports whether gen is still the latest recovery of h.
func (p *DurableProvider) current(h *volHandle, gen uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return h.recoverGen == gen
}

func (p *DurableProvider) chunkAuthorityWait(volumeID string, h *volHandle, gen uint64) {
	defer p.inflight.Done()
	defer func() {
		p.mu.Lock()
		if h.waitGen == gen {
			h.waitGen = 0
		}
		p.mu.Unlock()
	}()
	t := time.NewTicker(chunkAuthorityPoll)
	defer t.Stop()
	for {
		select {
		case <-p.closeCtx.Done():
			return
		case <-t.C:
		}
		if !p.current(h, gen) {
			return // a later recovery (successful or failed) owns the volume
		}
		if !chunkAuthority(p.view.Projection(), volumeID) {
			continue
		}
		h.attachMu.Lock()
		if !p.current(h, gen) { // re-checked under the per-volume serialization
			h.attachMu.Unlock()
			return
		}
		ctx, cancel := context.WithTimeout(p.closeCtx, chunkAttachTimeout)
		err := p.attachChunk(ctx, volumeID, h)
		cancel()
		if err == nil {
			h.backend.SetOperational(true, "chunk layer attached under master authority")
		}
		h.attachMu.Unlock()
		switch {
		case err == nil, errors.Is(err, errProviderClosed):
			return
		case errors.Is(err, errChunkNoAuthority):
			continue
		}
		log.Printf("durable: chunk layer attach for volume=%s: %v (retrying)", volumeID, err)
		select {
		case <-p.closeCtx.Done():
			return
		case <-time.After(chunkAttachRetry):
		}
	}
}

// attachChunk attaches the chunk layer once, under the projection's
// master authority, then retires the fresh marker. The caller holds
// h.attachMu and is registered with Close. ctx bounds the attach's remote
// calls (with provider Close); it does not bound the attached layer.
func (p *DurableProvider) attachChunk(ctx context.Context, volumeID string, h *volHandle) error {
	p.mu.Lock()
	layer := h.chunk
	p.mu.Unlock()
	if layer == nil {
		var err error
		if layer, err = p.newChunkLayer(ctx, volumeID, h); err != nil {
			return err
		}
		p.mu.Lock()
		if p.closed {
			// Not published: the store still owns the layer and closes it
			// in WALStore.Close, which Close runs after this call settles.
			p.mu.Unlock()
			return errProviderClosed
		}
		h.chunk = layer
		if p.cfg.ChunkCaptureInterval > 0 {
			ctx, cancel := context.WithCancel(context.Background())
			h.chunkStop, h.chunkDone = cancel, make(chan struct{})
			go p.chunkLoop(ctx, volumeID, layer, h.chunkDone)
		}
		p.mu.Unlock()
	}
	if h.fresh {
		// The attach persisted the absence of every HEAD chunk; only now
		// may a restart treat the local volume as authoritative.
		if err := removeChunkFreshMarker(h.path); err != nil {
			return err
		}
		h.fresh = false
	}
	return nil
}

func (p *DurableProvider) newChunkLayer(ctx context.Context, volumeID string, h *volHandle) (storage.ChunkLayer, error) {
	ws, ok := h.storage.(*storage.WALStore)
	if !ok {
		return nil, fmt.Errorf("durable: %s storage is %T, not a walstore", ImplWALStoreChunk, h.storage)
	}
	proj := p.view.Projection()
	if !chunkAuthority(proj, volumeID) {
		return nil, errChunkNoAuthority
	}
	inner, err := p.cfg.ChunkRemoteFor(volumeID)
	if err != nil {
		return nil, fmt.Errorf("durable: chunk remote for %s: %w", volumeID, err)
	}
	lin := chunkLineage{volumeID: proj.VolumeID, replicaID: proj.ReplicaID, epoch: proj.Epoch, endpoint: proj.EndpointVersion}
	holder := p.cfg.ChunkHolder
	if holder == "" {
		holder = proj.ReplicaID
	}
	remote := &closeBoundRemote{ChunkRemote: inner, done: p.closeCtx}
	remote.setAttach(ctx) // the attach's calls also end with the caller's ctx
	defer remote.setAttach(nil)
	return storage.AttachChunkLayer(ws, remote, storage.ChunkLayerConfig{
		ChunkBlocks:     p.cfg.ChunkBlocks,
		Epoch:           lin.epoch,
		Holder:          holder,
		FreshFromHead:   h.fresh,
		ReadMissTimeout: p.cfg.ChunkReadMissTimeout,
		Bitmap:          storage.NewFileChunkBitmapStore(h.path + ".chunkbitmap"),
		CaptureAllowed: func() error {
			cur := p.view.Projection()
			if !cur.Healthy || cur.VolumeID != volumeID || lin.volumeID != volumeID {
				return fmt.Errorf("%w: volume %s is not a healthy primary here", ErrChunkNotPrimary, volumeID)
			}
			if cur.ReplicaID != lin.replicaID || cur.Epoch != lin.epoch || cur.EndpointVersion != lin.endpoint {
				return fmt.Errorf("%w: volume %s lineage moved from replica %q epoch %d endpoint %d to replica %q epoch %d endpoint %d",
					ErrChunkEpochStale, volumeID, lin.replicaID, lin.epoch, lin.endpoint, cur.ReplicaID, cur.Epoch, cur.EndpointVersion)
			}
			return nil
		},
	})
}

// closeBoundRemote ends every remote call when the provider closes, so a
// shutdown never waits on the remote for an attach or a cut. While an
// attach runs, its calls also end with the attach caller's context.
type closeBoundRemote struct {
	storage.ChunkRemote
	done context.Context

	mu     sync.Mutex
	attach context.Context
}

func (r *closeBoundRemote) setAttach(ctx context.Context) {
	r.mu.Lock()
	r.attach = ctx
	r.mu.Unlock()
}

func (r *closeBoundRemote) bind(ctx context.Context) (context.Context, func()) {
	r.mu.Lock()
	attach := r.attach
	r.mu.Unlock()
	c, cancel := context.WithCancel(ctx)
	stops := []func() bool{context.AfterFunc(r.done, cancel)}
	if attach != nil {
		stops = append(stops, context.AfterFunc(attach, cancel))
	}
	// AfterFunc runs its function in a new goroutine; a context that has
	// already ended must cancel before the call starts, not after it.
	if r.done.Err() != nil || (attach != nil && attach.Err() != nil) {
		cancel()
	}
	return c, func() {
		for _, stop := range stops {
			stop()
		}
		cancel()
	}
}

func (r *closeBoundRemote) Put(ctx context.Context, key string, data []byte) error {
	c, end := r.bind(ctx)
	defer end()
	return r.ChunkRemote.Put(c, key, data)
}

func (r *closeBoundRemote) Get(ctx context.Context, key string) ([]byte, error) {
	c, end := r.bind(ctx)
	defer end()
	return r.ChunkRemote.Get(c, key)
}

func (r *closeBoundRemote) GetHead(ctx context.Context) (storage.ChunkHead, error) {
	c, end := r.bind(ctx)
	defer end()
	return r.ChunkRemote.GetHead(c)
}

func (r *closeBoundRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	c, end := r.bind(ctx)
	defer end()
	return r.ChunkRemote.CASHead(c, expected, next)
}

func (r *closeBoundRemote) Delete(ctx context.Context, key string) error {
	c, end := r.bind(ctx)
	defer end()
	return r.ChunkRemote.Delete(c, key)
}

func (r *closeBoundRemote) PutIntent(ctx context.Context, in storage.ChunkIntent) (string, error) {
	c, end := r.bind(ctx)
	defer end()
	return r.ChunkRemote.PutIntent(c, in)
}

func (r *closeBoundRemote) DeleteIntent(ctx context.Context, id string) error {
	c, end := r.bind(ctx)
	defer end()
	return r.ChunkRemote.DeleteIntent(c, id)
}

func (r *closeBoundRemote) ListIntents(ctx context.Context) ([]storage.ChunkIntent, error) {
	c, end := r.bind(ctx)
	defer end()
	return r.ChunkRemote.ListIntents(c)
}

// The fresh marker: <volume path>.chunkfresh.

func chunkFreshMarker(path string) string { return path + ".chunkfresh" }

func writeChunkFreshMarker(path string) error {
	m := chunkFreshMarker(path)
	f, err := os.OpenFile(m, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("durable: fresh marker: %w", err)
	}
	_, werr := f.WriteString("fresh\n")
	serr := f.Sync()
	cerr := f.Close()
	if err := errors.Join(werr, serr, cerr); err != nil {
		return fmt.Errorf("durable: fresh marker: %w", err)
	}
	return syncParentDir(m)
}

func removeChunkFreshMarker(path string) error {
	m := chunkFreshMarker(path)
	if err := os.Remove(m); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("durable: remove fresh marker: %w", err)
	}
	return syncParentDir(m)
}

// syncParentDir makes a create or remove in path's directory durable. A
// directory cannot be fsynced on Windows; there it is a no-op (no Windows
// crash-durability claim).
func syncParentDir(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("durable: open directory: %w", err)
	}
	serr := d.Sync()
	cerr := d.Close()
	if err := errors.Join(serr, cerr); err != nil {
		return fmt.Errorf("durable: fsync directory: %w", err)
	}
	return nil
}

// chunkLoop takes a periodic cut. A refused cut (not a healthy primary,
// BASE install) is normal and skipped quietly.
func (p *DurableProvider) chunkLoop(ctx context.Context, volumeID string, layer storage.ChunkLayer, done chan struct{}) {
	defer close(done)
	t := time.NewTicker(p.cfg.ChunkCaptureInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if _, err := layer.Capture(ctx); err != nil && !errors.Is(err, storage.ErrChunkCaptureRefused) && ctx.Err() == nil {
			log.Printf("durable: chunk cut for volume=%s: %v", volumeID, err)
		}
	}
}

func (p *DurableProvider) stopChunkLoop(h *volHandle) {
	p.mu.Lock()
	stop, done := h.chunkStop, h.chunkDone
	h.chunkStop = nil
	p.mu.Unlock()
	if stop != nil {
		stop()
		<-done
	}
}

func (p *DurableProvider) chunkLayer(volumeID string) (storage.ChunkLayer, error) {
	p.mu.Lock()
	h, ok := p.volumes[volumeID]
	p.mu.Unlock()
	if !ok {
		return nil, ErrVolumeNotOpen
	}
	p.mu.Lock()
	layer, awaiting := h.chunk, h.waitGen != 0
	p.mu.Unlock()
	if layer == nil && awaiting {
		return nil, ErrChunkAwaitingAuthority
	}
	if layer == nil {
		return nil, ErrNotChunkVolume
	}
	return layer, nil
}

// CaptureVolume takes one quiesced cut of volumeID and publishes it to the
// remote. It returns the cut's LSN.
func (p *DurableProvider) CaptureVolume(ctx context.Context, volumeID string) (uint64, error) {
	layer, err := p.chunkLayer(volumeID)
	if err != nil {
		return 0, err
	}
	return layer.Capture(ctx)
}

// MakeVolumeRemoteDurable returns nil only once volumeID's writes up to lsn
// are published in the remote HEAD (snapshot, detach, migrate).
func (p *DurableProvider) MakeVolumeRemoteDurable(ctx context.Context, volumeID string, lsn uint64) error {
	layer, err := p.chunkLayer(volumeID)
	if err != nil {
		return err
	}
	return layer.MakeRemoteDurable(ctx, lsn)
}
