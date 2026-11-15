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
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// ErrNotChunkVolume is returned by the chunk operations for a volume that
// has no attached chunk layer (another impl, or not recovered yet).
var ErrNotChunkVolume = errors.New("durable: volume has no chunk layer")

var errProviderClosed = errors.New("durable: provider closed")

// chunkLineage is the authority a chunk layer publishes under: the
// projection identity at attach time. Every capture admission requires the
// current projection to be healthy and to carry this same lineage; a moved
// lineage refuses the cut instead of publishing under the old epoch.
type chunkLineage struct {
	volumeID, replicaID string
	epoch, endpoint     uint64
}

// attachChunk attaches the chunk layer after a successful Recover. The
// caller (RecoverVolume) holds h.attachMu and is registered with Close.
// A fresh volume starts from the remote HEAD; an existing local volume
// stays authoritative. The manifest epoch is the master-assigned epoch
// from the projection, held by the layer separately from the frontend
// Identity.
func (p *DurableProvider) attachChunk(volumeID string, h *volHandle) error {
	p.mu.Lock()
	layer := h.chunk
	p.mu.Unlock()
	if layer == nil {
		var err error
		if layer, err = p.newChunkLayer(volumeID, h); err != nil {
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

func (p *DurableProvider) newChunkLayer(volumeID string, h *volHandle) (storage.ChunkLayer, error) {
	ws, ok := h.storage.(*storage.WALStore)
	if !ok {
		return nil, fmt.Errorf("durable: %s storage is %T, not a walstore", ImplWALStoreChunk, h.storage)
	}
	remote, err := p.cfg.ChunkRemoteFor(volumeID)
	if err != nil {
		return nil, fmt.Errorf("durable: chunk remote for %s: %w", volumeID, err)
	}
	proj := p.view.Projection()
	lin := chunkLineage{volumeID: proj.VolumeID, replicaID: proj.ReplicaID, epoch: proj.Epoch, endpoint: proj.EndpointVersion}
	holder := p.cfg.ChunkHolder
	if holder == "" {
		holder = proj.ReplicaID
	}
	return storage.AttachChunkLayer(ws, closeBoundRemote{remote, p.closeCtx}, storage.ChunkLayerConfig{
		ChunkBlocks:     p.cfg.ChunkBlocks,
		Epoch:           lin.epoch,
		Holder:          holder,
		FreshFromHead:   h.fresh,
		ReadMissTimeout: p.cfg.ChunkReadMissTimeout,
		Bitmap:          storage.NewFileChunkBitmapStore(h.path + ".chunkbitmap"),
		CaptureAllowed: func() error {
			cur := p.view.Projection()
			if !cur.Healthy || cur.VolumeID != volumeID || lin.volumeID != volumeID {
				return fmt.Errorf("volume %s is not a healthy primary here", volumeID)
			}
			if cur.ReplicaID != lin.replicaID || cur.Epoch != lin.epoch || cur.EndpointVersion != lin.endpoint {
				return fmt.Errorf("volume %s lineage moved from replica %q epoch %d endpoint %d to replica %q epoch %d endpoint %d",
					volumeID, lin.replicaID, lin.epoch, lin.endpoint, cur.ReplicaID, cur.Epoch, cur.EndpointVersion)
			}
			return nil
		},
	})
}

// closeBoundRemote ends every remote call when the provider closes, so a
// shutdown never waits on the remote for an attach or a cut.
type closeBoundRemote struct {
	storage.ChunkRemote
	done context.Context
}

func (r closeBoundRemote) bind(ctx context.Context) (context.Context, func()) {
	c, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.done, cancel)
	return c, func() { stop(); cancel() }
}

func (r closeBoundRemote) Put(ctx context.Context, key string, data []byte) error {
	c, end := r.bind(ctx)
	defer end()
	return r.ChunkRemote.Put(c, key, data)
}

func (r closeBoundRemote) Get(ctx context.Context, key string) ([]byte, error) {
	c, end := r.bind(ctx)
	defer end()
	return r.ChunkRemote.Get(c, key)
}

func (r closeBoundRemote) GetHead(ctx context.Context) (storage.ChunkHead, error) {
	c, end := r.bind(ctx)
	defer end()
	return r.ChunkRemote.GetHead(c)
}

func (r closeBoundRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	c, end := r.bind(ctx)
	defer end()
	return r.ChunkRemote.CASHead(c, expected, next)
}

func (r closeBoundRemote) Delete(ctx context.Context, key string) error {
	c, end := r.bind(ctx)
	defer end()
	return r.ChunkRemote.Delete(c, key)
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
	layer := h.chunk
	p.mu.Unlock()
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
