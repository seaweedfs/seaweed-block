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

// ChunkGeometry is a chunk volume's shape as this host serves it.
type ChunkGeometry struct {
	NumBlocks   uint64
	BlockSize   uint32
	ChunkBlocks uint32
}

// ErrChunkNotPrimary and ErrChunkEpochStale are the causes of a cut the
// capture admission refused (wrapped with storage.ErrChunkCaptureRefused):
// the projection is not a healthy primary of the volume here (the caller
// goes elsewhere), or its lineage moved since the layer attached (a new
// attach is needed).
var (
	ErrChunkNotPrimary = errors.New("durable: not a healthy primary for the chunk layer")
	ErrChunkEpochStale = errors.New("durable: chunk layer lineage moved since attach")
)

// ErrChunkStaleLocalRoot refuses to attach an existing local volume whose
// HEAD another holder has published: the local image predates that HEAD
// (a workspace that moved away and back), so serving or cutting it would
// lose the other holder's acknowledged writes. It wraps
// storage.ErrChunkLayerFenced: the volume is not served and the attach is
// not retried. Removing the local volume lets the next attach start fresh
// from HEAD.
var ErrChunkStaleLocalRoot = errors.New("durable: local volume predates a HEAD published by another holder")

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
	return proj.VolumeID == volumeID && proj.Healthy && proj.Epoch > 0 && proj.ReplicaID != ""
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
	limit := p.cfg.ChunkAssignmentTimeout
	if limit <= 0 {
		limit = 30 * time.Second
	}
	waitCtx, endWait := context.WithTimeout(p.closeCtx, limit)
	defer endWait()
	defer func() {
		if !errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
			return
		}
		// The old wait must not overwrite a newer recovery's readiness.
		h.attachMu.Lock()
		defer h.attachMu.Unlock()
		if p.current(h, gen) && h.chunk == nil {
			h.backend.SetOperational(false, "chunk assignment unavailable: "+waitCtx.Err().Error())
		}
	}()
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
		case <-waitCtx.Done():
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
		ctx, cancel := context.WithTimeout(waitCtx, chunkAttachTimeout)
		err := p.attachChunk(ctx, volumeID, h)
		cancel()
		if err == nil {
			err = chunkFenced(volumeID, h)
		}
		if err == nil && !h.chunkDetach {
			h.backend.SetOperational(true, "chunk layer attached under master authority")
		}
		h.attachMu.Unlock()
		switch {
		case err == nil, errors.Is(err, errProviderClosed):
			return
		case errors.Is(err, storage.ErrChunkLayerFenced):
			log.Printf("durable: chunk layer for volume=%s: %v (not serving)", volumeID, err)
			if errors.Is(err, ErrChunkStaleLocalRoot) {
				h.attachMu.Lock()
				if p.current(h, gen) && h.chunk == nil {
					h.backend.SetOperational(false, "chunk layer attach refused: "+err.Error())
				}
				h.attachMu.Unlock()
			}
			return
		case errors.Is(err, errChunkNoAuthority):
			continue
		}
		log.Printf("durable: chunk layer attach for volume=%s: %v (retrying)", volumeID, err)
		select {
		case <-waitCtx.Done():
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
		h.backend.mu.Lock()
		h.backend.chunkFenced = layer.Fenced
		h.backend.mu.Unlock()
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
	if check := p.cfg.ChunkAttachCheck; check != nil {
		g := ChunkGeometry{NumBlocks: uint64(ws.NumBlocks()), BlockSize: uint32(ws.BlockSize()), ChunkBlocks: p.cfg.ChunkBlocks}
		cctx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(p.closeCtx, cancel)
		err := check(cctx, volumeID, g)
		stop()
		cancel()
		if err != nil {
			return nil, fmt.Errorf("durable: chunk attach check for %s: %w", volumeID, err)
		}
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
	bound := &closeBoundRemote{ChunkRemote: inner, done: p.closeCtx}
	bound.setAttach(ctx) // the attach's calls also end with the caller's ctx
	defer bound.setAttach(nil)
	var remote storage.ChunkRemote = bound
	if builder, ok := inner.(storage.ChunkManifestBuilder); ok {
		remote = &manifestBoundRemote{closeBoundRemote: bound, builder: builder}
	}
	cfg := storage.ChunkLayerConfig{
		ChunkBlocks:       p.cfg.ChunkBlocks,
		Epoch:             lin.epoch,
		Holder:            holder,
		FreshFromHead:     h.fresh,
		ReadMissTimeout:   p.cfg.ChunkReadMissTimeout,
		Bitmap:            storage.NewFileChunkBitmapStore(h.path + ".chunkbitmap"),
		BarrierBound:      p.cfg.ChunkDirtyThreshold,
		UploadConcurrency: p.cfg.ChunkUploadConcurrency,
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
	}
	if p.testWALStoreFactory != nil {
		// Dark mapped caller controls must not rely on the legacy C11 sidecar.
		cfg.Bitmap = nil
	}
	if !h.fresh {
		// An existing local volume is this holder's image only while HEAD is
		// still this holder's publication. Another holder's HEAD means the
		// workspace moved away and back: refuse rather than serve, or cut
		// over, the newer image with the stale one.
		head, err := remote.GetHead(ctx)
		if err != nil {
			return nil, fmt.Errorf("durable: read HEAD before attaching existing volume %s: %w", volumeID, err)
		}
		if head.Gen != 0 && head.Holder != holder {
			err := fmt.Errorf("%w: %w: volume %s local root %s, HEAD holder %q gen %d epoch %d, local holder %q; remove or quarantine the local root to attach fresh from HEAD",
				ErrChunkStaleLocalRoot, storage.ErrChunkLayerFenced, volumeID, h.path, head.Holder, head.Gen, head.Epoch, holder)
			log.Printf("durable: refusing to attach volume=%s root=%s: HEAD holder=%q gen=%d epoch=%d is not this holder %q (remove or quarantine the local root)",
				volumeID, h.path, head.Holder, head.Gen, head.Epoch, holder)
			return nil, err
		}
	}
	if h.fresh {
		// Attach preparation is gated. Seed the exact observed HEAD before
		// its takeover CAS; only then finalize lazy attachment and expose I/O.
		cfg.PrepareRestore = func(head storage.ChunkHead) error {

			// No HEAD is a new lineage; preserve any pre-assignment local
			// writes instead of replacing their frontier with zero.
			if head.Gen == 0 {
				return nil
			}
			// EnsureVolume's initial ZERO HEAD is not a remote data image.
			// A local frontier already advanced before assignment belongs to
			// this bootstrap: retain its extent bytes and unknown dirty set,
			// rather than seeding backwards or installing lazy absence.
			if head.RemoteLSN == 0 && len(head.Chunks) == 0 && head.State != storage.ChunkHeadDetached && head.DetachedBy == nil && ws.NextLSN() > 1 {
				cfg.FreshFromHead = false
				return nil
			}
			// Every component that cached the next LSN learns the seeded
			// one before the takeover CAS and before the volume serves I/O,
			// serialized with observer installs. A refused rebase fails the
			// restore here, so HEAD is never claimed for a mis-ordered ship.
			return h.backend.seedAndRebase(func() error { return ws.SeedChunkFrontier(head.RemoteLSN) })
		}
		head, err := storage.ClaimChunkHead(ctx, remote, cfg)
		if err != nil {
			return nil, fmt.Errorf("durable: restore HEAD claim: %w", err)
		}
		cfg.RestoreGuard = &storage.ChunkHeadVersion{Gen: head.Gen, Epoch: head.Epoch}
	}

	return storage.AttachChunkLayer(ws, remote, cfg)
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

// Cut scheduler constants (B9 W3 ruling).
const (
	chunkSchedulePoll  = 100 * time.Millisecond
	chunkCutMinSpacing = time.Second
	chunkCutBackoffMin = time.Second
	chunkCutBackoffMax = 30 * time.Second
)

// cutPolicy decides when an automatic cut is due. The remote holds
// snapshots, not live data: cuts are published at Snapshot, Detach/stop
// and explicit Cut, and automatically only by the operator's timer:
//   - the timer fires once interval has passed since the last successful
//     cut of any kind (explicit Cut, Snapshot and Detach included); zero
//     disables it, and with it every automatic cut;
//   - the dirty-chunk count never triggers a cut;
//   - automatic cuts are at least minSpacing apart (explicit ones are not
//     subject to it);
//   - a failed automatic cut resets nothing and is retried after a backoff
//     doubling from backoffMin to backoffMax; any later successful cut (a
//     no-op cut and an explicit cut included) clears the backoff and
//     restarts the timer.
type cutPolicy struct {
	interval, minSpacing, backoffMin, backoffMax time.Duration
}

type cutSchedule struct {
	lastSuccess time.Time // last successful cut of any kind
	lastAuto    time.Time // start of the last automatic cut
	failedAt    time.Time // end of the last failed automatic cut
	retryAt     time.Time // a failed automatic cut's backoff ends here
	backoff     time.Duration
}

// observe takes in a successful cut the layer reports (any trigger). A
// success after the last failed automatic cut makes that failure's
// backoff obsolete.
func (s cutSchedule) observe(lastCutNs int64) cutSchedule {
	if lastCutNs > 0 {
		c := time.Unix(0, lastCutNs)
		if c.After(s.lastSuccess) {
			s.lastSuccess = c
		}
		if !s.failedAt.IsZero() && c.After(s.failedAt) {
			s.backoff, s.retryAt, s.failedAt = 0, time.Time{}, time.Time{}
		}
	}
	return s
}

// due reports whether an automatic cut is due at now, and why. Nothing is
// due while the layer reconciles its dirty set: no HEAD moves until then,
// and the skipped tick is not a failure (no backoff).
func (pol cutPolicy) due(s cutSchedule, now time.Time, reconciling bool) (string, bool) {
	if reconciling || now.Before(s.retryAt) {
		return "", false
	}
	if !s.lastAuto.IsZero() && now.Sub(s.lastAuto) < pol.minSpacing {
		return "", false
	}
	if pol.interval > 0 && now.Sub(s.lastSuccess) >= pol.interval {
		return "periodic", true
	}
	return "", false
}

// after records the outcome of an automatic cut started at start.
func (pol cutPolicy) after(s cutSchedule, start, end time.Time, err error) cutSchedule {
	s.lastAuto = start
	if err == nil {
		s.backoff, s.retryAt, s.failedAt = 0, time.Time{}, time.Time{}
		if end.After(s.lastSuccess) {
			s.lastSuccess = end
		}
		return s
	}
	switch {
	case s.backoff == 0:
		s.backoff = pol.backoffMin
	case s.backoff*2 > pol.backoffMax:
		s.backoff = pol.backoffMax
	default:
		s.backoff *= 2
	}
	s.failedAt, s.retryAt = end, end.Add(s.backoff)
	return s
}

// chunkLoop runs the volume's automatic cuts under cutPolicy. A refused
// cut (not a healthy primary of this lineage, BASE install) is expected
// and logged only when it is something else. A fenced layer can never
// publish again: the loop ends.
func (p *DurableProvider) chunkLoop(ctx context.Context, volumeID string, layer storage.ChunkLayer, done chan struct{}) {
	defer close(done)
	pol := cutPolicy{
		interval:   p.cfg.ChunkCaptureInterval,
		minSpacing: chunkCutMinSpacing,
		backoffMin: chunkCutBackoffMin,
		backoffMax: chunkCutBackoffMax,
	}
	s := cutSchedule{lastSuccess: time.Now()}
	t := time.NewTicker(chunkSchedulePoll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if layer.Fenced() {
			return
		}
		ls := layer.Stats()
		s = s.observe(ls.LastCutNs)
		start := time.Now()
		reason, ok := pol.due(s, start, ls.Reconciling)
		if !ok {
			continue
		}
		_, err := layer.Cut(ctx)
		s = pol.after(s, start, time.Now(), err)
		if err != nil && !errors.Is(err, storage.ErrChunkCaptureRefused) && ctx.Err() == nil {
			log.Printf("durable: %s chunk cut for volume=%s: %v (retry in %s)", reason, volumeID, err, s.backoff)
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
	_, layer, err := p.chunkHandle(volumeID)
	return layer, err
}

// chunkHandle resolves volumeID's handle and its attached layer in one
// critical section; the caller keeps using that handle even if Close
// removes it from the map afterwards.
func (p *DurableProvider) chunkHandle(volumeID string) (*volHandle, storage.ChunkLayer, error) {
	p.mu.Lock()
	h, ok := p.volumes[volumeID]
	var layer storage.ChunkLayer
	awaiting := false
	if ok {
		layer, awaiting = h.chunk, h.waitGen != 0
	}
	p.mu.Unlock()
	switch {
	case !ok:
		return nil, nil, ErrVolumeNotOpen
	case layer == nil && awaiting:
		return nil, nil, ErrChunkAwaitingAuthority
	case layer == nil:
		return nil, nil, ErrNotChunkVolume
	}
	return h, layer, nil
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

// chunkFenced refuses a volume whose layer attached fenced: HEAD belongs to
// a newer epoch or another holder (another server serving this replica),
// so this host must not serve it (integration ruling F2).
func chunkFenced(volumeID string, h *volHandle) error {
	if h.chunk != nil && h.chunk.Fenced() {
		return fmt.Errorf("durable: volume %s: HEAD belongs to a newer epoch or another holder: %w", volumeID, storage.ErrChunkLayerFenced)
	}
	return nil
}

// CutVolume takes one quiesced cut of volumeID and returns what it
// installed: the cut LSN, the HEAD (the current one when nothing changed)
// and the cut's statistics (B9 W2 Â§2.4, Â§3.1). With minLSN > 0 it is
// MakeRemoteDurable(minLSN) returning the cut: an LSN never written is
// refused before anything is published, and the cut must reach minLSN.
func (p *DurableProvider) CutVolume(ctx context.Context, volumeID string, minLSN uint64) (storage.ChunkCutResult, error) {
	h, layer, err := p.chunkHandle(volumeID)
	if err != nil {
		return storage.ChunkCutResult{}, err
	}
	if hook := p.testAfterChunkResolve; hook != nil {
		hook()
	}
	if minLSN > 0 {
		if last := h.storage.NextLSN() - 1; minLSN > last {
			return storage.ChunkCutResult{}, fmt.Errorf("durable: cut of %s to LSN %d: that LSN was never written (last %d)", volumeID, minLSN, last)
		}
	}
	r, err := layer.Cut(ctx)
	if err != nil {
		return storage.ChunkCutResult{}, err
	}
	if r.H < minLSN {
		return storage.ChunkCutResult{}, fmt.Errorf("durable: cut of %s reached LSN %d, below %d", volumeID, r.H, minLSN)
	}
	return r, nil
}

// DetachVolume is a clean stop's final cut (B9 W2 Â§3.6): it stops the
// volume's frontend I/O and waits (bounded by ctx) for the I/O already
// admitted to finish, stops the periodic cuts, then publishes one cut with
// HEAD state detached. It also returns the last LSN written. From its
// start, the volume stays stopped for this process, through any later
// RecoverVolume, even when the detach fails; a retry settles it and a
// repeated call returns the first result.
func (p *DurableProvider) DetachVolume(ctx context.Context, volumeID string) (storage.ChunkCutResult, uint64, error) {
	p.mu.Lock()
	h, ok := p.volumes[volumeID]
	p.mu.Unlock()
	if !ok {
		return storage.ChunkCutResult{}, 0, ErrVolumeNotOpen
	}
	h.attachMu.Lock()
	defer h.attachMu.Unlock()
	layer, err := p.chunkLayer(volumeID)
	if err != nil {
		return storage.ChunkCutResult{}, 0, err
	}
	h.chunkDetach = true
	if err := h.backend.StopIO(ctx, "chunk detach: I/O stopped until a new attach"); err != nil {
		return storage.ChunkCutResult{}, 0, err
	}
	p.stopChunkLoop(h)
	r, err := layer.Detach(ctx)
	if err != nil {
		return storage.ChunkCutResult{}, 0, err
	}
	return r, h.storage.NextLSN() - 1, nil
}

// Preserve the optional manifest capability only when the inner remote has it;
// legacy in-memory/flat remotes keep their existing no-op cut behavior.
type manifestBoundRemote struct {
	*closeBoundRemote
	builder storage.ChunkManifestBuilder
}

func (r *manifestBoundRemote) PrepareManifest(ctx context.Context, base, next storage.ChunkHead, chunkBytes uint64) (storage.ChunkHead, []string, error) {
	c, end := r.bind(ctx)
	defer end()
	return r.builder.PrepareManifest(c, base, next, chunkBytes)
}
