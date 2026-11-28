package durable

// walstore-chunk: attaching the storage chunk layer to a provider volume.

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/hostlimit"
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
	errProviderClosed    = errors.New("durable: provider closed")
	errChunkForeignClaim = errors.New("durable: HEAD claim refused by another holder")
	errChunkNoAuthority  = errors.New("durable: projection carries no master authority for the chunk layer")
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
	return chunkAssignment(proj, volumeID) && proj.Healthy
}

// A sealed recovery is non-serving but its assigned identity can still answer
// the read-only plain-seal replay (INV-WS-SEAL-REPLAY-001).
func chunkAssignment(proj frontend.Projection, volumeID string) bool {
	return proj.VolumeID == volumeID && proj.Epoch > 0 && proj.ReplicaID != ""
}

// chunkAuthorityGap names the projection fields that fail chunkAuthority,
// for the logs ("" when none fails).
func chunkAuthorityGap(proj frontend.Projection, volumeID string) string {
	var gap []string
	if proj.VolumeID != volumeID {
		gap = append(gap, fmt.Sprintf("volume=%q", proj.VolumeID))
	}
	if !proj.Healthy {
		gap = append(gap, "healthy=false")
	}
	if proj.Epoch == 0 {
		gap = append(gap, "epoch=0")
	}
	if proj.ReplicaID == "" {
		gap = append(gap, `replica=""`)
	}
	return strings.Join(gap, " ")
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
	if (errors.Is(err, storage.ErrChunkLayerFenced) &&
		(!errors.Is(err, ErrChunkStaleLocalRoot) || errors.Is(err, errChunkForeignClaim))) ||
		finalLayoutStartRefusal(err) {
		p.endChunkStartup(h, gen, err)
	}
	return err == nil, err
}

// finalLayoutStartRefusal distinguishes a store's definite Start refusal
// from an unavailable read or a conditional-write race that may be retried.
// A detached HEAD has its separate replay-retained disposition below.
func finalLayoutStartRefusal(err error) bool {
	return errors.Is(err, layout.ErrStaleLocalRoot) || errors.Is(err, layout.ErrMissingLocalRoot) ||
		errors.Is(err, layout.ErrGeometryMismatch) || errors.Is(err, layout.ErrHeadAbsent) ||
		errors.Is(err, layout.ErrVolumeDeleting) || errors.Is(err, layout.ErrStaleEpoch) ||
		errors.Is(err, layout.ErrNotHolder) || errors.Is(err, layout.ErrIdentity) ||
		errors.Is(err, layout.ErrCorrupt) || errors.Is(err, layout.ErrMalformed) ||
		errors.Is(err, layout.ErrMissing) || errors.Is(err, layout.ErrHeadTooLarge) ||
		errors.Is(err, chunkstore.ErrLegacyStore)
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
	start := time.Now()
	bound := fmt.Sprintf("up to %s", limit)
	if p.cfg.ChunkMasterWait != nil {
		bound = fmt.Sprintf("%s of answered contact with the master", limit)
	}
	log.Printf("durable: chunk layer for volume=%s waits for master authority (%s): projection lacks %s",
		volumeID, bound, chunkAuthorityGap(p.view.Projection(), volumeID))
	ended := "superseded by a later recovery"
	defer func() {
		log.Printf("durable: chunk authority wait for volume=%s ended after %s: %s", volumeID, time.Since(start), ended)
	}()
	retained := false // this generation already chose sealed replay or diagnostics
	// expired: the wait gave up (deadline, or a permanent refusal). Without
	// ChunkMasterWait the deadline is the fixed limit from now; with it the
	// host's bound, which does not run while the master gives no reply.
	expired := false
	var waitCtx context.Context
	var endWait context.CancelFunc
	if p.cfg.ChunkMasterWait == nil {
		waitCtx, endWait = context.WithTimeout(p.closeCtx, limit)
	} else {
		waitCtx, endWait = context.WithCancel(p.closeCtx)
	}
	defer endWait()
	// gaveUp: a permanent refusal, or the host's one bound for this wait has
	// passed (the window opens at the first reply after this wait started;
	// none while the master gives no reply or refuses the node).
	gaveUp := func() bool {
		if p.cfg.ChunkMasterWait == nil {
			return false
		}
		deadline, final := p.cfg.ChunkMasterWait(start)
		return final || (!deadline.IsZero() && !time.Now().Before(deadline))
	}
	defer func() {
		if retained || !(expired || errors.Is(waitCtx.Err(), context.DeadlineExceeded)) {
			return
		}
		// The old wait must not overwrite a newer recovery's readiness.
		h.attachMu.Lock()
		defer h.attachMu.Unlock()
		if p.current(h, gen) && h.chunk == nil {
			ended = fmt.Sprintf("assignment unavailable, not serving (projection lacks %s)", chunkAuthorityGap(p.view.Projection(), volumeID))
			why := "master wait ended"
			if !expired {
				why = waitCtx.Err().Error() // the fixed limit: context deadline exceeded
			}
			h.backend.SetOperational(false, "chunk assignment unavailable: "+why)
			p.endChunkStartup(h, gen, nil)
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
	attempts := 0
	for {
		select {
		case <-waitCtx.Done():
			ended = "provider closed"
			if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
				ended = "deadline"
			}
			return
		case <-t.C:
		}
		if !p.current(h, gen) {
			return // a later recovery (successful or failed) owns the volume
		}
		proj := p.view.Projection()
		if !chunkAuthority(proj, volumeID) {
			// Authority first: a grant that arrives on the same tick as the
			// bound attaches instead of giving up.
			if gaveUp() {
				expired, ended = true, "master wait ended (bound reached or permanent refusal)"
				return
			}
			continue
		}
		if attempts++; attempts == 1 {
			log.Printf("durable: chunk layer for volume=%s: master authority after %s (replica=%q epoch=%d endpoint=%d); first attach attempt",
				volumeID, time.Since(start), proj.ReplicaID, proj.Epoch, proj.EndpointVersion)
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
		case err == nil:
			ended = fmt.Sprintf("attached after %d attempt(s)", attempts)
			return
		case errors.Is(err, errProviderClosed):
			ended = "provider closed"
			return
		case errors.Is(err, storage.ErrChunkLayerDetached), errors.Is(err, layout.ErrDetached):
			// INV-WS-STARTUP-RETRY-001: an assigned sealed incarnation stays
			// non-serving for exact seal replay; restarting cannot unseal it.
			retained = true
			ended = "sealed assignment, retained for replay"
			h.backend.SetOperational(false, "sealed assignment; replay only")
			return
		case errors.Is(err, storage.ErrChunkLayerFenced):
			ended = "fenced, not serving"
			log.Printf("durable: chunk layer for volume=%s: %v (not serving)", volumeID, err)
			if errors.Is(err, ErrChunkStaleLocalRoot) && !errors.Is(err, errChunkForeignClaim) {
				retained = true
				h.attachMu.Lock()
				if p.current(h, gen) && h.chunk == nil {
					h.backend.SetOperational(false, "chunk layer attach refused: "+err.Error())
				}
				h.attachMu.Unlock()
			} else {
				p.endChunkStartup(h, gen, err)
			}
			return
		case finalLayoutStartRefusal(err):
			ended = "layout Start refused, not serving"
			log.Printf("durable: chunk layer for volume=%s: %v (final refusal)", volumeID, err)
			h.attachMu.Lock()
			if p.current(h, gen) && h.chunk == nil {
				h.backend.SetOperational(false, "chunk layer attach refused: "+err.Error())
				p.endChunkStartup(h, gen, err)
			}
			h.attachMu.Unlock()
			return
		case errors.Is(err, errChunkNoAuthority):
			log.Printf("durable: chunk layer attach for volume=%s: authority lost before attempt %d (projection lacks %s); waiting",
				volumeID, attempts, chunkAuthorityGap(p.view.Projection(), volumeID))
			continue
		}
		log.Printf("durable: chunk layer attach for volume=%s: %v (retrying)", volumeID, err)
		// A granted attach that keeps failing stays inside the bound: it
		// must not hold the lease while never serving.
		if gaveUp() {
			expired, ended = true, "master wait ended while the attach kept failing"
			return
		}
		select {
		case <-waitCtx.Done():
			ended = "provider closed during the retry backoff"
			if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
				ended = "deadline during the retry backoff"
			}
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
	if layer == nil {
		layer = h.chunkPending
	}
	p.mu.Unlock()
	if layer == nil {
		var err error
		if layer, err = p.newChunkLayer(ctx, volumeID, h); err != nil {
			return err
		}
		p.mu.Lock()
		if p.closed {
			// The store owns this unpublished layer and closes it after
			// the admitted attach settles.
			p.mu.Unlock()
			return errProviderClosed
		}
		h.chunkPending = layer
		p.mu.Unlock()
	}
	// The owner commit is serialized with a later BASE begin. The callback
	// does marker I/O only: no store/layer reentry while installMu is held.
	commit := func() error { return nil }
	if h.fresh {
		if hook := p.testBeforeChunkFreshRetire; hook != nil {
			if err := hook(); err != nil {
				return err
			}
		}
	}
	if ws, ok := h.storage.(*storage.WALStore); ok {
		if err := ws.CommitChunkAttachV3(commit); err != nil {
			return err
		}
	} else if err := commit(); err != nil {
		return err
	}
	ws := h.storage.(*storage.WALStore)
	id, _, _ := ws.LayoutIdentity()
	if err := ws.SetLayoutIdentity(id, false); err != nil {
		return err
	}
	// RULE-FRESH-KNOWN-ZERO order: the marker is retired (above), THEN the
	// marking is made durable, THEN the volume serves (after this attach).
	if h.fresh {
		if hook := p.testAfterChunkFreshRetire; hook != nil {
			if err := hook(); err != nil {
				return err
			}
		}
	}
	if kz, ok := layer.(interface{ ApplyFreshKnownZero() }); ok {
		kz.ApplyFreshKnownZero()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errProviderClosed
	}
	h.fresh = false
	if h.chunk == nil {
		// No explicit operation can resolve the layer, and no scheduler
		// can publish, until marker retirement is durably committed.
		h.chunk = layer
		h.chunkPending = nil
		h.backend.mu.Lock()
		h.backend.chunkFenced = layer.Fenced
		h.backend.mu.Unlock()
		p.startChunkLoop(volumeID, h, layer)
		p.inflight.Add(1) // under p.mu with closed false: Close joins the watcher
		go p.watchChunkFence(volumeID, h, layer)
	}
	return nil
}

// startChunkLoop starts the periodic cuts of an attached layer, if configured
// and not already running. The caller holds h.attachMu and p.mu.
func (p *DurableProvider) startChunkLoop(volumeID string, h *volHandle, layer storage.ChunkLayer) {
	snapshotsOn := p.cfg.ChunkSnapshotPeriod > 0 && p.cfg.ChunkScheduledSnapshot != nil
	if (p.cfg.ChunkCaptureInterval <= 0 && (p.cfg.ChunkMaintenanceInterval <= 0 || p.cfg.ChunkMaintenance == nil) && !snapshotsOn) || h.chunkStop != nil {
		return
	}
	loopCtx, cancel := context.WithCancel(context.Background())
	h.chunkStop, h.chunkDone = cancel, make(chan struct{})
	// A re-armed loop (for example after a refused detach) keeps the
	// volume's schedule: its slots, retries and history.
	if !snapshotsOn {
		h.snapshots = nil
	} else if h.snapshots == nil {
		h.snapshots = &snapshotSlot{s: newSnapshotSchedule(volumeID, p.cfg.ChunkSnapshotPeriod, time.Now())}
	}
	if p.cfg.ChunkCaptureInterval <= 0 {
		h.cutWait = nil
	} else if h.cutWait == nil {
		h.cutWait = &cutWaitSlot{}
	}
	if p.cfg.ChunkMaintenanceInterval <= 0 || p.cfg.ChunkMaintenance == nil {
		h.maintKick = nil
	} else if h.maintKick == nil {
		h.maintKick = make(chan struct{}, 1)
	}
	lin := h.chunkLineage
	current := func() bool { return p.maintenanceCurrent(volumeID, h, layer, lin) }
	go p.chunkBackground(loopCtx, cancel, volumeID, layer, h.chunkDone, current, h.snapshots, h.cutWait, h.maintKick)
}

func (p *DurableProvider) newChunkLayer(ctx context.Context, volumeID string, h *volHandle) (storage.ChunkLayer, error) {
	// Close cancels local takeover cooling as well as each bound remote RPC.
	ctx, cancelAttach := context.WithCancel(ctx)
	stopClose := context.AfterFunc(p.closeCtx, cancelAttach)
	defer func() { stopClose(); cancelAttach() }()
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
	lin := chunkLineage{volumeID: proj.VolumeID, replicaID: proj.ReplicaID, epoch: proj.Epoch, endpoint: proj.EndpointVersion}
	holder := p.chunkHolder(proj)
	var inner storage.ChunkRemote
	var layoutHead layout.Head
	var err error
	st, e := p.cfg.ChunkLayoutHeadFor(volumeID)
	if e != nil {
		return nil, e
	}
	layoutHead, err = startLayoutRoot(ctx, ws, st, layout.HolderID{Epoch: proj.Epoch, Holder: holder})
	if err != nil {
		return nil, err
	}
	me := layout.HolderID{Epoch: layoutHead.Epoch, Holder: layoutHead.Holder, ClaimID: layoutHead.ClaimID}
	p.mu.Lock()
	if h.layoutHolder == me {
		inner = h.layoutRemote
	}
	p.mu.Unlock()
	if inner == nil {
		inner, err = p.cfg.ChunkLayoutRemoteFor(volumeID, me)
	}
	if err == nil {
		p.mu.Lock()
		h.layoutHolder, h.layoutHead, h.layoutRemote = me, st, inner
		p.mu.Unlock()
	}
	if err != nil {
		return nil, fmt.Errorf("durable: chunk remote for %s: %w", volumeID, err)
	}
	bound := &closeBoundRemote{ChunkRemote: inner, done: p.closeCtx}
	if pace, ok := p.cfg.ChunkBudget.(interface {
		WaitUpload(context.Context, int) error
	}); ok {
		bound.waitUpload = pace.WaitUpload
	}
	bound.setAttach(ctx) // the attach's calls also end with the caller's ctx
	defer bound.setAttach(nil)
	keyer, ok := inner.(storage.ChunkKeyer)
	if !ok {
		return nil, errors.New("durable: layout remote lacks content keys")
	}
	var remote storage.ChunkRemote = &layoutBoundRemote{closeBoundRemote: bound, keyer: keyer}
	cfg := storage.ChunkLayerConfig{
		OnPublishSettled:    p.publishSettledHook(volumeID),
		ChunkBlocks:         p.cfg.ChunkBlocks,
		RestoreChunkBytes:   uint64(ws.BlockSize()) * uint64(p.cfg.ChunkBlocks),
		HeadCoolingWindow:   p.cfg.ChunkHeadCoolingWindow,
		Epoch:               lin.epoch,
		Holder:              holder,
		FreshFromHead:       h.fresh,
		CreatedThisStart:    h.fresh && h.createdThisStart,
		ReadMissTimeout:     p.cfg.ChunkReadMissTimeout,
		HydrationWorkers:    p.cfg.ChunkHydrationWorkers,
		HydrationQueueDepth: p.cfg.ChunkHydrationQueueDepth,
		UploadConcurrency:   p.cfg.ChunkUploadConcurrency,
		PayloadMemoryBudget: p.cfg.ChunkPayloadMemoryBudget,
		Budget:              p.cfg.ChunkBudget,
		COWSpillLimit:       p.cfg.ChunkCOWSpillLimit,
		COWSpillFree:        spillFree(p.cfg.StorageRoot),
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
	if p.cfg.ChunkLayoutHeadFor != nil {
		// The kernel already claimed and validated this root. Seed only a fresh
		// root; an existing root keeps its acknowledged WAL and synced position.
		path, err := canStartFrom(ws, h.fresh, layoutHead)
		if err != nil {
			return nil, err
		}
		switch path {
		case startAttachBase:
			cfg.FreshFromHead = false
		case startSeed:
			if err := h.backend.seedAndRebase(func() error { return ws.SeedChunkFrontier(layoutHead.Position) }); err != nil {
				return nil, err
			}
		}
	}
	layer, err := storage.AttachChunkLayer(ws, remote, cfg)
	if err == nil {
		h.chunkLineage = lin
	}
	return layer, err
}

// closeBoundRemote ends every remote call when the provider closes, so a
// shutdown never waits on the remote for an attach or a cut. While an
// attach runs, its calls also end with the attach caller's context.
type closeBoundRemote struct {
	storage.ChunkRemote
	done       context.Context
	waitUpload func(context.Context, int) error

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
	if r.waitUpload != nil {
		if err := r.waitUpload(c, len(data)); err != nil {
			return fmt.Errorf("hostlimit: upload pacing before put %s: %w", key, err)
		}
	}
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

func (r *closeBoundRemote) GetHeadRoot(ctx context.Context) (storage.ChunkHead, error) {
	c, end := r.bind(ctx)
	defer end()
	if reader, ok := r.ChunkRemote.(storage.ChunkHeadRootReader); ok {
		return reader.GetHeadRoot(c)
	}
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

func (r *closeBoundRemote) Begin(ctx context.Context) (string, error) {
	c, end := r.bind(ctx)
	defer end()
	return r.ChunkRemote.Begin(c)
}

// Abort never blocks; it is passed through even after Close (a cut left
// open is rolled back at the next HEAD read).
func (r *closeBoundRemote) Abort() { r.ChunkRemote.Abort() }

// Cut scheduler constants (B9 W3 ruling).
const (
	chunkSchedulePoll  = 100 * time.Millisecond
	chunkCutMinSpacing = time.Second
	chunkCutBackoffMin = time.Second
	chunkCutBackoffMax = 30 * time.Second
	chunkSlotDelayMax  = 200 * time.Millisecond
)

// cutPolicy decides when an automatic cut is due. The remote holds
// snapshots, not live data: cuts are published at Snapshot, Detach/stop
// and explicit Cut, and automatically by the configured timer:
//   - the timer fires once interval has passed since the last successful
//     cut of any kind (explicit Cut, Snapshot and Detach included); zero
//     disables it, and with it every automatic cut;
//   - the dirty-chunk count never triggers a cut;
//   - automatic cuts are at least minSpacing apart (explicit ones are not
//     subject to it);
//   - a failed automatic cut resets nothing and is retried after a backoff
//     doubling from backoffMin to backoffMax; any later successful cut (a
//     no-op cut and an explicit cut included) clears the backoff and
//     restarts the timer;
//   - an automatic cut refused because every host slot is held ran
//     nothing: it is retried after a random delay drawn from a range that
//     shrinks as the volume falls behind its interval (a late volume is
//     likely, not certain, to retry first), with no backoff and no
//     minimum spacing, and the wait lasts until a cut succeeds.
type cutPolicy struct {
	interval, minSpacing, backoffMin, backoffMax time.Duration
	// slotDelay is the delay before retrying a cut refused for held host
	// slots, given how late the volume is past its interval; nil retries
	// at the next poll.
	slotDelay func(late time.Duration) time.Duration
}

// newSlotDelay draws a retry delay below chunkSlotDelayMax scaled by
// interval/(interval+late): a volume one interval late draws below half of
// it, nine intervals late below a tenth.
func newSlotDelay(interval time.Duration, r *rand.Rand) func(time.Duration) time.Duration {
	return func(late time.Duration) time.Duration {
		limit := float64(chunkSlotDelayMax)
		if interval > 0 && late > 0 {
			limit *= float64(interval) / float64(interval+late)
		}
		return time.Duration(r.Float64() * limit)
	}
}

// cutSchedule runs on in-process monotonic time: every time in it is a
// time.Now() reading of this process (or derived from one), never the
// layer's persisted LastCutNs, so wall-clock steps move nothing and a
// restart starts a fresh schedule. The layer's CutSeq identifies the
// latest cut: a value not seen before is a new successful cut.
type cutSchedule struct {
	lastSuccess   time.Time // last successful cut of any kind, as observed here
	lastAuto      time.Time // start of the last automatic cut
	failedAt      time.Time // end of the last failed automatic cut
	retryAt       time.Time // a failed automatic cut's backoff ends here
	backoff       time.Duration
	firstDue      time.Time // startup phase only; never a fabricated success
	seenSeq       uint64    // the layer's CutSeq last taken in
	failedSeenSeq uint64    // the layer's CutSeq when the last automatic cut failed
	slotWaitSince time.Time // first refusal for held host slots since the last success; zero when not waiting
	slotWaits     uint64    // automatic cuts refused for held host slots
}

// observe takes in, at now, the latest cut the layer reports (any
// trigger): a CutSeq not seen before is a success at now. A cut other
// than the one the last failure saw makes that failure's backoff obsolete.
func (s cutSchedule) observe(seq uint64, now time.Time) cutSchedule {
	if seq == 0 || seq == s.seenSeq {
		return s
	}
	s.seenSeq = seq
	if now.After(s.lastSuccess) {
		s.lastSuccess = now
	}
	s.firstDue = time.Time{}
	if !s.slotWaitSince.IsZero() {
		s.retryAt = time.Time{} // the slot retry is moot: a cut succeeded
	}
	s.slotWaitSince = time.Time{}
	if !s.failedAt.IsZero() && seq != s.failedSeenSeq {
		s.backoff, s.retryAt, s.failedAt = 0, time.Time{}, time.Time{}
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
	deadline := s.lastSuccess.Add(pol.interval)
	if !s.firstDue.IsZero() {
		deadline = s.firstDue
	}
	if pol.interval > 0 && !now.Before(deadline) {
		return "periodic", true
	}
	return "", false
}

// after records the outcome of an automatic cut started at start, with
// the layer's CutSeq read after it.
func (pol cutPolicy) after(s cutSchedule, start, end time.Time, err error, seq uint64) cutSchedule {
	if errors.Is(err, hostlimit.ErrSlotsHeld) {
		s.slotWaits++
		if s.slotWaitSince.IsZero() {
			s.slotWaitSince = start
		}
		var delay time.Duration
		if pol.slotDelay != nil {
			delay = pol.slotDelay(max(0, end.Sub(s.lastSuccess.Add(pol.interval))))
		}
		s.retryAt = end.Add(delay)
		return s
	}
	s.lastAuto = start
	if err == nil {
		s.seenSeq = seq
		s.firstDue = time.Time{}
		s.slotWaitSince = time.Time{}
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
	s.failedAt, s.retryAt, s.failedSeenSeq = end, end.Add(s.backoff), seq
	return s
}

// initialCutSchedule starts a fresh schedule at now; seq is the layer's
// CutSeq at start, so a cut already made is not taken in as new.
func initialCutSchedule(volumeID string, interval time.Duration, now time.Time, stagger bool, seq uint64) cutSchedule {
	s := cutSchedule{lastSuccess: now, seenSeq: seq}
	if stagger && interval > 0 {
		phase := hostlimit.Phase(volumeID, interval)
		startup := hostlimit.Phase(volumeID+":startup", min(interval, 10*time.Second))
		s.firstDue = now.Add(max(phase, startup))
	}
	return s
}

// chunkLoop runs the volume's automatic cuts under cutPolicy. A refused
// cut (not a healthy primary of this lineage, BASE install) is expected
// and logged only when it is something else. A fenced layer can never
// publish again: the loop ends.
func (p *DurableProvider) chunkLoop(ctx context.Context, volumeID string, layer storage.ChunkLayer, done chan struct{}, wait *cutWaitSlot) {
	defer close(done)
	pol := cutPolicy{
		interval:   p.cfg.ChunkCaptureInterval,
		minSpacing: chunkCutMinSpacing,
		backoffMin: chunkCutBackoffMin,
		backoffMax: chunkCutBackoffMax,
		slotDelay:  newSlotDelay(p.cfg.ChunkCaptureInterval, rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(hostlimit.Phase(volumeID, time.Hour))))),
	}
	s := initialCutSchedule(volumeID, pol.interval, time.Now(), p.cfg.ChunkStagger, layer.Stats().CutSeq)
	if wait != nil {
		s.slotWaits = wait.status(0).HostSlotWaits
	}
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
		start := time.Now()
		s = s.observe(ls.CutSeq, start)
		wait.record(s)
		reason, ok := pol.due(s, start, ls.Reconciling)
		if !ok {
			continue
		}
		cutCtx, cancel := p.chunkCutContext(ctx)
		trace := p.testChunkLoopTrace
		if trace != nil {
			trace("due", ls.LastCutNs, false)
		}
		var err error
		if pc, ok := layer.(storage.ChunkPeriodicCutter); ok {
			// A cut that completed after this decision (an explicit one
			// running meanwhile) restarted the timer: no cut now.
			var skipped bool
			_, skipped, err = pc.CutUnlessCutSince(cutCtx, ls.CutSeq)
			if trace != nil {
				trace("cut", ls.LastCutNs, skipped)
			}
			if skipped {
				cancel()
				continue
			}
		} else {
			_, err = layer.Cut(cutCtx)
			if trace != nil {
				trace("cut", ls.LastCutNs, false)
			}
		}
		cancel()
		s = pol.after(s, start, time.Now(), err, layer.Stats().CutSeq)
		wait.record(s)
		if err != nil && !errors.Is(err, storage.ErrChunkCaptureRefused) && ctx.Err() == nil {
			if note, stuck := capturePendingNote(err); stuck {
				log.Printf("durable: %s chunk cut for volume=%s REFUSED: %s (retry in %s)", reason, volumeID, note, s.backoff)
			} else {
				log.Printf("durable: %s chunk cut for volume=%s: %v (retry in %s)", reason, volumeID, err, s.backoff)
			}
		}
	}
}

// chunkBackground owns both workers and their cancellation. A fence is observed
// even while either worker is waiting on a cooperative remote call. INV-MAINT-JOIN:
// done closes only after every admitted worker has returned.
func (p *DurableProvider) chunkBackground(ctx context.Context, cancel context.CancelFunc, volumeID string, layer storage.ChunkLayer, done chan struct{}, current func() bool, snapshots *snapshotSlot, cutWait *cutWaitSlot, kick <-chan struct{}) {
	defer close(done)
	defer cancel()
	var workers sync.WaitGroup
	if p.cfg.ChunkCaptureInterval > 0 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			cutDone := make(chan struct{})
			p.chunkLoop(ctx, volumeID, layer, cutDone, cutWait)
		}()
	}
	if p.cfg.ChunkMaintenanceInterval > 0 && p.cfg.ChunkMaintenance != nil {
		workers.Add(1)
		go func() { defer workers.Done(); p.chunkMaintenanceLoop(ctx, volumeID, layer, current, kick) }()
	}
	if snapshots != nil {
		workers.Add(1)
		go func() { defer workers.Done(); p.chunkSnapshotLoop(ctx, volumeID, layer, current, snapshots) }()
	}
	timer := time.NewTicker(chunkSchedulePoll)
	defer timer.Stop()
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-timer.C:
			if layer.Fenced() {
				cancel()
			}
		}
	}
	workers.Wait()
}

// maintenanceCurrent binds the pass to the attached handle AND its lineage.
// The projection can remain healthy while a different owner replaces it.
func (p *DurableProvider) maintenanceCurrent(volumeID string, h *volHandle, layer storage.ChunkLayer, lin chunkLineage) bool {
	p.mu.Lock()
	active := !p.closed && p.volumes[volumeID] == h && h.chunk == layer
	p.mu.Unlock()
	cur := p.view.Projection()
	return active && chunkAuthority(cur, volumeID) && cur.VolumeID == lin.volumeID && cur.ReplicaID == lin.replicaID && cur.Epoch == lin.epoch && cur.EndpointVersion == lin.endpoint
}

// maintenanceKickRetry is how soon a kicked pass that found the volume busy
// (a cut session open: the cleaning never runs during one) is tried again.
var maintenanceKickRetry = 30 * time.Second // a var only so tests can shorten it

// KickMaintenance asks the volume's maintenance loop for one pass now, as
// after a snapshot delete. It reports whether a running loop was there to
// ask (a stopped or ended loop is not); kicks coalesce, and the pass is the
// same bounded pass the timer runs.
func (p *DurableProvider) KickMaintenance(volumeID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	h := p.volumes[volumeID]
	if h == nil || h.maintKick == nil || h.chunkStop == nil || h.chunkDone == nil {
		return false
	}
	select {
	case <-h.chunkDone:
		return false // the loop ended (fence, lineage change) and was not re-armed
	default:
	}
	select {
	case h.maintKick <- struct{}{}:
	default: // a pass is already asked for
	}
	return true
}

func (p *DurableProvider) chunkMaintenanceLoop(ctx context.Context, volumeID string, layer storage.ChunkLayer, current func() bool, kick <-chan struct{}) {
	interval := p.cfg.ChunkMaintenanceInterval
	due := time.Now().Add(interval)
	if p.cfg.ChunkStagger {
		due = time.Now().Add(hostlimit.Phase(volumeID+":maintenance", interval))
	}
	timer := time.NewTimer(time.Until(due))
	defer timer.Stop()
	kicked := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-kick:
			kicked = true
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		if ctx.Err() != nil || layer.Fenced() {
			return
		}
		if !current() {
			return
		}
		timeout := p.cfg.ChunkMaintenanceTimeout
		if timeout <= 0 {
			timeout = time.Minute
		}
		err := boundedPass(ctx, layer, current, timeout, func(passCtx context.Context) error {
			return p.cfg.ChunkMaintenance(passCtx, volumeID)
		})
		if err != nil && ctx.Err() == nil {
			log.Printf("durable: maintenance volume=%s: %v", volumeID, err)
		}
		// The holder's cleaning may be the first call to see HEAD move away
		// (an idle volume runs no cut): its refusal latches the layer too.
		if obs, ok := layer.(interface{ ObserveRemoteError(error) error }); ok && err != nil {
			_ = obs.ObserveRemoteError(err)
		}
		// Completion-relative reset coalesces all elapsed ticks; no catch-up queue.
		// A kick stays pending until a pass the volume did not refuse as busy:
		// while a session is open, every retry comes after the short delay,
		// not an interval later. The delay only paces the retries; whether a
		// pass may clean is decided by HEAD's activity, never by this timer.
		next := interval
		if kicked && errors.Is(err, layout.ErrBusy) {
			if maintenanceKickRetry < next {
				next = maintenanceKickRetry
			}
		} else {
			kicked = false
		}
		timer.Reset(next)
	}
}

// boundedPass runs fn with a context that ends after timeout, or as soon
// as the layer is fenced or the attachment is no longer current. The
// projection has no change channel: one bounded watcher belongs to the
// pass, never to an individual tick, and is joined before return.
func boundedPass(ctx context.Context, layer storage.ChunkLayer, current func() bool, timeout time.Duration, fn func(context.Context) error) error {
	passCtx, cancel := context.WithTimeout(ctx, timeout)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		tick := time.NewTicker(chunkSchedulePoll)
		defer tick.Stop()
		for {
			select {
			case <-passCtx.Done():
				return
			case <-tick.C:
				if layer.Fenced() || !current() {
					cancel()
					return
				}
			}
		}
	}()
	err := fn(passCtx)
	cancel()
	<-watchDone
	return err
}

func (p *DurableProvider) stopChunkLoop(h *volHandle) {
	p.mu.Lock()
	stop, done := h.chunkStop, h.chunkDone
	p.mu.Unlock()
	if stop != nil {
		stop()
	}
	// Every concurrent stopper joins the same lifetime; none can clear ownership
	// while an earlier stopper is still waiting for an admitted remote call.
	if done != nil {
		<-done
	}
	p.mu.Lock()
	if h.chunkDone == done {
		h.chunkStop = nil
		h.chunkDone = nil
	}
	p.mu.Unlock()
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
	ctx, cancel := p.chunkCutContext(ctx)
	defer cancel()
	layer, err := p.chunkLayer(volumeID)
	if err != nil {
		return 0, err
	}
	return layer.Capture(ctx)
}

// MakeVolumeRemoteDurable returns nil only once volumeID's writes up to lsn
// are published in the remote HEAD (snapshot, detach, migrate).
func (p *DurableProvider) MakeVolumeRemoteDurable(ctx context.Context, volumeID string, lsn uint64) error {
	ctx, cancel := p.chunkCutContext(ctx)
	defer cancel()
	layer, err := p.chunkLayer(volumeID)
	if err != nil {
		return err
	}
	return layer.MakeRemoteDurable(hostlimit.WithClass(ctx, hostlimit.Urgent), lsn)
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
func (p *DurableProvider) chunkCutContext(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := p.cfg.ChunkCutTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	return context.WithTimeout(ctx, timeout)
}

func (p *DurableProvider) CutVolume(ctx context.Context, volumeID string, minLSN uint64) (storage.ChunkCutResult, error) {
	ctx, cancel := p.chunkCutContext(ctx)
	defer cancel()
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
	if minLSN > 0 {
		ctx = hostlimit.WithClass(ctx, hostlimit.Urgent)
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
// volume's frontend I/O and waits for the I/O already admitted to finish,
// stops the periodic cuts, then publishes one cut with HEAD state detached.
// It also returns the last LSN written. The freeze (drain + final cut) is
// bounded by DetachFreezeTimeout and ctx. After a successful detach the
// volume stays stopped for this process, through any later RecoverVolume,
// and a repeated call returns the first result. A failed detach never leaves
// the guest frozen by a detach that did not happen (MIGRATION E4):
//   - nothing published (the drain or the cut failed before its CAS, or the
//     CAS was refused, or an unresolved detach CAS was cancelled): serving
//     resumes and ErrDetachAborted is returned;
//   - the detach CAS applied (its reply was lost): the detached result;
//   - HEAD belongs to another holder: stays stopped (fenced);
//   - undecidable (HEAD unreadable): stays stopped, ErrDetachOutcomeUnknown.
func (p *DurableProvider) DetachVolume(ctx context.Context, volumeID string) (storage.ChunkCutResult, uint64, error) {
	return p.detachVolume(ctx, volumeID, false)
}

// DetachVolumeFrozen uses one budgeted cut when the guest is already frozen.
func (p *DurableProvider) DetachVolumeFrozen(ctx context.Context, volumeID string, opts storage.FrozenDetachOptions) (storage.ChunkCutResult, uint64, error) {
	ctx, err := storage.WithFrozenDetach(ctx, opts)
	if err != nil {
		return storage.ChunkCutResult{}, 0, err
	}
	return p.detachVolume(ctx, volumeID, false)
}

// ErrPlainDetachBusy refuses a plain detach without closing frontend admission.
var ErrPlainDetachBusy = errors.New("durable: plain detach requires idle frontend I/O")

// DetachVolumePlain bypasses pre-copy cost admission only after atomically
// closing an idle frontend (including a closed handle). M5 PlainStop -> Quiesce uses
// only the caller context, not the live-serving freeze timer.
func (p *DurableProvider) DetachVolumePlain(ctx context.Context, volumeID string) (storage.ChunkCutResult, uint64, error) {
	result, last, err := p.detachVolume(ctx, volumeID, true)
	if err != nil && !errors.Is(err, ErrNotChunkVolume) && !errors.Is(err, ErrChunkAwaitingAuthority) {
		return result, last, err
	}
	// INV-WS-SEAL-REPLAY-001: success is always a fresh remote proof, also
	// after restart when no layer exists, including deferred authority startup.
	// A cached result is not sufficient.
	return p.replayPlainSeal(ctx, volumeID, result)
}

func (p *DurableProvider) detachVolume(ctx context.Context, volumeID string, plain bool) (storage.ChunkCutResult, uint64, error) {
	p.mu.Lock()
	h, ok := p.volumes[volumeID]
	p.mu.Unlock()
	if !ok {
		return storage.ChunkCutResult{}, 0, ErrVolumeNotOpen
	}
	h.attachMu.Lock()
	defer h.attachMu.Unlock()
	if h.settling {
		return storage.ChunkCutResult{}, 0, ErrDetachOutcomeUnknown
	}
	layer, err := p.chunkLayer(volumeID)
	if err != nil {
		return storage.ChunkCutResult{}, 0, err
	}
	if plain {
		if err := ctx.Err(); err != nil {
			return storage.ChunkCutResult{}, 0, err
		}
	}
	guestFreeze, guestFrozen := storage.FrozenDetachFromContext(ctx)
	precopy := (p.cfg.ChunkDetachPrecopy || guestFrozen) && !plain
	freeze := p.cfg.DetachFreezeTimeout
	if freeze <= 0 {
		freeze = defaultDetachFreezeTimeout
	}
	if guestFrozen && !guestFreeze.AllowOverBudget {
		freeze = detachFreezeTarget
	} else if precopy && !guestFrozen {
		// Admission's five-second ceiling is also the physical final-freeze
		// deadline; the ordinary detach timeout may be longer (20s by default).
		freeze = min(freeze, detachFreezeTarget)
	}
	// One freeze budget covers the drain, the final cut AND the settlement:
	// the cut gets three quarters, the settlement what is left. The bound is
	// cooperative (contexts): a syscall that never returns is not bounded here.
	var start time.Time
	// stopIOBegan is the backend's flip (admission closed, under its mu: an
	// I/O whose gate runs after it is refused; one gated before it is
	// admitted even if it registers later), stopIOReturned when the admitted
	// I/O has drained; both are logged so a measurement can tell a refusal
	// after the flip from an admitted I/O that failed.
	var stopIOBegan, stopIOReturned time.Time
	defer func() {
		if !stopIOReturned.IsZero() {
			// Emit after the cut releases its barrier, using the actual times.
			if !stopIOBegan.IsZero() {
				log.Printf("durable: volume=%s admission closed at %s", volumeID, stopIOBegan.UTC().Format(time.RFC3339Nano))
			}
			log.Printf("durable: volume=%s StopIO returned at %s", volumeID, stopIOReturned.UTC().Format(time.RFC3339Nano))
		}
	}()
	accountCtx, countCAS := storage.WithChunkCASAccounting(ctx)
	var casAtFreeze uint64
	cutCtx, cancel := context.WithCancelCause(accountCtx)
	defer cancel(context.Canceled)
	var timer *time.Timer
	var settleDeadline time.Time
	beginFreeze := func() {
		if timer != nil {
			return
		}
		start = time.Now()
		if guestFrozen {
			start = guestFreeze.StartedAt
		}
		casAtFreeze = countCAS()
		if plain { // M5 PlainStop -> Quiesce: no live frontend, caller owns the bound.
			settleDeadline, _ = ctx.Deadline()
			return
		}
		operationStart := start
		if guestFrozen && guestFreeze.AllowOverBudget {
			operationStart = time.Now()
		}
		settleDeadline = operationStart.Add(freeze)
		remaining := max(time.Duration(0), time.Until(settleDeadline))
		// An already exhausted normal guest budget must reach the no-I/O
		// forecast refusal, rather than racing a zero-delay cancellation.
		if remaining > 0 {
			timer = time.AfterFunc(remaining*3/4, func() { cancel(context.DeadlineExceeded) })
		}
	}
	defer func() {
		if timer != nil {
			timer.Stop()
		}
		if !plain && !start.IsZero() {
			if d := time.Since(start); d > detachFreezeTarget {
				log.Printf("durable: ALARM detach of volume=%s held guest I/O for %s (target %s, bound %s)", volumeID, d.Round(time.Millisecond), detachFreezeTarget, freeze)
			}
		}
	}()
	var resumed func()
	notifyDetach := func() {
		if hook := p.cfg.OnChunkDetach; hook != nil {
			attempt := hook(volumeID)
			if done := p.cfg.OnChunkDetachResumed; done != nil {
				resumed = func() { done(volumeID, attempt) }
			}
		}
	}
	var r storage.ChunkCutResult
	if guestFrozen {
		beginFreeze()
	}
	if precopy {
		owner, ok := layer.(interface {
			PrecopyDetachWithAdmission(context.Context, func(context.Context) (func(), error), func() bool) (storage.ChunkCutResult, error)
		})
		if !ok {
			return r, 0, fmt.Errorf("%w: layer lacks pre-copy", storage.ErrPrecopyUnknown)
		}
		// Notify outside captureMu: the host callback may take its own state lock.
		notifyDetach()
		// Cancel/join the periodic owner before any final freeze, while serving.
		p.stopChunkLoop(h)
		r, err = owner.PrecopyDetachWithAdmission(hostlimit.WithClass(cutCtx, hostlimit.Urgent), h.backend.BeginPrecopyPause, func() bool {
			if !h.backend.TryStopIO("chunk pre-copy: final detached cut") {
				return false
			}
			stopIOReturned = time.Now()
			stopIOBegan = h.backend.StopFlipAt()
			h.chunkDetach = true
			beginFreeze()
			if trace := p.testDetachTrace; trace != nil {
				trace("cut", settleDeadline.Add(-freeze/4))
			}
			return true
		})
		// Log only after capture releases its barrier; at most five records.
		for _, a := range r.Precopy.Rounds {
			p := a.FixedParts
			log.Printf("durable: pre-copy fixed spans volume=%s attempt=%d candidate_ms=%.3f window_setup_ms=%.3f publish_prepare_ms=%.3f manifest_ms=%.3f commit_state_ms=%.3f non_payload_copy_ms=%.3f barrier_overage_ms=%.3f live_drain_ms=%.3f live_capture_lock_ms=%.3f live_sync_ms=%.3f live_candidate_ms=%.3f live_other_ms=%.3f live_sync_lifecycle_wait_ms=%.3f live_sync_store_wait_ms=%.3f live_sync_group_commit_ms=%.3f live_sync_completion_wait_ms=%.3f", volumeID, a.Attempt, float64(p.Candidate)/float64(time.Millisecond), float64(p.WindowSetup)/float64(time.Millisecond), float64(p.PublishPrepare)/float64(time.Millisecond), float64(p.Manifest)/float64(time.Millisecond), float64(p.CommitState)/float64(time.Millisecond), float64(p.NonPayloadCopy)/float64(time.Millisecond), float64(a.BarrierOverage)/float64(time.Millisecond), float64(a.BarrierParts.Drain)/float64(time.Millisecond), float64(a.BarrierParts.StoreLock)/float64(time.Millisecond), float64(a.BarrierParts.DataSync)/float64(time.Millisecond), float64(a.BarrierParts.Candidate)/float64(time.Millisecond), float64(a.BarrierParts.Other)/float64(time.Millisecond), float64(a.BarrierParts.SyncPhases.LifecycleWaitNs)/float64(time.Millisecond), float64(a.BarrierParts.SyncPhases.StoreWaitNs)/float64(time.Millisecond), float64(a.BarrierParts.SyncPhases.GroupCommitNs)/float64(time.Millisecond), float64(a.BarrierParts.SyncPhases.CompletionWaitNs)/float64(time.Millisecond))
			log.Printf("durable: pre-copy attempt volume=%s attempt=%d drain_ms=%.3f barrier_ms=%.3f pause_ms=%.3f reason=%s tail=%d rate_samples=%d estimate_ms=%.3f frozen=%t fixed_ms=%.3f drain_cost_ms=%.3f store_lock_ms=%.3f local_sync_ms=%.3f head_cas_ms=%.3f readback_ms=%.3f map_persist_ms=%.3f other_fixed_ms=%.3f tail_ms=%.3f declared_freeze_ms=%.3f err=%s", volumeID, a.Attempt, float64(a.Drain)/float64(time.Millisecond), float64(a.Barrier)/float64(time.Millisecond), float64(a.Pause)/float64(time.Millisecond), a.Reason, a.TailBytes, a.RateSamples, float64(a.Estimate)/float64(time.Millisecond), a.Frozen, float64(a.Fixed)/float64(time.Millisecond), float64(a.FixedParts.Drain)/float64(time.Millisecond), float64(a.FixedParts.StoreLock)/float64(time.Millisecond), float64(a.FixedParts.DataSync)/float64(time.Millisecond), float64(a.FixedParts.HeadCAS)/float64(time.Millisecond), float64(a.FixedParts.HeadReadback)/float64(time.Millisecond), float64(a.FixedParts.MapPersist)/float64(time.Millisecond), float64(a.FixedParts.Other)/float64(time.Millisecond), float64(a.TailEstimate)/float64(time.Millisecond), float64(a.DeclaredFreeze)/float64(time.Millisecond), a.Error)
		}
		log.Printf("durable: pre-copy volume=%s attempts=%d measurement_rounds=%d samples_at_start=%d tail=%d rate=%.0f sample_age=%s fixed=%s estimate=%s frozen=%t reason=%s err=%v", volumeID, r.Precopy.Attempts, r.Precopy.MeasurementRounds, r.Precopy.SamplesAtStart, r.Precopy.TailBytes, r.Precopy.BytesPerSecond, r.Precopy.SampleAge, r.Precopy.Fixed, r.Precopy.Estimate, r.Precopy.Frozen, r.Precopy.Reason, err)
		if err == nil {
			if !start.IsZero() && !r.Precopy.IsReplay() {
				elapsed := time.Since(start)
				if !guestFrozen {
					elapsed += r.Precopy.BarrierBeforeFreeze
				}
				r.Precopy.Finish(elapsed, 0, false)
				if recorder, ok := layer.(interface{ RememberDetachReport(storage.PrecopyReport) }); ok {
					recorder.RememberDetachReport(r.Precopy)
				}
			}
			return r, h.storage.NextLSN() - 1, nil
		}
		if !r.Precopy.Frozen {
			p.mu.Lock()
			active := !p.closed && h.chunk == layer
			if active {
				p.startChunkLoop(volumeID, h, layer)
			}
			p.mu.Unlock()
			// No final freeze occurred: clear only this still-serving attempt's
			// fast-poll token. A prior stopped/unknown detach must remain pending.
			if active && !h.chunkDetach && h.backend.operational.Load() && !layer.Fenced() && resumed != nil {
				resumed()
			}
			return r, 0, err
		}
	} else {
		beginFreeze()
		if plain {
			if err := h.backend.stopPlainDetachIfIdle(); err != nil {
				return r, 0, err
			}
		}
		if trace := p.testDetachTrace; trace != nil {
			deadline := start.Add(freeze * 3 / 4)
			if plain {
				deadline, _ = cutCtx.Deadline()
			}
			trace("cut", deadline)
		}
		h.chunkDetach = true
		notifyDetach()
		if stop := p.testDetachStopIO; stop != nil {
			err = stop(cutCtx)
		} else {
			err = h.backend.StopIO(cutCtx, "chunk detach: I/O stopped until a new attach")
		}
		if err == nil {
			stopIOReturned = time.Now()
			stopIOBegan = h.backend.StopFlipAt()
		}
		if err == nil {
			p.stopChunkLoop(h)
			if r, err = layer.Detach(hostlimit.WithClass(cutCtx, hostlimit.Urgent)); err == nil {
				return r, h.storage.NextLSN() - 1, nil
			}
		}
	}
	err = errors.Join(err, context.Cause(cutCtx))
	// Every failure, a failed drain included, is settled before anything
	// resumes: an earlier detach that applied, an unresolved detach CAS or a
	// fenced layer must never resume serving. The settlement keeps the rest
	// of the freeze budget even when the caller's context is gone.
	sctx, scancel := context.WithCancel(cutCtx)
	if !plain { // Live detach retains its reserved settlement budget.
		scancel()
		sctx, scancel = context.WithDeadline(context.WithoutCancel(cutCtx), settleDeadline)
	}
	defer scancel()
	if trace := p.testDetachTrace; trace != nil {
		dl, _ := sctx.Deadline()
		trace("settle", dl)
	}
	var how storage.DetachSettlement
	var dr storage.ChunkCutResult
	var serr error
	if h.layoutHead != nil {
		if remote, ok := h.layoutRemote.(layoutStopRemote); ok {
			how, dr, serr = settleLayoutStop(sctx, remote, h.layoutHolder)
		} else {
			serr = errors.New("durable: layout remote cannot settle its open stop session")
		}
	} else {
		how, dr, serr = layer.SettleDetach(sctx)
	}

	if precopy {
		attempts := countCAS() - casAtFreeze
		extra := uint64(0)
		if attempts > 0 {
			extra = attempts - 1
		}
		elapsed := time.Since(start)
		if !guestFrozen {
			elapsed += r.Precopy.BarrierBeforeFreeze
		}
		r.Precopy.Finish(elapsed, extra, true)
		log.Printf("durable: pre-copy settlement volume=%s elapsed=%s extra_cas=%d declared_worst=%s latency_gate_failed=%t outcome=%v err=%v", volumeID, r.Precopy.FreezeElapsed, extra, r.Precopy.DeclaredWorst, r.Precopy.LatencyGateFailed, how, serr)
		dr.Precopy = r.Precopy
		if recorder, ok := layer.(interface{ RememberDetachReport(storage.PrecopyReport) }); ok {
			recorder.RememberDetachReport(r.Precopy)
		}
	}
	switch {
	case serr != nil:
		h.backend.SetOperational(false, "chunk detach settlement waiting for filer: "+serr.Error())
		p.retryUnknownDetach(volumeID, h, layer, resumed)
		return storage.ChunkCutResult{}, 0, fmt.Errorf("%w: %w (settle: %w)", ErrDetachOutcomeUnknown, err, serr)
	case how == storage.DetachApplied:
		return dr, h.storage.NextLSN() - 1, nil
	case how == storage.DetachFenced:
		return storage.ChunkCutResult{}, 0, fmt.Errorf("durable: detach of %s: HEAD belongs to a newer epoch or another holder; I/O stays stopped: %w: %w", volumeID, storage.ErrChunkLayerFenced, err)
	default:
		return storage.ChunkCutResult{}, 0, p.resumeAfterDetach(volumeID, h, layer, err, resumed)
	}
}

// retryUnknownDetach owns one stopped volume until the filer can decide the
// outcome. It never infers an abort from time passing: only the existing
// conditional abort and HEAD read may allow serving again.
func (p *DurableProvider) retryUnknownDetach(volumeID string, h *volHandle, layer storage.ChunkLayer, resumed func()) {
	if h.settling { // attachMu is held by detachVolume
		return
	}
	p.mu.Lock()
	if p.closed || h.chunk != layer {
		p.mu.Unlock()
		return
	}
	h.settling = true
	p.inflight.Add(1) // under p.mu, before Close can join
	p.mu.Unlock()
	go func() {
		defer p.inflight.Done()
		defer func() {
			h.attachMu.Lock()
			h.settling = false
			h.attachMu.Unlock()
		}()
		backoff := 100 * time.Millisecond
		for {
			select {
			case <-p.closeCtx.Done():
				return
			case <-time.After(backoff):
			}
			h.attachMu.Lock()
			p.mu.Lock()
			active := !p.closed && h.chunk == layer && h.chunkDetach
			p.mu.Unlock()
			if !active {
				h.attachMu.Unlock()
				return
			}
			ctx, cancel := context.WithTimeout(p.closeCtx, 2*time.Second)
			var how storage.DetachSettlement
			var err error
			if h.layoutHead != nil {
				if remote, ok := h.layoutRemote.(layoutStopRemote); ok {
					how, _, err = settleLayoutStop(ctx, remote, h.layoutHolder)
				} else {
					err = errors.New("durable: layout remote cannot settle its open stop session")
				}
			} else {
				how, _, err = layer.SettleDetach(ctx)
			}
			cancel()
			switch {
			case err == nil && how == storage.DetachApplied:
				log.Printf("durable: unknown stop of volume=%s resolved: detached", volumeID)
				h.attachMu.Unlock()
				return
			case err == nil && how == storage.DetachFenced:
				log.Printf("durable: unknown stop of volume=%s resolved: fenced", volumeID)
				h.attachMu.Unlock()
				return
			case err == nil && how == storage.DetachNotPublished:
				if e := p.resumeAfterDetach(volumeID, h, layer, ErrDetachOutcomeUnknown, resumed); e != nil {
					log.Printf("durable: unknown stop of volume=%s resolved: %v", volumeID, e)
				}
				h.attachMu.Unlock()
				return
			}
			if err != nil {
				h.backend.SetOperational(false, "chunk detach settlement waiting for filer: "+err.Error())
				log.Printf("durable: unknown stop of volume=%s waiting for filer: %v", volumeID, err)
			}
			h.attachMu.Unlock()
			backoff = min(backoff*2, 2*time.Second)
		}
	}()
}

const (
	defaultDetachFreezeTimeout = 20 * time.Second
	detachFreezeTarget         = 5 * time.Second
)

// resumeAfterDetach undoes a detach that provably did not happen: the
// periodic cuts restart, the stop is lifted, and the volume serves again.
// Only then is resumed (if set) called. The caller holds h.attachMu.
func (p *DurableProvider) resumeAfterDetach(volumeID string, h *volHandle, layer storage.ChunkLayer, cause error, resumed func()) error {
	p.mu.Lock()
	closed := p.closed || h.chunk != layer
	if !closed {
		p.startChunkLoop(volumeID, h, layer)
	}
	p.mu.Unlock()
	if closed { // a concurrent Close or detach of the layer owns the volume now
		return fmt.Errorf("durable: detach of %s aborted while the provider closed: %w", volumeID, cause)
	}
	h.chunkDetach = false
	h.backend.SetOperational(true, "chunk detach aborted: serving resumed")
	if resumed != nil {
		resumed()
	}
	log.Printf("durable: detach of volume=%s aborted, nothing published; serving resumed: %v", volumeID, cause)
	return fmt.Errorf("%w: %w", ErrDetachAborted, cause)
}

// publishSettledHook adapts ChunkOnPublishSettled to one volume's layer.
func (p *DurableProvider) publishSettledHook(volumeID string) func(context.Context, storage.ChunkHead, storage.ChunkHead) {
	hook := p.cfg.ChunkOnPublishSettled
	if hook == nil {
		return nil
	}
	return func(ctx context.Context, prev, next storage.ChunkHead) { hook(ctx, volumeID, prev, next) }
}

// cutWaitSlot publishes the automatic-cut timer's host slot waits to
// status. A nil slot records nothing. A wait is recorded with the layer's
// CutSeq it started under, so any later successful cut ends it in status,
// also one made after the timer stopped (a detach's seal).
type cutWaitSlot struct {
	mu      sync.Mutex
	st      CutScheduleStatus
	waitSeq uint64
}

func (w *cutWaitSlot) record(s cutSchedule) {
	if w == nil {
		return
	}
	st := CutScheduleStatus{HostSlotWaits: s.slotWaits}
	if !s.slotWaitSince.IsZero() {
		st.HostSlotWaitSinceNs = s.slotWaitSince.UnixNano()
	}
	w.mu.Lock()
	w.st, w.waitSeq = st, s.seenSeq
	w.mu.Unlock()
}

// status is the recorded state as of the layer's current CutSeq.
func (w *cutWaitSlot) status(cutSeq uint64) CutScheduleStatus {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := w.st
	if st.HostSlotWaitSinceNs != 0 && cutSeq != w.waitSeq {
		st.HostSlotWaitSinceNs = 0
	}
	return st
}
