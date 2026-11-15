package storage

// Chunk layer: the implementation of the seam in chunk_seam.go.
//
// Lock order (never reversed):
//
//	capMu -> WALStore.captureMu -> WALStore.lifecycleMu -> WALStore.mu
//	extentMu[i] -> stMu
//	WALStore.mu -> bmMu -> stMu (BeforeCheckpointAdvance runs under mu)
//
// stMu guards the layer's in-memory state and is never held across a
// WALStore call, a bitmap store call or a remote call.

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// chunkHooks is what the WALStore holds for an attached layer.
type chunkHooks struct {
	sink   chunkSink
	closer func() error
}

const defaultReadMissTimeout = 30 * time.Second

type chunkLayerImpl struct {
	s      *WALStore
	remote chunkRemote
	cfg    chunkLayerConfig

	blockSize   int
	chunkBlocks uint64
	numChunks   uint64
	nonce       string

	ctx    context.Context // cancelled by Close
	cancel context.CancelFunc
	wg     sync.WaitGroup // captures and fills in flight

	capMu  sync.Mutex // serializes Capture, Evict, Begin/EndBaseInstall
	bmMu   sync.Mutex // serializes bitmap store calls
	presMu sync.Mutex // serializes a whole presence save: snapshot, fsync, replace, version

	closeDone chan struct{} // closed when Close has finished

	stMu     sync.Mutex
	closed   bool
	head     chunkHead
	pending  *chunkHead // a proposal whose CAS outcome is unresolved
	fenced   bool
	detached *chunkCutResult // set by a successful Detach
	// pendingDetach says the unresolved proposal in pending is a Detach;
	// pendingCut is its cut result, installed as the detach result if the
	// re-read shows it applied.
	pendingDetach bool
	pendingCut    chunkCutResult
	// pendingBase is the version the unresolved CAS expected. While HEAD is
	// still at it, that CAS may still land, so a read alone never settles it.
	pendingBase chunkHeadVersion
	remoteLSN   uint64
	hdr         chunkBitmapHeader // last header written or loaded
	dirty       map[uint64]bool   // chunks dirtied since the cut in progress or the last HEAD
	inflight    map[uint64]bool   // chunks captured by a cut whose HEAD is not yet installed
	allDirty    bool              // a BASE block landed: every chunk is dirty
	hashMode    bool              // the dirty set is unknown: compare every chunk by hash
	cutLSN      uint64            // H of the latest cut taken (in flight or installed)
	keySeq      uint64

	// Test-only scheduling hooks; nil in production.
	testPresenceSnapshotHook func()                 // after a presence snapshot, before it is written
	testAfterAdmitHook       func()                 // right after a capture is admitted
	syncDirFn                func(dir string) error // directory fsync after a rename; syncDir in production

	// Absence: chunks whose local bytes are not authoritative (fresh host,
	// evicted), and blocks written locally inside such chunks since.
	absent        map[uint64]bool
	absentGen     map[uint64]uint64 // bumped on every eviction; binds a fill to its generation
	localBlocks   map[uint64]bool
	presenceVer   uint64
	savedPresVer  uint64
	dirtyVer      uint64
	savedDirtyVer uint64
}

func attachChunkLayerImpl(s *WALStore, remote chunkRemote, cfg chunkLayerConfig) (chunkLayer, error) {
	if remote == nil || cfg.Bitmap == nil || cfg.ChunkBlocks == 0 {
		return nil, errors.New("storage: chunk layer needs a remote, a bitmap store and ChunkBlocks > 0")
	}
	if cfg.ReadMissTimeout <= 0 {
		cfg.ReadMissTimeout = defaultReadMissTimeout
	}
	nb := uint64(s.NumBlocks())
	l := &chunkLayerImpl{
		s:           s,
		remote:      remote,
		cfg:         cfg,
		blockSize:   s.BlockSize(),
		chunkBlocks: uint64(cfg.ChunkBlocks),
		numChunks:   (nb + uint64(cfg.ChunkBlocks) - 1) / uint64(cfg.ChunkBlocks),
		dirty:       map[uint64]bool{},
		inflight:    map[uint64]bool{},
		absent:      map[uint64]bool{},
		absentGen:   map[uint64]uint64{},
		localBlocks: map[uint64]bool{},
		closeDone:   make(chan struct{}),
		syncDirFn:   syncDir,
	}
	var nb8 [8]byte
	if _, err := rand.Read(nb8[:]); err != nil {
		return nil, err
	}
	l.nonce = hex.EncodeToString(nb8[:])
	l.ctx, l.cancel = context.WithCancel(context.Background())

	head, err := remote.GetHead(l.ctx)
	if err != nil {
		l.cancel()
		return nil, fmt.Errorf("storage: chunk layer attach: read HEAD: %w", err)
	}
	l.head = copyHead(head)
	l.remoteLSN = head.RemoteLSN
	// A HEAD that already belongs to a newer epoch or a competing holder is
	// never used as this holder's publication guard.
	l.fenced = l.supersededBy(head)

	hdr, saved, ok, err := cfg.Bitmap.Load()
	if err != nil {
		l.cancel()
		return nil, fmt.Errorf("storage: chunk layer attach: load bitmap: %w", err)
	}
	switch {
	case ok && hdr.Base == chunkBaseInstalling:
		l.hdr = hdr // an interrupted BASE install: cuts stay refused
	case ok && hdr.Base == chunkBaseInstalledAwaitingCut:
		l.hdr, l.hashMode = hdr, true
	case ok && hdr.BaseLSN == head.RemoteLSN && hdr.HeadGen == head.Gen && hdr.Epoch == head.Epoch:
		l.hdr = hdr
		for _, idx := range saved {
			l.dirty[idx] = true
		}
	default:
		if ok {
			l.hdr = hdr
		}
		l.hashMode = true
	}

	if cfg.FreshFromHead {
		for idx := range head.Chunks {
			l.absent[idx] = true
			l.absentGen[idx]++
		}
		l.presenceVer++
	} else if err := l.loadPresence(); err != nil {
		l.cancel()
		return nil, err
	}
	hooks := &chunkHooks{sink: l, closer: l.Close}
	if !s.chunk.CompareAndSwap(nil, hooks) {
		l.cancel()
		return nil, errors.New("storage: a chunk layer is already attached")
	}
	if err := l.persistPresence(); err != nil {
		s.chunk.CompareAndSwap(hooks, nil)
		l.cancel()
		return nil, err
	}
	return l, nil
}

func copyHead(h chunkHead) chunkHead {
	out := h
	if h.DetachedBy != nil {
		d := *h.DetachedBy
		out.DetachedBy = &d
	}
	out.Chunks = make(map[uint64]chunkRef, len(h.Chunks))
	for k, v := range h.Chunks {
		out.Chunks[k] = v
	}
	return out
}

func (l *chunkLayerImpl) chunkOf(lba uint64) uint64 { return lba / l.chunkBlocks }

// chunkHasLocalLocked reports whether any block of an absent chunk was
// written locally since it became absent. Caller holds stMu.
func (l *chunkLayerImpl) chunkHasLocalLocked(idx uint64) bool {
	for i := uint64(0); i < l.chunkBlocks; i++ {
		if l.localBlocks[idx*l.chunkBlocks+i] {
			return true
		}
	}
	return false
}

// ---- sink callbacks ----

// OnExtentWrite is O(1): it only marks memory. It runs under extent or
// publication locks.
func (l *chunkLayerImpl) OnExtentWrite(lba uint64, lsn uint64) {
	l.stMu.Lock()
	defer l.stMu.Unlock()
	idx := l.chunkOf(lba)
	switch {
	case lsn == 0:
		l.allDirty = true
	case lsn > l.cutLSN:
		l.dirty[idx] = true
		// An entry at or below the latest cut is already in that cut's image
		// (an aborted cut returns its chunks to the dirty set).
	}
	if l.absent[idx] {
		l.localBlocks[lba] = true
		l.presenceVer++
	}
	l.dirtyVer++
}

// BeforeCheckpointAdvance runs inside persistCheckpoint and the direct
// frontier path, under WALStore.mu: it makes the dirty set (and any changed
// absence state) durable before the checkpoint can pass the entries.
func (l *chunkLayerImpl) BeforeCheckpointAdvance(lsn uint64) error {
	if err := l.saveDirty(); err != nil {
		return err
	}
	return l.persistPresence()
}

// saveDirty persists dirty ∪ inflight ∪ the chunks of WAL entries still in
// the dirty map (all chunks when a BASE block landed or the set is unknown).
func (l *chunkLayerImpl) saveDirty() error {
	l.bmMu.Lock()
	defer l.bmMu.Unlock()
	set := l.dirtySetLocked(nil)
	l.stMu.Lock()
	ver := l.dirtyVer
	if ver == l.savedDirtyVer && ver != 0 {
		l.stMu.Unlock()
		return nil
	}
	l.stMu.Unlock()
	if err := l.cfg.Bitmap.Save(sortedKeys(set)); err != nil {
		return err
	}
	l.stMu.Lock()
	if ver > l.savedDirtyVer {
		l.savedDirtyVer = ver
	}
	l.stMu.Unlock()
	return nil
}

// dirtySetLocked returns the chunk set the persisted bitmap must cover.
// Caller holds bmMu (or capMu); stMu is taken here.
func (l *chunkLayerImpl) dirtySetLocked(extra map[uint64]bool) map[uint64]bool {
	set := map[uint64]bool{}
	for _, e := range l.s.dm.snapshot() {
		set[l.chunkOf(e.LBA)] = true
	}
	l.stMu.Lock()
	defer l.stMu.Unlock()
	if l.allDirty || l.hashMode {
		for i := uint64(0); i < l.numChunks; i++ {
			set[i] = true
		}
	}
	for idx := range l.dirty {
		set[idx] = true
	}
	for idx := range l.inflight {
		set[idx] = true
	}
	for idx := range extra {
		set[idx] = true
	}
	return set
}

func sortedKeys(m map[uint64]bool) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// OnReadMiss fetches an absent block's chunk from HEAD and installs every
// block that has no newer local version, then lets the store read its
// extent. It runs with no walstore lock held.
func (l *chunkLayerImpl) OnReadMiss(lba uint64) ([]byte, error) {
	idx := l.chunkOf(lba)
	l.stMu.Lock()
	if l.closed || !l.absent[idx] || l.localBlocks[lba] {
		l.stMu.Unlock()
		return nil, nil
	}
	l.wg.Add(1)
	l.stMu.Unlock()
	defer l.wg.Done()
	// One deadline covers every attempt. Each attempt fetches the chunk as
	// the current HEAD names it; a fill that went stale (eviction generation
	// or HEAD ref changed meanwhile) installs nothing and the read retries,
	// so the extent is read only once the block is resolved: the chunk is no
	// longer absent, or the block has a local version.
	ctx, cancel := context.WithTimeout(l.ctx, l.cfg.ReadMissTimeout)
	defer cancel()
	for {
		l.stMu.Lock()
		if !l.absent[idx] || l.localBlocks[lba] {
			l.stMu.Unlock()
			return nil, nil
		}
		ref, ok := l.head.Chunks[idx]
		gen := l.absentGen[idx]
		l.stMu.Unlock()
		if !ok { // ZERO in the current HEAD
			return make([]byte, l.blockSize), nil
		}
		resolved, err := l.fillOnce(ctx, idx, gen, ref)
		if err != nil {
			return nil, err
		}
		if resolved {
			return nil, nil
		}
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("storage: chunk %d fetch: fill kept going stale: %w", idx, err)
		}
	}
}

// fillOnce fetches one chunk version and installs every block that has no
// newer local version, as long as the fill still belongs to the chunk's
// current absence (generation and HEAD ref unchanged). It reports whether
// it resolved the absence.
func (l *chunkLayerImpl) fillOnce(ctx context.Context, idx, gen uint64, ref chunkRef) (bool, error) {
	// current reports whether this fill still belongs to the chunk's present
	// absence: same generation, same HEAD ref. Caller holds stMu.
	current := func() bool {
		return l.absent[idx] && l.absentGen[idx] == gen && l.head.Chunks[idx] == ref
	}
	data, err := l.remote.Get(ctx, ref.Key)
	if err != nil {
		return false, fmt.Errorf("storage: chunk %d fetch: %w", idx, err)
	}
	if sha256.Sum256(data) != ref.SHA256 || len(data) != int(l.chunkBlocks)*l.blockSize {
		return false, fmt.Errorf("storage: chunk %d fetch: content does not match HEAD", idx)
	}
	first := idx * l.chunkBlocks
	for i := uint64(0); i < l.chunkBlocks; i++ {
		b := first + i
		if b >= uint64(l.s.NumBlocks()) {
			break
		}
		block := data[int(i)*l.blockSize : int(i+1)*l.blockSize]
		if err := l.s.installFetchedBlock(uint32(b), block, func() bool {
			l.stMu.Lock()
			defer l.stMu.Unlock()
			return current() && !l.localBlocks[b]
		}); err != nil {
			return false, err
		}
	}
	l.stMu.Lock()
	defer l.stMu.Unlock()
	if !current() {
		return false, nil
	}
	delete(l.absent, idx)
	for i := uint64(0); i < l.chunkBlocks; i++ {
		delete(l.localBlocks, first+i)
	}
	l.presenceVer++
	return true, nil
}

// installFetchedBlock writes a fetched block into the extent under its
// extent lock, unless stillAbsent reports that the block got a local extent
// version since the fetch began. A newer dirty-map (WAL) entry does not
// stop it: reads resolve the dirty map first and the flusher replaces the
// extent later, so installing HEAD's block beneath it is safe, and it keeps
// the extent at HEAD's value if that unsynced write is lost in a crash. It
// never notifies the chunk layer: a fill is not new dirt.
func (s *WALStore) installFetchedBlock(lba uint32, data []byte, stillAbsent func() bool) error {
	s.lifecycleMu.RLock()
	defer s.lifecycleMu.RUnlock()
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return errors.New("storage: chunk fill after Close")
	}
	lock := &s.extentMu[uint64(lba)%uint64(len(s.extentMu))]
	lock.Lock()
	defer lock.Unlock()
	if !stillAbsent() {
		return nil
	}
	return s.writeExtentUnlocked(lba, data)
}

// ---- BASE ----

func (l *chunkLayerImpl) BeginBaseInstall() error {
	l.capMu.Lock()
	defer l.capMu.Unlock()
	return l.writeBaseState(chunkBaseInstalling, func() { l.allDirty = true })
}

func (l *chunkLayerImpl) EndBaseInstall(frontier uint64) error {
	l.capMu.Lock()
	defer l.capMu.Unlock()
	if cp := l.s.CheckpointLSN(); cp < frontier {
		return fmt.Errorf("storage: EndBaseInstall: frontier %d is not durable (checkpoint %d)", frontier, cp)
	}
	return l.writeBaseState(chunkBaseInstalledAwaitingCut, func() { l.hashMode = true })
}

func (l *chunkLayerImpl) writeBaseState(state chunkBaseState, apply func()) error {
	l.bmMu.Lock()
	defer l.bmMu.Unlock()
	l.stMu.Lock()
	hdr := l.hdr
	l.stMu.Unlock()
	hdr.Base = state
	if err := l.cfg.Bitmap.WriteHeader(hdr); err != nil {
		return err
	}
	l.stMu.Lock()
	l.hdr = hdr
	apply()
	l.dirtyVer++
	l.stMu.Unlock()
	return nil
}

// ---- capture ----

func (l *chunkLayerImpl) opCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	c, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(l.ctx, cancel)
	return c, func() { stop(); cancel() }
}

// register admits new work and registers it with Close in one critical
// section: once Close has started, no work registers.
func (l *chunkLayerImpl) register() (done func(), err error) {
	l.stMu.Lock()
	defer l.stMu.Unlock()
	if l.closed {
		return nil, errors.New("storage: chunk layer closed")
	}
	l.wg.Add(1)
	return l.wg.Done, nil
}

func (l *chunkLayerImpl) admit() error {
	l.stMu.Lock()
	closed, fenced, base, detached := l.closed, l.fenced, l.hdr.Base, l.detached != nil
	l.stMu.Unlock()
	switch {
	case closed:
		return errors.New("storage: chunk layer closed")
	case fenced:
		return errChunkLayerFenced
	case detached:
		return errChunkLayerDetached
	case base == chunkBaseInstalling:
		return fmt.Errorf("%w: BASE install in progress", errChunkCaptureRefused)
	}
	if gate := l.cfg.CaptureAllowed; gate != nil {
		if err := gate(); err != nil {
			return fmt.Errorf("%w: %w", errChunkCaptureRefused, err)
		}
	}
	return nil
}

func (l *chunkLayerImpl) Cut(ctx context.Context) (chunkCutResult, error) {
	l.capMu.Lock()
	defer l.capMu.Unlock()
	return l.captureLocked(ctx, false)
}

func (l *chunkLayerImpl) Capture(ctx context.Context) (uint64, error) {
	r, err := l.Cut(ctx)
	if err != nil {
		return 0, err
	}
	return r.H, nil
}

func (l *chunkLayerImpl) MakeRemoteDurable(ctx context.Context, t uint64) error {
	l.capMu.Lock()
	defer l.capMu.Unlock()
	l.stMu.Lock()
	fenced, detached := l.fenced, l.detached != nil
	l.stMu.Unlock()
	if fenced {
		return errChunkLayerFenced
	}
	if detached {
		return errChunkLayerDetached
	}
	if last := l.s.NextLSN() - 1; t > last {
		return fmt.Errorf("storage: MakeRemoteDurable(%d): LSN was never written (last %d)", t, last)
	}
	r, err := l.captureLocked(ctx, false)
	if err != nil {
		return err
	}
	if r.H < t {
		return fmt.Errorf("storage: MakeRemoteDurable(%d): cut reached only %d", t, r.H)
	}
	return nil
}

type capturedChunk struct {
	idx  uint64
	data []byte
	sum  [32]byte
	zero bool
}

func (l *chunkLayerImpl) captureLocked(ctx context.Context, detach bool) (chunkCutResult, error) {
	done, err := l.register()
	if err != nil {
		return chunkCutResult{}, err
	}
	defer done()
	if err := l.admit(); err != nil {
		return chunkCutResult{}, err
	}
	if hook := l.testAfterAdmitHook; hook != nil {
		hook()
	}
	ctx, cancel := l.opCtx(ctx)
	defer cancel()

	if err := l.resolvePending(ctx); err != nil {
		return chunkCutResult{}, err
	}

	// Quiesced cut.
	l.s.captureMu.Lock()
	h, err := l.s.Sync()
	if err != nil {
		l.s.captureMu.Unlock()
		return chunkCutResult{}, fmt.Errorf("storage: capture sync: %w", err)
	}
	candidates := map[uint64]bool{}
	for _, e := range l.s.dm.snapshot() {
		candidates[l.chunkOf(e.LBA)] = true
	}
	l.stMu.Lock()
	all := l.hashMode || l.allDirty
	for idx := range l.dirty {
		candidates[idx] = true
	}
	if all {
		for i := uint64(0); i < l.numChunks; i++ {
			// An absent chunk with no local write is HEAD's content by
			// definition: comparing it would only fetch it back.
			if l.absent[i] && !l.chunkHasLocalLocked(i) {
				continue
			}
			candidates[i] = true
		}
	}
	l.inflight = candidates
	l.dirty = map[uint64]bool{}
	l.allDirty = false
	l.cutLSN = h
	baseHead := copyHead(l.head)
	l.stMu.Unlock()

	var captured []capturedChunk
	var copyErr error
	for _, idx := range sortedKeys(candidates) {
		buf := make([]byte, int(l.chunkBlocks)*l.blockSize)
		for i := uint64(0); i < l.chunkBlocks; i++ {
			lba := idx*l.chunkBlocks + i
			if lba >= uint64(l.s.NumBlocks()) {
				break
			}
			b, err := l.s.Read(uint32(lba))
			if err != nil {
				copyErr = err
				break
			}
			copy(buf[int(i)*l.blockSize:], b)
		}
		if copyErr != nil {
			break
		}
		captured = append(captured, capturedChunk{idx: idx, data: buf, sum: sha256.Sum256(buf), zero: isAllZero(buf)})
	}
	l.s.captureMu.Unlock()
	if copyErr != nil {
		l.abortCut(all)
		return chunkCutResult{}, fmt.Errorf("storage: capture copy: %w", copyErr)
	}

	// Upload only what changed against HEAD.
	next := copyHead(baseHead)
	next.Epoch, next.Holder, next.RemoteLSN = l.cfg.Epoch, l.cfg.Holder, h
	next.State, next.DetachedBy = chunkHeadActive, nil
	if detach {
		next.State = chunkHeadDetached
		next.DetachedBy = &chunkDetachedBy{Holder: l.cfg.Holder, Epoch: l.cfg.Epoch, Gen: baseHead.Gen + 1}
	}
	var changed []uint64
	newKeys := map[uint64]string{}
	for _, c := range captured {
		ref, had := baseHead.Chunks[c.idx]
		switch {
		case c.zero && !had, had && !c.zero && ref.SHA256 == c.sum:
			continue
		case c.zero:
			delete(next.Chunks, c.idx)
			changed = append(changed, c.idx)
			continue
		}
		key := l.newKey(c.idx)
		if err := l.remote.Put(ctx, key, c.data); err != nil {
			l.abortCut(all)
			return chunkCutResult{}, fmt.Errorf("storage: capture upload chunk %d: %w", c.idx, err)
		}
		next.Chunks[c.idx] = chunkRef{Key: key, SHA256: c.sum}
		newKeys[c.idx] = key
		changed = append(changed, c.idx)
	}
	if !detach && len(changed) == 0 && h == baseHead.RemoteLSN && baseHead.Gen != 0 && !chunkHeadIsDetached(baseHead) {
		l.finishCut(baseHead, h, nil, nil) // nothing new: no CAS
		return chunkCutResult{H: h, Head: copyHead(baseHead), Noop: true, FullHash: all}, nil
	}
	if err := l.admit(); err != nil { // admission re-checked right before the CAS
		l.abortCut(all)
		return chunkCutResult{}, err
	}
	if l.supersededBy(baseHead) {
		l.abortCut(all)
		l.stMu.Lock()
		l.fenced = true
		l.stMu.Unlock()
		return chunkCutResult{}, errChunkLayerFenced
	}
	expected := chunkHeadVersion{Gen: baseHead.Gen, Epoch: baseHead.Epoch}
	gen, casErr := l.remote.CASHead(ctx, expected, next)
	if casErr == nil {
		next.Gen = gen
		l.finishCut(next, h, changed, newKeys)
		return l.cutDone(chunkCutResult{H: h, Head: copyHead(next), FullHash: all}, detach), nil
	}
	// Precondition or unknown outcome: resolve by re-reading HEAD, never by
	// a blind retry.
	keepPending := func() {
		l.stMu.Lock()
		p := copyHead(next)
		l.pending = &p
		l.pendingDetach, l.pendingCut, l.pendingBase = detach, chunkCutResult{H: h, FullHash: all}, expected
		l.stMu.Unlock()
		l.abortCut(all)
	}
	cur, gerr := l.remote.GetHead(ctx)
	if gerr != nil {
		keepPending()
		return chunkCutResult{}, fmt.Errorf("storage: capture CAS outcome unresolved (%v): %w: %w", casErr, errChunkCASOutcomeUnknown, gerr)
	}
	if !errors.Is(casErr, errChunkHeadPrecondition) && !sameProposal(cur, next) && cur.Gen == expected.Gen && cur.Epoch == expected.Epoch {
		// HEAD is still the CAS's base and the server gave no definite
		// refusal: the CAS may still land. Settle it before any later
		// capture or durability point (resolvePending re-proposes it).
		keepPending()
		return chunkCutResult{}, fmt.Errorf("storage: capture CAS outcome unresolved (%v): HEAD unchanged: %w", casErr, errChunkCASOutcomeUnknown)
	}
	if sameProposal(cur, next) {
		l.finishCut(cur, h, changed, newKeys)
		return l.cutDone(chunkCutResult{H: h, Head: copyHead(cur), FullHash: all}, detach), nil
	}
	l.abortCut(all)
	if l.supersededBy(cur) {
		l.stMu.Lock()
		l.fenced = true
		l.head = copyHead(cur)
		l.stMu.Unlock()
		return chunkCutResult{}, errors.Join(errChunkLayerFenced, casErr)
	}
	l.stMu.Lock()
	l.head = copyHead(cur)
	l.stMu.Unlock()
	return chunkCutResult{}, fmt.Errorf("storage: capture CAS not applied: %w", casErr)
}

func (l *chunkLayerImpl) Detach(ctx context.Context) (chunkCutResult, error) {
	l.capMu.Lock()
	defer l.capMu.Unlock()
	if r, ok := l.storedDetach(); ok {
		return r, nil
	}
	r, err := l.captureLocked(ctx, true)
	if errors.Is(err, errChunkLayerDetached) {
		if d, ok := l.storedDetach(); ok {
			return d, nil // an earlier, unresolved detach CAS had applied
		}
	}
	return r, err
}

func (l *chunkLayerImpl) storedDetach() (chunkCutResult, bool) {
	l.stMu.Lock()
	defer l.stMu.Unlock()
	if d := l.detached; d != nil {
		r := *d
		r.Head = copyHead(d.Head)
		return r, true
	}
	return chunkCutResult{}, false
}

// cutDone records a successful detach cut; the layer is detached from then on.
func (l *chunkLayerImpl) cutDone(r chunkCutResult, detach bool) chunkCutResult {
	if detach {
		l.stMu.Lock()
		keep := r
		keep.Head = copyHead(r.Head)
		l.detached = &keep
		l.stMu.Unlock()
	}
	return r
}

// supersededBy reports whether cur belongs to a newer or competing holder.
func (l *chunkLayerImpl) supersededBy(cur chunkHead) bool {
	return cur.Epoch > l.cfg.Epoch || (cur.Epoch == l.cfg.Epoch && cur.Holder != l.cfg.Holder && cur.Gen != 0)
}

func sameProposal(cur, next chunkHead) bool {
	if cur.Epoch != next.Epoch || cur.Holder != next.Holder || cur.RemoteLSN != next.RemoteLSN || len(cur.Chunks) != len(next.Chunks) {
		return false
	}
	if chunkHeadIsDetached(cur) != chunkHeadIsDetached(next) || (cur.DetachedBy == nil) != (next.DetachedBy == nil) ||
		(cur.DetachedBy != nil && *cur.DetachedBy != *next.DetachedBy) {
		return false
	}
	for k, v := range next.Chunks {
		if cur.Chunks[k] != v {
			return false
		}
	}
	return true
}

// resolvePending re-reads HEAD when an earlier CAS outcome is unresolved.
func (l *chunkLayerImpl) resolvePending(ctx context.Context) error {
	l.stMu.Lock()
	pending, base := l.pending, l.pendingBase
	l.stMu.Unlock()
	if pending == nil {
		return nil
	}
	cur, err := l.remote.GetHead(ctx)
	if err != nil {
		return fmt.Errorf("storage: capture: earlier CAS outcome still unresolved: %w: %w", errChunkCASOutcomeUnknown, err)
	}
	if !sameProposal(cur, *pending) && !l.supersededBy(cur) && cur.Gen == base.Gen && cur.Epoch == base.Epoch {
		// HEAD is still the unresolved CAS's base, so that CAS may still
		// land. Settle it by issuing the same proposal at the same base:
		// whichever lands first, HEAD ends at this proposal and the other
		// fails its precondition. That is a new publication, so it passes
		// the capture admission again first: the HEAD read above may have
		// blocked across a demotion or a lineage change. A refusal keeps
		// the CAS unresolved.
		if err := l.admit(); err != nil {
			return fmt.Errorf("storage: capture: earlier CAS outcome still unresolved (%w); re-proposal refused: %w", errChunkCASOutcomeUnknown, err)
		}
		gen, casErr := l.remote.CASHead(ctx, base, copyHead(*pending))
		if casErr == nil {
			cur = copyHead(*pending)
			cur.Gen = gen
		} else if cur, err = l.remote.GetHead(ctx); err != nil {
			return fmt.Errorf("storage: capture: earlier CAS outcome still unresolved (%v): %w: %w", casErr, errChunkCASOutcomeUnknown, err)
		} else if !sameProposal(cur, *pending) && !l.supersededBy(cur) && cur.Gen == base.Gen && cur.Epoch == base.Epoch {
			return fmt.Errorf("storage: capture: earlier CAS outcome still unresolved (%v): HEAD unchanged: %w", casErr, errChunkCASOutcomeUnknown)
		}
	}
	l.stMu.Lock()
	defer l.stMu.Unlock()
	l.pending = nil
	wasDetach, cut := l.pendingDetach, l.pendingCut
	l.pendingDetach, l.pendingCut, l.pendingBase = false, chunkCutResult{}, chunkHeadVersion{}
	switch {
	case sameProposal(cur, *pending):
		l.head, l.remoteLSN = copyHead(cur), cur.RemoteLSN
		if wasDetach {
			cut.Head = copyHead(cur)
			l.detached = &cut
			return errChunkLayerDetached
		}
	case l.supersededBy(cur):
		l.fenced = true
		l.head = copyHead(cur)
		return errChunkLayerFenced
	default:
		// HEAD moved past the unresolved CAS's base without it: that CAS
		// can never land.
		l.head = copyHead(cur)
	}
	return nil
}

// abortCut returns a failed cut's chunks to the dirty set.
func (l *chunkLayerImpl) abortCut(all bool) {
	l.stMu.Lock()
	defer l.stMu.Unlock()
	for idx := range l.inflight {
		l.dirty[idx] = true
	}
	l.inflight = map[uint64]bool{}
	if all {
		l.allDirty = true
	}
	l.dirtyVer++
}

// finishCut installs HEAD in memory and rolls the bitmap over: only chunks
// dirtied after the cut (and WAL entries above it) stay set.
func (l *chunkLayerImpl) finishCut(head chunkHead, h uint64, changed []uint64, newKeys map[uint64]string) {
	l.bmMu.Lock()
	defer l.bmMu.Unlock()
	still := map[uint64]bool{}
	for _, e := range l.s.dm.snapshot() {
		if e.LSN > h {
			still[l.chunkOf(e.LBA)] = true
		}
	}
	l.stMu.Lock()
	for idx := range l.dirty {
		still[idx] = true
	}
	if l.allDirty {
		for i := uint64(0); i < l.numChunks; i++ {
			still[i] = true
		}
	}
	prevBase := l.hdr.BaseLSN
	l.head, l.remoteLSN, l.pending = copyHead(head), h, nil
	l.inflight = map[uint64]bool{}
	l.hashMode = false
	hdr := chunkBitmapHeader{BaseLSN: h, HeadGen: head.Gen, Epoch: head.Epoch, Base: chunkBaseNone}
	l.hdr = hdr
	l.dirtyVer++
	ver := l.dirtyVer
	l.stMu.Unlock()
	sort.Slice(changed, func(i, j int) bool { return changed[i] < changed[j] })
	delta := chunkDelta{FromLSN: prevBase, ToLSN: h, Gen: head.Gen, Dirty: changed, NewKeys: newKeys}
	// HEAD is published; a failed Reset only leaves a stale header, which a
	// restart treats as a mismatch (hash compare): safe.
	if err := l.cfg.Bitmap.Reset(hdr, sortedKeys(still), delta); err == nil {
		l.stMu.Lock()
		if ver > l.savedDirtyVer {
			l.savedDirtyVer = ver
		}
		l.stMu.Unlock()
	}
}

func (l *chunkLayerImpl) newKey(idx uint64) string {
	l.stMu.Lock()
	l.keySeq++
	seq := l.keySeq
	l.stMu.Unlock()
	return fmt.Sprintf("chunks/e%d/c%d/%s-%d", l.cfg.Epoch, idx, l.nonce, seq)
}

func isAllZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

// ---- eviction and presence ----

func (l *chunkLayerImpl) EvictChunk(idx uint64) error {
	l.capMu.Lock()
	defer l.capMu.Unlock()
	if idx >= l.numChunks {
		return fmt.Errorf("%w: chunk %d out of range", errChunkEvictRefused, idx)
	}
	walDirty := false
	for _, e := range l.s.dm.snapshot() {
		if l.chunkOf(e.LBA) == idx {
			walDirty = true
			break
		}
	}
	l.stMu.Lock()
	switch {
	case l.head.Gen == 0, l.hashMode, l.allDirty, l.hdr.Base != chunkBaseNone, l.dirty[idx], l.inflight[idx], walDirty:
		l.stMu.Unlock()
		return fmt.Errorf("%w: chunk %d is dirty since HEAD or HEAD is unknown", errChunkEvictRefused, idx)
	}
	if _, inHead := l.head.Chunks[idx]; !inHead {
		l.stMu.Unlock()
		return nil // ZERO in HEAD: nothing to make absent
	}
	l.absent[idx] = true
	l.absentGen[idx]++
	first := idx * l.chunkBlocks
	for i := uint64(0); i < l.chunkBlocks; i++ {
		delete(l.localBlocks, first+i)
	}
	l.presenceVer++
	l.stMu.Unlock()
	return l.persistPresence()
}

func (l *chunkLayerImpl) presencePath() string { return l.s.path + ".chunkpresence" }

// persistPresence makes the absence state durable. It fsyncs the volume
// first, so a chunk recorded as present has its filled bytes on disk.
func (l *chunkLayerImpl) persistPresence() error {
	// The whole save runs under presMu, so a snapshot can never be written
	// after a newer one. Lock order: WALStore.mu -> presMu -> stMu.
	l.presMu.Lock()
	defer l.presMu.Unlock()
	l.stMu.Lock()
	ver := l.presenceVer
	if ver == l.savedPresVer {
		l.stMu.Unlock()
		return nil
	}
	var b strings.Builder
	for _, idx := range sortedKeys(l.absent) {
		fmt.Fprintf(&b, "absent %d\n", idx)
	}
	for _, lba := range sortedKeys(l.localBlocks) {
		fmt.Fprintf(&b, "local %d\n", lba)
	}
	l.stMu.Unlock()
	if hook := l.testPresenceSnapshotHook; hook != nil {
		hook()
	}
	if fd := l.s.fd; fd != nil {
		if err := fd.Sync(); err != nil {
			return fmt.Errorf("storage: chunk presence: fsync volume: %w", err)
		}
	}
	if err := writeFileDurable(l.presencePath(), []byte(b.String())); err != nil {
		return err
	}
	// The rename itself must be durable before the presence counts as saved;
	// on failure the version stays unsaved and the caller (a checkpoint
	// advance or an eviction) is refused.
	if err := l.syncDirFn(filepath.Dir(l.presencePath())); err != nil {
		return fmt.Errorf("storage: chunk presence: fsync directory: %w", err)
	}
	l.stMu.Lock()
	if ver > l.savedPresVer {
		l.savedPresVer = ver
	}
	l.stMu.Unlock()
	return nil
}

func (l *chunkLayerImpl) loadPresence() error {
	data, err := os.ReadFile(l.presencePath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("storage: chunk presence: %w", err)
	}
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		kind, num, ok := strings.Cut(sc.Text(), " ")
		if !ok {
			return fmt.Errorf("storage: chunk presence: bad line %q", sc.Text())
		}
		n, err := strconv.ParseUint(num, 10, 64)
		if err != nil {
			return fmt.Errorf("storage: chunk presence: bad line %q", sc.Text())
		}
		switch kind {
		case "absent":
			l.absent[n] = true
		case "local":
			l.localBlocks[n] = true
		default:
			return fmt.Errorf("storage: chunk presence: bad line %q", sc.Text())
		}
	}
	l.savedPresVer = l.presenceVer
	return sc.Err()
}

func writeFileDurable(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// syncDir makes a rename in dir durable. Windows cannot fsync a directory
// handle (NTFS orders the rename itself), so there it is a no-op; the
// block volume runs on Linux.
func syncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// ---- status and lifetime ----

func (l *chunkLayerImpl) RemoteLSN() uint64 {
	l.stMu.Lock()
	defer l.stMu.Unlock()
	return l.remoteLSN
}

func (l *chunkLayerImpl) Fenced() bool {
	l.stMu.Lock()
	defer l.stMu.Unlock()
	return l.fenced
}

// Close cancels the layer's remote calls and joins its captures and fills.
// Idempotent; called by WALStore.Close before the store closes.
func (l *chunkLayerImpl) Close() error {
	l.stMu.Lock()
	if l.closed {
		l.stMu.Unlock()
		<-l.closeDone // every caller returns only after shutdown completes
		return nil
	}
	l.closed = true
	l.stMu.Unlock()
	l.cancel()
	l.wg.Wait()
	close(l.closeDone)
	return nil
}
