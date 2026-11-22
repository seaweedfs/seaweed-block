package storage

// Chunk layer: the implementation of the seam in chunk_seam.go.
//
// Lock order (never reversed):
//
//	installMu (BASE transitions only) -> capMu -> WALStore.captureMu -> WALStore.lifecycleMu -> WALStore.mu
//	extentMu[i] -> stMu
//	WALStore.mu -> presMu -> stMu (BeforeCheckpointAdvance runs under mu)
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
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweed-block/core/holder"
)

// chunkHooks is what the WALStore holds for an attached layer.
type chunkHooks struct {
	beforeMutation    func(first, count uint64)
	beforeDestructive func() func()

	sink   chunkSink
	closer func() error
	// mutated runs after every dirty-map insert (a write, a batch write,
	// a replica apply) with the LBA: O(1), lock-free, on the write path.
	mutated func(lba uint64)
	// recovered runs when Recover republished the dirty map (any chunk
	// may have changed), before reads resume.
	recovered func()
	// Acquired only under installMu exclusive, before every lower store lock.
	baseTransitionV3 func() (release func(), err error)
}

const defaultReadMissTimeout = 30 * time.Second

type chunkLayerImpl struct {
	// capMu serializes pre-copy admission and its recent measurements.
	precopyFreeze  func(uint64, time.Time, chunkCutStats) bool
	precopyPrepare func(context.Context) (func(), error)
	precopySamples []precopySample
	precopyPause   func(time.Duration)

	s      *WALStore
	remote chunkRemote
	cfg    chunkLayerConfig

	blockSize   int
	chunkBlocks uint64
	numChunks   uint64
	nonce       string

	ctx    context.Context // cancelled by Close
	cancel context.CancelFunc
	// settled: confirmed publications not yet reported to
	// cfg.OnPublishSettled, in publication order; settledDraining: a caller
	// is delivering them (both stMu).
	settled         []headTransition
	settledDraining bool
	wg              sync.WaitGroup // captures and fills in flight

	cowMu          sync.Mutex // window admission only; never held during reads or joins
	cow            *cutCOWWindow
	cowActive      atomic.Pointer[cutCOWWindow]
	cowTransitions int
	cowClosed      bool

	capMu sync.Mutex // serializes Capture, Evict, Begin/EndBaseInstall

	presMu sync.Mutex // serializes a whole presence save: snapshot, fsync, replace, version

	hydration        map[uint64]*chunkHydrationFlight // under stMu
	hydrationWake    chan struct{}
	hydrationStarted bool
	closeDone        chan struct{} // closed when Close has finished

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
	// pendingIntents are the intents (staging rounds and the barrier) of
	// the unresolved proposal in pending; deleted once it is settled.
	pendingIntents []string
	// stats holds the cumulative cut counters (under stMu).
	stats chunkLayerStats
	// leftoverIntents are this layer's own intents whose delete failed
	// after their cut was settled; every cut retries them.
	leftoverIntents []string
	// Immutable completed uploads survive a refused point. Guarded by stMu;
	// versions and HEAD generation prevent reuse across mutations/recovery.
	reusable  map[uint64]stagedChunk
	remoteLSN uint64
	hdr       chunkBitmapHeader // last header written or loaded
	dirty     map[uint64]bool   // chunks dirtied since the cut in progress or the last HEAD
	inflight  map[uint64]bool   // chunks captured by a cut whose HEAD is not yet installed
	allDirty  bool              // a BASE block landed: every chunk is dirty
	// own names the chunks OwnBatch re-uploads under this layer's own keys in
	// the cut it runs (set and cleared under capMu, read under stMu).
	own map[uint64]bool

	cutLSN uint64 // H of the latest cut taken (in flight or installed)
	// chunkMut counts the mutations of each chunk: every dirty-map insert
	// (any LSN, a replica apply's lower LSN included) and every BASE
	// block. Flushing moves bytes without changing them and never counts.
	// A chunk staged at count n is unchanged while its count is n.
	chunkMut []atomic.Uint64
	keySeq   uint64

	// Test-only scheduling hooks; nil in production.
	testHydrationWait            func(uint64) // after promise registration, outside locks
	testHydrateAllPause          func()       // HydrateAll yielded to a guest waiter, outside locks
	testCOWWait                  func()
	testCOWSaverWait             func()
	testCOWRead                  func(idx uint64, saved bool)
	testCOWSave                  func() error
	testPresenceSnapshotHook     func()                 // after a presence snapshot, before it is written
	testBaseTransitionBeforeLock func()                 // test handshake before capMu drain
	testBeforeTailScanHook       func()                 // diagnostic tail scan cost witness
	testFrozenNow                func() time.Time       // controlled guest elapsed at admission
	testAfterAdmitHook           func()                 // right after a capture is admitted
	testAfterStageHook           func(round int)        // after each pre-copy staging round
	testAfterStageReadHook       func(idx uint64)       // after a staging read, before its result is kept
	testAfterBarrierSyncHook     func(h uint64)         // under the capture barrier, right after Sync fixed H
	testBeforeBarrierLockHook    func()                 // right before the capture barrier's Lock request
	syncDirFn                    func(dir string) error // directory fsync after a rename; syncDir in production

	// Absence: chunks whose local bytes are not authoritative (fresh host,
	// evicted), and blocks written locally inside such chunks since.
	absent       map[uint64]bool
	absentGen    map[uint64]uint64 // bumped on every eviction; binds a fill to its generation
	localBlocks  map[uint64]bool
	presenceVer  uint64
	savedPresVer uint64
	dirtyVer     uint64
}

func attachChunkLayerImpl(s *WALStore, remote chunkRemote, cfg chunkLayerConfig) (chunkLayer, error) {
	if s.v3 == nil || s.v3.mapProof == nil {
		return nil, fmt.Errorf("%w: chunk layer requires the native owned map", ErrMapInvalid)
	}
	if remote == nil || cfg.ChunkBlocks == 0 {
		return nil, errors.New("storage: chunk layer needs a remote and ChunkBlocks > 0")
	}
	var localBase bool
	s.captureMu.Lock()
	defer s.captureMu.Unlock()
	if s.attachClosed {
		return nil, errChunkLayerClosed
	}
	{

		// Serialize hook publication with begin/complete. Their outer hook
		// snapshot is rechecked after taking this same barrier.
		d := s.v3.pub.current()
		if d.BaseState == BaseInstalling {
			return nil, ErrBaseIncomplete
		}
		localBase = d.BaseState == BaseInstalled
		if localBase {
			cfg.FreshFromHead = false
		}
	}

	if s.v3.ownedMap != nil && uint64(cfg.ChunkBlocks) != s.v3.ownedMap.chunkBlocks {
		return nil, fmt.Errorf("%w: chunk layer geometry differs from owned map", ErrMapInvalid)
	}
	if cfg.UploadConcurrency < 0 || cfg.UploadConcurrency > 64 {
		return nil, errors.New("storage: upload concurrency must be between 1 and 64 (0 defaults to 8)")
	}
	if cfg.UploadConcurrency == 0 {
		cfg.UploadConcurrency = 8
	}
	if cfg.HydrationWorkers < 0 || cfg.HydrationWorkers > 64 || cfg.HydrationQueueDepth < 0 || cfg.HydrationQueueDepth > 65536 {
		return nil, errors.New("storage: hydration workers must be 1..64 and queue depth 1..65536 (0 uses defaults)")
	}
	if cfg.HydrationWorkers == 0 {
		cfg.HydrationWorkers = defaultHydrationWorkers
	}
	if cfg.HydrationQueueDepth == 0 {
		cfg.HydrationQueueDepth = defaultHydrationQueueDepth
	}
	if cfg.ReadMissTimeout <= 0 {
		cfg.ReadMissTimeout = defaultReadMissTimeout
	}
	chunkBytes := uint64(s.BlockSize()) * uint64(cfg.ChunkBlocks)
	if chunkBytes > math.MaxInt64/6 || cfg.PayloadMemoryBudget < 0 {
		return nil, errors.New("storage: invalid payload memory budget or chunk geometry")
	}
	minimumPayload := 6 * int64(chunkBytes) // one uploader and one saver
	if cfg.PayloadMemoryBudget == 0 {
		cfg.PayloadMemoryBudget = max(DefaultCutPayloadMemoryBudget, minimumPayload)
	} else if cfg.PayloadMemoryBudget < minimumPayload {
		return nil, fmt.Errorf("storage: payload memory budget must hold at least six chunks (%d bytes)", minimumPayload)
	}
	if cfg.COWSpillLimit < 0 {
		return nil, errors.New("storage: COW spill limit must not be negative (0 defaults to 1 GiB)")
	}
	if cfg.COWSpillLimit == 0 {
		cfg.COWSpillLimit = defaultCOWSpillLimit
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
	l.chunkMut = make([]atomic.Uint64, l.numChunks)
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
	if cfg.RestoreGuard != nil && (head.Gen != cfg.RestoreGuard.Gen || head.Epoch != cfg.RestoreGuard.Epoch) {
		l.cancel()
		return nil, fmt.Errorf("%w: HEAD changed after restore claim/seed", errChunkHeadPrecondition)
	}
	l.head = copyHead(head)
	l.remoteLSN = head.RemoteLSN
	// A HEAD that already belongs to a newer epoch or a competing holder is
	// never used as this holder's publication guard.
	l.fenced = l.supersededBy(head)

	// Dirt is derived from the owned LSN map against the actual remote HEAD;
	// no local HEAD-period bitmap header is loaded or created for impl3.
	l.hdr = chunkBitmapHeader{BaseLSN: head.RemoteLSN, HeadGen: head.Gen, Epoch: head.Epoch}

	if localBase {
		// The completed BASE image is local authority, including ZERO blocks.
		// Never revive old HEAD absence after a crash before its first cut.
		l.hdr.Base = chunkBaseInstalledAwaitingCut
		l.allDirty = true
		l.presenceVer++
	} else if cfg.FreshFromHead {
		// An existing HEAD defines the fresh host's lazy image. With no HEAD,
		// local writes may already have checkpointed out of the WAL dirty map:
		// preserve ALL chunks as candidates for the first hash-comparing cut.
		// Persist that obligation before publishing hooks/readiness.
		l.hdr = chunkBitmapHeader{BaseLSN: head.RemoteLSN, HeadGen: head.Gen, Epoch: cfg.Epoch}
		l.dirty = map[uint64]bool{}

		for idx := range head.Chunks {
			l.absent[idx] = true
			l.absentGen[idx]++
		}
		l.presenceVer++
	} else if err := l.loadPresence(); err != nil {
		l.cancel()
		return nil, err
	}
	hooks := &chunkHooks{sink: l, closer: l.Close, mutated: l.noteMutation, recovered: l.noteAllMutated}

	hooks.baseTransitionV3 = l.holdBaseTransitionV3
	hooks.beforeMutation = l.beforeCOWMutation
	hooks.beforeDestructive = l.holdCOWDestructive

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
	if h.Segments != nil {
		out.Segments = make(map[uint64]ChunkSegment, len(h.Segments))
		for k, v := range h.Segments {
			out.Segments[k] = v
		}
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
	if lsn == 0 { // a BASE block lands in the extent directly
		l.noteMutation(lba)
	}
	l.dirtyVer++
}

// noteMutation counts a mutation of lba's chunk. It runs after the
// mutation is visible to reads, so a staging read that sampled the count
// first either sees the new bytes or a stale count (never reused).
func (l *chunkLayerImpl) noteMutation(lba uint64) {
	if idx := l.chunkOf(lba); idx < uint64(len(l.chunkMut)) {
		l.chunkMut[idx].Add(1)
	}
}

// noteAllMutated counts a mutation of every chunk: Recover republished
// the dirty map, so no staged pre-copy may be reused across it. O(chunks),
// on recovery only.
func (l *chunkLayerImpl) noteAllMutated() {
	for i := range l.chunkMut {
		l.chunkMut[i].Add(1)
	}
}

// captureCandidates includes current WAL entries and impl3 owned-map dirt.
// UNKNOWN never certifies clean; a max LSN beyond HEAD must be captured even
// after the WAL entry has checkpointed and the legacy bitmap says clean.
func (l *chunkLayerImpl) captureCandidates() map[uint64]bool {
	out := map[uint64]bool{}
	var base uint64

	l.stMu.Lock()
	base = l.remoteLSN
	l.stMu.Unlock()

	for _, e := range l.s.dm.snapshot() {
		// Retention in the local WAL is not dirt since HEAD. Lower-LSN
		// BASE/direct changes remain conservative through UNKNOWN below.
		if e.LSN > base {
			out[l.chunkOf(e.LBA)] = true
		}
	}
	{

		l.stMu.Lock()
		absent := map[uint64]bool{}
		for idx := range l.absent {
			if !l.chunkHasLocalLocked(idx) {
				absent[idx] = true
			}
		}
		l.addOwnCandidatesLocked(out)
		l.stMu.Unlock()
		l.s.mu.RLock()
		for idx, e := range l.s.v3.mapEntries {
			i := uint64(idx)
			if !absent[i] && (e.Unknown || e.LSN > base) {
				out[i] = true
			}
		}
		l.s.mu.RUnlock()
	}

	return out
}

// chunkVersion is chunk idx's mutation count.
func (l *chunkLayerImpl) chunkVersion(idx uint64) uint64 {
	return l.chunkMut[idx].Load()
}

// BeforeCheckpointAdvance runs inside persistCheckpoint and the direct
// frontier path, under WALStore.mu: it makes the dirty set (and any changed
// absence state) durable before the checkpoint can pass the entries.
func (l *chunkLayerImpl) BeforeCheckpointAdvance(lsn uint64) error {
	if l.s.v3.pub.current().BaseState == BaseInstalling {
		// Serving/capture remain blocked by the installing descriptor. Settle
		// local BASE authority before the installed descriptor can publish;
		// a failure leaves the volume installing, never serving a partial image.
		l.stMu.Lock()
		l.absent = map[uint64]bool{}
		l.localBlocks = map[uint64]bool{}
		l.presenceVer++
		l.stMu.Unlock()
	}
	return l.persistPresence()
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
	return l.OnReadMissContext(context.Background(), lba)
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
	defer l.unlockCap()
	return fmt.Errorf("%w: BASE state must be published by the descriptor install protocol", errV3NotYet)
}
func (l *chunkLayerImpl) EndBaseInstall(frontier uint64) error {
	l.capMu.Lock()
	defer l.unlockCap()
	if cp := l.s.CheckpointLSN(); cp < frontier {
		return fmt.Errorf("storage: EndBaseInstall: frontier %d is not durable (checkpoint %d)", frontier, cp)
	}
	return fmt.Errorf("%w: BASE state must be published by the descriptor install protocol", errV3NotYet)
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
		return nil, errChunkLayerClosed
	}
	l.wg.Add(1)
	return l.wg.Done, nil
}

func (l *chunkLayerImpl) admit() (err error) {
	defer func() {
		if err != nil {
			l.abortCOW(err)
		}
	}()
	if l.s.v3.pub.current().BaseState == BaseInstalling {
		return fmt.Errorf("%w: %w", errChunkCaptureRefused, errChunkBaseInstalling)
	}
	l.stMu.Lock()
	closed, fenced, base, detached := l.closed, l.fenced, l.hdr.Base, l.detached != nil
	l.stMu.Unlock()
	switch {
	case closed:
		return errChunkLayerClosed
	case fenced:
		return errChunkLayerFenced
	case detached:
		return errChunkLayerDetached
	case base == chunkBaseInstalling && !l.usesOwnedMapV3():
		return fmt.Errorf("%w: %w", errChunkCaptureRefused, errChunkBaseInstalling)
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
	defer l.unlockCap()
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
	defer l.unlockCap()
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
	version uint64
	intent  string
	idx     uint64
	data    []byte
	sum     [32]byte
	zero    bool
	key     string // staged: already uploaded under this key (data is nil)
	birth   uint64
	size    int
	missing []uint64 // local copy awaits the pinned HEAD, after the local entry is released
}

// stagedChunk is a chunk pre-copied outside the barrier at chunk version
// ver against HEAD generation headGen (key empty: nothing to upload).
type stagedChunk struct {
	capturedChunk
	ver     uint64
	headGen uint64
}

func (l *chunkLayerImpl) captureLocked(ctx context.Context, detach bool) (res chunkCutResult, err error) {
	done, err := l.register()
	if err != nil {
		return chunkCutResult{}, err
	}
	defer done()
	start := time.Now()
	var st chunkCutStats
	persistCapturedMap := func(versions map[uint64]uint64) {
		start := time.Now()
		l.persistCapturedMapV3(versions)
		st.MapPersistNs += time.Since(start).Nanoseconds()
	}
	readCapturedHead := func() (chunkHead, error) {
		start := time.Now()
		head, err := readChunkHead(ctx, l.remote)
		st.HeadReadbackNs += time.Since(start).Nanoseconds()
		return head, err
	}
	dropCapturedIntents := func(ctx context.Context, ids []string) {
		start := time.Now()
		l.dropIntents(ctx, ids)
		st.IntentCleanupNs += time.Since(start).Nanoseconds()
	}
	finishCapturedCut := func(head chunkHead, h uint64, changed []uint64, keys map[uint64]string) {
		start := time.Now()
		l.finishCut(head, h, changed, keys)
		st.CommitStateNs += time.Since(start).Nanoseconds()
	}
	var pauseStart time.Time
	var payloadBeforeBarrier int64
	defer func() {
		var tailBytes uint64
		if err == nil {
			scanStart := time.Now()
			tailBytes = l.candidateTailBytes()
			st.CandidateNs += time.Since(scanStart).Nanoseconds()
		}
		st.TotalNs = time.Since(start).Nanoseconds()
		if err == nil {
			res.Stats = st
			if !pauseStart.IsZero() {
				fixed := time.Since(pauseStart).Nanoseconds() - st.PayloadNs + payloadBeforeBarrier
				l.samplePrecopy(st, fixed)
			}
			res.CandidateTail = l.estimateCandidateTail(tailBytes)
		}
		if errors.Is(err, errChunkLayerFenced) {
			l.stMu.Lock()
			var ids []string
			for _, sc := range l.reusable {
				if sc.intent != "" {
					ids = append(ids, sc.intent)
				}
			}
			l.reusable = nil
			l.stMu.Unlock()
			dropCapturedIntents(ctx, ids)
		}
		if observe := l.precopyPause; observe != nil {
			observe(time.Duration(st.BarrierPauseNs))
		}
		l.recordCut(res, st, err)
	}()
	if err := l.admit(); err != nil {
		return chunkCutResult{}, err
	}
	if hook := l.testAfterAdmitHook; hook != nil {
		hook()
	}

	ctx, cancel := l.opCtx(ctx)
	defer cancel()
	var budget *cutBudgetReservation
	if host := l.cfg.Budget; host != nil {
		budgetCtx, abort := context.WithCancelCause(ctx)
		ctx = budgetCtx
		_, _, payload := l.cutPayloadWorkers()
		ticket, admitErr := host.Admit(ctx, payload, func(cause error) {
			if cause == nil {
				cause = errChunkCaptureRefused
			}
			abort(errors.Join(errCutCOWAborted, errCutBudgetRevoked, cause))
		})
		if admitErr != nil {
			abort(nil)
			return chunkCutResult{}, fmt.Errorf("%w: budget admission: %w", errChunkCaptureRefused, admitErr)
		}
		budget = &cutBudgetReservation{budget: host, ticket: ticket, ctx: ctx}
		defer func() {
			if err != nil {
				err = errors.Join(err, context.Cause(budgetCtx))
			}
			abort(nil)
			host.Release(ticket)
		}()
		if cause := context.Cause(ctx); cause != nil {
			return chunkCutResult{}, cause
		}
	}

	if err := l.resolvePending(ctx); err != nil {
		return chunkCutResult{}, err
	}
	l.retryLeftoverIntents(ctx)

	// Pre-copy large native dirty sets outside the barrier, bounded by the
	// remaining COW exposure rather than a fixed candidate count.
	stageStart := time.Now()
	var staged map[uint64]stagedChunk
	var intents []string
	_, guestFrozen := FrozenDetachFromContext(ctx)
	spill := l.readCutSpill()
	if !guestFrozen {
		staged, intents, err = l.stage(ctx, &st, spill.bytes)
	}
	if st.UploadedBytes > 0 {
		st.PayloadNs += time.Since(stageStart).Nanoseconds()
	}
	if err != nil {
		dropCapturedIntents(ctx, intents)
		return chunkCutResult{}, err
	}

	if cause := context.Cause(ctx); cause != nil {
		dropCapturedIntents(ctx, intents)
		return chunkCutResult{}, cause
	}

	// Quiesced cut. The guest pause starts at the Lock REQUEST: a pending
	// writer blocks new readers of captureMu, so a write arriving while the cut
	// waits for writers already holding it (e.g. parked in WAL admission) is
	// paused too. LockWaitNs reports that wait on its own.
	payloadBeforeBarrier = st.PayloadNs
	pauseStart = time.Now()
	var releasePrecopy func()
	if prepare := l.precopyPrepare; prepare != nil {
		releasePrecopy, _ = prepare(ctx)
		st.DrainNs = time.Since(pauseStart).Nanoseconds()
	}
	defer func() {
		if releasePrecopy != nil && !detach {
			releasePrecopy()
		}
	}()
	if cause := context.Cause(ctx); cause != nil {
		if releasePrecopy != nil {
			releasePrecopy()
			releasePrecopy = nil
		}
		dropCapturedIntents(ctx, intents)
		return chunkCutResult{}, cause
	}
	// Resume rejected tentative admission before any ordinary COW upload.
	unlockCapture := func() {
		l.s.captureMu.Unlock()
		if !detach && releasePrecopy != nil {
			releasePrecopy()
			releasePrecopy = nil
		}
	}
	if hook := l.testBeforeBarrierLockHook; hook != nil {
		hook()
	}
	l.s.captureMu.Lock()
	barrier := time.Now()
	st.LockWaitNs = barrier.Sub(pauseStart).Nanoseconds()
	h, err := l.s.syncWithPhases(&st.SyncPhases)
	st.SyncNs = time.Since(barrier).Nanoseconds()
	if hook := l.testAfterBarrierSyncHook; hook != nil && err == nil {
		hook(h)
	}
	if err != nil {
		unlockCapture()
		st.BarrierPauseNs = time.Since(pauseStart).Nanoseconds()
		dropCapturedIntents(ctx, intents) // a clean abort: staged keys are released
		return chunkCutResult{}, fmt.Errorf("storage: capture sync: %w", err)
	}
	candidateStart := time.Now()
	candidates := map[uint64]bool{}
	for idx := range l.captureCandidates() {
		candidates[idx] = true
	}
	l.stMu.Lock()
	all := l.allDirty
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
	// A staged chunk stands for the barrier copy while no write reached
	// it since staging (its version is unchanged) and HEAD is the one it
	// was compared against.
	var captured []capturedChunk
	var capturedVersions map[uint64]uint64

	capturedVersions = make(map[uint64]uint64, len(candidates))

	useStaged := map[uint64]bool{}
	for idx := range candidates {
		if capturedVersions != nil {
			capturedVersions[idx] = l.chunkVersion(idx)
		}
		if sc, ok := staged[idx]; ok && sc.ver == l.chunkVersion(idx) && sc.headGen == l.head.Gen {
			useStaged[idx] = true
			captured = append(captured, sc.capturedChunk)
		}
	}
	l.inflight = candidates
	l.dirty = map[uint64]bool{}
	l.allDirty = false
	l.cutLSN = h
	baseHead := copyHead(l.head)
	l.stMu.Unlock()
	// Candidates, mutation versions and births belong to the same barrier.
	// Do not sample the owned map after COW writers are admitted again.
	births := l.captureBirths(candidates, h)
	st.CandidateNs += time.Since(candidateStart).Nanoseconds()
	// The authoritative candidate set is fixed with mutation admission closed.
	// The owner callback is nonblocking and may stop only an already drained
	// frontend; rejected attempts remain ordinary COW cuts.
	if freeze := l.precopyFreeze; freeze != nil && context.Cause(ctx) == nil {
		tail := uint64(len(candidates)) * uint64(l.blockSize) * l.chunkBlocks
		detach = freeze(tail, pauseStart, st)
	}
	if guestFrozen && !detach {
		unlockCapture()
		st.BarrierPauseNs = time.Since(pauseStart).Nanoseconds()
		l.abortCut(all)
		dropCapturedIntents(ctx, intents)
		return chunkCutResult{}, errors.Join(ErrFrozenTailTooLarge, context.Cause(ctx), l.admit())
	}

	if h < baseHead.RemoteLSN && !l.supersededBy(baseHead) {
		unlockCapture()
		st.BarrierPauseNs = time.Since(pauseStart).Nanoseconds()
		// HEAD holds the image of a later LSN than this host's log has
		// reached: publishing would move remote_lsn backwards. Refused
		// before any upload. (A HEAD of a newer holder fences below.)
		l.abortCut(all)
		dropCapturedIntents(ctx, intents)
		return chunkCutResult{}, fmt.Errorf("%w: %w: cut LSN %d, HEAD remote_lsn %d", errChunkCaptureRefused, errChunkLSNBehind, h, baseHead.RemoteLSN)
	}

	st.CandidateChunks = len(candidates)
	var copyErr error
	copyStart := time.Now()
	bytesBeforeCopy := st.UploadedBytes
	{
		var w *cutCOWWindow
		windowStart := time.Now()
		w, copyErr = l.installCOWWindow(candidates, useStaged, baseHead.Chunks, spill)
		if copyErr == nil {
			w.budget = budget
		}
		unlockCapture()
		st.WindowSetupNs += time.Since(windowStart).Nanoseconds()
		st.BarrierPauseNs = time.Since(pauseStart).Nanoseconds()
		copyStart = time.Now()
		if copyErr == nil {
			var copied []capturedChunk
			var ids []string
			copied, ids, copyErr = l.copyCOWWindow(ctx, w, births, capturedVersions, baseHead)
			intents = append(intents, ids...)
			for _, c := range copied {
				if c.key != "" {
					st.UploadedChunks++
					st.UploadedBytes += int64(c.size)
				}
			}
			captured = append(captured, copied...)
			st.COWWALReadAttempts, st.COWLocalReadNs = w.walReads.Load(), w.localReadNs.Load()
			st.COWSavedChunks = int(w.saved.Load())
			st.COWSaveNs, st.COWWaitNs = w.saveNs.Load(), w.waitNs.Load()
		}
	}

	st.CopyNs = time.Since(copyStart).Nanoseconds()
	if st.UploadedBytes > bytesBeforeCopy {
		st.PayloadNs += st.CopyNs
	} else {
		st.NonPayloadCopyNs += st.CopyNs
	}
	st.UploadNs = st.CopyNs
	if copyErr != nil {
		l.abortCut(all)
		dropCapturedIntents(ctx, intents)
		return chunkCutResult{}, fmt.Errorf("storage: capture copy: %w", copyErr)
	}
	if b := l.cfg.BarrierBound; b > 0 && st.BarrierCopyChunks > b {
		st.BarrierOverBound = true
	}

	// Upload only what changed against HEAD.
	prepareStart := time.Now()
	next := copyHead(baseHead)
	next.Epoch, next.Holder, next.HolderRoot, next.RemoteLSN = l.cfg.Epoch, l.cfg.Holder, l.cfg.HolderRoot, h
	next.HolderRootGen = baseHead.Gen + 1
	next.State, next.DetachedBy = chunkHeadActive, nil
	if detach {
		next.State = chunkHeadDetached
		next.DetachedBy = &chunkDetachedBy{Holder: l.cfg.Holder, HolderRoot: l.cfg.HolderRoot, Epoch: l.cfg.Epoch, Gen: baseHead.Gen + 1}
	}
	var changed []uint64
	newKeys := map[uint64]string{}
	type upload struct {
		c   capturedChunk
		key string
	}
	var uploads []upload
	for _, c := range captured {
		c.birth = births[c.idx]
		ref, had := baseHead.Chunks[c.idx]
		switch {
		case c.zero && !had, unchangedFromHead(c, ref, had):
			st.UnchangedChunks++
			continue
		case c.zero:
			delete(next.Chunks, c.idx)
			changed = append(changed, c.idx)
			st.ZeroedChunks++
			continue
		}
		if c.key != "" { // staged: uploaded before the barrier
			next.Chunks[c.idx] = chunkRef{Key: c.key, SHA256: c.sum, Encoding: "full", BirthLSN: c.birth}
			newKeys[c.idx] = c.key
			changed = append(changed, c.idx)
			if useStaged[c.idx] {
				st.StagedChunksUsed++
			}
			continue
		}
		uploads = append(uploads, upload{c: c, key: l.newKey(c.idx)})
	}
	// The intent names every key this cut uploads and is durable before
	// the first upload: GC treats it as a root until the cut is settled.
	uploadStart := time.Now()
	if len(uploads) > 0 {
		keys := make([]string, len(uploads))
		for i, u := range uploads {
			keys[i] = u.key
		}
		id, err := l.remote.PutIntent(ctx, chunkIntent{Gen: baseHead.Gen, Epoch: l.cfg.Epoch, Holder: l.cfg.Holder, Keys: keys})
		if err != nil {
			l.abortCut(all)
			dropCapturedIntents(ctx, intents)
			return chunkCutResult{}, fmt.Errorf("storage: capture intent: %w: %w", errChunkRemoteUnavailable, err)
		}
		intents = append(intents, id)
	}
	st.PublishPrepareNs += time.Since(prepareStart).Nanoseconds()
	payloadStart := time.Now()
	put := l.beginPutRound(len(uploads))
	if err := l.uploadBatch(ctx, len(uploads), func(ctx context.Context, i int) error {
		u := uploads[i]
		return l.putCapturedWith(ctx, u.key, u.c.data, func(ctx context.Context) error { return put(ctx, i, u.key, u.c.data) })
	}); err != nil {
		l.abortCut(all)
		dropCapturedIntents(ctx, intents)
		return chunkCutResult{}, fmt.Errorf("storage: capture upload: %w", errors.Join(errChunkRemoteUnavailable, errCutUpload, err))
	}
	if len(uploads) > 0 {
		st.PayloadNs += time.Since(payloadStart).Nanoseconds()
	}
	prepareStart = time.Now()
	for _, u := range uploads {
		next.Chunks[u.c.idx] = chunkRef{Key: u.key, SHA256: u.c.sum, Encoding: "full", BirthLSN: u.c.birth}
		newKeys[u.c.idx] = u.key
		changed = append(changed, u.c.idx)
		st.UploadedChunks++
		st.UploadedBytes += int64(max(len(u.c.data), u.c.size))
	}
	st.PublishPrepareNs += time.Since(prepareStart).Nanoseconds()
	upgrade := false
	if builder, ok := l.remote.(ChunkManifestBuilder); ok {
		upgrade = baseHead.Segments == nil
		var ids []string
		var err error
		manifestStart := time.Now()
		next, ids, err = builder.PrepareManifest(ctx, baseHead, next, uint64(l.blockSize)*l.chunkBlocks)
		st.ManifestNs += time.Since(manifestStart).Nanoseconds()
		intents = append(intents, ids...)
		if err != nil {
			l.abortCut(all)
			dropCapturedIntents(ctx, intents)
			return chunkCutResult{}, err
		}
	}
	st.UploadNs += time.Since(uploadStart).Nanoseconds()
	if !upgrade && !detach && len(changed) == 0 && h == baseHead.RemoteLSN && baseHead.Gen != 0 && !chunkHeadIsDetached(baseHead) {
		// A no-op cut still observes authority: a fresh host may have
		// claimed HEAD without uploading a single chunk since our last cut.
		cur, err := readCapturedHead()
		if err != nil {
			l.abortCut(all)
			dropCapturedIntents(ctx, intents)
			return chunkCutResult{}, fmt.Errorf("storage: no-op cut authority read: %w: %w", errChunkRemoteUnavailable, err)
		}
		if cur.Gen != baseHead.Gen || !sameProposal(cur, baseHead) {
			l.abortCut(all)
			l.stMu.Lock()
			l.pendingIntents = append(l.pendingIntents, intents...)
			l.stMu.Unlock()
			l.fencePublication(cur)
			return chunkCutResult{}, errChunkLayerFenced
		}
		finishCapturedCut(baseHead, h, nil, nil) // nothing new: no CAS
		persistCapturedMap(capturedVersions)
		dropCapturedIntents(ctx, intents) // superseded staged uploads only
		return chunkCutResult{H: h, Head: copyHead(baseHead), Noop: true, FullHash: all}, nil
	}
	if err := l.admit(); err != nil { // admission re-checked right before the CAS
		l.abortCut(all)
		dropCapturedIntents(ctx, intents)
		return chunkCutResult{}, err
	}
	if l.supersededBy(baseHead) {
		l.abortCut(all)
		dropCapturedIntents(ctx, intents)
		l.stMu.Lock()
		l.fenced = true
		l.stMu.Unlock()
		l.abortCOW(errChunkLayerFenced)
		return chunkCutResult{}, errChunkLayerFenced
	}
	expected := chunkHeadVersion{Gen: baseHead.Gen, Epoch: baseHead.Epoch}
	casStart := time.Now()
	gen, casErr := casAndConfirmChunkHeadMeasured(ctx, l.remote, l.cfg.BeforeHeadCAS, expected, next, &st)
	st.CASNs = time.Since(casStart).Nanoseconds()
	st.CASResult = "ok"
	if errors.Is(casErr, errChunkHeadPrecondition) {
		st.CASResult = "precondition"
	} else if casErr != nil {
		st.CASResult = "unknown"
	}
	if casErr == nil {
		next.Gen = gen
		finishCapturedCut(next, h, changed, newKeys)
		persistCapturedMap(capturedVersions)
		dropCapturedIntents(ctx, intents)
		return l.cutDone(chunkCutResult{H: h, Head: copyHead(next), FullHash: all}, detach), nil
	}
	// Precondition or unknown outcome: resolve by re-reading HEAD, never by
	// a blind retry.
	keepPending := func() {
		l.stMu.Lock()
		p := copyHead(next)
		l.pending = &p
		l.pendingDetach, l.pendingCut, l.pendingBase = detach, chunkCutResult{H: h, FullHash: all}, expected
		l.pendingIntents = intents // kept until the proposal is settled
		l.stMu.Unlock()
		l.abortCut(all)
	}
	if errors.Is(casErr, errChunkLayerFenced) {
		keepPending()
		l.fencePublication(chunkHead{}) // confirmChunkHead already logged the observed root.
		return chunkCutResult{}, casErr
	}
	cur, gerr := readCapturedHead()
	if gerr != nil {
		keepPending()
		return chunkCutResult{}, fmt.Errorf("storage: capture CAS outcome unresolved (%v): %w: %w", casErr, errChunkCASOutcomeUnknown, gerr)
	}
	if !sameProposal(cur, next) && l.publicationOwnerChanged(cur) {
		keepPending() // Close must keep these roots as well.
		l.fencePublication(cur)
		return chunkCutResult{}, errors.Join(errChunkLayerFenced, casErr)
	}
	if !errors.Is(casErr, errChunkHeadPrecondition) && !sameProposal(cur, next) && cur.Gen == expected.Gen && cur.Epoch == expected.Epoch {
		// HEAD is still the CAS's base and the server gave no definite
		// refusal: the CAS may still land. Settle it before any later
		// capture or durability point (resolvePending re-proposes it).
		keepPending()
		return chunkCutResult{}, fmt.Errorf("storage: capture CAS outcome unresolved (%v): HEAD unchanged: %w", casErr, errChunkCASOutcomeUnknown)
	}
	if sameProposal(cur, next) {
		st.CASResult = "resolved-by-reread"
		finishCapturedCut(cur, h, changed, newKeys)
		persistCapturedMap(capturedVersions)
		dropCapturedIntents(ctx, intents)
		return l.cutDone(chunkCutResult{H: h, Head: copyHead(cur), FullHash: all}, detach), nil
	}
	// HEAD moved past the CAS's base without it: it can never land.
	l.abortCut(all)
	dropCapturedIntents(ctx, intents)
	if l.supersededBy(cur) {
		l.stMu.Lock()
		l.fenced = true
		l.head = copyHead(cur)
		l.stMu.Unlock()
		l.abortCOW(errChunkLayerFenced)
		return chunkCutResult{}, errors.Join(errChunkLayerFenced, casErr)
	}
	l.stMu.Lock()
	l.head = copyHead(cur)
	l.stMu.Unlock()
	// HEAD moved past the CAS's base without it, and not to a holder that
	// supersedes this one: a head conflict, whatever error the CAS returned.
	if !errors.Is(casErr, errChunkHeadPrecondition) {
		casErr = fmt.Errorf("%w (%w)", errChunkHeadPrecondition, casErr)
	}
	return chunkCutResult{}, fmt.Errorf("storage: capture CAS not applied: %w", casErr)
}

func (l *chunkLayerImpl) Detach(ctx context.Context) (chunkCutResult, error) {
	l.capMu.Lock()
	defer l.unlockCap()
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

// SettleDetach: see chunkLayer. Only an unresolved DETACH proposal needs a
// remote decision: if HEAD is still that CAS's base, it is cancelled by a CAS
// at the same base to the non-detached base image (this holder, this epoch),
// so whichever lands first decides and the detach can never land after a
// successful cancel. resolvePending would re-propose the detach instead.
func (l *chunkLayerImpl) SettleDetach(ctx context.Context) (detachSettlement, chunkCutResult, error) {
	l.capMu.Lock()
	defer l.unlockCap()
	if r, ok := l.storedDetach(); ok {
		return detachApplied, r, nil
	}
	l.stMu.Lock()
	fenced, pending, isDetach, base, intents := l.fenced, l.pending, l.pendingDetach, l.pendingBase, l.pendingIntents
	baseHead := copyHead(l.head)
	l.stMu.Unlock()
	if fenced {
		return detachFenced, chunkCutResult{}, nil
	}
	if pending == nil || !isDetach {
		// Nothing of this detach can land: it failed before its CAS or was
		// definitely refused (an unresolved ordinary cut settles as usual).
		return detachNotPublished, chunkCutResult{}, nil
	}
	cur, err := readChunkHead(ctx, l.remote)
	if err != nil {
		return 0, chunkCutResult{}, fmt.Errorf("storage: detach outcome unresolved: %w: %w", errChunkCASOutcomeUnknown, err)
	}
	if !sameProposal(cur, *pending) && !l.publicationOwnerChanged(cur) && cur.Gen == base.Gen && cur.Epoch == base.Epoch {
		if err := l.admit(); err != nil {
			return 0, chunkCutResult{}, fmt.Errorf("storage: detach outcome unresolved (%w); cancel refused: %w", errChunkCASOutcomeUnknown, err)
		}
		cancel := copyHead(baseHead)
		cancel.Epoch, cancel.Holder, cancel.HolderRoot = l.cfg.Epoch, l.cfg.Holder, l.cfg.HolderRoot
		cancel.HolderRootGen = base.Gen + 1
		cancel.State, cancel.DetachedBy = chunkHeadActive, nil
		gen, casErr := casAndConfirmChunkHead(ctx, l.remote, l.cfg.BeforeHeadCAS, base, cancel)
		if casErr == nil {
			cancel.Gen = gen
			cur = cancel
		} else if errors.Is(casErr, errChunkLayerFenced) {
			l.fencePublication(chunkHead{})
			return detachFenced, chunkCutResult{}, nil
		} else if cur, err = readChunkHead(ctx, l.remote); err != nil {
			return 0, chunkCutResult{}, fmt.Errorf("storage: detach outcome unresolved (cancel: %v): %w: %w", casErr, errChunkCASOutcomeUnknown, err)
		} else if !sameProposal(cur, *pending) && !l.publicationOwnerChanged(cur) && cur.Gen == base.Gen && cur.Epoch == base.Epoch {
			return 0, chunkCutResult{}, fmt.Errorf("storage: detach outcome unresolved (cancel: %v): HEAD unchanged: %w", casErr, errChunkCASOutcomeUnknown)
		}
	}
	if !sameProposal(cur, *pending) && l.publicationOwnerChanged(cur) {
		l.fencePublication(cur)
		return detachFenced, chunkCutResult{}, nil
	}
	// Settled: the detach landed, or it can never land.
	l.stMu.Lock()
	l.reusable = nil
	l.pendingIntents = nil // settled here; allow deletion of these roots below.
	l.stMu.Unlock()
	l.dropIntents(ctx, intents)
	l.stMu.Lock()
	defer l.stMu.Unlock()
	cut := l.pendingCut
	l.pending, l.pendingIntents = nil, nil
	l.pendingDetach, l.pendingCut, l.pendingBase = false, chunkCutResult{}, chunkHeadVersion{}
	switch {
	case sameProposal(cur, *pending):
		l.noteSettled(l.head, cur)
		l.head, l.remoteLSN = copyHead(cur), cur.RemoteLSN
		cut.Head = copyHead(cur)
		l.detached = &cut
		r := cut
		r.Head = copyHead(cur)
		return detachApplied, r, nil
	case l.supersededBy(cur):
		l.fenced = true
		l.head = copyHead(cur)
		return detachFenced, chunkCutResult{}, nil
	default:
		l.head = copyHead(cur)
		return detachNotPublished, chunkCutResult{}, nil
	}
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
	return headSupersedes(cur, l.cfg.Epoch, l.cfg.Holder, l.cfg.HolderRoot, holder.Same)
}

// headSupersedes reports whether cur belongs to a newer epoch or, at this
// epoch, to an owner that owns(cur's holder and root, ours) rejects: the one
// supersede rule of the running layer (owns = holder.Same) and of a claim
// (any root of this holder may claim; an existing root's lineage is
// PrepareRestore's: owns = holder.SameAssignment on the holders).
func headSupersedes(cur chunkHead, epoch uint64, holderID, root string, owns func(curHolder, curRoot, holderID, root string) bool) bool {
	return cur.Epoch > epoch || (cur.Epoch == epoch && cur.Gen != 0 && !owns(cur.Holder, cur.HolderRoot, holderID, root))
}

func sameProposal(cur, next chunkHead) bool {
	if (cur.Segments == nil) != (next.Segments == nil) || cur.ChunkBytes != next.ChunkBytes || cur.ManifestChunkCount != next.ManifestChunkCount || len(cur.Segments) != len(next.Segments) {
		return false
	}
	for k, v := range next.Segments {
		if cur.Segments[k] != v {
			return false
		}
	}

	if cur.Epoch != next.Epoch || !holder.Same(cur.Holder, cur.HolderRoot, next.Holder, next.HolderRoot) || cur.HolderRootGen != next.HolderRootGen || cur.RemoteLSN != next.RemoteLSN || len(cur.Chunks) != len(next.Chunks) {
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
func (l *chunkLayerImpl) resolvePending(ctx context.Context) (err error) {
	// Registered before the state unlock, so invalidation runs without stMu.
	defer func() {
		if errors.Is(err, errChunkLayerFenced) {
			l.abortCOW(err)
		}
	}()
	l.stMu.Lock()
	pending, base, intents := l.pending, l.pendingBase, l.pendingIntents
	l.stMu.Unlock()
	if pending == nil {
		return nil
	}
	cur, err := readChunkHead(ctx, l.remote)
	if err != nil {
		return fmt.Errorf("storage: capture: earlier CAS outcome still unresolved: %w: %w", errChunkCASOutcomeUnknown, err)
	}
	if !sameProposal(cur, *pending) && !l.publicationOwnerChanged(cur) && cur.Gen == base.Gen && cur.Epoch == base.Epoch {
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
		gen, casErr := casAndConfirmChunkHead(ctx, l.remote, l.cfg.BeforeHeadCAS, base, copyHead(*pending))
		if casErr == nil {
			cur = copyHead(*pending)
			cur.Gen = gen
		} else if errors.Is(casErr, errChunkLayerFenced) {
			l.fencePublication(chunkHead{})
			return errChunkLayerFenced
		} else if cur, err = readChunkHead(ctx, l.remote); err != nil {
			return fmt.Errorf("storage: capture: earlier CAS outcome still unresolved (%v): %w: %w", casErr, errChunkCASOutcomeUnknown, err)
		} else if !sameProposal(cur, *pending) && !l.publicationOwnerChanged(cur) && cur.Gen == base.Gen && cur.Epoch == base.Epoch {
			return fmt.Errorf("storage: capture: earlier CAS outcome still unresolved (%v): HEAD unchanged: %w", casErr, errChunkCASOutcomeUnknown)
		}
	}
	if !sameProposal(cur, *pending) && l.publicationOwnerChanged(cur) {
		l.fencePublication(cur)
		return errChunkLayerFenced
	}
	// Settled: the proposal landed or can never land.
	l.stMu.Lock()
	l.reusable = nil
	l.pendingIntents = nil // settled here; allow deletion of these roots below.
	l.stMu.Unlock()
	l.dropIntents(ctx, intents)
	l.stMu.Lock()
	defer l.stMu.Unlock()
	l.pending, l.pendingIntents = nil, nil
	wasDetach, cut := l.pendingDetach, l.pendingCut
	l.pendingDetach, l.pendingCut, l.pendingBase = false, chunkCutResult{}, chunkHeadVersion{}
	switch {
	case sameProposal(cur, *pending):
		l.noteSettled(l.head, cur)
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

// recordCut updates the cumulative counters after one cut attempt.
func (l *chunkLayerImpl) recordCut(r chunkCutResult, st chunkCutStats, err error) {
	l.stMu.Lock()
	defer l.stMu.Unlock()
	switch st.CASResult {
	case "precondition":
		l.stats.CASConflicts++
	case "unknown":
		if l.pending != nil { // the HEAD re-read did not settle it
			l.stats.CASUnknown++
		}
	}
	abortReason := ""
	switch {
	case errors.Is(err, errCutSaverCapacity):
		abortReason = "saver_capacity"
	case errors.Is(err, errCutFreeUnreadable):
		abortReason = "free_space_unreadable"
	case errors.Is(err, errCutNoRoom):
		abortReason = "no_room_to_cut"
	case errors.Is(err, errCutCOWCapacity):
		abortReason = "cow_capacity"
	case errors.Is(err, errCutBudgetRevoked):
		abortReason = "budget_revoked"
	case errors.Is(err, errCutUpload):
		abortReason = "upload"
	}
	if abortReason != "" {
		if l.stats.CutsAborted == nil {
			l.stats.CutsAborted = map[string]uint64{}
		}
		l.stats.CutsAborted[abortReason]++
	}
	reason := ""
	switch {
	case err == nil && r.Noop:
		l.stats.CutsNoop++
	case err == nil:
		l.stats.CutsOK++
	case errors.Is(err, errChunkReconciling):
		reason = "reconciling"
	case errors.Is(err, errChunkCaptureRefused):
		reason = "admission"
	case errors.Is(err, errChunkLayerFenced):
		reason = "fenced"
	case errors.Is(err, errChunkLayerDetached):
		reason = "detached"
	default:
		l.stats.CutsFailed++
	}
	if reason != "" {
		if l.stats.CutsRefused == nil {
			l.stats.CutsRefused = map[string]uint64{}
		}
		l.stats.CutsRefused[reason]++
	}
	if err == nil {
		l.stats.LastCut, l.stats.LastCutH, l.stats.LastCutNs = st, r.H, time.Now().UnixNano()
		l.stats.CutSeq++
	}
	if st.BarrierOverBound {
		l.stats.BarrierOverBound++
	}
}

func (l *chunkLayerImpl) Stats() chunkLayerStats {
	l.stMu.Lock()
	out := l.stats
	out.Hydration.WorkerLimit = l.cfg.HydrationWorkers
	out.Hydration.QueueDepth = l.cfg.HydrationQueueDepth
	for _, f := range l.hydration {
		if !f.running {
			out.Hydration.Queued++
		}
	}
	out.CutsRefused = make(map[string]uint64, len(l.stats.CutsRefused))
	for k, v := range l.stats.CutsRefused {
		out.CutsRefused[k] = v
	}
	out.CutsAborted = make(map[string]uint64, len(l.stats.CutsAborted))
	for k, v := range l.stats.CutsAborted {
		out.CutsAborted[k] = v
	}
	out.PendingUnresolved = l.pending != nil
	out.DirtyChunks = len(l.dirty)
	out.Reconciling = false
	out.ReconcileTotal = 0
	out.ReconcileDone = 0
	l.stMu.Unlock()

	out.DirtyChunks = len(l.captureCandidates())
	out.AbsentChunks = l.absentHydratable()

	return out
}

// dropIntent deletes this layer's own intent id (none if empty) once its
// cut is settled. A failed delete is kept and retried by the next cut; it
// never fails a cut whose outcome is already settled.
func (l *chunkLayerImpl) dropIntent(ctx context.Context, id string) {
	l.dropIntents(ctx, []string{id})
}

// readChunk copies chunk idx from the store (the barrier copy, the
// pre-copy staging and the reconciler use it).
func (l *chunkLayerImpl) readChunk(idx uint64) ([]byte, error) {
	buf := make([]byte, int(l.chunkBlocks)*l.blockSize)
	for i := uint64(0); i < l.chunkBlocks; i++ {
		lba := idx*l.chunkBlocks + i
		if lba >= uint64(l.s.NumBlocks()) {
			break
		}
		b, err := l.s.Read(uint32(lba))
		if err != nil {
			return nil, err
		}
		copy(buf[int(i)*l.blockSize:], b)
	}
	return buf, nil
}

// dropIntents drops every intent of a settled cut (staging rounds and
// the barrier).
func (l *chunkLayerImpl) dropIntents(ctx context.Context, ids []string) {
	// A wave roots several chunks with one intent. Build the live root set
	// once, rather than rescanning every cached chunk for each delete.
	l.stMu.Lock()
	keep := make(map[string]bool)
	for _, id := range l.pendingIntents {
		keep[id] = true
	}
	for idx, sc := range l.reusable {
		if l.closed || l.fenced || sc.ver != l.chunkVersion(idx) || sc.headGen != l.head.Gen {
			delete(l.reusable, idx)
			continue
		}
		keep[sc.intent] = true
	}
	l.stMu.Unlock()
	seen := make(map[string]bool)
	for _, id := range ids {
		if id == "" || keep[id] || seen[id] {
			continue
		}
		seen[id] = true
		if err := l.remote.DeleteIntent(ctx, id); err != nil {
			l.stMu.Lock()
			l.leftoverIntents = append(l.leftoverIntents, id)
			l.stMu.Unlock()
		}
	}
}

// stage uploads large native dirty sets through logical reads, before fixing H.
// It stops when the spill-budget remainder is reached or a round makes no
// progress. Versions/HEAD generation bind every reusable immutable result.
func (l *chunkLayerImpl) stage(ctx context.Context, st *chunkCutStats, spillCap int64) (map[uint64]stagedChunk, []string, error) {
	// Leave at most half the side budget exposed at the barrier. Streaming
	// staging uses no local payload file, even for a much larger image.
	bound := int(spillCap / (2 * int64(l.blockSize) * int64(l.chunkBlocks)))

	staged := map[uint64]stagedChunk{}
	var intents []string
	l.stMu.Lock()
	for idx, sc := range l.reusable {
		staged[idx] = sc
		if sc.intent != "" {
			intents = append(intents, sc.intent)
		}
	}
	l.stMu.Unlock()
	previous := int(^uint(0) >> 1)
	for round := 0; ; round++ {
		wal := l.captureCandidates()
		l.stMu.Lock()
		need := map[uint64]bool{}
		consider := func(idx uint64) {
			if sc, ok := staged[idx]; ok && sc.ver == l.chunkVersion(idx) && sc.headGen == l.head.Gen {
				return // still current
			}
			need[idx] = true
		}
		for idx := range wal {
			consider(idx)
		}
		for idx := range l.dirty {
			consider(idx)
		}
		if l.allDirty {
			for i := uint64(0); i < l.numChunks; i++ {
				if l.absent[i] && !l.chunkHasLocalLocked(i) {
					continue
				}
				consider(i)
			}
		}
		if len(need) <= bound || (len(need) >= previous) {
			l.stMu.Unlock()
			break
		}
		previous = len(need)
		recent := make(map[uint64]uint64, len(need))
		// Release layer state before sampling the store; never invert store -> stMu.
		l.stMu.Unlock()
		l.s.mu.RLock()

		for idx := range need {
			recent[idx] = l.s.v3.mapEntries[idx].LSN
		}

		l.s.mu.RUnlock()
		l.stMu.Lock()
		batch := hotChunkKeys(need, recent)

		vers := make(map[uint64]uint64, len(batch))
		for _, idx := range batch {
			vers[idx] = l.chunkVersion(idx) // before the read: a later mutation raises it
		}
		head := copyHead(l.head)
		l.stMu.Unlock()
		st.StageRounds++
		roundChunks, ids, err := l.stagePayloadRound(ctx, batch, vers, head, st)
		intents = append(intents, ids...)
		if err != nil {
			return nil, intents, err
		}
		for idx, sc := range roundChunks {
			staged[idx] = sc
		}
		st.StagedChunks += len(batch)
		if hook := l.testAfterStageHook; hook != nil {
			hook(round)
		}
	}
	return staged, intents, nil
}

// retryLeftoverIntents retries the deletes dropIntent could not make.
func (l *chunkLayerImpl) retryLeftoverIntents(ctx context.Context) {
	l.stMu.Lock()
	ids := l.leftoverIntents
	l.leftoverIntents = nil
	l.stMu.Unlock()
	for _, id := range ids {
		l.dropIntent(ctx, id)
	}
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
	l.stMu.Lock()
	defer l.stMu.Unlock()
	l.noteSettled(l.head, head)
	l.head, l.remoteLSN, l.pending = copyHead(head), h, nil
	l.reusable = nil
	l.inflight = map[uint64]bool{}
	l.hdr = chunkBitmapHeader{BaseLSN: h, HeadGen: head.Gen, Epoch: head.Epoch, Base: chunkBaseNone}
	l.dirtyVer++
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
	defer l.unlockCap()
	if idx >= l.numChunks {
		return fmt.Errorf("%w: chunk %d out of range", errChunkEvictRefused, idx)
	}
	// The same owned-map/WAL dirt that capture must publish also forbids
	// eviction: a later fill from the older HEAD must not overwrite it.
	dirtyCandidate := l.captureCandidates()[idx]
	l.stMu.Lock()
	switch {
	case l.head.Gen == 0, l.allDirty, l.hdr.Base != chunkBaseNone, l.dirty[idx], l.inflight[idx], dirtyCandidate:
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
	l.closeHydrationLocked()
	l.stMu.Unlock()
	l.cowMu.Lock()
	l.cowClosed = true
	w := l.cow
	if w != nil {
		w.abort(errChunkLayerClosed)
	}
	l.cowMu.Unlock()
	l.cancel()
	if w != nil {
		w.join()
	}
	if h := testCloseJoinHook; h != nil {
		h() // tests: Close has cancelled the layer and now joins its owners, the drainer among them
	}
	l.wg.Wait()
	if h := testCloseJoinedHook; h != nil {
		h() // tests: the join returned
	}
	l.stMu.Lock()
	protected := make(map[string]bool, len(l.pendingIntents))
	for _, id := range l.pendingIntents {
		protected[id] = true
	}
	var settled []string
	for _, sc := range l.reusable {
		if !protected[sc.intent] {
			settled = append(settled, sc.intent)
		}
	}
	for _, id := range l.leftoverIntents {
		if !protected[id] {
			settled = append(settled, id)
		}
	}
	l.reusable = nil
	l.leftoverIntents = nil
	l.stMu.Unlock()
	// Layer cancellation has joined all owners. Cleanup has a separate bounded
	// context; an unknown CAS retains its roots for later epoch fencing.
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
	if cleaner, ok := l.remote.(interface {
		DeleteSettledIntent(context.Context, string) error
	}); ok {
		seen := map[string]bool{}
		for _, id := range settled {
			if id == "" || seen[id] {
				continue
			}
			seen[id] = true
			_ = cleaner.DeleteSettledIntent(cleanupCtx, id)
		}
	} else {
		l.dropIntents(cleanupCtx, settled)
	}
	cleanupCancel()
	close(l.closeDone)
	return nil
}
