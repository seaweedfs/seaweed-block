package storage

// Cut-window ownership. Lock order: layer.cowMu -> window.mu -> entry.mu.
// None of these locks spans a read, side-store I/O, wait, or store lock.
// captureMu shared covers the write hook, but lifecycleMu is acquired AFTER it.
import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const cutCOWMemoryLimit = DefaultCutPayloadMemoryBudget

// defaultCOWSpillLimit caps a copy window's side-file bytes by default.
const defaultCOWSpillLimit = 1 << 30

// cowSpillFreeReserve is the free space a cut's side file never takes,
// on top of leaving half of the rest.
const cowSpillFreeReserve = 1 << 30

var (
	// errCutNoRoom: the free space leaves this cut no side-file room.
	errCutNoRoom = errors.New("storage: no room to cut")
	// errCutFreeUnreadable: the free space could not be read, so this cut
	// has no side-file room.
	errCutFreeUnreadable = errors.New("storage: free space unreadable")
)

// cowSpillCap is one cut's side-file cap. cause is set when the free space
// made it zero: errCutNoRoom or errCutFreeUnreadable.
type cowSpillCap struct {
	bytes int64
	cause error
}

// cowSpillLimit is this cut's side-file cap: COWSpillLimit, lowered to
// half the free space minus cowSpillFreeReserve when COWSpillFree is set.
// A read error gives a cap of 0 with its own cause.
func (l *chunkLayerImpl) cowSpillLimit() cowSpillCap {
	limit := l.cfg.COWSpillLimit
	if l.cfg.COWSpillFree == nil {
		return cowSpillCap{bytes: limit}
	}
	free, err := l.cfg.COWSpillFree()
	if err != nil {
		return cowSpillCap{cause: fmt.Errorf("%w: %w", errCutFreeUnreadable, err)}
	}
	c := cowSpillCap{bytes: max(0, min(limit, free/2-cowSpillFreeReserve))}
	if c.bytes == 0 {
		c.cause = errCutNoRoom
	}
	return c
}

// readCutSpill reads this cut's side-file cap once, for both the staging
// bound and the copy window, and records it in the stats.
func (l *chunkLayerImpl) readCutSpill() cowSpillCap {
	c := l.cowSpillLimit()
	reason := ""
	switch {
	case errors.Is(c.cause, errCutFreeUnreadable):
		reason = "free_space_unreadable"
	case errors.Is(c.cause, errCutNoRoom):
		reason = "no_room_to_cut"
	}
	l.stMu.Lock()
	l.stats.COWSpillCap, l.stats.COWSpillCapReason = c.bytes, reason
	l.stMu.Unlock()
	return c
}

const (
	cowPending uint8 = iota
	cowCopying
	cowSaving
	cowSaved
	cowDone
)

var errCutCOWAborted = errors.New("storage: cut copy window aborted; retry cut")

var (
	errCutSaverCapacity = errors.New("storage: saver capacity")
	errCutCOWCapacity   = errors.New("storage: COW capacity")
	errCutBudgetRevoked = errors.New("storage: cut budget revoked")
	errCutUpload        = errors.New("storage: cut upload failed")
)

type cutCOWEntry struct {
	mu           sync.Mutex
	state        uint8
	ready        chan struct{}
	data         []byte
	slot         int64
	spilled      bool
	readingSaved bool
	// missing lists the block offsets that were lazily absent when the bytes
	// were captured: their image at H is HEAD's, filled by the copier from the
	// cut's base HEAD after it released the entry (never under a guest write).
	missing []uint64
}
type cutCOWWindow struct {
	layer       *chunkLayerImpl
	budget      *cutBudgetReservation // immutable before captureMu releases writers
	mu          sync.Mutex
	terminal    bool
	err         error
	ended       chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc
	refs        sync.WaitGroup // Add under mu, only while OPEN; Wait only after terminal
	sideReady   chan struct{}
	cleanupErr  error
	entries     map[uint64]*cutCOWEntry
	head        map[uint64]chunkRef // the cut's base HEAD: the source of missing blocks
	memory      int64
	spillBytes  int64 // bytes written to the side file; at most spillLimit
	spillLimit  int64
	spillCause  error // why spillLimit is 0, when the free space made it so
	chunkBytes  int64
	memoryLimit int64
	saverSlots  chan struct{}
	side        *os.File
	sideErr     error
	cleanup     sync.Once
	walReads    atomic.Uint64
	localReadNs atomic.Int64
	saved       atomic.Int64
	saveNs      atomic.Int64
	waitNs      atomic.Int64
	// Scheduling/fault controls are nil in production and set before installation.
}

func (s *WALStore) beforeChunkMutation(first, count uint64) {
	if h := s.chunk.Load(); h != nil && h.beforeMutation != nil {
		h.beforeMutation(first, count)
	}
}

// Return with the requested capture barrier held. A first attach that races the
// optimistic hook snapshot forces admission/drain to be repeated with its hooks.
func (s *WALStore) holdChunkDestructive(exclusive bool) func() {
	for {
		h := s.chunk.Load()
		if hook := s.testDestructiveSnapshot; hook != nil {
			hook()
		}
		var release func()
		if h != nil && h.beforeDestructive != nil {
			release = h.beforeDestructive()
		}
		if exclusive {
			s.captureMu.Lock()
		} else {
			s.captureMu.RLock()
		}
		if s.chunk.Load() == h {
			return func() {
				if exclusive {
					s.captureMu.Unlock()
				} else {
					s.captureMu.RUnlock()
				}
				if release != nil {
					release()
				}
			}
		}
		if exclusive {
			s.captureMu.Unlock()
		} else {
			s.captureMu.RUnlock()
		}
		if release != nil {
			release()
		}
	}
}

func (l *chunkLayerImpl) holdCOWDestructive() func() {
	l.cowMu.Lock()
	l.cowTransitions++
	w := l.cow
	if w != nil {
		w.abort(errCutCOWAborted)
	}
	l.cowMu.Unlock()
	if w != nil {
		w.join()
	}
	return func() { l.cowMu.Lock(); l.cowTransitions--; l.cowMu.Unlock() }
}

// The caller owns captureMu exclusive, so no writer can miss this publication.
func (l *chunkLayerImpl) installCOWWindow(candidates, staged map[uint64]bool, head map[uint64]chunkRef, spill cowSpillCap) (*cutCOWWindow, error) {
	size := uint64(l.blockSize) * l.chunkBlocks
	if size == 0 || size > math.MaxInt64 || uint64(len(candidates)) > math.MaxInt64/size {
		return nil, fmt.Errorf("%w: side-store geometry", errCutCOWAborted)
	}
	w := &cutCOWWindow{layer: l, ended: make(chan struct{}), entries: make(map[uint64]*cutCOWEntry), head: head, spillLimit: spill.bytes, spillCause: spill.cause, chunkBytes: int64(size)}
	w.ctx, w.cancel = context.WithCancel(context.Background())
	_, reserved, limit := l.cutPayloadWorkers()
	savers := min(max(1, int((16<<20)/int64(size))), int((limit-reserved)/(2*int64(size))))
	// Active saver buffers are reserved independently of upload concurrency.
	// (4*U + 2*S)*chunkBytes + cached saves <= memoryLimit.
	reserved += int64(2*savers) * int64(size)
	w.memory, w.memoryLimit = reserved, limit
	w.saverSlots = make(chan struct{}, savers)
	var slot int64
	for _, idx := range sortedKeys(candidates) {
		state := cowPending
		if staged[idx] {
			state = cowDone
		}
		w.entries[idx] = &cutCOWEntry{state: state, ready: make(chan struct{}), slot: slot}
		slot++
	}
	l.cowMu.Lock()
	defer l.cowMu.Unlock()
	if l.cowClosed || l.cowTransitions != 0 {
		return nil, errCutCOWAborted
	}
	l.cow = w
	l.cowActive.Store(w)
	return w, nil
}

func (w *cutCOWWindow) abort(err error) {
	w.mu.Lock()
	if !w.terminal {
		w.terminal, w.err = true, fmt.Errorf("%w: %w", errCutCOWAborted, err)
		close(w.ended)
		w.cancel()
	}
	w.mu.Unlock()
}
func (w *cutCOWWindow) join() error {
	w.refs.Wait()
	// Only terminal windows are joined. Saved readers never outlive their ref.
	w.cleanup.Do(func() {
		if w.side != nil {
			name := w.side.Name()
			w.cleanupErr = errors.Join(w.side.Close(), os.Remove(name))
		}
		for _, e := range w.entries {
			e.data = nil
		}
	})
	return w.cleanupErr
}
func (w *cutCOWWindow) status() error { w.mu.Lock(); defer w.mu.Unlock(); return w.err }

// A nil result without a notification means done/terminal. Waiters own a
// lifetime reference through wakeup/accounting, never an entry or I/O lock.
func (w *cutCOWWindow) claim(idx uint64, saving bool) (*cutCOWEntry, uint8, <-chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.terminal {
		return nil, cowDone, nil
	}
	e := w.entries[idx]
	if e == nil {
		return nil, cowDone, nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	state := e.state
	switch state {
	case cowPending:
		if saving {
			e.state = cowSaving
		} else {
			e.state = cowCopying
		}
		w.refs.Add(1)
		return e, state, nil
	case cowSaved:
		if saving {
			return nil, cowDone, nil
		} // live bytes are already saved
		if e.readingSaved {
			w.refs.Add(1)
			return nil, state, e.ready
		}
		e.readingSaved = true
		w.refs.Add(1)
		return e, state, nil
	case cowSaving, cowCopying:
		// Waiters acquire while OPEN too: drain includes their accounting
		// before the coordinator snapshots aggregate wait time.
		w.refs.Add(1)
		return nil, state, e.ready
	default:
		return nil, cowDone, nil
	}
}
func (w *cutCOWWindow) finish(e *cutCOWEntry, state uint8) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	e.mu.Lock()
	defer e.mu.Unlock()
	if w.terminal {
		return false
	}
	e.state = state
	e.readingSaved = false
	close(e.ready)
	e.ready = make(chan struct{})
	return true
}
func (w *cutCOWWindow) wait(ch <-chan struct{}) {
	defer w.refs.Done()
	start := time.Now()
	if hook := w.layer.testCOWWait; hook != nil {
		hook()
	}
	select {
	case <-ch:
	case <-w.ended:
	}
	w.waitNs.Add(time.Since(start).Nanoseconds())
}

// Saver admission is bounded independently of local physical I/O. Expiry
// drops only this cut; it never fails the guest mutation.
const cutSaverWaitLimit = 250 * time.Millisecond

func (w *cutCOWWindow) acquireSaver() bool {
	select {
	case w.saverSlots <- struct{}{}:
		return true
	default:
	}
	start := time.Now()
	timer := time.NewTimer(cutSaverWaitLimit)
	defer timer.Stop()
	defer func() { w.waitNs.Add(time.Since(start).Nanoseconds()) }()
	if hook := w.layer.testCOWSaverWait; hook != nil {
		hook()
	}
	select {
	case w.saverSlots <- struct{}{}:
		return true
	case <-w.ended:
		return false
	case <-w.ctx.Done():
		return false
	case <-timer.C:
		w.abort(errors.Join(errCutCOWAborted, errCutSaverCapacity))
		return false
	}
}

func (l *chunkLayerImpl) beforeCOWMutation(first, count uint64) {
	w := l.cowActive.Load()
	if w == nil || count == 0 {
		return
	}
	last := (first + count - 1) / l.chunkBlocks
	for idx := first / l.chunkBlocks; idx <= last; idx++ {
		for {
			e, _, wait := w.claim(idx, true)
			if wait != nil {
				w.wait(wait)
				continue
			}
			if e == nil {
				break
			}
			// Wait briefly for an existing local saver, never for remote I/O.
			// The claimed entry owns no payload until a slot becomes available.
			if !w.acquireSaver() {
				w.refs.Done()
				break
			}
			if w.status() != nil {
				<-w.saverSlots
				w.refs.Done()
				break
			}
			start := time.Now()
			// Local blocks only: a guest write never waits on SeaweedFS.
			data, missing, err := l.readCOWChunk(w, idx)
			if err == nil {
				err = w.save(e, data, missing)
			}
			if err != nil {
				w.abort(err)
			} else if w.finish(e, cowSaved) {
				w.saved.Add(1)
			}
			w.saveNs.Add(time.Since(start).Nanoseconds())
			// In particular an aborting saver releases itself BEFORE anyone joins.
			w.refs.Done()
			<-w.saverSlots
			break
		}
	}
}

func (w *cutCOWWindow) save(e *cutCOWEntry, data []byte, missing []uint64) error {
	// No window lock spans the host callback: it may revoke this or another cut.
	if w.budget != nil {
		if err := w.budget.reserve(w.chunkBytes); err != nil {
			return fmt.Errorf("%w: COW budget reservation: %w", errors.Join(errCutCOWAborted, errCutBudgetRevoked), err)
		}
	}
	if hook := w.layer.testCOWSave; hook != nil {
		if err := hook(); err != nil {
			return err
		}
	}
	w.mu.Lock()
	if w.terminal {
		w.mu.Unlock()
		return errCutCOWAborted
	}
	inMemory := w.memory+w.chunkBytes <= w.memoryLimit
	if inMemory {
		w.memory += w.chunkBytes
	} else if w.spillBytes+w.chunkBytes > w.spillLimit {
		// The side file is sparse but its saved chunks are real bytes on the
		// volume's filesystem: abort the cut (retried) instead of filling it.
		w.mu.Unlock()
		if w.spillCause != nil {
			return fmt.Errorf("%w: %w: %w: COW spill cap is 0", errCutCOWAborted, errCutCOWCapacity, w.spillCause)
		}
		return fmt.Errorf("%w: %w: COW spill would exceed its cap of %d bytes", errCutCOWAborted, errCutCOWCapacity, w.spillLimit)
	} else {
		w.spillBytes += w.chunkBytes
	}
	w.mu.Unlock()
	// e.missing (and e.data / the spilled bytes) are published to readers by
	// w.finish(e, cowSaved), which takes w.mu and e.mu after these writes: a
	// copier reads them only after claiming the entry in state cowSaved.
	e.missing = missing
	if inMemory {
		e.data = data
		return nil
	}
	if err := w.openSide(); err != nil {
		return err
	}
	if w.sideErr != nil {
		return w.sideErr
	}
	n, err := w.side.WriteAt(data, e.slot*w.chunkBytes)
	if err == nil && n != len(data) {
		err = fmt.Errorf("short COW side write: %d/%d", n, len(data))
	}
	e.spilled = true
	return err
}

// readCOWChunk reads chunk idx's LOCAL blocks and never fetches: a block that
// is lazily absent (no local version, HEAD names the chunk) is left zero and
// listed in missing; its image at H is HEAD's by definition.
func (l *chunkLayerImpl) readCOWChunk(w *cutCOWWindow, idx uint64) ([]byte, []uint64, error) {
	start := time.Now()
	defer func() { w.localReadNs.Add(time.Since(start).Nanoseconds()) }()
	buf := make([]byte, int(w.chunkBytes))
	var missing []uint64
	for i := uint64(0); i < l.chunkBlocks; i++ {
		select {
		case <-w.ended:
			return nil, nil, errCutCOWAborted
		default:
		}
		lba := idx*l.chunkBlocks + i
		if lba >= uint64(l.s.NumBlocks()) {
			break
		}
		b, local, err := l.readLocalBlock(lba, w, buf[int(i)*l.blockSize:int(i+1)*l.blockSize])
		if err != nil {
			return nil, nil, err
		}
		if !local {
			missing = append(missing, i)
			continue
		}
		copy(buf[int(i)*l.blockSize:], b)
	}
	return buf, missing, nil
}

// readLocalBlock reads lba unless it is lazily absent (local=false): the dirty
// map/WAL first, then the extent of a present block (or of a block written
// since its chunk became absent), zeros for an absent chunk HEAD stores as
// ZERO. It never calls the miss hook. The order is safe against a concurrent
// flush (dirty map before presence) and fill (HEAD's bytes either way); a new
// write of the block waits for the caller's COW entry.
func (l *chunkLayerImpl) readLocalBlock(lba uint64, w *cutCOWWindow, dst []byte) ([]byte, bool, error) {
	s := l.s
	if data, handled, err := s.readDirtyV3Into(uint32(lba), dst, func() { w.walReads.Add(1) }); handled {
		return data, err == nil, err
	}
	idx := l.chunkOf(lba)
	l.stMu.Lock()
	absent := l.absent[idx] && !l.localBlocks[lba]
	_, inHead := l.head.Chunks[idx]
	l.stMu.Unlock()
	if absent && inHead {
		return nil, false, nil
	}
	if absent {
		return make([]byte, l.blockSize), true, nil
	}
	data, err := s.readExtentBlock(uint32(lba))
	return data, err == nil, err
}

// fillMissing completes a COW capture: the blocks listed missing get the cut's
// base HEAD bytes. It runs after the local entry is released, bounded by
// ReadMissTimeout and the owning cut's cancellation.
func (l *chunkLayerImpl) fillMissing(ctx context.Context, base map[uint64]chunkRef, idx uint64, data []byte, missing []uint64) ([]byte, error) {
	ref, ok := base[idx]
	if !ok {
		return nil, fmt.Errorf("storage: cut chunk %d: lazily absent blocks but HEAD has no chunk", idx)
	}
	ctx, cancel := context.WithTimeout(ctx, l.cfg.ReadMissTimeout)
	defer cancel()
	head, err := l.remote.Get(ctx, ref.Key)
	if err != nil {
		return nil, fmt.Errorf("storage: cut chunk %d HEAD fetch: %w", idx, err)
	}
	if sha256.Sum256(head) != ref.SHA256 || len(head) != int(l.chunkBlocks)*l.blockSize {
		return nil, fmt.Errorf("storage: cut chunk %d HEAD fetch: content does not match HEAD", idx)
	}
	out := append([]byte(nil), data...)
	for _, i := range missing {
		copy(out[int(i)*l.blockSize:int(i+1)*l.blockSize], head[int(i)*l.blockSize:int(i+1)*l.blockSize])
	}
	return out, nil
}

func (l *chunkLayerImpl) copyCOWEntry(w *cutCOWWindow, idx uint64) (capturedChunk, bool, error) {
	for {
		if err := l.admit(); err != nil {
			w.abort(err)
			return capturedChunk{}, false, err
		}
		e, state, wait := w.claim(idx, false)
		if wait != nil {
			w.wait(wait)
			continue
		}
		if e == nil {
			return capturedChunk{}, false, w.status()
		}
		var data []byte
		var missing []uint64
		var err error
		if state == cowSaved {
			missing = e.missing
			if e.spilled {
				data = make([]byte, int(w.chunkBytes))
				_, err = w.side.ReadAt(data, e.slot*w.chunkBytes)
			} else {
				data = e.data
			}
		} else {
			data, missing, err = l.readCOWChunk(w, idx)
		}
		if hook := l.testCOWRead; hook != nil {
			hook(idx, state == cowSaved)
		}
		if err == nil {
			err = l.admit()
		}
		if err != nil {
			w.abort(err)
		}
		kept := false
		if err == nil {
			kept = w.finish(e, cowDone)
		}
		w.refs.Done()
		if !kept {
			return capturedChunk{}, false, w.status()
		}
		return capturedChunk{idx: idx, data: data, sum: sha256.Sum256(data), zero: isAllZero(data), missing: missing}, true, nil
	}
}

// Each worker owns one immutable image from local read through remote Put.
// Once copied, its entry is done: guest writes need not wait for remote I/O.
// The window remains installed for candidates not yet read by a worker.
func (l *chunkLayerImpl) copyCOWWindow(ctx context.Context, w *cutCOWWindow, recent, versions map[uint64]uint64, head chunkHead) ([]capturedChunk, []string, error) {
	parent := ctx
	abortStop := context.AfterFunc(parent, func() { w.abort(parent.Err()) })
	defer abortStop()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(w.ctx, cancel)
	defer stop()
	defer cancel()
	defer func() {
		w.abort(errCutCOWAborted)
		w.join()
		l.cowMu.Lock()
		if l.cow == w {
			l.cow = nil
			l.cowActive.Store(nil)
		}
		l.cowMu.Unlock()
	}()
	indices := make(map[uint64]bool, len(w.entries))
	for idx, e := range w.entries {
		e.mu.Lock()
		done := e.state == cowDone
		e.mu.Unlock()
		if !done {
			indices[idx] = true
		}
	}
	order := hotChunkKeys(indices, recent)
	out, intents, err := l.streamCutPayloads(ctx, order, head, func(ctx context.Context, idx uint64) (capturedChunk, error) {
		c, ok, err := l.copyCOWEntry(w, idx)
		if err != nil {
			return capturedChunk{}, err
		}
		if !ok {
			return capturedChunk{}, errCutCOWAborted
		}
		if len(c.missing) > 0 {
			c.data, err = l.fillMissing(ctx, w.head, c.idx, c.data, c.missing)
			if err != nil {
				return capturedChunk{}, errors.Join(errCutCOWAborted, err)
			}
			c.sum, c.zero, c.missing = sha256.Sum256(c.data), isAllZero(c.data), nil
		}
		c.version = versions[idx]
		return c, nil
	})
	w.mu.Lock()
	if !w.terminal {
		w.terminal = true
		close(w.ended)
		w.cancel()
	}
	err = errors.Join(err, w.err)
	w.mu.Unlock()
	err = errors.Join(err, w.join())
	if err != nil {
		return nil, intents, err
	}
	return out, intents, nil
}

// Creation is single-owner, but no allocation/lifetime lock spans filesystem I/O.
func (w *cutCOWWindow) openSide() error {
	w.mu.Lock()
	if w.sideReady != nil {
		ready := w.sideReady
		w.mu.Unlock()
		select {
		case <-ready:
			return w.sideErr
		case <-w.ended:
			return errCutCOWAborted
		}
	}
	w.sideReady = make(chan struct{})
	w.mu.Unlock()
	side, err := os.CreateTemp(filepath.Dir(w.layer.s.path), filepath.Base(w.layer.s.path)+".cut-cow-")
	w.mu.Lock()
	w.side, w.sideErr = side, err
	close(w.sideReady)
	w.mu.Unlock()
	return err
}

// Called only by an exclusive, locked RW open. Side payloads are never decoded
// or imported, and directories/symlinks/other volume prefixes are untouched.
func discardCutCOWFiles(path string) error {
	dir := filepath.Dir(path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	prefix := filepath.Base(path) + ".cut-cow-"
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), prefix) || !e.Type().IsRegular() {
			continue
		}
		// Type zero includes regular files only; Info also protects unknown dir types.
		info, err := e.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			continue
		}
		if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// Called only after releasing stMu; it invalidates without joining its caller.
func (l *chunkLayerImpl) abortCOW(err error) {
	if w := l.cowActive.Load(); w != nil {
		w.abort(err)
	}
}
