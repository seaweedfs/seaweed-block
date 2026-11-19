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

const cutCOWMemoryLimit = 64 << 20
const (
	cowPending uint8 = iota
	cowCopying
	cowSaving
	cowSaved
	cowDone
)

var errCutCOWAborted = errors.New("storage: cut copy window aborted; retry cut")

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
	layer      *chunkLayerImpl
	mu         sync.Mutex
	terminal   bool
	err        error
	ended      chan struct{}
	refs       sync.WaitGroup // Add under mu, only while OPEN; Wait only after terminal
	sideReady  chan struct{}
	cleanupErr error
	entries    map[uint64]*cutCOWEntry
	head       map[uint64]chunkRef // the cut's base HEAD: the source of missing blocks
	memory     int64
	chunkBytes int64
	side       *os.File
	sideErr    error
	cleanup    sync.Once
	saved      atomic.Int64
	saveNs     atomic.Int64
	waitNs     atomic.Int64
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
func (l *chunkLayerImpl) installCOWWindow(candidates, staged map[uint64]bool, head map[uint64]chunkRef) (*cutCOWWindow, error) {
	size := uint64(l.blockSize) * l.chunkBlocks
	if size == 0 || size > math.MaxInt64 || uint64(len(candidates)) > math.MaxInt64/size {
		return nil, fmt.Errorf("%w: side-store geometry", errCutCOWAborted)
	}
	w := &cutCOWWindow{layer: l, ended: make(chan struct{}), entries: make(map[uint64]*cutCOWEntry), head: head, chunkBytes: int64(size)}
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
			break
		}
	}
}

func (w *cutCOWWindow) save(e *cutCOWEntry, data []byte, missing []uint64) error {
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
	inMemory := w.memory+w.chunkBytes <= cutCOWMemoryLimit
	if inMemory {
		w.memory += w.chunkBytes
	}
	w.mu.Unlock()
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
		b, local, err := l.readLocalBlock(lba)
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
func (l *chunkLayerImpl) readLocalBlock(lba uint64) ([]byte, bool, error) {
	s := l.s
	if s.v3 != nil {
		if data, handled, err := s.readDirtyV3(uint32(lba)); handled {
			return data, err == nil, err
		}
	} else if walRelOff, dataOffset, _, _, ok := s.dm.get(lba); ok {
		data, err := s.readFromWAL(walRelOff, dataOffset)
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
// base HEAD bytes. It runs with no entry held (no guest waits on it), bounded
// by ReadMissTimeout and the window's end.
func (l *chunkLayerImpl) fillMissing(w *cutCOWWindow, idx uint64, data []byte, missing []uint64) ([]byte, error) {
	ref, ok := w.head[idx]
	if !ok {
		return nil, fmt.Errorf("storage: cut chunk %d: %d lazily absent block(s) but HEAD has no chunk", idx, len(missing))
	}
	ctx, cancel := context.WithTimeout(l.ctx, l.cfg.ReadMissTimeout)
	defer cancel()
	go func() {
		select {
		case <-w.ended:
			cancel()
		case <-ctx.Done():
		}
	}()
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
		if kept && len(missing) > 0 {
			// The entry is released: guests no longer wait on this chunk.
			if data, err = l.fillMissing(w, idx, data, missing); err != nil {
				w.abort(err)
				kept = false
			}
		}
		w.refs.Done()
		if !kept {
			return capturedChunk{}, false, w.status()
		}
		return capturedChunk{idx: idx, data: data, sum: sha256.Sum256(data), zero: isAllZero(data)}, true, nil
	}
}

func (l *chunkLayerImpl) copyCOWWindow(ctx context.Context, w *cutCOWWindow) ([]capturedChunk, error) {
	stop := context.AfterFunc(ctx, func() { w.abort(ctx.Err()) })
	defer stop()
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
	for idx := range w.entries {
		indices[idx] = true
	}
	keys := sortedKeys(indices)
	out := make([]capturedChunk, len(keys))
	kept := make([]bool, len(keys))
	var next atomic.Int64
	var workers sync.WaitGroup
	for n := 0; n < min(l.cfg.UploadConcurrency, len(keys)); n++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= len(keys) {
					return
				}
				c, ok, err := l.copyCOWEntry(w, keys[i])
				if err != nil {
					return
				}
				out[i], kept[i] = c, ok
			}
		}()
	}
	workers.Wait()
	w.mu.Lock()
	if !w.terminal {
		w.terminal = true
		close(w.ended)
	}
	err := w.err
	w.mu.Unlock()
	cleanupErr := w.join()
	if cleanupErr != nil {
		err = errors.Join(err, errCutCOWAborted, cleanupErr)
	}
	if err != nil {
		return nil, err
	}
	var result []capturedChunk
	for i, c := range out {
		if kept[i] {
			result = append(result, c)
		}
	}
	return result, nil
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
