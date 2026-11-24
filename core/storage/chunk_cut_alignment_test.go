package storage

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A long copy window owns no WAL retention. A reader held after resolution
// must tolerate the flusher retiring that tuple and actually reusing its slot.
func TestCutCOWV3_CaptureDoesNotPinWAL(t *testing.T) {
	s, err := createMappedWALStoreV3ForTest(filepath.Join(t.TempDir(), "volume"), ctNumBlocks, ctBlockSize, 64*1024, WModeStrict, ctChunkBlocks)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = s.Close() })
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	cfg.Bitmap = nil
	cfg.BarrierBound = 0
	cfg.UploadConcurrency = 1
	l := mustAttach(t, s, f, cfg).(*chunkLayerImpl)
	if _, err = l.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	h, err := s.Write(0, makeBlock(ctBlockSize, 0x31))
	if err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	free := func() { once.Do(func() { close(release) }) }
	var armed atomic.Bool
	var invalid atomic.Int32
	armed.Store(true)
	s.v3.testReadResolved = func() {
		if armed.CompareAndSwap(true, false) {
			close(entered)
			<-release
		}
	}
	s.v3.testReadInvalid = func() { invalid.Add(1) }
	results := make(chan chunkCutResult, 1)
	errs := make(chan error, 1)
	t.Cleanup(func() { free(); cowWaitSignal(t, done, "held WAL reader cleanup") })
	cutCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	go func() { defer close(done); r, e := l.Cut(cutCtx); results <- r; errs <- e }()
	cowWaitSignal(t, entered, "resolved held WAL tuple")
	if err = s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if s.CheckpointLSN() < h {
		t.Fatal("cut window pinned checkpoint below its held image")
	}
	if _, _, _, _, live := s.dm.get(0); live {
		t.Fatal("held capture retained a durable dirty-map tuple")
	}
	for i := 0; i < 40; i++ {
		if _, err = s.Write(8, makeBlock(ctBlockSize, byte(0x80+i))); err != nil {
			t.Fatal(err)
		}
		if _, err = s.Sync(); err != nil {
			t.Fatal(err)
		}
		if err = s.flusher.flushOnce(); err != nil {
			t.Fatal(err)
		}
	}
	if floor, ok := s.v3.pub.reuseFloor(); !ok || floor <= 64*1024 {
		t.Fatalf("held read pinned ring reuse: floor=%d ok=%v", floor, ok)
	}
	free()
	cowWaitSignal(t, done, "re-resolved capture")
	if err = <-errs; err != nil {
		t.Fatal(err)
	}
	r := <-results
	if invalid.Load() != 1 {
		t.Fatalf("recycled tuple validations=%d, want exactly one mismatch and re-resolution", invalid.Load())
	}
	if r.H != h || !bytes.Equal(blockOf(f.imageOf(t, r.Head), 0), makeBlock(ctBlockSize, 0x31)) {
		t.Fatal("capture changed its fixed-frontier image")
	}
}

type retrySlotRemote struct {
	*fakeChunkRemote
	mu       sync.Mutex
	first    chan struct{}
	release  chan struct{}
	key      string
	attempts [][]byte
}

func (r *retrySlotRemote) Put(ctx context.Context, key string, data []byte) error {
	r.mu.Lock()
	if r.key == "" {
		r.key = key
	}
	target := key == r.key
	n := 0
	if target {
		r.attempts = append(r.attempts, append([]byte(nil), data...))
		n = len(r.attempts)
	}
	r.mu.Unlock()
	if n == 1 {
		close(r.first)
		select {
		case <-r.release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return errFakeRemoteDown
	}
	return r.fakeChunkRemote.Put(ctx, key, data)
}

// The guest overwrites an already-copied chunk while the first PUT is held.
// Its retry must send the frozen slot, never a fresh logical read.
func TestCutCOWV3_PutRetryUsesFrozenSlot(t *testing.T) {
	s, l, f, gate := cowFixture(t)
	gate.unblock()
	r := &retrySlotRemote{fakeChunkRemote: f, first: make(chan struct{}), release: make(chan struct{})}
	l.remote = r
	var fixed uint64
	l.testAfterBarrierSyncHook = func(h uint64) { fixed = h }
	var reads atomic.Int32
	l.testCOWRead = func(idx uint64, _ bool) {
		if idx == 0 {
			reads.Add(1)
		}
	}
	done := make(chan struct{})
	results := make(chan chunkCutResult, 1)
	errs := make(chan error, 1)
	var once sync.Once
	free := func() { once.Do(func() { close(r.release) }) }
	t.Cleanup(func() { free(); cowWaitSignal(t, done, "PUT retry cleanup") })
	go func() { defer close(done); result, err := l.Cut(ctTimeout(t)); results <- result; errs <- err }()
	cowWaitSignal(t, r.first, "first frozen-slot PUT")
	start := time.Now()
	if _, err := s.Write(1, makeBlock(ctBlockSize, 0xe1)); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) >= ctLocalBound {
		t.Fatal("copied chunk waited for remote PUT")
	}
	free()
	cowWaitSignal(t, done, "PUT retry")
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	result := <-results
	if result.H != fixed || result.Head.RemoteLSN != fixed {
		t.Fatalf("PUT retry replaced the point: fixed=%d result=%d remote=%d", fixed, result.H, result.Head.RemoteLSN)
	}
	r.mu.Lock()
	attempts := r.attempts
	r.mu.Unlock()
	if len(attempts) != 2 || !bytes.Equal(attempts[0], attempts[1]) {
		t.Fatal("PUT retry changed the frozen payload")
	}
	if reads.Load() != 1 || result.Stats.COWSavedChunks != 0 {
		t.Fatalf("copied chunk reread/saved: reads=%d saves=%d", reads.Load(), result.Stats.COWSavedChunks)
	}
	if !bytes.Equal(blockOf(f.imageOf(t, result.Head), 1), makeBlock(ctBlockSize, 1)) {
		t.Fatal("PUT retry included the post-H overwrite")
	}
}

// Supported zero-data writes use the ordinary before-append COW hook. They
// do not take the lifecycle abort/drain path used by Recover/reset/install.
func TestCutCOWV3_ZeroWritesDoNotAbortUpdates(t *testing.T) {
	s, l, f, gate := cowFixture(t)
	gate.unblock()
	for round := 0; round < 3; round++ {
		old := byte(0x50 + round)
		for _, lba := range []uint32{5, 1} {
			if _, err := s.Write(lba, makeBlock(ctBlockSize, old)); err != nil {
				t.Fatal(err)
			}
		}
		entered, release := holdPuts(t, f)
		results := make(chan chunkCutResult, 1)
		errs := make(chan error, 1)
		done := make(chan struct{})
		t.Cleanup(func() { release(); cowWaitSignal(t, done, "zero-write cut cleanup") })
		go func() { defer close(done); r, e := l.Cut(ctTimeout(t)); results <- r; errs <- e }()
		waitKey(t, entered, "copied hot chunk PUT")
		start := time.Now()
		var err error
		if round%2 == 0 {
			_, err = s.Write(5, makeBlock(ctBlockSize, 0))
		} else {
			zeroes := make([][]byte, ctChunkBlocks)
			for i := range zeroes {
				zeroes[i] = makeBlock(ctBlockSize, 0)
			}
			_, err = s.WriteBatch(ctChunkBlocks, zeroes)
		}
		if err != nil {
			t.Fatal(err)
		}
		if time.Since(start) >= ctLocalBound {
			t.Fatal("zero write waited for remote I/O")
		}
		release()
		cowWaitSignal(t, done, "zero-write point publication")
		if err = <-errs; err != nil {
			t.Fatal(err)
		}
		r := <-results
		if r.Stats.COWSavedChunks != 1 || !bytes.Equal(blockOf(f.imageOf(t, r.Head), 5), makeBlock(ctBlockSize, old)) {
			t.Fatal("zero write aborted the point or changed its pre-zero image")
		}
	}
	if l.Stats().CutsFailed != 0 {
		t.Fatal("regular zero writes aborted updates")
	}
}
