package storage

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// lazyChunk2Fixture: HEAD holds chunk 2 (LBA 8-11); the volume is fresh from
// HEAD (every chunk absent) and LBA 8 is written locally, so chunk 2 is a
// partly absent cut candidate.
func lazyChunk2Fixture(t *testing.T, readMiss time.Duration) (*WALStore, *chunkLayerImpl, *fakeChunkRemote, []byte, []byte) {
	t.Helper()
	src := make([]byte, ctNumBlocks*ctBlockSize)
	for lba := uint32(8); lba < 12; lba++ {
		copy(src[int(lba)*ctBlockSize:], makeBlock(ctBlockSize, byte(0x80+lba)))
	}
	f := newFakeChunkRemote()
	f.seedHead(t, src, 10, 1)
	s, _ := newChunkTestStore(t)
	if err := s.SeedChunkFrontier(10); err != nil {
		t.Fatal(err)
	}
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	cfg.ReadMissTimeout = readMiss
	l := mustAttach(t, s, f, cfg).(*chunkLayerImpl)
	local := makeBlock(ctBlockSize, 0xD8)
	if _, err := s.Write(8, local); err != nil {
		t.Fatal(err)
	}
	return s, l, f, src, local
}

// The copier's HEAD fill (after it released the entry) is cancelled by an
// aborted cut: the cut returns promptly, the window is drained, and the next
// cut publishes the right image.
func TestCutCOWV3_HeldFillIsCancelledByAbortAndDrained(t *testing.T) {
	_, l, f, src, local := lazyChunk2Fixture(t, time.Minute)
	gEntered, gRelease := holdGets(t, f)
	t.Cleanup(gRelease)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := l.Cut(ctx); done <- err }()
	waitKey(t, gEntered, "the copier's HEAD fill of chunk 2")
	cancel()
	err := waitErr(t, done, "aborted cut with a held fill")
	if err == nil || !(errors.Is(err, context.Canceled) || errors.Is(err, errCutCOWAborted)) {
		t.Fatalf("cut aborted during a held fill: %v, want cancellation", err)
	}
	if w := l.cowActive.Load(); w != nil {
		t.Fatal("copy window not drained after the abort")
	}
	gRelease()
	r, err := l.Cut(context.Background())
	if err != nil {
		t.Fatal("next cut:", err)
	}
	img := f.imageOf(t, r.Head)
	if !bytes.Equal(blockOf(img, 8), local) || !bytes.Equal(blockOf(img, 9), blockOf(src, 9)) {
		t.Fatal("next cut lost the local block or HEAD's bytes for the absent ones")
	}
}

// A held fill is bounded by ReadMissTimeout: the cut aborts (retryable) instead
// of waiting, and the window is drained.
func TestCutCOWV3_HeldFillTimesOutAndAborts(t *testing.T) {
	_, l, f, _, _ := lazyChunk2Fixture(t, 200*time.Millisecond)
	_, gRelease := holdGets(t, f)
	t.Cleanup(gRelease)
	done := make(chan error, 1)
	go func() { _, err := l.Cut(context.Background()); done <- err }()
	err := waitErr(t, done, "cut with a held fill past ReadMissTimeout")
	if !errors.Is(err, errCutCOWAborted) || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "HEAD fetch") {
		t.Fatalf("cut with a timed-out fill: %v, want an aborted cut naming the HEAD fetch deadline", err)
	}
	if w := l.cowActive.Load(); w != nil {
		t.Fatal("copy window not drained after the fill timeout")
	}
}

// Image control for the spill path: a saved chunk with lazily-absent blocks
// spills its local bytes and its missing list; the copier fills the missing
// blocks from HEAD, and the image is exact.
func TestCutCOWV3_LazySaverSpillImageIsExact(t *testing.T) {
	s, _ := newChunkTestStore(t)
	source := make([]byte, ctNumBlocks*ctBlockSize)
	for lba := 0; lba < 8; lba++ {
		copy(source[lba*ctBlockSize:], makeBlock(ctBlockSize, byte(0x30+lba)))
	}
	f := newFakeChunkRemote()
	f.seedHead(t, source, 0, 1)
	cfg := ctConfig(2)
	cfg.Bitmap = nil
	cfg.FreshFromHead = true
	cfg.BarrierBound = 0
	cfg.UploadConcurrency = 1
	l := mustAttach(t, s, f, cfg).(*chunkLayerImpl)
	if _, err := s.Write(1, makeBlock(ctBlockSize, 0x51)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteBatch(4, [][]byte{makeBlock(ctBlockSize, 0x54), makeBlock(ctBlockSize, 0x55)}); err != nil {
		t.Fatal(err)
	}
	live, releaseLive := make(chan struct{}), make(chan struct{})
	var once, relOnce sync.Once
	unblock := func() { relOnce.Do(func() { close(releaseLive) }) }
	var w *cutCOWWindow
	l.testCOWRead = func(idx uint64, _ bool) {
		if idx == 0 {
			once.Do(func() {
				w = l.cowActive.Load()
				w.mu.Lock()
				w.memory = cutCOWMemoryLimit // the next save spills
				w.mu.Unlock()
				close(live)
			})
			<-releaseLive
		}
	}
	cutDone := make(chan error, 1)
	t.Cleanup(unblock)
	go func() { _, err := l.Cut(context.Background()); cutDone <- err }()
	cowWaitSignal(t, live, "the window open, chunk 0 held")
	if _, err := s.Write(6, makeBlock(ctBlockSize, 0x96)); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	spilled := w.side != nil
	w.mu.Unlock()
	if !spilled {
		t.Fatal("the save did not spill")
	}
	unblock()
	if err := waitErr(t, cutDone, "spilled lazy cut"); err != nil {
		t.Fatal(err)
	}
	want := append([]byte(nil), source...)
	for lba, value := range map[int]byte{1: 0x51, 4: 0x54, 5: 0x55} {
		copy(want[lba*ctBlockSize:], makeBlock(ctBlockSize, value))
	}
	if !bytes.Equal(f.imageOf(t, f.stats().head), want) {
		t.Fatal("spilled COW save lost local bytes or HEAD's bytes for its missing blocks")
	}
}
