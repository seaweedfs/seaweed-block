package storage

import (
	"bytes"
	"context"
	"sync"
	"testing"
)

// A claimed chunk retains its WAL-backed entries, but unrelated chunks still
// materialize. Reclaim must not pass the held chunk's incomplete capture.
func TestCutCOWV3_FlusherDefersOnlyClaimedChunk(t *testing.T) {
	s, l, f, extentGate := cowFixture(t)
	extentGate.unblock()
	for _, lba := range []uint32{5, 1} {
		if _, err := s.Write(lba, makeBlock(ctBlockSize, byte(0x60+lba))); err != nil {
			t.Fatal(err)
		}
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	free := func() { once.Do(func() { close(release) }) }
	l.testCOWRead = func(idx uint64, _ bool) {
		if idx == 0 {
			close(entered)
			<-release
		}
	}
	errs := make(chan error, 1)
	t.Cleanup(func() { free(); cowWaitSignal(t, done, "held capture cleanup") })
	go func() { defer close(done); _, err := l.Cut(context.Background()); errs <- err }()
	cowWaitSignal(t, entered, "claimed WAL-backed chunk")
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, ok := s.dm.get(1); !ok {
		t.Error("flusher retired a claimed chunk before capture completed")
	}
	other, err := s.readExtentBlock(5)
	if err != nil || !bytes.Equal(other, makeBlock(ctBlockSize, 0x65)) {
		t.Errorf("unrelated chunk did not flush: %v", err)
	}
	free()
	cowWaitSignal(t, done, "capture completion")
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	image := f.imageOf(t, f.stats().head)
	if !bytes.Equal(blockOf(image, 1), makeBlock(ctBlockSize, 0x61)) {
		t.Fatal("capture lost its fixed-frontier bytes")
	}
}
