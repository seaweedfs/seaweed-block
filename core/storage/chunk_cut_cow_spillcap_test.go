package storage

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCutCOWV3_SaverCapacityAbortsWithoutParkingGuest(t *testing.T) {
	s, l, f, gate := cowFixture(t)
	if _, err := s.Write(9, makeBlock(ctBlockSize, 0x39)); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	l.testCOWSave = func() error { once.Do(func() { close(entered) }); <-release; return nil }
	var unblock sync.Once
	free := func() { unblock.Do(func() { close(release) }) }
	t.Cleanup(free)
	before := f.stats().head.Gen
	done, errs := cowStartCut(t, l, gate)
	writer := make(chan error, 1)
	go func() { _, err := s.Write(5, makeBlock(ctBlockSize, 0x45)); writer <- err }()
	t.Cleanup(func() { free(); gate.unblock() })
	cowWaitSignal(t, entered, "saving owner")
	second := make(chan error, 1)
	go func() { _, err := s.Write(9, makeBlock(ctBlockSize, 0x49)); second <- err }()
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(ctLocalBound):
		t.Fatal("memory pressure parked guest")
	}
	free()
	gate.unblock()
	select {
	case err := <-writer:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(ctLocalBound):
		t.Fatal("saving writer did not settle")
	}
	cowWaitSignal(t, done, "capacity-refused cut")
	if err := <-errs; !errors.Is(err, errCutCOWAborted) {
		t.Fatalf("cut: %v", err)
	}
	if f.stats().head.Gen != before {
		t.Fatal("capacity-refused cut published")
	}
}

func TestCutCOWV3_RemoteUploadStartsAfterWindowCloses(t *testing.T) {
	s, l, f, gate := cowFixture(t)
	entered, release := holdPuts(t, f)
	done, errs := cowStartCut(t, l, gate)
	gate.unblock()
	waitKey(t, entered, "upload after local copy")
	if l.cowActive.Load() != nil {
		t.Fatal("remote wait retained the COW window")
	}
	files, err := filepath.Glob(s.path + ".cut-cow-payload-*")
	if err != nil || len(files) != 1 {
		t.Fatalf("owned payload file during upload: %v %v", files, err)
	}
	if _, err = s.Write(5, makeBlock(ctBlockSize, 0xe5)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	release()
	cowWaitSignal(t, done, "uploaded cut")
	if err = <-errs; err != nil {
		t.Fatal(err)
	}
	files, err = filepath.Glob(s.path + ".cut-cow-*")
	if err != nil || len(files) != 0 {
		t.Fatalf("settled payload files: %v %v", files, err)
	}
}

// P3: once the in-memory budget is used, a save that would take the side file
// past its byte cap aborts the cut (retried) instead of growing the file; the
// guest write still completes and no side file is left.
func TestCutCOWV3_SpillCapAbortsTheCut(t *testing.T) {
	s, l, _, gate := cowFixture(t)
	cutDone, cutErr := cowStartCut(t, l, gate)
	w := l.cowActive.Load()
	w.mu.Lock()
	w.memory = cutCOWMemoryLimit    // the in-memory budget is used
	w.spillLimit = w.chunkBytes - 1 // the cap leaves no room for one chunk
	w.mu.Unlock()
	writeDone := make(chan error, 1)
	go func() { _, err := s.Write(5, makeBlock(ctBlockSize, 0xd5)); writeDone <- err }()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal("guest write refused by the spill cap:", err)
		}
	case <-time.After(ctLocalBound):
		t.Fatal("guest write did not complete when the spill cap aborted the cut")
	}
	w.mu.Lock()
	side, spilled := w.side, w.spillBytes
	w.mu.Unlock()
	if side != nil || spilled != 0 {
		t.Fatalf("side file %v / %d spilled bytes past the cap", side != nil, spilled)
	}
	gate.unblock()
	cowWaitSignal(t, cutDone, "capped cut")
	if err := <-cutErr; !errors.Is(err, errCutCOWAborted) || !strings.Contains(err.Error(), "spill would exceed its cap") {
		t.Fatalf("cut past the spill cap: %v, want the cap abort", err)
	}
	if _, err := l.Cut(context.Background()); err != nil {
		t.Fatal("retried cut:", err)
	}
}

// The default cap applies at attach (0), and a negative cap is refused.
func TestChunkLayer_COWSpillLimitDefaultAndRefusal(t *testing.T) {
	s, _ := newChunkTestStore(t)
	cfg := ctConfig(1)
	cfg.COWSpillLimit = -1
	if _, err := attachChunkLayer(s, newFakeChunkRemote(), cfg); err == nil || !strings.Contains(err.Error(), "spill limit") {
		t.Fatalf("negative COW spill limit: %v, want a refusal", err)
	}
	cfg.COWSpillLimit = 0
	l := mustAttach(t, s, newFakeChunkRemote(), cfg).(*chunkLayerImpl)
	if l.cfg.COWSpillLimit != defaultCOWSpillLimit {
		t.Fatalf("COW spill limit %d, want the default %d", l.cfg.COWSpillLimit, defaultCOWSpillLimit)
	}
}

// The shared cap covers one saver plus the two copied payloads. The saver is
// counted before the held copier runs; the exact three-chunk cap then succeeds.
func TestCutCOWV3_SpillWithinCapIsAccounted(t *testing.T) {
	s, l, _, gate := cowFixture(t)
	cutDone, cutErr := cowStartCut(t, l, gate)
	w := l.cowActive.Load()
	w.mu.Lock()
	w.memory = cutCOWMemoryLimit
	w.spillLimit = 3 * w.chunkBytes
	w.mu.Unlock()
	if _, err := s.Write(5, makeBlock(ctBlockSize, 0xd5)); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	side, spilled := w.side, w.spillBytes
	w.mu.Unlock()
	if side == nil || spilled != w.chunkBytes {
		t.Fatalf("spill within the cap: side file %v, %d bytes accounted, want one chunk (%d)", side != nil, spilled, w.chunkBytes)
	}
	gate.unblock()
	cowWaitSignal(t, cutDone, "cut within the spill cap")
	if err := <-cutErr; err != nil {
		t.Fatal(err)
	}
	if w.spillBytes != 3*w.chunkBytes {
		t.Fatalf("shared saver/payload bytes=%d, want %d", w.spillBytes, 3*w.chunkBytes)
	}
}

func TestCutCOWV3_SharedPayloadCapRefusesBeforePublication(t *testing.T) {
	s, l, f, gate := cowFixture(t)
	before := f.stats().head.Gen
	done, errs := cowStartCut(t, l, gate)
	w := l.cowActive.Load()
	w.mu.Lock()
	w.memory = cutCOWMemoryLimit
	w.spillLimit = 2 * w.chunkBytes
	w.mu.Unlock()
	if _, err := s.Write(5, makeBlock(ctBlockSize, 0xd6)); err != nil {
		t.Fatal(err)
	}
	gate.unblock()
	cowWaitSignal(t, done, "shared payload cap")
	if err := <-errs; !errors.Is(err, errCutCOWAborted) {
		t.Fatalf("over-cap cut: %v", err)
	}
	if f.stats().head.Gen != before {
		t.Fatal("over-cap image published")
	}
	files, err := filepath.Glob(s.path + ".cut-cow-*")
	if err != nil || len(files) != 0 {
		t.Fatalf("scratch files after refused cut: %v %v", files, err)
	}
}
