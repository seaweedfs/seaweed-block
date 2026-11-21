package storage

import (
	"bytes"
	"context"
	"errors"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCutCOWV3_SaverCapacityAbortsWithoutParkingGuest(t *testing.T) {
	out := &seedLog{}
	old := log.Writer()
	log.SetOutput(out)
	t.Cleanup(func() { log.SetOutput(old) })
	s, l, f, gate := cowFixture(t)
	for _, lba := range []uint32{9, 1} {
		if _, err := s.Write(lba, makeBlock(ctBlockSize, byte(lba))); err != nil {
			t.Fatal(err)
		}
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	l.testCOWSave = func() error { once.Do(func() { close(entered) }); <-release; return nil }
	var unblock sync.Once
	free := func() { unblock.Do(func() { close(release) }) }
	t.Cleanup(free)
	before := f.stats().head.Gen
	done, errs := cowStartCut(t, l, gate)
	w := l.cowActive.Load()
	w.saverSlots = make(chan struct{}, 1) // witness exhaustion of a fixed saver budget
	writer := make(chan error, 1)
	writerDone, secondDone := make(chan struct{}), make(chan struct{})
	secondStarted := false
	t.Cleanup(func() {
		free()
		gate.unblock()
		cowWaitSignal(t, writerDone, "saving writer cleanup")
		if secondStarted {
			cowWaitSignal(t, secondDone, "second writer cleanup")
		}
	})
	go func() { defer close(writerDone); _, err := s.Write(5, makeBlock(ctBlockSize, 0x45)); writer <- err }()
	cowWaitSignal(t, entered, "saving owner")
	second := make(chan error, 1)
	secondStarted = true
	go func() { defer close(secondDone); _, err := s.Write(9, makeBlock(ctBlockSize, 0x49)); second <- err }()
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
	if l.Stats().CutsAborted["saver_capacity"] != 1 {
		t.Fatal("missing saver abort status")
	}
	if f.stats().head.Gen != before {
		t.Fatal("capacity-refused cut published")
	}
	w.abort(errCutSaverCapacity) // terminal cleanup must not emit a second event.
	out.mu.Lock()
	logged := out.buf.String()
	out.mu.Unlock()
	if strings.Count(logged, "storage: cut_aborted ") != 1 ||
		!strings.Contains(logged, "saver_capacity=1 saver_used=1 window_chunks=3") ||
		!strings.Contains(logged, "storage: saver capacity") ||
		!strings.Contains(logged, "spill_bytes=0 spill_limit=1073741824") {
		t.Fatalf("missing abort-time capacity snapshot: %s", logged)
	}
}

func TestCutCOWV3_RemoteUploadDoesNotParkCopiedChunk(t *testing.T) {
	s, l, f, gate := cowFixture(t)
	entered, release := holdPuts(t, f)
	done, errs := cowStartCut(t, l, gate)
	t.Cleanup(release) // unblock remote I/O before cowStartCut's failure join
	gate.unblock()
	waitKey(t, entered, "upload after local copy")
	if l.cowActive.Load() == nil {
		t.Fatal("window closed with unsent candidates")
	}
	w := l.cowActive.Load()
	w.mu.Lock()
	w.memory = w.memoryLimit
	w.spillLimit = w.chunkBytes
	w.mu.Unlock()
	var err error
	if _, err = s.Write(1, makeBlock(ctBlockSize, 0xe5)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err = s.Write(5, makeBlock(ctBlockSize, 0xe5)); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) >= ctLocalBound {
		t.Fatal("unsent chunk waited for remote upload")
	}
	w.mu.Lock()
	saved := w.spillBytes
	w.mu.Unlock()
	if saved != w.chunkBytes {
		t.Fatalf("unsent chunk saved %d bytes", saved)
	}
	if _, err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	release()
	cowWaitSignal(t, done, "uploaded cut")
	if err = <-errs; err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(s.path + ".cut-cow-*")
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
	if l.Stats().CutsAborted["cow_capacity"] != 1 {
		t.Fatal("missing COW cap status")
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

// Only a guest overwrite consumes the side-file budget; uploads do not.
func TestCutCOWV3_SpillWithinCapIsAccounted(t *testing.T) {
	s, l, _, gate := cowFixture(t)
	cutDone, cutErr := cowStartCut(t, l, gate)
	w := l.cowActive.Load()
	w.mu.Lock()
	w.memory = cutCOWMemoryLimit
	w.spillLimit = w.chunkBytes
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
	if w.spillBytes != w.chunkBytes {
		t.Fatalf("saved bytes=%d, want %d", w.spillBytes, w.chunkBytes)
	}
}

func TestCutCOWV3_ChangedImageExceedsSpillCap(t *testing.T) {
	_, l, f, gate := cowFixture(t)
	before := f.stats().head.Gen
	done, errs := cowStartCut(t, l, gate)
	w := l.cowActive.Load()
	w.mu.Lock()
	w.spillLimit = w.chunkBytes / 2 // two changed chunks are four times the cap
	w.mu.Unlock()
	gate.unblock()
	cowWaitSignal(t, done, "streamed image")
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if f.stats().head.Gen != before+1 {
		t.Fatal("image did not publish")
	}
	img := f.imageOf(t, f.stats().head)
	for _, lba := range []uint32{1, 5} {
		if !bytes.Equal(blockOf(img, lba), makeBlock(ctBlockSize, byte(lba))) {
			t.Fatal("published bytes mismatch", lba)
		}
	}
	if w.spillBytes != 0 {
		t.Fatalf("unchanged local extents consumed %d scratch bytes", w.spillBytes)
	}
}

func TestCutCOWV3_OverwriteStormAbortsAndRetries(t *testing.T) {
	// Every invocation reports its own failure. Rolling and periodic callers
	// may issue a new invocation; a named caller must retain the failure.
	for _, request := range []string{"rolling", "periodic", "named"} {
		t.Run(request, func(t *testing.T) {
			s, l, f, gate := cowFixture(t)
			// Keep chunk 0 hottest, with two other candidates still waiting behind it.
			for _, lba := range []uint32{9, 1} {
				if _, err := s.Write(lba, makeBlock(ctBlockSize, byte(lba))); err != nil {
					t.Fatal(err)
				}
			}
			before := f.stats().head.Gen
			var fixed uint64
			l.testAfterBarrierSyncHook = func(h uint64) { fixed = h }
			done, errs := cowStartCut(t, l, gate)
			w := l.cowActive.Load()
			w.mu.Lock()
			w.memory = w.memoryLimit
			w.spillLimit = w.chunkBytes
			w.mu.Unlock()
			for _, lba := range []uint32{5, 9} {
				start := time.Now()
				if _, err := s.Write(lba, makeBlock(ctBlockSize, 0xd0+byte(lba))); err != nil {
					t.Fatal(err)
				}
				if time.Since(start) >= ctLocalBound {
					t.Fatal("overwrite exceeded guest budget")
				}
			}
			gate.unblock()
			cowWaitSignal(t, done, "spill-refused update")
			if err := <-errs; !errors.Is(err, errCutCOWAborted) || !strings.Contains(err.Error(), "spill would exceed its cap") {
				t.Fatalf("cut: %v", err)
			}
			if f.stats().head.Gen != before {
				t.Fatal("refused cut published")
			}
			if request == "named" {
				// The failed named invocation ends here: only a new request may
				// select another point, never an implicit replacement success.
				return
			}
			oldPoint := fixed
			result, err := l.Cut(context.Background())
			if err != nil {
				t.Fatal("retry:", err)
			}
			if result.H <= oldPoint || result.Head.RemoteLSN != result.H {
				t.Fatalf("new invocation reused aborted point: old=%d new=%d remote=%d", oldPoint, result.H, result.Head.RemoteLSN)
			}
			img := f.imageOf(t, result.Head)
			for _, lba := range []uint32{5, 9} {
				if !bytes.Equal(blockOf(img, lba), makeBlock(ctBlockSize, 0xd0+byte(lba))) {
					t.Fatal("retry lost overwrite", lba)
				}
			}
		})
	}
}

func TestChunkStageV3_ChangedImageExceedsSpillCap(t *testing.T) {
	s, l, f, gate := cowFixture(t)
	gate.unblock()
	l.cfg.BarrierBound = 1
	l.cfg.COWSpillLimit = int64(ctBlockSize) * int64(ctChunkBlocks) / 2
	for _, lba := range []uint32{1, 5, 9, 13} {
		if _, err := s.Write(lba, makeBlock(ctBlockSize, byte(lba))); err != nil {
			t.Fatal(err)
		}
	}
	result, err := l.Cut(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	img := f.imageOf(t, result.Head)
	for _, lba := range []uint32{1, 5, 9, 13} {
		if !bytes.Equal(blockOf(img, lba), makeBlock(ctBlockSize, byte(lba))) {
			t.Fatal("stage image mismatch", lba)
		}
	}
	files, err := filepath.Glob(s.path + ".cut-cow-*")
	if err != nil || len(files) != 0 {
		t.Fatalf("unexpected staging files: %v %v", files, err)
	}
}

func TestCutCOWV3_RecentChunkUploadsFirst(t *testing.T) {
	s, l, f, gate := cowFixture(t)
	gate.unblock()
	// Make index order differ from heat order: chunk 1, then 2, then 0.
	for _, lba := range []uint32{1, 9, 5} {
		if _, err := s.Write(lba, makeBlock(ctBlockSize, byte(lba))); err != nil {
			t.Fatal(err)
		}
	}
	f.mu.Lock()
	start := len(f.ops)
	f.mu.Unlock()
	var copied []uint64
	l.testCOWRead = func(idx uint64, _ bool) { copied = append(copied, idx) }
	result, err := l.Cut(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []uint64{1, 2, 0}
	if len(copied) != len(want) {
		t.Fatalf("copied %v, want %v", copied, want)
	}
	for i, idx := range want {
		if copied[i] != idx {
			t.Fatalf("local copy order=%v, want %v", copied, want)
		}
	}
	f.mu.Lock()
	ops := append([]string(nil), f.ops[start:]...)
	f.mu.Unlock()
	var puts []string
	for _, op := range ops {
		if strings.HasPrefix(op, "put:") {
			puts = append(puts, strings.TrimPrefix(op, "put:"))
		}
	}
	if len(puts) != len(want) {
		t.Fatalf("PUTs=%v, want three uploads", puts)
	}
	for i, idx := range want {
		if puts[i] != result.Head.Chunks[idx].Key {
			t.Fatalf("PUT %d=%q, want hot chunk %d key %q", i, puts[i], idx, result.Head.Chunks[idx].Key)
		}
	}
}

// Two guest saves exceed one slot; the second waits for local capacity and
// preserves the same cut point rather than aborting a transient burst.
func TestCutCOWV3_SaverBurstWaitsAndPublishes(t *testing.T) {
	s, l, f, gate := cowFixture(t)
	if _, err := s.Write(9, makeBlock(ctBlockSize, 9)); err != nil {
		t.Fatal(err)
	}
	// Keep chunk 0 hottest, so the held copier precedes both unread saves.
	if _, err := s.Write(1, makeBlock(ctBlockSize, 1)); err != nil {
		t.Fatal(err)
	}
	entered, release, waiting := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once, unblock sync.Once
	free := func() { unblock.Do(func() { close(release) }) }
	l.testCOWSave = func() error { once.Do(func() { close(entered); <-release }); return nil }
	l.testCOWSaverWait = func() { close(waiting) }
	done, errs := cowStartCut(t, l, gate)
	l.cowActive.Load().saverSlots = make(chan struct{}, 1)
	writers := make(chan error, 2)
	firstDone, secondDone := make(chan struct{}), make(chan struct{})
	secondStarted := false
	t.Cleanup(func() {
		free()
		gate.unblock()
		// A timeout reports failure without skipping the other owner's join.
		wait := func(done <-chan struct{}, name string) {
			select {
			case <-done:
			case <-time.After(ctLocalBound):
				t.Errorf("%s did not settle within %s", name, ctLocalBound)
			}
		}
		wait(firstDone, "first burst writer cleanup")
		if secondStarted {
			wait(secondDone, "second burst writer cleanup")
		}
	})
	go func() { defer close(firstDone); _, e := s.Write(5, makeBlock(ctBlockSize, 0xa5)); writers <- e }()
	cowWaitSignal(t, entered, "first saver held")
	secondStarted = true
	go func() { defer close(secondDone); _, e := s.Write(9, makeBlock(ctBlockSize, 0xa9)); writers <- e }()
	cowWaitSignal(t, waiting, "second saver queued")
	free()
	for i := 0; i < 2; i++ {
		select {
		case e := <-writers:
			if e != nil {
				t.Fatal(e)
			}
		case <-time.After(ctLocalBound):
			t.Fatal("writer did not settle")
		}
	}
	gate.unblock()
	cowWaitSignal(t, done, "burst cut")
	if e := <-errs; e != nil {
		t.Fatal(e)
	}
	image := f.imageOf(t, f.stats().head)
	for _, lba := range []int{5, 9} {
		if !bytes.Equal(image[lba*ctBlockSize:(lba+1)*ctBlockSize], makeBlock(ctBlockSize, byte(lba))) {
			t.Fatalf("cut changed at block %d", lba)
		}
	}
	if l.Stats().CutsAborted["saver_capacity"] != 0 {
		t.Fatal("transient burst aborted cut")
	}
}
