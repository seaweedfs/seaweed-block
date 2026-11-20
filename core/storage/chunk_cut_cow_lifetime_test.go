package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These fixtures use the public v3 factory, real reads/WAL/flusher, and actual
// cut/write/recovery/close callers. Hooks only hold an identified I/O boundary.
func cowFixture(t *testing.T) (*WALStore, *chunkLayerImpl, *fakeChunkRemote, *cowReadGate) {
	t.Helper()
	gate := &cowReadGate{entered: make(chan struct{}), release: make(chan struct{})}
	s, err := CreateWALStoreWithOptions(filepath.Join(t.TempDir(), "volume"), ctNumBlocks, ctBlockSize, WALStoreOptions{ChunkBlocks: ctChunkBlocks, MetadataFactory: func(_ BlockMetadataFile, layout BlockMetadataLayout) (BlockMetadataBinding, error) {
		return BlockMetadataBinding{ImplVersion: layout.ImplVersion, Hook: gate}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { gate.unblock(); _ = s.Close() })
	s.DisableAutoFlushForRecoveryTest()
	if _, err = s.Recover(); err != nil {
		t.Fatal(err)
	}
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	cfg.Bitmap = nil
	cfg.BarrierBound = 0
	cfg.UploadConcurrency = 1
	l := mustAttach(t, s, f, cfg).(*chunkLayerImpl)
	if _, err = l.Cut(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, lba := range []uint32{5, 1} {
		if _, err = s.Write(lba, makeBlock(ctBlockSize, byte(lba))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	gate.mu.Lock()
	gate.armed = true
	gate.mu.Unlock()
	return s, l, f, gate
}
func cowStartCut(t *testing.T, l *chunkLayerImpl, gate *cowReadGate) (<-chan struct{}, <-chan error) {
	t.Helper()
	done := make(chan struct{})
	errs := make(chan error, 1)
	t.Cleanup(func() { gate.unblock(); cowWaitSignal(t, done, "cut cleanup") })
	go func() { defer close(done); _, err := l.Cut(context.Background()); errs <- err }()
	cowWaitSignal(t, gate.entered, "live copier")
	return done, errs
}

func TestCutCOWV3_CopyingChunkWaitsButPendingChunkSaves(t *testing.T) {
	s, l, f, gate := cowFixture(t)
	waiting := make(chan struct{})
	var once sync.Once
	l.testCOWWait = func() { once.Do(func() { close(waiting) }) }
	cutDone, cutErr := cowStartCut(t, l, gate)
	writeDone := make(chan struct{})
	var writeErr error
	t.Cleanup(func() { gate.unblock(); cowWaitSignal(t, writeDone, "writer cleanup") })
	go func() { defer close(writeDone); _, writeErr = s.Write(1, makeBlock(ctBlockSize, 0xa1)) }()
	cowWaitSignal(t, waiting, "same-chunk wait admission")
	select {
	case <-writeDone:
		t.Fatal("copying chunk changed before old image finished")
	default:
	}
	if _, err := s.Write(5, makeBlock(ctBlockSize, 0xa5)); err != nil {
		t.Fatal(err)
	}
	gate.unblock()
	cowWaitSignal(t, writeDone, "same-chunk write")
	cowWaitSignal(t, cutDone, "cut")
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if err := <-cutErr; err != nil {
		t.Fatal(err)
	}
	if l.Stats().LastCut.COWWaitNs <= 0 {
		t.Fatal("drained window omitted its real wait time")
	}
	image := f.imageOf(t, f.stats().head)
	if !bytes.Equal(image[ctBlockSize:2*ctBlockSize], makeBlock(ctBlockSize, 1)) || !bytes.Equal(image[5*ctBlockSize:6*ctBlockSize], makeBlock(ctBlockSize, 5)) {
		t.Fatal("torn or post-H cut image")
	}
}

func TestCutCOWV3_SaveFailureAbortsOnlyCut(t *testing.T) {
	s, l, f, gate := cowFixture(t)
	var calls atomic.Int32
	l.testCOWSave = func() error { calls.Add(1); return errors.New("injected side ENOSPC") }
	before := f.stats().head
	cutDone, cutErr := cowStartCut(t, l, gate)
	if _, err := s.Write(5, makeBlock(ctBlockSize, 0xb5)); err != nil {
		t.Fatalf("COW failure escaped into guest write: %v", err)
	}
	gate.unblock()
	cowWaitSignal(t, cutDone, "aborted cut")
	if err := <-cutErr; !errors.Is(err, errCutCOWAborted) {
		t.Fatalf("cut error=%v", err)
	}
	if f.stats().head.Gen != before.Gen || calls.Load() != 1 {
		t.Fatal("failed cut published or save not exercised")
	}
	l.testCOWSave = nil
	r, err := l.Cut(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	image := f.imageOf(t, r.Head)
	if !bytes.Equal(image[ctBlockSize:2*ctBlockSize], makeBlock(ctBlockSize, 1)) || !bytes.Equal(image[5*ctBlockSize:6*ctBlockSize], makeBlock(ctBlockSize, 0xb5)) {
		t.Fatal("retry lost original candidates or new dirt")
	}
}

func TestCutCOWV3_RecoveryAndCloseDrainSavingOwner(t *testing.T) {
	for _, action := range []string{"Recover", "Reset", "LayerClose", "StoreClose", "Cancel"} {
		t.Run(action, func(t *testing.T) {
			s, l, _, gate := cowFixture(t)
			saveEntered, saveRelease := make(chan struct{}), make(chan struct{})
			var once sync.Once
			releaseSave := func() { once.Do(func() { close(saveRelease) }) }
			l.testCOWSave = func() error { close(saveEntered); <-saveRelease; return nil }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cutDone, writeDone, actionDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var writeErr, actionErr, cutErr error
			actionStarted := false
			t.Cleanup(func() {
				releaseSave()
				gate.unblock()
				cowWaitSignal(t, writeDone, "save writer cleanup")
				cowWaitSignal(t, cutDone, "cut cleanup")
				if actionStarted {
					cowWaitSignal(t, actionDone, "transition cleanup")
				}
			})
			go func() { defer close(cutDone); _, cutErr = l.Cut(ctx) }()
			cowWaitSignal(t, gate.entered, "held copier")
			w := l.cowActive.Load()
			if w == nil {
				t.Fatal("window not installed")
			}
			go func() { defer close(writeDone); _, writeErr = s.Write(5, makeBlock(ctBlockSize, 0xc5)) }()
			cowWaitSignal(t, saveEntered, "saving owner")
			actionStarted = true
			go func() {
				defer close(actionDone)
				switch action {
				case "Recover":
					_, actionErr = s.Recover()
				case "Reset":
					actionErr = s.resetIncarnationV3()
				case "LayerClose":
					actionErr = l.Close()
				case "StoreClose":
					actionErr = s.Close()
				case "Cancel":
					cancel()
					<-cutDone
				}
			}()
			// Store Close first takes capture exclusive, held by the saver; release
			// the actual saver for that arm before expecting layer/window cancellation.
			if action == "StoreClose" {
				releaseSave()
			}
			cowWaitSignal(t, w.ended, "terminal notification")
			if action != "StoreClose" {
				// ended is the real abort handshake, not a scheduling guess.
				// Keep actual I/O held for this bounded non-completion check;
				// an immediate default select could miss a no-join mutant.
				select {
				case <-actionDone:
					t.Fatal("transition returned with saver/live copy still held")
				case <-time.After(100 * time.Millisecond):
				}
			}
			releaseSave()
			gate.unblock()
			cowWaitSignal(t, writeDone, "saving writer")
			cowWaitSignal(t, cutDone, "aborted cut")
			cowWaitSignal(t, actionDone, "transition")
			if writeErr != nil {
				t.Fatal(writeErr)
			}
			if cutErr == nil {
				t.Fatal("terminal window published")
			}
			if actionErr != nil && action != "Reset" {
				t.Fatal(actionErr)
			}
			if action == "Recover" || action == "Reset" || action == "Cancel" {
				l.testCOWSave = nil
				if _, err := l.Cut(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestCutCOWV3_CloseSnapshotRejectsFirstAttachAndSharesCompletion(t *testing.T) {
	s, _ := newChunkTestStore(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	s.testCloseSnapshot = func() { close(entered); <-release }
	first, second := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		unblock()
		cowWaitSignal(t, first, "first close cleanup")
		cowWaitSignal(t, second, "second close cleanup")
	})
	go func() { defer close(first); _ = s.Close() }()
	cowWaitSignal(t, entered, "Close stable snapshot")
	go func() { defer close(second); _ = s.Close() }()
	cfg := ctConfig(1)
	cfg.Bitmap = nil
	layer, err := attachChunkLayer(s, newFakeChunkRemote(), cfg)
	if err == nil {
		// Isolated missing-gate negative: the real first layer installs an
		// OPEN window while Close is parked after its nil snapshot. No fake
		// sentinel hook stands in for that snapshot.
		l := layer.(*chunkLayerImpl)
		live, releaseLive := make(chan struct{}), make(chan struct{})
		var liveOnce sync.Once
		unblockLive := func() { liveOnce.Do(func() { close(releaseLive) }) }
		l.testCOWRead = func(idx uint64, _ bool) {
			if idx == 0 {
				close(live)
				<-releaseLive
			}
		}
		cutDone := make(chan struct{})
		t.Cleanup(func() { unblockLive(); cowWaitSignal(t, cutDone, "missed first-attach cut"); _ = l.Close() })
		go func() { defer close(cutDone); _, _ = l.Cut(context.Background()) }()
		cowWaitSignal(t, live, "missed first-attach live owner")
		unblock()
		cowWaitSignal(t, first, "Close after missing first attach")
		select {
		case <-cutDone:
		default:
			t.Error("Close returned before newly admitted live owner drained")
		}
		unblockLive()
		t.Error("first attach admitted after permanent shutdown")
	} else if !errors.Is(err, errChunkLayerClosed) {
		t.Fatal(err)
	}
	select {
	case <-second:
		t.Fatal("second Close skipped the first drain")
	default:
	}
	unblock()
	cowWaitSignal(t, first, "first close")
	cowWaitSignal(t, second, "second close")
}

func TestCutCOWV3_SpillIsEphemeralAndCleanupIsPrefixScoped(t *testing.T) {
	s, l, _, gate := cowFixture(t)
	cutDone, cutErr := cowStartCut(t, l, gate)
	w := l.cowActive.Load()
	w.mu.Lock()
	w.memory = cutCOWMemoryLimit
	w.mu.Unlock() // force real file spill without a large volume
	if _, err := s.Write(5, makeBlock(ctBlockSize, 0xd5)); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	side := w.side
	w.mu.Unlock()
	if side == nil {
		t.Fatal("spill not exercised")
	}
	name := side.Name()
	gate.unblock()
	cowWaitSignal(t, cutDone, "spilled cut")
	if err := <-cutErr; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(name); !os.IsNotExist(err) {
		t.Fatalf("side store survives terminal drain: %v", err)
	}
	path := s.path
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	stale := path + ".cut-cow-stale"
	other := path + "-other.cut-cow-stale"
	directory := path + ".cut-cow-dir"
	if err := os.WriteFile(stale, []byte("corrupt side, not recovery input"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(other, []byte("other owner"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.DisableAutoFlushForRecoveryTest()
	if _, err = reopened.Recover(); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("stale own side payload retained")
	}
	for _, p := range []string{other, directory} {
		if _, err = os.Stat(p); err != nil {
			t.Fatal("cleanup touched other data", err)
		}
	}
	got, err := reopened.Read(5)
	if err != nil || !bytes.Equal(got, makeBlock(ctBlockSize, 0xd5)) {
		t.Fatal("reopen trusted side payload or lost synced bytes", err)
	}
}

func TestCutCOWV3_TwoWritersOneSaveAndCloseJoinsSavedReader(t *testing.T) {
	s, l, f, gate := cowFixture(t)
	saved, releaseSave := make(chan struct{}), make(chan struct{})
	reader, releaseReader := make(chan struct{}), make(chan struct{})
	waiting := make(chan struct{})
	var waitOnce, saveOnce, readerOnce sync.Once
	var saves atomic.Int32
	unblockSave := func() { saveOnce.Do(func() { close(releaseSave) }) }
	unblockReader := func() { readerOnce.Do(func() { close(releaseReader) }) }
	l.testCOWSave = func() error { saves.Add(1); close(saved); <-releaseSave; return nil }
	l.testCOWWait = func() { waitOnce.Do(func() { close(waiting) }) }
	l.testCOWRead = func(idx uint64, side bool) {
		if idx == 1 && side {
			close(reader)
			<-releaseReader
		}
	}
	cutDone, cutErr := cowStartCut(t, l, gate)
	w := l.cowActive.Load()
	w.mu.Lock()
	w.memory = cutCOWMemoryLimit
	w.mu.Unlock()
	first, second, closeDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var err1, err2, closeErr error
	closeStarted := false
	t.Cleanup(func() {
		unblockSave()
		unblockReader()
		gate.unblock()
		cowWaitSignal(t, first, "first writer")
		cowWaitSignal(t, second, "second writer")
		if closeStarted {
			cowWaitSignal(t, closeDone, "saved reader close")
		}
	})
	go func() { defer close(first); _, err1 = s.Write(5, makeBlock(ctBlockSize, 0xe1)) }()
	cowWaitSignal(t, saved, "single saving owner")
	go func() { defer close(second); _, err2 = s.Write(6, makeBlock(ctBlockSize, 0xe2)) }()
	cowWaitSignal(t, waiting, "second writer waits on same chunk")
	select {
	case <-second:
		t.Fatal("second writer bypassed saving owner")
	default:
	}
	unblockSave()
	cowWaitSignal(t, first, "first write")
	cowWaitSignal(t, second, "second write")
	if err1 != nil || err2 != nil || saves.Load() != 1 {
		t.Fatalf("single owner: %v %v saves=%d", err1, err2, saves.Load())
	}
	w.mu.Lock()
	side := w.side
	w.mu.Unlock()
	if side == nil {
		t.Fatal("saved read must use real spill file")
	}
	name := side.Name()
	gate.unblock()
	cowWaitSignal(t, reader, "actual saved-file read completed, reference held")
	thirdDone := make(chan struct{})
	var thirdErr error
	t.Cleanup(func() { unblockReader(); cowWaitSignal(t, thirdDone, "writer during saved-file read") })
	go func() { defer close(thirdDone); _, thirdErr = s.Write(7, makeBlock(ctBlockSize, 0xe3)) }()
	cowWaitSignal(t, thirdDone, "saved chunk remains writable during saved-file read")
	if thirdErr != nil {
		t.Fatal(thirdErr)
	}
	closeStarted = true
	go func() { defer close(closeDone); closeErr = l.Close() }()
	cowWaitSignal(t, w.ended, "close aborts saved-reader window")
	select {
	case <-closeDone:
		t.Fatal("Close returned before saved reader released")
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := os.Stat(name); err != nil {
		t.Fatal("side removed before reader reference drained", err)
	}
	unblockReader()
	cowWaitSignal(t, closeDone, "saved reader drain")
	cowWaitSignal(t, cutDone, "aborted saved-read cut")
	if closeErr != nil || <-cutErr == nil {
		t.Fatal("Close/cut result", closeErr)
	}
	if _, err := os.Stat(name); !os.IsNotExist(err) {
		t.Fatal("side survived saved-reader drain", err)
	}
	if f.stats().head.RemoteLSN >= s.NextLSN()-1 {
		t.Fatal("aborted cut published post-H mutations")
	}
}

func TestCutCOWV3_ObservedFenceAbortsAndWakesWriters(t *testing.T) {
	s, l, f, gate := cowFixture(t)
	var fenced atomic.Bool
	l.cfg.CaptureAllowed = func() error {
		if fenced.Load() {
			return errChunkLayerFenced
		}
		return nil
	}
	l.cfg.UploadConcurrency = 2
	other, releaseOther := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(releaseOther) }) }
	l.testCOWRead = func(idx uint64, _ bool) {
		if idx == 1 {
			close(other)
			<-releaseOther
		}
	}
	cutDone, cutErr := cowStartCut(t, l, gate)
	t.Cleanup(unblock)
	cowWaitSignal(t, other, "second copier before ordinary authority recheck")
	w := l.cowActive.Load()
	before := f.stats().head.Gen
	fenced.Store(true)
	unblock()
	cowWaitSignal(t, w.ended, "ordinary capture gate observes fence")
	if _, err := s.Write(1, makeBlock(ctBlockSize, 0xf1)); err != nil {
		t.Fatalf("COW fence refused ordinary write: %v", err)
	}
	select {
	case <-cutDone:
		t.Fatal("cut returned while live read reference held")
	default:
	}
	gate.unblock()
	cowWaitSignal(t, cutDone, "fenced cut drain")
	if !errors.Is(<-cutErr, errCutCOWAborted) || f.stats().head.Gen != before {
		t.Fatal("fenced window published")
	}
}

func TestCutCOWV3_InvalidInputsDoNotSaveOrAbortWindow(t *testing.T) {
	s, l, _, gate := cowFixture(t)
	var saves atomic.Int32
	l.testCOWSave = func() error { saves.Add(1); return errors.New("invalid input touched COW") }
	cutDone, cutErr := cowStartCut(t, l, gate)
	w := l.cowActive.Load()
	if _, err := s.Write(ctNumBlocks, makeBlock(ctBlockSize, 1)); err == nil {
		t.Fatal("invalid Write accepted")
	}
	if _, err := s.Write(5, []byte{1}); err == nil {
		t.Fatal("short Write accepted")
	}
	if _, err := s.WriteBatch(ctNumBlocks-1, [][]byte{makeBlock(ctBlockSize, 1), makeBlock(ctBlockSize, 2)}); err == nil {
		t.Fatal("invalid batch accepted")
	}
	if _, err := s.WriteBatch(5, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyEntry(5, []byte{1}, s.NextLSN()); err == nil {
		t.Fatal("short Apply accepted")
	}
	if saves.Load() != 0 {
		t.Fatal("invalid caller performed COW I/O")
	}
	select {
	case <-w.ended:
		t.Fatal("invalid input aborted cut")
	default:
	}
	gate.unblock()
	cowWaitSignal(t, cutDone, "valid cut after invalid requests")
	if err := <-cutErr; err != nil {
		t.Fatal(err)
	}
}

func TestCutCOWV3_OverlappingRealBatchesPreserveCut(t *testing.T) {
	s, l, f, gate := cowFixture(t)
	for _, lba := range []uint32{9, 13} {
		if _, err := s.Write(lba, makeBlock(ctBlockSize, byte(lba))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	cutDone, cutErr := cowStartCut(t, l, gate)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	done := make(chan struct{})
	t.Cleanup(func() { gate.unblock(); cowWaitSignal(t, done, "batch cleanup") })
	for _, start := range []uint32{5, 9} {
		wg.Add(1)
		go func(start uint32) {
			defer wg.Done()
			blocks := make([][]byte, 5)
			for i := range blocks {
				blocks[i] = makeBlock(ctBlockSize, byte(start)+0x80+byte(i))
			}
			_, err := s.WriteBatch(start, blocks)
			errs <- err
		}(start)
	}
	go func() { wg.Wait(); close(done) }()
	cowWaitSignal(t, done, "overlapping batches while earlier copy held")
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	gate.unblock()
	cowWaitSignal(t, cutDone, "batch cut")
	if err := <-cutErr; err != nil {
		t.Fatal(err)
	}
	want := make([]byte, ctNumBlocks*ctBlockSize)
	for _, lba := range []int{1, 5, 9, 13} {
		copy(want[lba*ctBlockSize:], makeBlock(ctBlockSize, byte(lba)))
	}
	if !bytes.Equal(f.imageOf(t, f.stats().head), want) {
		t.Fatal("batch published into the pre-H cut")
	}
	// Both submitted batches assign the same literal bytes to overlapping
	// LBAs (9). Derive the next HEAD oracle from those inputs, never local Read.
	for lba := 5; lba <= 13; lba++ {
		copy(want[lba*ctBlockSize:], makeBlock(ctBlockSize, byte(lba)+0x80))
	}
	r, err := l.Cut(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f.imageOf(t, r.Head), want) {
		t.Fatal("following cut lost overlapping batch writes")
	}
}

func TestCutCOWV3_DestructiveSnapshotRetriesFirstAttachment(t *testing.T) {
	for _, reset := range []bool{false, true} {
		t.Run(map[bool]string{false: "Recover", true: "Reset"}[reset], func(t *testing.T) {
			s, _ := newChunkTestStore(t)
			snapshot, release := make(chan struct{}), make(chan struct{})
			var once, releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			s.testDestructiveSnapshot = func() { once.Do(func() { close(snapshot); <-release }) }
			done := make(chan struct{})
			var transitionErr error
			t.Cleanup(func() { unblock(); cowWaitSignal(t, done, "first-attach transition cleanup") })
			go func() {
				defer close(done)
				if reset {
					transitionErr = s.resetIncarnationV3()
				} else {
					_, transitionErr = s.Recover()
				}
			}()
			cowWaitSignal(t, snapshot, "nil hook snapshot before barrier")
			cfg := ctConfig(1)
			cfg.Bitmap = nil
			cfg.BarrierBound = 0
			cfg.UploadConcurrency = 1
			l := mustAttach(t, s, newFakeChunkRemote(), cfg).(*chunkLayerImpl)
			live, releaseLive := make(chan struct{}), make(chan struct{})
			var liveOnce sync.Once
			unblockLive := func() { liveOnce.Do(func() { close(releaseLive) }) }
			l.testCOWRead = func(idx uint64, _ bool) {
				if idx == 0 {
					close(live)
					<-releaseLive
				}
			}
			cutDone := make(chan struct{})
			var cutErr error
			t.Cleanup(func() { unblockLive(); cowWaitSignal(t, cutDone, "first attach cut cleanup") })
			go func() { defer close(cutDone); _, cutErr = l.Cut(context.Background()) }()
			cowWaitSignal(t, live, "newly attached OPEN live-copy owner")
			w := l.cowActive.Load()
			unblock()
			cowWaitSignal(t, w.ended, "hook snapshot retry aborts new window")
			select {
			case <-done:
				t.Fatal("destructive path missed first attach's owner")
			default:
			}
			unblockLive()
			cowWaitSignal(t, cutDone, "first attach cut drain")
			cowWaitSignal(t, done, "destructive retry")
			if cutErr == nil {
				t.Fatal("first-attach window survived destructive transition")
			}
			if transitionErr != nil {
				t.Fatal(transitionErr)
			}
		})
	}
}

// A process stop while OPEN deliberately bypasses all Close/abort cleanups.
// The parent gets the actual last remote HEAD, not a reconstructed proposal.
func TestCutCOWV3_ProcessCrashDropsSideAndRestoresSyncedImage(t *testing.T) {
	if path := os.Getenv("SWBLOCK_COW_CRASH_PATH"); path != "" {
		gate := &cowReadGate{entered: make(chan struct{}), release: make(chan struct{})}
		s, err := CreateWALStoreWithOptions(path, ctNumBlocks, ctBlockSize, WALStoreOptions{ChunkBlocks: ctChunkBlocks, MetadataFactory: func(_ BlockMetadataFile, layout BlockMetadataLayout) (BlockMetadataBinding, error) {
			return BlockMetadataBinding{ImplVersion: layout.ImplVersion, Hook: gate}, nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { gate.unblock(); _ = s.Close() })
		s.DisableAutoFlushForRecoveryTest()
		if _, err = s.Recover(); err != nil {
			t.Fatal(err)
		}
		f := newFakeChunkRemote()
		cfg := ctConfig(1)
		cfg.Bitmap = nil
		cfg.BarrierBound = 0
		cfg.UploadConcurrency = 1
		l := mustAttach(t, s, f, cfg).(*chunkLayerImpl)
		baseline, err := l.Cut(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, lba := range []uint32{5, 1} {
			if _, err = s.Write(lba, makeBlock(ctBlockSize, byte(lba))); err != nil {
				t.Fatal(err)
			}
		}
		if _, err = s.Sync(); err != nil {
			t.Fatal(err)
		}
		if err = s.flusher.flushOnce(); err != nil {
			t.Fatal(err)
		}
		gate.mu.Lock()
		gate.armed = true
		gate.mu.Unlock()
		cutDone, _ := cowStartCut(t, l, gate)
		w := l.cowActive.Load()
		w.mu.Lock()
		w.memory = cutCOWMemoryLimit
		w.mu.Unlock()
		if _, err = s.Write(5, makeBlock(ctBlockSize, 0x77)); err != nil {
			t.Fatal(err)
		}
		if _, err = s.Sync(); err != nil {
			t.Fatal(err)
		}
		if w.side == nil {
			t.Fatal("child has no live side file")
		}
		head := f.stats().head
		if head.Gen != baseline.Head.Gen {
			t.Fatal("OPEN cut already moved HEAD")
		}
		raw, err := json.Marshal(head)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path+".remote-head.json", raw, 0600); err != nil {
			t.Fatal(err)
		}
		select {
		case <-cutDone:
			t.Fatal("cut not OPEN at crash")
		default:
		}
		os.Exit(0) // intentional process crash: no cleanup runs
	}
	path := filepath.Join(t.TempDir(), "volume")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCutCOWV3_ProcessCrashDropsSideAndRestoresSyncedImage$", "-test.timeout=20s")
	cmd.Env = append(os.Environ(), "SWBLOCK_COW_CRASH_PATH="+path)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("crash child: %v: %s", err, output)
	}
	stale, err := filepath.Glob(path + ".cut-cow-*")
	if err != nil || len(stale) != 1 {
		t.Fatalf("OPEN crash did not leave one spill: %v %v", stale, err)
	}
	raw, err := os.ReadFile(path + ".remote-head.json")
	if err != nil {
		t.Fatal(err)
	}
	var head chunkHead
	if err = json.Unmarshal(raw, &head); err != nil {
		t.Fatal(err)
	}
	if head.RemoteLSN != 0 || len(head.Chunks) != 0 {
		t.Fatal("OPEN crash changed old HEAD")
	}
	s, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.DisableAutoFlushForRecoveryTest()
	if _, err = s.Recover(); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Read(5); err != nil || !bytes.Equal(got, makeBlock(ctBlockSize, 0x77)) {
		t.Fatal("synced post-H write lost on reopen", err)
	}
	for _, name := range stale {
		if _, err = os.Stat(name); !os.IsNotExist(err) {
			t.Fatal("crash side not discarded", err)
		}
	}
	f := newFakeChunkRemote()
	f.head = head
	cfg := ctConfig(1)
	cfg.Bitmap = nil
	cfg.BarrierBound = 0
	l := mustAttach(t, s, f, cfg)
	r, err := l.Cut(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, ctNumBlocks*ctBlockSize)
	copy(want[ctBlockSize:], makeBlock(ctBlockSize, 1))
	copy(want[5*ctBlockSize:], makeBlock(ctBlockSize, 0x77))
	if !bytes.Equal(f.imageOf(t, r.Head), want) {
		t.Fatal("next cut imported stale side bytes")
	}
}

func TestCutCOWV3_SaverUsesValidatedWALBatchAndLazyHEAD(t *testing.T) {
	for _, flushed := range []bool{false, true} {
		t.Run(map[bool]string{false: "WAL-only-batch", true: "flushed-batch"}[flushed], func(t *testing.T) {
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
			if flushed {
				if _, err := s.Sync(); err != nil {
					t.Fatal(err)
				}
				if err := s.flusher.flushOnce(); err != nil {
					t.Fatal(err)
				}
			}
			// Keep chunk 0 hotter than chunk 1 so the uploader holds chunk 0.
			if _, err := s.Write(1, makeBlock(ctBlockSize, 0x51)); err != nil {
				t.Fatal(err)
			}
			live, releaseLive := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblockLive := func() { once.Do(func() { close(releaseLive) }) }
			l.testCOWRead = func(idx uint64, _ bool) {
				if idx == 0 {
					close(live)
					<-releaseLive
				}
			}
			cutDone, writeDone := make(chan struct{}), make(chan struct{})
			var cutErr, writeErr error
			writeStarted := false
			t.Cleanup(func() {
				unblockLive()
				cowWaitSignal(t, cutDone, "lazy cut cleanup")
				if writeStarted {
					cowWaitSignal(t, writeDone, "lazy saver cleanup")
				}
			})
			go func() { defer close(cutDone); _, cutErr = l.Cut(context.Background()) }()
			cowWaitSignal(t, live, "first actual copied chunk")
			// The window is open. No guest write may wait on SeaweedFS (design
			// rules 1/2): the saver captures only the chunk's LOCAL blocks and
			// lists the lazily-absent ones as missing (the copier fills them from
			// HEAD later, off the write path), so with every HEAD fetch held the
			// saving write must finish without one.
			gets, releaseGet := holdGets(t, f)
			t.Cleanup(releaseGet)
			writeStarted = true
			go func() { defer close(writeDone); _, writeErr = s.Write(6, makeBlock(ctBlockSize, 0x96)) }()
			cowWaitSignal(t, writeDone, "lazy saving write without a remote fetch")
			if w := l.cowActive.Load(); w == nil || w.saved.Load() != 1 {
				t.Fatal("target write skipped the saver path")
			}
			select {
			case k := <-gets:
				t.Fatalf("a guest write fetched %s from the remote inside the cut window", k)
			default:
			}
			releaseGet()
			if writeErr != nil {
				t.Fatal(writeErr)
			}
			unblockLive()
			cowWaitSignal(t, cutDone, "lazy cut")
			if cutErr != nil {
				t.Fatal(cutErr)
			}
			want := append([]byte(nil), source...)
			for lba, value := range map[int]byte{1: 0x51, 4: 0x54, 5: 0x55} {
				copy(want[lba*ctBlockSize:], makeBlock(ctBlockSize, value))
			}
			if !bytes.Equal(f.imageOf(t, f.stats().head), want) {
				t.Fatal("COW saver lost WAL/batch bytes or lazy HEAD backing")
			}
			l.testCOWRead = nil
			next, err := l.Cut(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			copy(want[6*ctBlockSize:], makeBlock(ctBlockSize, 0x96))
			if !bytes.Equal(f.imageOf(t, next.Head), want) {
				t.Fatal("post-save mutation omitted by next cut")
			}
		})
	}
}
