package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
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
	for _, lba := range []uint32{1, 5} {
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
				select {
				case <-actionDone:
					t.Fatal("transition returned with saver/live copy still held")
				default:
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
		_ = layer.Close()
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
