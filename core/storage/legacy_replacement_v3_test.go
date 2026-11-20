package storage

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

func TestNativeReplacementAllBlocksAfterCheckpoint(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	for lba, fill := range map[uint32]byte{1: 0x11, 7: 0x77} {
		if _, err := s.Write(lba, blk(fill)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if s.CheckpointLSN() != 2 || s.dm.len() != 0 {
		t.Fatalf("setup: checkpoint=%d dirty=%d", s.CheckpointLSN(), s.dm.len())
	}
	all, err := s.AllBlocks()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 || !bytes.Equal(all[1], blk(0x11)) || !bytes.Equal(all[7], blk(0x77)) {
		t.Fatal("checkpointed LBAs missing from AllBlocks or bytes changed")
	}
}

func TestNativeReplacementMutationsAfterClose(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	mapPath := s.v3.ownedMap.fd.Name()
	if _, err := s.Write(1, blk(0x11)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before := map[string][]byte{}
	for _, p := range []string{path, mapPath} {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		before[p] = b
	}
	ops := map[string]func() error{
		"Write":             func() error { _, e := s.Write(1, blk(0x22)); return e },
		"WriteBatch":        func() error { _, e := s.WriteBatch(1, [][]byte{blk(0x22), blk(0x33)}); return e },
		"ApplyEntry":        func() error { return s.ApplyEntry(1, blk(0x22), 2) },
		"WriteExtentDirect": func() error { return s.WriteExtentDirect(1, blk(0x22)) },
		"SeedChunkFrontier": func() error { return s.SeedChunkFrontier(7) },
	}
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			if err := op(); err == nil {
				t.Fatal("closed store accepted mutation")
			}
		})
	}
	for p, want := range before {
		got, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("closed mutation changed %s", p)
		}
	}
}

func TestNativeReplacementCloseJoinsSync(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	if _, err := s.Write(1, blk(0x61)); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	closeEntered := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	syncDone, closeDone := make(chan struct{}), make(chan struct{})
	var syncErr, closeErr error
	t.Cleanup(func() {
		releaseOnce()
		for name, done := range map[string]<-chan struct{}{"Sync": syncDone, "Close": closeDone} {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Errorf("%s worker did not join", name)
			}
		}
	})
	real := s.syncCache
	s.syncCache = func() error { close(entered); <-release; return real() }
	s.testCloseSnapshot = func() { close(closeEntered) }
	go func() { _, syncErr = s.Sync(); close(syncDone) }()
	waitV3(t, entered, "held native Sync")
	go func() { closeErr = s.Close(); close(closeDone) }()
	waitV3(t, closeEntered, "Close admitted while Sync held")
	select {
	case <-closeDone:
		t.Fatal("Close returned while Sync still owns I/O")
	case <-time.After(25 * time.Millisecond):
	}
	releaseOnce()
	waitV3(t, syncDone, "native Sync completion")
	waitV3(t, closeDone, "native Close completion")
	if syncErr != nil || closeErr != nil {
		t.Fatalf("Sync=%v Close=%v", syncErr, closeErr)
	}
}

func TestNativeReplacementRecycledRangeIsTyped(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	for i := uint32(1); i <= 5; i++ {
		if _, err := s.Write(i, blk(byte(i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	emitted := 0
	err := s.ScanLBAs(1, func(RecoveryEntry) error { emitted++; return nil })
	var failure *RecoveryFailure
	if !errors.As(err, &failure) || failure.Kind != StorageRecoveryFailureWALRecycled || !errors.Is(err, ErrWALRecycled) {
		t.Fatalf("recycled range: %T %v", err, err)
	}
	if emitted != 0 {
		t.Fatal("recycled range emitted partial entries")
	}
}

func TestNativeReplacementCutBehindHEADRefuses(t *testing.T) {
	f := newFakeChunkRemote()
	image := make([]byte, ctNumBlocks*ctBlockSize)
	copy(image, blk(0x44))
	f.seedHead(t, image, 10, 1)
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	newStore := func() *WALStore {
		s, err := createMappedWALStoreV3ForTest(t.TempDir()+"/native", ctNumBlocks, ctBlockSize, v3TestWAL, WModeStrict, ctChunkBlocks)
		if err != nil {
			t.Fatal(err)
		}
		s.DisableAutoFlushForRecoveryTest()
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	s := newStore()
	layer := mustAttach(t, s, f, cfg)
	if _, err := s.Write(3, blk(0x33)); err != nil {
		t.Fatal(err)
	}
	before := f.stats()
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkLSNBehind) {
		t.Fatalf("behind cut: %v", err)
	}
	if _, err := layer.Detach(ctTimeout(t)); !errors.Is(err, errChunkLSNBehind) {
		t.Fatalf("behind detach: %v", err)
	}
	after := f.stats()
	if before.puts != after.puts || before.cas != after.cas || before.head.Gen != after.head.Gen {
		t.Fatal("behind native store published")
	}
	// The production restore path seeds before attach. A separate valid new
	// owner continues from the exact inherited frontier instead of resetting it.
	seeded := newStore()
	if err := seeded.SeedChunkFrontier(10); err != nil {
		t.Fatal(err)
	}
	next := mustAttach(t, seeded, f, cfg)
	if lsn, err := seeded.Write(4, blk(0x34)); err != nil || lsn != 11 {
		t.Fatalf("seeded append: %d %v", lsn, err)
	}
	cut, err := next.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if cut.Head.RemoteLSN != 11 {
		t.Fatalf("HEAD LSN=%d", cut.Head.RemoteLSN)
	}
	copy(image[4*ctBlockSize:], blk(0x34))
	if !bytes.Equal(f.imageOf(t, cut.Head), image) {
		t.Fatal("seeded native owner changed inherited bytes")
	}
}

func TestNativeReplacementCallerPublicationFaults(t *testing.T) {
	for _, caller := range []string{"seed", "checkpoint", "recover"} {
		for _, fault := range []string{"write", "fsync"} {
			t.Run(caller+"/"+fault, func(t *testing.T) {
				s, path := newMappedCheckpointStoreV3(t)
				if caller != "seed" {
					writeSynced(t, s, 1, 1)
				}
				before := s.v3.pub.current()
				next := s.NextLSN()
				boom := errors.New("injected native caller publication " + fault)
				calls := 0
				if fault == "write" {
					s.v3.pub.io.writeSlot = func(int64, []byte) error { calls++; return boom }
				} else {
					s.v3.pub.io.fsync = func() error { calls++; return boom }
				}
				var err error
				switch caller {
				case "seed":
					err = s.SeedChunkFrontier(7)
				case "checkpoint":
					err = s.flusher.flushOnce()
				case "recover":
					_, err = s.Recover()
				}
				if calls == 0 || !errors.Is(err, boom) {
					t.Fatalf("%s did not hit %s publication: calls=%d err=%v", caller, fault, calls, err)
				}
				if s.v3.pub.current() != before || s.NextLSN() != next {
					t.Fatal("failed publication advanced the descriptor or allocator")
				}
				rawBefore, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = s.Write(2, blk(0x52)); err == nil {
					t.Fatal("append accepted after uncertain publication")
				}
				rawAfter, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(rawBefore, rawAfter) {
					t.Fatal("refused append changed physical bytes")
				}
				crashReadOnlyStoreV3(t, s)
				restored, err := openWALStoreV3ForTest(path)
				if err != nil {
					t.Fatal(err)
				}
				restored.DisableAutoFlushForRecoveryTest()
				t.Cleanup(func() { _ = restored.Close() })
				if _, err = restored.Recover(); err != nil {
					t.Fatal(err)
				}
				if caller == "seed" {
					if err = restored.SeedChunkFrontier(7); err != nil {
						t.Fatal(err)
					}
					if restored.NextLSN() != 8 || restored.CheckpointLSN() != 7 {
						t.Fatal("reopened seed failed to persist/retry inherited frontier")
					}
				} else {
					got, err := restored.Read(1)
					if err != nil || !bytes.Equal(got, blk(1)) {
						t.Fatalf("acknowledged image after failed %s: %v", caller, err)
					}
					if _, err = restored.Write(2, blk(0x52)); err != nil {
						t.Fatal(err)
					}
					if _, err = restored.Sync(); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}
