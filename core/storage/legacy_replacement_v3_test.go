package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestNativeReplacementLegacyBytesRefusedWithoutMutation(t *testing.T) {
	for _, impl := range []uint32{1, 2} {
		// Frozen v2 header bytes, independent of its retired writer/codec.
		raw := make([]byte, 4096+65536+16*4096)
		copy(raw, "SWBK")
		binary.LittleEndian.PutUint16(raw[4:], 2)
		binary.LittleEndian.PutUint64(raw[24:], 16*4096)
		binary.LittleEndian.PutUint32(raw[32:], 4096)
		binary.LittleEndian.PutUint32(raw[36:], 4096)
		binary.LittleEndian.PutUint64(raw[40:], 4096)
		binary.LittleEndian.PutUint64(raw[48:], 65536)
		raw[88] = 1 // walstore
		binary.LittleEndian.PutUint32(raw[92:], impl)
		path := filepath.Join(t.TempDir(), "legacy")
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if s, err := OpenWALStore(path); !errors.Is(err, ErrUnsupportedLegacyFormat) {
			if s != nil {
				_ = s.Close()
			}
			t.Fatalf("v2 impl %d writable open: %v", impl, err)
		}
		if s, err := OpenReadOnly(path); !errors.Is(err, ErrUnsupportedLegacyFormat) {
			if s != nil {
				_ = s.Close()
			}
			t.Fatalf("v2 impl %d readonly open: %v", impl, err)
		}
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, raw) {
			t.Fatalf("refusal mutated v2 bytes: %v", err)
		}
		entries, err := os.ReadDir(filepath.Dir(path))
		if err != nil || len(entries) != 1 {
			t.Fatalf("refusal created sidecars: %v %v", entries, err)
		}
	}
}

func TestNativeReplacementFlusherConcurrentOverwriteAndReopen(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	for lba := uint32(0); lba < 12; lba++ {
		if _, err := s.Write(lba, blk(byte(lba+1))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	// A later same-LBA write is excluded from the captured snapshot.
	s.flusher.afterSnapshotForTest = func() {
		s.flusher.afterSnapshotForTest = nil
		if _, err := s.Write(3, blk(0xE3)); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if s.CheckpointLSN() != 12 || s.dm.len() != 1 {
		t.Fatalf("first checkpoint=%d dirty=%d", s.CheckpointLSN(), s.dm.len())
	}
	if got, err := s.Read(3); err != nil || !bytes.Equal(got, blk(0xE3)) {
		t.Fatalf("new dirty bytes lost: %v", err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if s.CheckpointLSN() != 13 || s.dm.len() != 0 {
		t.Fatalf("final checkpoint=%d dirty=%d", s.CheckpointLSN(), s.dm.len())
	}
	crashReadOnlyStoreV3(t, s)
	for round := 0; round < 3; round++ {
		r, err := OpenWALStore(path)
		if err != nil {
			t.Fatal(err)
		}
		r.DisableAutoFlushForRecoveryTest()
		t.Cleanup(func() { _ = r.Close() })
		if _, err := r.Recover(); err != nil {
			t.Fatal(err)
		}
		if r.CheckpointLSN() != 13 || r.NextLSN() != 14 {
			t.Fatalf("round %d: checkpoint=%d next=%d", round, r.CheckpointLSN(), r.NextLSN())
		}
		for lba := uint32(0); lba < 12; lba++ {
			want := byte(lba + 1)
			if lba == 3 {
				want = 0xE3
			}
			if got, err := r.Read(lba); err != nil || !bytes.Equal(got, blk(want)) {
				t.Fatalf("round %d LBA %d: %v", round, lba, err)
			}
		}
		crashReadOnlyStoreV3(t, r)
	}
}

func TestNativeReplacementDataFsyncFailureIsSticky(t *testing.T) {
	for _, caller := range []string{"wal", "extent"} {
		t.Run(caller, func(t *testing.T) {
			s, path := newMappedCheckpointStoreV3(t)
			if _, err := s.Write(1, blk(0x61)); err != nil {
				t.Fatal(err)
			}
			boom := errors.New("native data fsync failure")
			calls := 0
			var err error
			if caller == "wal" {
				real := s.syncWALFile
				s.syncWALFile = func() error { calls++; return boom }
				_, err = s.Sync()
				s.syncWALFile = real
			} else {
				if _, err := s.Sync(); err != nil {
					t.Fatal(err)
				}
				real := s.syncExtentFile
				s.syncExtentFile = func() error { calls++; return boom }
				err = s.flusher.flushOnce()
				s.syncExtentFile = real
			}
			if calls != 1 || !errors.Is(err, boom) {
				t.Fatalf("fault did not reach data fsync: calls=%d err=%v", calls, err)
			}
			if s.CheckpointLSN() != 0 || s.dm.len() != 1 {
				t.Fatal("failed data sync retired dirty WAL")
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write(2, blk(0x62)); !errors.Is(err, ErrFsyncFailed) {
				t.Fatalf("write after fault: %v", err)
			}
			if _, err := s.Sync(); !errors.Is(err, ErrFsyncFailed) {
				t.Fatalf("Sync after fault: %v", err)
			}
			if err := s.flusher.flushOnce(); !errors.Is(err, ErrFsyncFailed) {
				t.Fatalf("flush after fault: %v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("latched operations changed bytes: %v", err)
			}
		})
	}
}

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

func TestNativeReplacementCloseFinalCheckpoint(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "ok", true: "publication_fault"}[fail], func(t *testing.T) {
			s, path := newMappedCheckpointStoreV3(t)
			if _, err := s.Write(1, blk(0xA1)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			boom := errors.New("final native checkpoint publication")
			calls := 0
			if fail {
				s.v3.pub.io.fsync = func() error { calls++; return boom }
			}
			// The fixture's old flusher is already joined; install an idle
			// production flusher so Close owns its one final flush.
			s.flusher = newFlusher(s, flusherConfig{Interval: time.Hour})
			go s.flusher.run()
			err := s.Close()
			if fail {
				if calls == 0 || !errors.Is(err, boom) || s.CheckpointLSN() != 0 {
					t.Fatalf("Close fault: calls=%d cp=%d err=%v", calls, s.CheckpointLSN(), err)
				}
			} else if err != nil || s.CheckpointLSN() != 1 || s.dm.len() != 0 {
				t.Fatalf("Close checkpoint=%d dirty=%d err=%v", s.CheckpointLSN(), s.dm.len(), err)
			}
			r, err := OpenWALStore(path)
			if err != nil {
				t.Fatal(err)
			}
			r.DisableAutoFlushForRecoveryTest()
			t.Cleanup(func() { _ = r.Close() })
			if _, err := r.Recover(); err != nil {
				t.Fatal(err)
			}
			if got, err := r.Read(1); err != nil || !bytes.Equal(got, blk(0xA1)) {
				t.Fatalf("Close lost acknowledged bytes: %v", err)
			}
		})
	}
}

func TestNativeReplacementFlusherRejectsInvalidRecord(t *testing.T) {
	for _, fault := range []string{"crc", "nonce", "lsn", "lba", "barrier"} {
		t.Run(fault, func(t *testing.T) {
			s, path := newMappedCheckpointStoreV3(t)
			if _, err := s.Write(2, blk(0x22)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			off, _, _, _, ok := s.dm.get(2)
			if !ok {
				t.Fatal("dirty record missing")
			}
			original := make([]byte, walEntryHeaderSizeV3+4096)
			absolute := int64(s.sb.WALOffset + off)
			if _, err := s.fd.ReadAt(original, absolute); err != nil {
				t.Fatal(err)
			}
			e, err := decodeWALEntryV3(original, 4096, 64)
			if err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "nonce":
				e.Nonce[0] ^= 0x7f
			case "lsn":
				e.LSN++
			case "lba":
				e.LBA++
			case "barrier":
				e.Type, e.Length, e.Data = walEntryBarrier, 0, nil
			}
			bad, err := encodeWALEntryV3(e, 4096, 64)
			if err != nil {
				t.Fatal(err)
			}
			if fault == "crc" {
				bad[len(bad)-1] ^= 0x7f
			}
			if _, err := s.fd.WriteAt(bad, absolute); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.flusher.flushOnce(); err == nil {
				t.Fatal("invalid record checkpointed")
			}
			if s.CheckpointLSN() != 0 || s.dm.len() != 1 {
				t.Fatal("invalid record retired dirty state")
			}
			i := s.FlusherInstrumentation()
			if i.ValidationFailures != 1 || i.ValidatedRecords != 0 || i.CyclesFailed != 1 || i.ExtentWriteOps != 0 {
				t.Fatalf("invalid record instrumentation: %+v", i)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("refused flush modified physical bytes: %v", err)
			}
			if _, err := s.fd.WriteAt(original, absolute); err != nil {
				t.Fatal(err)
			}
			if err := s.flusher.flushOnce(); err != nil {
				t.Fatal(err)
			}
			if got, err := s.Read(2); err != nil || !bytes.Equal(got, blk(0x22)) {
				t.Fatalf("restored record bytes: %v", err)
			}
		})
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

func TestNativeReplacementRefusedBatchPreservesWrapBytes(t *testing.T) {
	f, p, w := newWALIOTestV3(t, 0, 0)
	d := p.current()
	for i := uint64(1); i <= 3; i++ {
		if _, err := w.append(walEntryV3{LSN: i, Type: walEntryWrite, LBA: i, Length: d.BlockSize, Data: blk(byte(i))}, i-1, nil); err != nil {
			t.Fatal(err)
		}
	}
	head := w.logicalHeadV3()
	payload := append(blk(0x41), blk(0x42)...)
	if d.WALSize-head%d.WALSize >= uint64(walEntryHeaderSizeV3+len(payload)) {
		t.Fatal("setup does not force wrap padding")
	}
	before, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	real := w.writeAt
	w.writeAt = func(b []byte, off int64) (int, error) { calls++; return real(b, off) }
	_, err = w.append(walEntryV3{LSN: 4, Type: walEntryWriteBatch, LBA: 10, BatchCount: 2, Length: uint32(len(payload)), Data: payload}, 3, nil)
	if !errors.Is(err, errWALFull) {
		t.Fatalf("pinned batch append: %v", err)
	}
	after, e := os.ReadFile(f.Name())
	if e != nil {
		t.Fatal(e)
	}
	if calls != 0 || w.logicalHeadV3() != head || w.highestLSN != 3 || !bytes.Equal(before, after) {
		t.Fatalf("refusal altered physical head/LSN/wrap bytes: calls=%d head=%d", calls, w.logicalHeadV3())
	}
}

func TestNativeReplacementRecoveredBatchFrontier(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	if _, err := s.WriteBatch(10, batchBlocks(0x40, 4)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
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
	if restored.NextLSN() != 5 {
		t.Fatalf("recovered next LSN=%d", restored.NextLSN())
	}
	if lsn, e := restored.Write(20, blk(0x65)); e != nil || lsn != 5 {
		t.Fatalf("append after recovered batch: %d %v", lsn, e)
	}
	if _, err = restored.Sync(); err != nil {
		t.Fatal(err)
	}
	for i := uint32(0); i < 4; i++ {
		got, e := restored.Read(10 + i)
		if e != nil || !bytes.Equal(got, blk(0x40+byte(i))) {
			t.Fatalf("batch block%d changed: %v", i, e)
		}
	}
	got, err := restored.Read(20)
	if err != nil || !bytes.Equal(got, blk(0x65)) {
		t.Fatalf("post-recovery append bytes: %v", err)
	}
}

func TestNativeReplacementApplyDirtSurvivesCheckpoint(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	if err := s.SeedChunkFrontier(4); err != nil {
		t.Fatal(err)
	}
	f := newFakeChunkRemote()
	l := mustAttach(t, s, f, ctConfig(1))
	if err := s.ApplyEntry(16, blk(0x16), 5); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if s.CheckpointLSN() != 5 || s.dm.len() != 0 {
		t.Fatal("setup did not checkpoint applied WAL")
	}
	if err := l.EvictChunk(4); !errors.Is(err, errChunkEvictRefused) {
		t.Fatalf("eviction ignored applied native map dirt: %v", err)
	}
	cut, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	got := f.imageOf(t, cut.Head)
	if !bytes.Equal(got[16*ctBlockSize:17*ctBlockSize], blk(0x16)) {
		t.Fatal("capture omitted checkpointed ApplyEntry bytes")
	}
}

func TestNativeReplacementRecoverInvalidatesStagedImage(t *testing.T) {
	s, f, l, _ := precopyLayer(t, 1, 3)
	if !s.FormatV3() {
		t.Fatal("replacement requires native v3")
	}
	want := blk(0xA7)
	recovered := false
	l.testAfterStageHook = func(round int) {
		if round != 0 || recovered {
			return
		}
		d := s.v3.pub.current()
		head := s.v3.w.logicalHeadV3()
		record, err := encodeWALEntryV3(walEntryV3{LSN: s.NextLSN(), Type: walEntryWrite, LBA: 0, Length: d.BlockSize, Data: want, Nonce: d.Nonce, LogicalStart: head, DurableLSN: d.W}, d.BlockSize, d.VolumeSize/uint64(d.BlockSize))
		if err != nil {
			t.Error(err)
			return
		}
		// A complete unacknowledged record can remain beyond the captured writer
		// head. Its nonce/cursor/CRC are native v3, not a legacy scanner fixture.
		if _, err = s.fd.WriteAt(record, int64(d.WALOffset+head%d.WALSize)); err != nil {
			t.Error(err)
			return
		}
		if _, err = s.Recover(); err != nil {
			t.Error(err)
			return
		}
		got, err := s.Read(0)
		if err != nil || !bytes.Equal(got, want) {
			t.Errorf("native prefix not recovered: %v", err)
			return
		}
		recovered = true
	}
	cut, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if !recovered || cut.Stats.StagedChunks == 0 {
		t.Fatal("staged/recovery schedule did not run")
	}
	if !bytes.Equal(f.imageOf(t, cut.Head)[:ctBlockSize], want) {
		t.Fatal("cut reused bytes staged before native Recover")
	}
	assertNoIntentsLeft(t, f)
}
