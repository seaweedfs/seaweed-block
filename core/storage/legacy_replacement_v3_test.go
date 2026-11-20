package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
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

func TestNativeReplacementLowerApplyAfterPrecopy(t *testing.T) {
	s, f, l, _ := precopyLayer(t, 1, 3)
	if err := s.ApplyEntry(0, blk(0xA0), 4); err != nil {
		t.Fatal(err)
	}
	image := make([]byte, ctNumBlocks*ctBlockSize)
	for lba := uint32(0); lba < ctNumBlocks; lba++ {
		b, err := s.Read(lba)
		if err != nil {
			t.Fatal(err)
		}
		copy(image[int(lba)*ctBlockSize:], b)
	}
	refused := false
	l.testAfterStageHook = func(round int) {
		if round != 0 {
			return
		}
		if err := s.ApplyEntry(1, blk(0xB1), 3); !errors.Is(err, ErrWALLSNOrder) {
			t.Errorf("native lower-LSN apply after staging: %v", err)
			return
		}
		refused = true
	}
	r, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if !refused || r.Stats.StagedChunks == 0 {
		t.Fatal("precopy/lower-apply schedule did not run")
	}
	if !bytes.Equal(f.imageOf(t, r.Head), image) {
		t.Fatal("refused lower apply changed the cut image")
	}
}

func TestNativeReplacementPublicDefaultBatch(t *testing.T) {
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "volume"), 32, 4096)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.DisableAutoFlushForRecoveryTest()
	if !s.FormatV3() {
		t.Fatal("public default selected an old engine")
	}
	if ns, err := s.WriteBatch(4, batchBlocks(0x41, 3)); err != nil || len(ns) != 3 || ns[0] != 1 || ns[2] != 3 {
		t.Fatalf("default batch=%v %v", ns, err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if i := s.WriteInstrumentation(); i.WALAppendOps != 1 || i.WALEncodeOps != 1 {
		t.Fatalf("native batch was not one record: %+v", i)
	}
	readBatchV3(t, s, 4, 0x41, 3, "public default batch")
}

func TestNativeReplacementHeldFailureCleanup(t *testing.T) {
	var s *WALStore
	var done chan struct{}
	var workerErr error
	boom := errors.New("held WAL failure on cleanup path")
	t.Run("stop_with_worker_held", func(t *testing.T) {
		s, _ = newMappedCheckpointStoreV3(t)
		if _, err := s.Write(1, blk(1)); err != nil {
			t.Fatal(err)
		}
		entered, resume := make(chan struct{}), make(chan struct{})
		done = make(chan struct{})
		release := sync.OnceFunc(func() { close(resume) })
		t.Cleanup(func() {
			release()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("held failing worker did not join")
			}
		})
		s.syncWALFile = func() error { close(entered); <-resume; return boom }
		go func() { _, workerErr = s.Sync(); close(done) }()
		waitV3(t, entered, "worker held on deliberate test exit")
		t.SkipNow()
	})
	select {
	case <-done:
	default:
		t.Fatal("cleanup returned before worker settled")
	}
	if !errors.Is(workerErr, boom) {
		t.Fatalf("held failure lost its cause: %v", workerErr)
	}
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if !closed || s.fd != nil {
		t.Fatal("cleanup did not close the native store after joining")
	}
}

func TestNativeReplacementTornWrapKeepsAcknowledgedSuffix(t *testing.T) {
	for _, duringFlush := range []bool{false, true} {
		t.Run(map[bool]string{false: "full_checkpoint", true: "append_during_checkpoint"}[duringFlush], func(t *testing.T) {
			s, path := newMappedCheckpointStoreV3(t)
			s.admission = nil // isolate physical reuse, not the pressure timer
			write := func(lba uint32) {
				t.Helper()
				if _, err := s.Write(lba, blk(byte(0x10+lba))); err != nil {
					t.Fatal(err)
				}
			}
			syncStore := func() {
				t.Helper()
				if _, err := s.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			write(0)
			syncStore()
			if duringFlush {
				s.flusher.afterSnapshotForTest = func() { s.flusher.afterSnapshotForTest = nil; write(1); syncStore() }
			}
			if err := s.flusher.flushOnce(); err != nil {
				t.Fatal(err)
			}
			if s.CheckpointLSN() != 1 {
				t.Fatalf("checkpoint=%d", s.CheckpointLSN())
			}
			start := uint32(1)
			if duringFlush {
				start = 2
			}
			for lba := start; lba <= 14; lba++ {
				write(lba)
			}
			syncStore()
			write(15) // unsynced wrap over only checkpoint-released bytes
			off, _, _, _, ok := s.dm.get(15)
			if !ok || off != 0 {
				t.Fatalf("latest append did not wrap: %d", off)
			}
			rec := uint64(walEntryHeaderSizeV3 + 4096)
			end := 15 * rec
			if _, err := s.fd.WriteAt(make([]byte, s.sb.WALSize-end), int64(s.sb.WALOffset+end)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.fd.WriteAt(make([]byte, 2048), int64(s.sb.WALOffset+walEntryPrefixSizeV3)); err != nil {
				t.Fatal(err)
			}
			crashReadOnlyStoreV3(t, s)
			r, err := OpenWALStore(path)
			if err != nil {
				t.Fatal(err)
			}
			r.DisableAutoFlushForRecoveryTest()
			t.Cleanup(func() { _ = r.Close() })
			if n, err := r.Recover(); err != nil || n != 15 {
				t.Fatalf("torn-wrap recovery=%d %v", n, err)
			}
			for lba := uint32(0); lba <= 14; lba++ {
				if got, err := r.Read(lba); err != nil || !bytes.Equal(got, blk(byte(0x10+lba))) {
					t.Fatalf("ACK LBA %d after torn wrap: %v", lba, err)
				}
			}
		})
	}
}

func TestNativeReplacementAbruptCrashWindows(t *testing.T) {
	for _, phase := range []string{"before_sync", "after_sync", "after_checkpoint"} {
		t.Run(phase, func(t *testing.T) {
			s, path := newMappedCheckpointStoreV3(t)
			if _, err := s.Write(1, blk(0x11)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write(2, blk(0x22)); err != nil {
				t.Fatal(err)
			}
			if phase != "before_sync" {
				if _, err := s.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "after_checkpoint" {
				if err := s.flusher.flushOnce(); err != nil {
					t.Fatal(err)
				}
			}
			crashReadOnlyStoreV3(t, s)
			r, err := OpenWALStore(path)
			if err != nil {
				t.Fatal(err)
			}
			r.DisableAutoFlushForRecoveryTest()
			t.Cleanup(func() { _ = r.Close() })
			if _, err := r.Recover(); err != nil {
				t.Fatal(err)
			}
			if got, err := r.Read(1); err != nil || !bytes.Equal(got, blk(0x11)) {
				t.Fatalf("ACK lost at %s: %v", phase, err)
			}
			if phase != "before_sync" {
				if got, err := r.Read(2); err != nil || !bytes.Equal(got, blk(0x22)) {
					t.Fatalf("second ACK lost at %s: %v", phase, err)
				}
			}
		})
	}
}

func TestNativeReplacementDataFaultDuringCheckpoint(t *testing.T) {
	for _, caller := range []string{"wal", "extent"} {
		t.Run(caller, func(t *testing.T) {
			s, _ := newMappedCheckpointStoreV3(t)
			if _, err := s.Write(1, blk(1)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			before := s.v3.pub.current()
			entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(resume) })
			var flushErr error
			t.Cleanup(func() {
				release()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Error("checkpoint worker did not join")
				}
			})
			s.v3.testMapPrepared = func() { close(entered); <-resume }
			var writes atomic.Int32
			real := s.v3.pub.io.writeSlot
			s.v3.pub.io.writeSlot = func(off int64, b []byte) error { writes.Add(1); return real(off, b) }
			go func() { flushErr = s.flusher.flushOnce(); close(done) }()
			waitV3(t, entered, "checkpoint map prepared before publication")
			boom := errors.New("data fsync fault while checkpoint publication waits")
			var err error
			if caller == "wal" {
				if _, err = s.Write(2, blk(2)); err != nil {
					t.Fatal(err)
				}
				s.syncWALFile = func() error { return boom }
				_, err = s.Sync()
			} else {
				s.syncExtentFile = func() error { return boom }
				err = s.flusher.flushOnce()
			}
			if !errors.Is(err, boom) {
				t.Fatalf("actual %s fsync: %v", caller, err)
			}
			release()
			waitV3(t, done, "checkpoint refused after data fault")
			if !errors.Is(flushErr, boom) || writes.Load() != 0 || s.v3.pub.current() != before || s.CheckpointLSN() != 0 {
				t.Fatalf("checkpoint passed concurrent data fault: err=%v writes=%d", flushErr, writes.Load())
			}
			if _, _, lsn, _, ok := s.dm.get(1); !ok || lsn != 1 {
				t.Fatal("refused checkpoint retired acknowledged WAL")
			}
		})
	}
}

func TestNativeReplacementRecoverRejectsOverflowBatch(t *testing.T) {
	for name, mutate := range map[string]func([]byte){
		"count_overflow":        func(b []byte) { binary.LittleEndian.PutUint64(b[8:16], (uint64(1)<<52)+3) },
		"lsn_overflow":          func(b []byte) { binary.LittleEndian.PutUint64(b[:8], ^uint64(0)-1) },
		"count_length_mismatch": func(b []byte) { binary.LittleEndian.PutUint64(b[8:16], 2) },
		"length_overflow":       func(b []byte) { binary.LittleEndian.PutUint32(b[26:30], ^uint32(0)) },
	} {
		t.Run(name, func(t *testing.T) {
			s, path := newMappedCheckpointStoreV3(t)
			if _, err := s.WriteBatch(4, batchBlocks(0xA1, 3)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			off, _, _, _, ok := s.dm.get(4)
			if !ok {
				t.Fatal("missing batch")
			}
			b := make([]byte, walEntryHeaderSizeV3+3*4096)
			if _, err := s.fd.ReadAt(b, int64(s.sb.WALOffset+off)); err != nil {
				t.Fatal(err)
			}
			mutate(b)
			end := len(b) - 8
			binary.LittleEndian.PutUint32(b[end:], crc32.Checksum(b[:end], walEntryCRC32CV3))
			if _, err := s.fd.WriteAt(b, int64(s.sb.WALOffset+off)); err != nil {
				t.Fatal(err)
			}
			crashReadOnlyStoreV3(t, s)
			r, err := OpenWALStore(path)
			if err != nil {
				t.Fatal(err)
			}
			r.DisableAutoFlushForRecoveryTest()
			t.Cleanup(func() { _ = r.Close() })
			if _, err := r.Recover(); !isWALCorrupt(err) {
				t.Fatalf("malformed committed native batch: %v", err)
			}
			if r.dm.len() != 0 {
				t.Fatal("malformed batch published dirty entries")
			}
		})
	}
}

func TestNativeReplacementExtentCallbackIsInMemory(t *testing.T) {
	s, path := newChunkTestStore(t)
	f := newFakeChunkRemote()
	l := mustAttach(t, s, f, ctConfig(1))
	mapPath := s.v3.ownedMap.fd.Name()
	before := map[string][]byte{}
	for _, p := range []string{path, mapPath} {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		before[p] = b
	}
	stats := f.stats()
	s.mu.Lock()
	f.mu.Lock()
	unlock := sync.OnceFunc(func() { f.mu.Unlock(); s.mu.Unlock() })
	done := make(chan struct{})
	t.Cleanup(func() {
		unlock()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("callback did not settle")
		}
	})
	go func() { l.OnExtentWrite(12, 7); l.OnExtentWrite(13, 0); close(done) }()
	waitV3(t, done, "native callback with store and remote locked")
	unlock()
	after := f.stats()
	if after.puts != stats.puts || after.gets != stats.gets || after.getHeads != stats.getHeads || after.cas != stats.cas {
		t.Fatal("callback called remote")
	}
	for p, want := range before {
		got, err := os.ReadFile(p)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("callback persisted %s: %v", p, err)
		}
	}
}

func TestNativeReplacementFsyncSerialization(t *testing.T) {
	s, _ := newMappedCheckpointStoreV3(t)
	if _, err := s.Write(0, blk(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(1, blk(2)); err != nil {
		t.Fatal(err)
	}
	snapshot, resumeSnapshot := make(chan struct{}), make(chan struct{})
	walEntered, resumeWAL := make(chan struct{}), make(chan struct{})
	boundary, extentEntered := make(chan struct{}), make(chan struct{})
	flushDone, syncDone := make(chan struct{}), make(chan struct{})
	releaseSnapshot := sync.OnceFunc(func() { close(resumeSnapshot) })
	releaseWAL := sync.OnceFunc(func() { close(resumeWAL) })
	var flushErr, syncErr error
	t.Cleanup(func() {
		releaseSnapshot()
		releaseWAL()
		for _, done := range []chan struct{}{flushDone, syncDone} {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("fsync caller did not join")
			}
		}
	})
	s.flusher.afterSnapshotForTest = func() { close(snapshot); <-resumeSnapshot }
	go func() { flushErr = s.flusher.flushOnce(); close(flushDone) }()
	waitV3(t, snapshot, "native flusher snapshot")
	realWAL := s.syncWALFile
	s.syncWALFile = func() error { close(walEntered); <-resumeWAL; return realWAL() }
	go func() { _, syncErr = s.Sync(); close(syncDone) }()
	waitV3(t, walEntered, "held WAL fsync")
	s.testBeforeFileSync = sync.OnceFunc(func() { close(boundary) })
	realExtent := s.syncExtentFile
	s.syncExtentFile = func() error { close(extentEntered); return realExtent() }
	releaseSnapshot()
	waitV3(t, boundary, "flusher reached shared-file sync boundary")
	select {
	case <-extentEntered:
		t.Fatal("extent fsync overlapped held WAL fsync")
	case <-time.After(50 * time.Millisecond):
	}
	releaseWAL()
	waitV3(t, syncDone, "WAL sync completion")
	waitV3(t, flushDone, "flusher completion")
	if syncErr != nil || flushErr != nil {
		t.Fatalf("Sync=%v flush=%v", syncErr, flushErr)
	}
	waitV3(t, extentEntered, "extent fsync after WAL release")
}

func TestNativeReplacementFlusherCannotOverwriteBASE(t *testing.T) {
	for _, phase := range []string{"snapshot", "extent_sync"} {
		t.Run(phase, func(t *testing.T) {
			s, path := newMappedCheckpointStoreV3(t)
			if _, err := s.Write(5, blk(0x55)); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			replaced := false
			replace := func() {
				h, err := s.beginBaseInstallV3(7, idA)
				if err != nil {
					t.Fatal(err)
				}
				if err := h.WriteBase(5, blk(0xB5)); err != nil {
					t.Fatal(err)
				}
				if err := h.MarkBaseComplete(); err != nil {
					t.Fatal(err)
				}
				replaced = true
			}
			if phase == "snapshot" {
				s.flusher.afterSnapshotForTest = replace
			} else {
				s.flusher.afterExtentSyncForTest = replace
			}
			if err := s.flusher.flushOnce(); err != nil && !errors.Is(err, ErrMapCoverageStale) {
				t.Fatal(err)
			}
			if !replaced {
				t.Fatal("BASE replacement schedule not reached")
			}
			s.flusher.afterSnapshotForTest = nil
			s.flusher.afterExtentSyncForTest = nil
			if got, err := s.Read(5); err != nil || !bytes.Equal(got, blk(0xB5)) {
				t.Fatalf("old flush overwrote BASE: %v", err)
			}
			crashReadOnlyStoreV3(t, s)
			r, err := OpenWALStore(path)
			if err != nil {
				t.Fatal(err)
			}
			r.DisableAutoFlushForRecoveryTest()
			t.Cleanup(func() { _ = r.Close() })
			if _, err := r.Recover(); err != nil {
				t.Fatal(err)
			}
			if got, err := r.Read(5); err != nil || !bytes.Equal(got, blk(0xB5)) {
				t.Fatalf("old WAL resurfaced over BASE: %v", err)
			}
		})
	}
}

func TestNativeReplacementDirectExtentRefusedPreservesWAL(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	if _, err := s.Write(5, blk(0x55)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.WriteExtentDirect(5, blk(0xB5)); !errors.Is(err, errV3NotYet) {
		t.Fatalf("unguarded direct BASE write: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) || s.dm.len() != 1 {
		t.Fatalf("refused direct write changed bytes/dirty WAL: %v", err)
	}
	if got, err := s.Read(5); err != nil || !bytes.Equal(got, blk(0x55)) {
		t.Fatalf("refusal lost ACK bytes: %v", err)
	}
	crashReadOnlyStoreV3(t, s)
	r, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = r.Close() })
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	if got, err := r.Read(5); err != nil || !bytes.Equal(got, blk(0x55)) {
		t.Fatalf("refused direct write lost bytes at reopen: %v", err)
	}
}

func TestNativeReplacementQueuedWritersAfterFailedAppend(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "write", true: "batch"}[batch], func(t *testing.T) {
			s, path := newMappedCheckpointStoreV3(t)
			s.admission = nil
			entered, release, admitted := make(chan struct{}), make(chan struct{}), make(chan struct{})
			open := sync.OnceFunc(func() { close(release) })
			a, b := make(chan struct{}), make(chan struct{})
			var errA, errB error
			t.Cleanup(func() {
				open()
				for _, done := range []chan struct{}{a, b} {
					select {
					case <-done:
					case <-time.After(10 * time.Second):
						t.Error("append worker did not settle")
					}
				}
			})
			boom := errors.New("held native batch pwrite failure")
			var calls atomic.Int32
			s.v3.w.writeAt = func(data []byte, off int64) (int, error) {
				if calls.Add(1) == 1 {
					close(entered)
					<-release
				}
				n, _ := s.fd.WriteAt(data, off)
				return n, boom
			}
			next, head := s.NextLSN(), s.v3.w.logicalHeadV3()
			go func() { _, errA = s.WriteBatch(10, [][]byte{blk(0x31), blk(0x32)}); close(a) }()
			waitV3(t, entered, "failing native append")
			s.testAfterAdmit = sync.OnceFunc(func() { close(admitted) })
			go func() {
				if batch {
					_, errB = s.WriteBatch(20, [][]byte{blk(0x41), blk(0x42)})
				} else {
					_, errB = s.Write(20, blk(0x41))
				}
				close(b)
			}()
			waitV3(t, admitted, "queued writer admission")
			open()
			waitV3(t, a, "failed append return")
			waitV3(t, b, "queued writer return")
			if !errors.Is(errA, boom) || !errors.Is(errB, boom) {
				t.Fatalf("append=%v queued=%v", errA, errB)
			}
			if calls.Load() != 1 || s.NextLSN() != next || s.v3.w.logicalHeadV3() != head || s.dm.len() != 0 {
				t.Fatal("queued writer appended, consumed an LSN or published dirty state after failure")
			}
			crashReadOnlyStoreV3(t, s)
			r, err := OpenWALStore(path)
			if err != nil {
				t.Fatal(err)
			}
			r.DisableAutoFlushForRecoveryTest()
			t.Cleanup(func() { _ = r.Close() })
			if _, err := r.Recover(); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Write(11, blk(0xAC)); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Sync(); err != nil {
				t.Fatal(err)
			}
			crashReadOnlyStoreV3(t, r)
			q, err := OpenWALStore(path)
			if err != nil {
				t.Fatal(err)
			}
			q.DisableAutoFlushForRecoveryTest()
			t.Cleanup(func() { _ = q.Close() })
			if _, err := q.Recover(); err != nil {
				t.Fatal(err)
			}
			if got, err := q.Read(11); err != nil || !bytes.Equal(got, blk(0xAC)) {
				t.Fatalf("failed batch resurfaced over later ACK: %v", err)
			}
		})
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
				if caller == "checkpoint" {
					i := s.FlusherInstrumentation()
					if s.dm.len() != 1 || i.CheckpointFailures != 1 || i.CyclesFailed != 1 || i.CyclesSucceeded != 0 {
						t.Fatalf("failed checkpoint lost retry state or counters: dirty=%d %+v", s.dm.len(), i)
					}
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

func TestNativeReplacementRetainedBatchAcrossWrap(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	for i := byte(1); i <= 9; i++ {
		if _, err := s.Write(0, blk(i)); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := s.flusher.flushOnce(); err != nil {
			t.Fatal(err)
		}
	}
	before := s.v3.w.logicalHeadV3()
	if _, err := s.WriteBatch(8, batchBlocks(0x80, 8)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if s.v3.w.logicalHeadV3() <= s.sb.WALSize || before >= s.sb.WALSize {
		t.Fatal("batch did not physically wrap")
	}
	s.SetRecycleFloorSource(&fakeRecycleFloorSource{floor: 13, anyActive: true})
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if s.CheckpointLSN() != 13 {
		t.Fatalf("partial checkpoint=%d", s.CheckpointLSN())
	}
	crashReadOnlyStoreV3(t, s)
	r, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	r.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = r.Close() })
	if _, err := r.Recover(); err != nil {
		t.Fatal(err)
	}
	if r.NextLSN() != 18 || r.CheckpointLSN() != 13 {
		t.Fatalf("recovered next=%d cp=%d", r.NextLSN(), r.CheckpointLSN())
	}
	var suffix []RecoveryEntry
	if err := r.ScanLBAs(14, func(e RecoveryEntry) error { suffix = append(suffix, e); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(suffix) != 4 {
		t.Fatalf("retained suffix entries=%d", len(suffix))
	}
	for i, e := range suffix {
		if e.LSN != uint64(14+i) || e.LBA != uint32(12+i) || !bytes.Equal(e.Data, blk(byte(0x84+i))) {
			t.Fatalf("suffix %d: %+v", i, e)
		}
	}
	// The retained batch plus wrap padding occupies the hard watermark.
	// This fixture disables automatic flushing, so settle its remainder
	// explicitly before admitting the next guest write.
	if err := r.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if r.CheckpointLSN() != 17 {
		t.Fatalf("settled batch checkpoint=%d", r.CheckpointLSN())
	}
	if n, err := r.Write(20, blk(0xF1)); err != nil || n != 18 {
		t.Fatalf("post-recovery append=%d %v", n, err)
	}
	if _, err := r.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := r.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	readBatchV3(t, r, 8, 0x80, 8, "wrapped retained batch")
	if got, err := r.Read(20); err != nil || !bytes.Equal(got, blk(0xF1)) {
		t.Fatalf("later append: %v", err)
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
