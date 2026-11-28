package storage

import (
	"bytes"
	"testing"
)

// A volume fresh from HEAD (a restore target) that only READ its blocks back
// has nothing to compare at its next cut: a chunk filled from HEAD holds
// HEAD's bytes by construction. Re-hashing every filled chunk makes the
// final cut of a fully read volume as long as reading the whole volume.
func TestFreshFromHeadReadBackLeavesNoCutCandidates(t *testing.T) {
	src := make([]byte, ctNumBlocks*ctBlockSize)
	for lba := 0; lba < ctNumBlocks; lba++ {
		copy(src[lba*ctBlockSize:], makeBlock(ctBlockSize, byte(0x40+lba)))
	}
	f := newFakeChunkRemote()
	f.seedHead(t, src, 10, 1)
	s, _ := newChunkTestStore(t)
	if err := s.SeedChunkFrontier(10); err != nil {
		t.Fatal(err)
	}
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	l := mustAttach(t, s, f, cfg).(*chunkLayerImpl)
	for lba := uint32(0); lba < ctNumBlocks; lba++ {
		got, err := s.Read(lba)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, blockOf(src, lba)) {
			t.Fatalf("lba %d read back other bytes", lba)
		}
	}
	l.stMu.Lock()
	absent := len(l.absent)
	l.stMu.Unlock()
	if absent != 0 {
		t.Fatalf("setup: %d chunks still absent after reading every block", absent)
	}
	r, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if r.Stats.CandidateChunks != 0 {
		t.Errorf("a read-only fresh volume's cut compared %d of %d chunks (want 0): its final cut re-reads every filled chunk", r.Stats.CandidateChunks, ctNumBlocks/ctChunkBlocks)
	}
	again, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if again.Stats.CandidateChunks != 0 {
		t.Errorf("control: the cut after a finished cut compared %d chunks", again.Stats.CandidateChunks)
	}
	t.Logf("first cut compared %d chunks, second %d", r.Stats.CandidateChunks, again.Stats.CandidateChunks)
	assertNoRemoteMisuse(t, f)
}

// Control: the same volume with nothing read compares nothing.
func TestFreshFromHeadUnreadLeavesNoCutCandidates(t *testing.T) {
	src := make([]byte, ctNumBlocks*ctBlockSize)
	for lba := 0; lba < ctNumBlocks; lba++ {
		copy(src[lba*ctBlockSize:], makeBlock(ctBlockSize, byte(0x40+lba)))
	}
	f := newFakeChunkRemote()
	f.seedHead(t, src, 10, 1)
	s, _ := newChunkTestStore(t)
	if err := s.SeedChunkFrontier(10); err != nil {
		t.Fatal(err)
	}
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	l := mustAttach(t, s, f, cfg).(*chunkLayerImpl)
	r, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if r.Stats.CandidateChunks != 0 {
		t.Errorf("an unread fresh volume compared %d chunks", r.Stats.CandidateChunks)
	}
}

// Control: a filled chunk written locally afterwards is a candidate again,
// and its cut publishes the write.
func TestFreshFromHeadFilledThenWrittenIsACandidate(t *testing.T) {
	src := make([]byte, ctNumBlocks*ctBlockSize)
	for lba := 0; lba < ctNumBlocks; lba++ {
		copy(src[lba*ctBlockSize:], makeBlock(ctBlockSize, byte(0x40+lba)))
	}
	f := newFakeChunkRemote()
	f.seedHead(t, src, 10, 1)
	s, _ := newChunkTestStore(t)
	if err := s.SeedChunkFrontier(10); err != nil {
		t.Fatal(err)
	}
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	l := mustAttach(t, s, f, cfg).(*chunkLayerImpl)
	for lba := uint32(0); lba < ctNumBlocks; lba++ {
		if _, err := s.Read(lba); err != nil {
			t.Fatal(err)
		}
	}
	local := makeBlock(ctBlockSize, 0xEE)
	if _, err := s.Write(ctChunkBlocks+1, local); err != nil {
		t.Fatal(err)
	}
	r, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if r.Stats.CandidateChunks != 1 {
		t.Errorf("filled, then one chunk written: %d candidates, want 1", r.Stats.CandidateChunks)
	}
	if !bytes.Equal(blockOf(f.imageOf(t, r.Head), ctChunkBlocks+1), local) {
		t.Fatal("the cut lost the write to a filled chunk")
	}
}

// filledFreshVolume is a fresh-from-HEAD volume whose every chunk a read has
// filled from HEAD, with its clean-fill records in place.
func filledFreshVolume(t *testing.T) (*WALStore, *chunkLayerImpl, *fakeChunkRemote) {
	t.Helper()
	src := make([]byte, ctNumBlocks*ctBlockSize)
	for lba := 0; lba < ctNumBlocks; lba++ {
		copy(src[lba*ctBlockSize:], makeBlock(ctBlockSize, byte(0x40+lba)))
	}
	f := newFakeChunkRemote()
	f.seedHead(t, src, 10, 1)
	s, _ := newChunkTestStore(t)
	if err := s.SeedChunkFrontier(10); err != nil {
		t.Fatal(err)
	}
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	l := mustAttach(t, s, f, cfg).(*chunkLayerImpl)
	for lba := uint32(0); lba < ctNumBlocks; lba++ {
		if _, err := s.Read(lba); err != nil {
			t.Fatal(err)
		}
	}
	l.stMu.Lock()
	marks := len(l.filledClean)
	l.stMu.Unlock()
	if marks != ctNumBlocks/ctChunkBlocks {
		t.Fatalf("setup: %d clean-fill records, want %d", marks, ctNumBlocks/ctChunkBlocks)
	}
	return s, l, f
}

// Guard enumeration: every path that writes a chunk's local bytes after a
// clean fill bumps that chunk's version, so the fill's record no longer
// holds and the chunk is a cut candidate again. A frontend trim or
// write-zeroes reaches the store as a write.
func TestEveryLocalWriteAfterACleanFillMakesTheChunkACandidate(t *testing.T) {
	const idx = 1
	lba := uint32(idx * ctChunkBlocks)
	data := makeBlock(ctBlockSize, 0xA5)
	for _, tc := range []struct {
		name string
		do   func(t *testing.T, s *WALStore)
	}{
		{"write", func(t *testing.T, s *WALStore) {
			if _, err := s.Write(lba+1, data); err != nil {
				t.Fatal(err)
			}
		}},
		{"write, then flushed to the extent", func(t *testing.T, s *WALStore) {
			if _, err := s.Write(lba+1, data); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := s.flusher.flushOnce(); err != nil {
				t.Fatal(err)
			}
		}},
		{"batch of one block", func(t *testing.T, s *WALStore) {
			if _, err := s.WriteBatch(lba+1, [][]byte{data}); err != nil {
				t.Fatal(err)
			}
		}},
		{"batch of several blocks", func(t *testing.T, s *WALStore) {
			if _, err := s.WriteBatch(lba, [][]byte{data, data}); err != nil {
				t.Fatal(err)
			}
		}},
		{"replicated entry (rebuild, catch-up)", func(t *testing.T, s *WALStore) {
			if err := s.ApplyEntry(lba+1, data, s.NextLSN()); err != nil {
				t.Fatal(err)
			}
		}},
		{"recovery republishing the WAL", func(t *testing.T, s *WALStore) {
			s.noteChunkRecovery()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, l, _ := filledFreshVolume(t)
			before := l.chunkVersion(idx)
			if l.captureCandidates()[idx] {
				t.Fatal("setup: the clean-filled chunk is already a candidate")
			}
			tc.do(t, s)
			if l.chunkVersion(idx) == before {
				t.Errorf("chunk %d's version did not move", idx)
			}
			if !l.captureCandidates()[idx] {
				t.Errorf("chunk %d is not a cut candidate after the local write", idx)
			}
		})
	}
}

// A HEAD that no longer names the ref a chunk was filled from ends the
// fill's record, whatever replaced it: the chunk is a candidate again.
// (While attached, this layer's own cuts change only candidates' refs, and
// another publisher fences the layer; this guards any other path.)
func TestACleanFillRecordEndsWithItsHeadRef(t *testing.T) {
	const idx = 2
	_, l, _ := filledFreshVolume(t)
	if l.captureCandidates()[idx] {
		t.Fatal("setup: the clean-filled chunk is already a candidate")
	}
	l.stMu.Lock()
	h := copyHead(l.head)
	ref := h.Chunks[idx]
	ref.Key += "-replaced"
	ref.SHA256[0] ^= 0xFF
	h.Chunks[idx] = ref
	l.head = h
	l.stMu.Unlock()
	if !l.captureCandidates()[idx] {
		t.Fatal("a chunk whose HEAD ref changed after its fill is not a candidate")
	}
}

// Guard enumeration, BASE: a BASE install after clean fills takes local
// authority; its records end and every chunk is a candidate of the next cut.
func TestABaseInstallAfterCleanFillsEndsTheirRecords(t *testing.T) {
	s, l, _ := filledFreshVolume(t)
	h, err := s.BeginBaseInstall(s.NextLSN()+10, idA)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.WriteBase(ctChunkBlocks+1, makeBlock(ctBlockSize, 0xB5)); err != nil {
		t.Fatal(err)
	}
	if err := h.MarkBaseComplete(); err != nil {
		t.Fatal(err)
	}
	l.stMu.Lock()
	marks := len(l.filledClean)
	l.stMu.Unlock()
	if marks != 0 {
		t.Errorf("%d clean-fill records survived a BASE install", marks)
	}
	r, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if r.Stats.CandidateChunks != ctNumBlocks/ctChunkBlocks {
		t.Errorf("the cut after a BASE install compared %d chunks, want all %d", r.Stats.CandidateChunks, ctNumBlocks/ctChunkBlocks)
	}
}
