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
