package storage

import (
	"bytes"
	"testing"
)

// fkzConfig is a fresh attach's config (FreshFromHead) or an existing
// store's.
func fkzConfig(fresh bool) chunkLayerConfig {
	cfg := ctConfig(1)
	cfg.FreshFromHead, cfg.CreatedThisStart = fresh, fresh
	return cfg
}

// fkzAttach attaches and applies the certification, as the provider does
// after its attach commit.
func fkzAttach(t *testing.T, s *WALStore, r chunkRemote, cfg chunkLayerConfig) chunkLayer {
	t.Helper()
	l := mustAttach(t, s, r, cfg)
	l.(*chunkLayerImpl).ApplyFreshKnownZero()
	return l
}

func fkzCandidates(l chunkLayer) int { return l.Stats().DirtyChunks }

// A fresh attach certifies every chunk the claimed HEAD does not name whose
// owned-map entry is still the creation value; a chunk whose entry recorded
// a write (LSN >= 1) is never certified and stays a candidate, even where
// that LSN is not above HEAD's RemoteLSN (so "LSN above HEAD" alone would
// not make it one): here HEAD is at 100 and the write at 1.
func TestFreshKnownZeroNeverCertifiesARecordedWrite(t *testing.T) {
	s, _ := newChunkTestStore(t)
	if _, err := s.Write(uint32(2*ctChunkBlocks+1), bytes.Repeat([]byte{0x22}, ctBlockSize)); err != nil {
		t.Fatal(err)
	}
	remote := newFakeChunkRemote()
	remote.head = chunkHead{Gen: 1, Epoch: 1, RemoteLSN: 100, State: ChunkHeadActive}
	l := fkzAttach(t, s, remote, fkzConfig(true))
	if got := fkzCandidates(l); got != 1 {
		t.Fatalf("fresh attach after a recorded write to chunk 2: %d candidates; want 1 (that chunk only)", got)
	}
}

// Without the fresh premise (an existing store) nothing is certified: every
// chunk keeps its persisted UNKNOWN.
func TestFreshKnownZeroNotWithoutTheFreshPremise(t *testing.T) {
	s, _ := newChunkTestStore(t)
	l := fkzAttach(t, s, newFakeChunkRemote(), fkzConfig(false))
	if got, want := fkzCandidates(l), int(l.(*chunkLayerImpl).numChunks); got != want {
		t.Fatalf("existing-store attach: %d candidates; want all %d", got, want)
	}
}

// The certification clears only chunks whose mutation token is unchanged
// since it was sampled under the attach barrier: a write landing between the
// sample and the clear keeps its chunk a candidate.
func TestFreshKnownZeroSkipsAChunkWrittenAfterTheSample(t *testing.T) {
	s, _ := newChunkTestStore(t)
	raw, err := attachChunkLayerImpl(s, newFakeChunkRemote(), fkzConfig(true))
	if err != nil {
		t.Fatal(err)
	}
	l := raw.(*chunkLayerImpl)
	t.Cleanup(func() { _ = l.Close() })
	if len(l.freshKnownZero) == 0 {
		t.Fatal("no chunk sampled for certification")
	}
	if _, err := s.Write(uint32(3*ctChunkBlocks), bytes.Repeat([]byte{0x33}, ctBlockSize)); err != nil {
		t.Fatal(err)
	}
	l.ApplyFreshKnownZero()
	if got := fkzCandidates(l); got != 1 {
		t.Fatalf("a write between the sample and the clear: %d candidates; want 1 (the written chunk)", got)
	}
}

// A fresh marker alone is not trusted: an attach whose store this start did
// not create (CreatedThisStart false) certifies nothing.
func TestFreshKnownZeroNeedsThisStartsCreate(t *testing.T) {
	s, _ := newChunkTestStore(t)
	cfg := fkzConfig(true)
	cfg.CreatedThisStart = false
	l := fkzAttach(t, s, newFakeChunkRemote(), cfg)
	if got, want := fkzCandidates(l), int(l.(*chunkLayerImpl).numChunks); got != want {
		t.Fatalf("fresh attach of a store this start did not create: %d candidates; want all %d", got, want)
	}
}
