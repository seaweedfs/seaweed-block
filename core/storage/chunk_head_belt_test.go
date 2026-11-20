package storage

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

// The fake's direct root replacement models a delayed request accepted at a
// second serialization point; it deliberately bypasses the first filer's CAS.
func TestHeadBeltTakeoverRefusesLateOldCAS(t *testing.T) {
	f := newFakeChunkRemote()
	f.head = chunkHead{Gen: 1, Epoch: 1, Holder: "old", Chunks: map[uint64]chunkRef{}}
	f.afterCAS = func(f *fakeChunkRemote) {
		f.head = chunkHead{Gen: 2, Epoch: 1, Holder: "old", Chunks: map[uint64]chunkRef{}}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	_, err := ClaimChunkHead(ctx, f, ChunkLayerConfig{Epoch: 2, Holder: "new"})
	if !errors.Is(err, ErrChunkLayerFenced) {
		t.Fatalf("late old CAS allowed takeover to serve: %v", err)
	}
}

func TestHeadBeltOldCASUnconfirmedKeepsIntentsAndFrontier(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(1))
	want := bytes.Repeat([]byte{0x67}, 4096)
	if _, err := s.Write(0, want); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	f.afterCAS = func(f *fakeChunkRemote) { f.failGet = errors.New("confirmation unavailable near lease horizon") }
	_, err := layer.Cut(ctTimeout(t))
	if err == nil {
		t.Fatal("unconfirmed CAS returned success")
	}
	if layer.RemoteLSN() != 0 {
		t.Fatal("unconfirmed CAS advanced remoteLSN", layer.RemoteLSN())
	}
	if len(liveIntents(t, f)) == 0 {
		t.Fatal("unconfirmed CAS removed its intent")
	}
	if got, err := s.Read(0); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("local acknowledged bytes changed: %v", err)
	}
}

func TestHeadBeltLowerEpochConflictFencesWithoutCleanup(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(2))
	if _, err := s.Write(0, makeBlock(4096, 0x31)); err != nil {
		t.Fatal(err)
	}
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	frontier := layer.RemoteLSN()
	if _, err := s.Write(1, makeBlock(4096, 0x32)); err != nil {
		t.Fatal(err)
	}
	f.beforeCAS = func(f *fakeChunkRemote) { f.head.Gen++; f.head.Epoch = 1; f.head.Holder = "old" }
	_, err := layer.Cut(ctTimeout(t))
	if !errors.Is(err, ErrChunkLayerFenced) || !layer.Fenced() {
		t.Fatalf("different old owner not fenced: %v", err)
	}
	if layer.RemoteLSN() != frontier {
		t.Fatal("refused cut advanced remote frontier")
	}
	if len(liveIntents(t, f)) == 0 {
		t.Fatal("fenced cut removed its intent")
	}
	calls := f.stats().cas
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, ErrChunkLayerFenced) || f.stats().cas != calls {
		t.Fatalf("fenced owner retried publication: %v", err)
	}
}
