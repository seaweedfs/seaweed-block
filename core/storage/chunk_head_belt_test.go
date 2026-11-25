package storage

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

// The fake's direct root replacement models a delayed request accepted at a
// second serialization point; it deliberately bypasses the first filer's CAS.
func TestHeadBeltOldCASUnconfirmedKeepsSessionAndFrontier(t *testing.T) {
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
	mark := len(f.stats().ops)
	_, err := layer.Cut(ctTimeout(t))
	if err == nil {
		t.Fatal("unconfirmed CAS returned success")
	}
	if layer.RemoteLSN() != 0 {
		t.Fatal("unconfirmed CAS advanced remoteLSN", layer.RemoteLSN())
	}
	// The CAS landed (its commit ended the session at the remote) but the
	// layer could not confirm it: the layer must not Abort.
	if n := countOps(opsSince(f, mark), "abort:"); n != 0 {
		t.Fatal("unconfirmed CAS aborted its session")
	}
	if got, err := s.Read(0); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("local acknowledged bytes changed: %v", err)
	}
	if err := layer.Close(); err != nil {
		t.Fatal(err)
	}
	if n := countOps(opsSince(f, mark), "abort:"); n != 0 {
		t.Fatal("Close aborted an unconfirmed commit's session")
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
	// fenced: what it staged is dropped, and with it its own session
	if f.openSession() != "" {
		t.Fatal("fenced cut kept its session")
	}
	calls := f.stats().cas
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, ErrChunkLayerFenced) || f.stats().cas != calls {
		t.Fatalf("fenced owner retried publication: %v", err)
	}
	if err := layer.Close(); err != nil {
		t.Fatal(err)
	}
	if f.openSession() != "" {
		t.Fatal("a session is open after the fenced layer closed")
	}
}

func TestHeadBeltEmptyBaseForeignPublicationFences(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	layer := mustAttach(t, s, f, ctConfig(2))
	if _, err := s.Write(0, makeBlock(4096, 0x51)); err != nil {
		t.Fatal(err)
	}
	f.beforeCAS = func(f *fakeChunkRemote) {
		f.head = chunkHead{Gen: 1, Epoch: 1, Holder: "old", Chunks: map[uint64]chunkRef{}}
	}
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, ErrChunkLayerFenced) || !layer.Fenced() {
		t.Fatalf("empty-base foreign publication not fenced: %v", err)
	}
	if layer.RemoteLSN() != 0 || f.openSession() != "" {
		t.Fatal("fenced first cut settled, or kept its session")
	}
	calls := f.stats().cas
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, ErrChunkLayerFenced) || f.stats().cas != calls {
		t.Fatal("fenced first cut published again", err)
	}
	if err := layer.Close(); err != nil {
		t.Fatal(err)
	}
	if f.openSession() != "" {
		t.Fatal("a session is open after the fenced layer closed")
	}
}

type rootOnlyBeltRemote struct {
	*fakeChunkRemote
	rootReads int
}

func (r *rootOnlyBeltRemote) GetHeadRoot(ctx context.Context) (ChunkHead, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rootReads++
	return ChunkHead{Gen: r.head.Gen, Epoch: r.head.Epoch, Holder: r.head.Holder}, nil
}

func TestHeadBeltConfirmationUsesOnlyRoot(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	r := &rootOnlyBeltRemote{fakeChunkRemote: f}
	layer := mustAttach(t, s, r, ctConfig(1))
	if _, err := s.Write(0, makeBlock(4096, 0x42)); err != nil {
		t.Fatal(err)
	}
	f.afterCAS = func(f *fakeChunkRemote) { f.failGet = errors.New("full manifest read unavailable") }
	result, err := layer.Cut(ctTimeout(t))
	if err != nil {
		t.Fatalf("root confirmation depended on full manifest read: %v", err)
	}
	if r.rootReads != 1 || result.H != 1 || layer.RemoteLSN() != 1 {
		t.Fatalf("root confirmation reads=%d result=%d remote=%d", r.rootReads, result.H, layer.RemoteLSN())
	}
	if got := f.imageOf(t, f.stats().head); !bytes.Equal(got[:4096], makeBlock(4096, 0x42)) {
		t.Fatal("confirmed wrong image")
	}
}
