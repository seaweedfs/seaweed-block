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
	if f.openSession() == "" {
		t.Fatal("fenced cut aborted its unresolved commit's session")
	}
	calls := f.stats().cas
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, ErrChunkLayerFenced) || f.stats().cas != calls {
		t.Fatalf("fenced owner retried publication: %v", err)
	}
	if err := layer.Close(); err != nil {
		t.Fatal(err)
	}
	if f.openSession() == "" {
		t.Fatal("Close aborted a fenced, unresolved commit's session")
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
	if layer.RemoteLSN() != 0 || f.openSession() == "" {
		t.Fatal("fenced first cut settled or deleted intents")
	}
	calls := f.stats().cas
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, ErrChunkLayerFenced) || f.stats().cas != calls {
		t.Fatal("fenced first cut published again", err)
	}
	if err := layer.Close(); err != nil {
		t.Fatal(err)
	}
	if f.openSession() == "" {
		t.Fatal("Close removed first-cut intents")
	}
}

func TestHeadBeltSinglePointHasNoCoolingDelay(t *testing.T) {
	f := newFakeChunkRemote()
	f.head = chunkHead{Gen: 1, Epoch: 1, Holder: "old", Chunks: map[uint64]chunkRef{}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := ClaimChunkHead(ctx, f, ChunkLayerConfig{Epoch: 2, Holder: "new"}); err != nil {
		t.Fatal("single-point takeover delayed", err)
	}
	if f.stats().getHeads != 2 {
		t.Fatal("single-point claim skipped readback")
	}
}

type deadlineBeltRemote struct {
	*fakeChunkRemote
	casDeadline, readDeadline bool
}

func (r *deadlineBeltRemote) CASHead(ctx context.Context, v ChunkHeadVersion, h ChunkHead) (uint64, error) {
	d, ok := ctx.Deadline()
	r.casDeadline = ok && time.Until(d) <= 2*time.Second
	return r.fakeChunkRemote.CASHead(ctx, v, h)
}
func (r *deadlineBeltRemote) GetHeadRoot(ctx context.Context) (ChunkHead, error) {
	d, ok := ctx.Deadline()
	r.readDeadline = ok && time.Until(d) <= 2*time.Second
	return r.fakeChunkRemote.GetHead(ctx)
}
func TestHeadBeltOperationsHaveOwnDeadline(t *testing.T) {
	r := &deadlineBeltRemote{fakeChunkRemote: newFakeChunkRemote()}
	r.head = chunkHead{Gen: 1, Epoch: 1, Holder: "old", Chunks: map[uint64]chunkRef{}}
	if _, err := ClaimChunkHead(context.Background(), r, ChunkLayerConfig{Epoch: 2, Holder: "new"}); err != nil {
		t.Fatal(err)
	}
	if !r.casDeadline || !r.readDeadline {
		t.Fatalf("claim deadlines: CAS=%t read=%t", r.casDeadline, r.readDeadline)
	}
	r.casDeadline, r.readDeadline = false, false
	head := r.stats().head
	if _, err := casAndConfirmChunkHead(context.Background(), r, ChunkHeadVersion{Gen: head.Gen, Epoch: head.Epoch}, head); err != nil {
		t.Fatal(err)
	}
	if !r.casDeadline || !r.readDeadline {
		t.Fatalf("cut deadlines: CAS=%t read=%t", r.casDeadline, r.readDeadline)
	}
}

func TestHeadBeltTakeoverWaitsBeforeConfirmation(t *testing.T) {
	f := newFakeChunkRemote()
	f.head = chunkHead{Gen: 1, Epoch: 1, Holder: "old", Chunks: map[uint64]chunkRef{}}
	var committedAt, confirmedAt time.Time
	f.afterCAS = func(f *fakeChunkRemote) { committedAt = time.Now() }
	f.onGetHead = func(f *fakeChunkRemote) {
		if !committedAt.IsZero() {
			confirmedAt = time.Now()
		}
	}
	window := chunkHeadOperationTimeout + chunkHeadSettleTime
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	head, err := ClaimChunkHead(ctx, f, ChunkLayerConfig{Epoch: 2, Holder: "new", HeadCoolingWindow: window})
	if err != nil {
		t.Fatal(err)
	}
	if committedAt.IsZero() || confirmedAt.IsZero() || confirmedAt.Sub(committedAt) < window || f.stats().getHeads != 2 {
		t.Fatal("confirmation read preceded cooling", confirmedAt.Sub(committedAt))
	}
	if head.Gen != 2 || head.Epoch != 2 || head.Holder != "new" {
		t.Fatal("confirmed the wrong owner", head)
	}
}

func TestHeadBeltCoolingWindowCoversCASAndSettle(t *testing.T) {
	for _, window := range []time.Duration{-1, time.Millisecond, chunkHeadOperationTimeout, chunkHeadOperationTimeout + chunkHeadSettleTime - 1} {
		f := newFakeChunkRemote()
		if _, err := ClaimChunkHead(context.Background(), f, ChunkLayerConfig{Epoch: 2, Holder: "new", HeadCoolingWindow: window}); err == nil || f.stats().getHeads != 0 {
			t.Fatalf("unsafe cooling window %s admitted or touched remote: %v", window, err)
		}
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

func TestHeadBeltRetryOfUnconfirmedClaimStillCools(t *testing.T) {
	f := newFakeChunkRemote()
	f.head = chunkHead{Gen: 1, Epoch: 1, Holder: "old", Chunks: map[uint64]chunkRef{}}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := ClaimChunkHead(ctx, f, ChunkLayerConfig{Epoch: 2, Holder: "new", HeadCoolingWindow: chunkHeadOperationTimeout + chunkHeadSettleTime}); !errors.Is(err, context.DeadlineExceeded) || f.stats().cas != 1 {
		t.Fatalf("claim did not stop inside cooling after CAS: %v, calls=%d", err, f.stats().cas)
	}
	retryCtx, retryCancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer retryCancel()
	start := time.Now()
	if _, err := ClaimChunkHead(retryCtx, f, ChunkLayerConfig{Epoch: 2, Holder: "new", HeadCoolingWindow: chunkHeadOperationTimeout + chunkHeadSettleTime}); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < chunkHeadOperationTimeout+chunkHeadSettleTime || f.stats().cas != 2 {
		t.Fatal("retry bypassed cooling or failed to fence the previous incarnation")
	}
}
