package storage

import (
	"context"
	"errors"
	"testing"
)

// HEAD's remote_lsn never decreases (B9 W2 appendix, 2026-11-15 14:30Z): a
// host whose log is behind HEAD (a fresh or migrated host before W4b carries
// LSN continuity) cannot cut or detach; nothing is uploaded and no CAS is
// sent. Once its log continues from HEAD's remote_lsn, it cuts.
func TestChunkCut_RemoteLSNNeverDecreases(t *testing.T) {
	src := make([]byte, ctNumBlocks*ctBlockSize)
	copy(src, makeBlock(ctBlockSize, 0x44))
	f := newFakeChunkRemote()
	f.seedHead(t, src, 10, 1)
	s, _ := newChunkTestStore(t)
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	layer := mustAttach(t, s, f, cfg)
	w := newWriteLog()
	w.write(t, s, 3, 0x33) // local LSN 1, HEAD remote_lsn 10
	before := f.stats()
	if _, err := layer.Cut(ctTimeout(t)); !errors.Is(err, errChunkCaptureRefused) || !errors.Is(err, errChunkLSNBehind) {
		t.Fatalf("cut behind HEAD's remote_lsn: %v; want refused with errChunkLSNBehind", err)
	}
	if _, err := layer.Detach(ctTimeout(t)); !errors.Is(err, errChunkLSNBehind) {
		t.Fatalf("detach behind HEAD's remote_lsn: %v; want errChunkLSNBehind", err)
	}
	after := f.stats()
	if after.cas != before.cas || after.puts != before.puts || after.head.Gen != before.head.Gen {
		t.Fatalf("a refused cut published: %d CAS, %d puts, HEAD gen %d -> %d", after.cas-before.cas, after.puts-before.puts, before.head.Gen, after.head.Gen)
	}

	s.AdvanceFrontier(10) // continuity from HEAD (W4b's job in production)
	w.write(t, s, 4, 0x34)
	r, err := layer.Cut(ctTimeout(t))
	if err != nil || r.Head.RemoteLSN < 10 || r.Head.Gen != before.head.Gen+1 {
		t.Fatalf("cut once the log continues from HEAD: %+v %v", r.Head, err)
	}
}

// MemChunkRemote refuses a proposal that lowers remote_lsn.
func TestMemChunkRemote_RefusesLowerRemoteLSN(t *testing.T) {
	ctx := context.Background()
	m := NewMemChunkRemote()
	gen, err := m.CASHead(ctx, chunkHeadVersion{}, chunkHead{Epoch: 1, Holder: "a", RemoteLSN: 10, Chunks: map[uint64]chunkRef{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.CASHead(ctx, chunkHeadVersion{Gen: gen, Epoch: 1}, chunkHead{Epoch: 1, Holder: "a", RemoteLSN: 9, Chunks: map[uint64]chunkRef{}}); !errors.Is(err, errChunkHeadPrecondition) {
		t.Fatalf("CAS lowering remote_lsn 10 -> 9: %v; want a precondition refusal", err)
	}
	if _, err := m.CASHead(ctx, chunkHeadVersion{Gen: gen, Epoch: 1}, chunkHead{Epoch: 1, Holder: "a", RemoteLSN: 10, Chunks: map[uint64]chunkRef{}}); err != nil {
		t.Fatalf("CAS keeping remote_lsn: %v", err)
	}
}
