package storage

import "testing"

func TestChunkBirthTracksNewestLocalWriteAtCut(t *testing.T) {
	for _, bound := range []int{0, 1} {
		t.Run(string(rune('0'+bound)), func(t *testing.T) {
			s, _ := newChunkTestStore(t)
			f := newFakeChunkRemote()
			cfg := ctConfig(1)
			cfg.FreshFromHead = true
			cfg.BarrierBound = bound
			l := mustAttach(t, s, f, cfg)
			x, err := s.Write(0, makeBlock(ctBlockSize, 0x41))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write(ctChunkBlocks, makeBlock(ctBlockSize, 0x42)); err != nil {
				t.Fatal(err)
			}
			y, err := s.Write(ctChunkBlocks+1, makeBlock(ctBlockSize, 0x43))
			if err != nil {
				t.Fatal(err)
			}
			first, err := l.Cut(ctTimeout(t))
			if err != nil {
				t.Fatal(err)
			}
			if first.Head.Chunks[0].BirthLSN != x || first.Head.Chunks[1].BirthLSN != y || first.Head.Chunks[0].Encoding != "full" || first.Head.RemoteLSN != y {
				t.Fatalf("births did not preserve distinct local write frontiers: %+v, want %d/%d", first.Head, x, y)
			}
			z, err := s.Write(ctChunkBlocks, makeBlock(ctBlockSize, 0x44))
			if err != nil {
				t.Fatal(err)
			}
			second, err := l.Cut(ctTimeout(t))
			if err != nil {
				t.Fatal(err)
			}
			if second.Head.Chunks[0] != first.Head.Chunks[0] || second.Head.Chunks[1].BirthLSN != z {
				t.Fatal("rewrite changed a cold chunk birth or failed to advance the changed version")
			}
		})
	}
}
