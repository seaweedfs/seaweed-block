package storage

import (
	
	"testing"
)

func TestChunkBirthSeededFrontierPreservesInheritedImage(t *testing.T) {
	s, _ := newChunkTestStore(t)
	if err := s.SeedChunkFrontier(7); err != nil {
		t.Fatal(err)
	}
	f := newFakeChunkRemote()
	image := make([]byte, ctNumBlocks*ctBlockSize)
	copy(image[ctChunkBlocks*ctBlockSize:], makeBlock(ctBlockSize, 0x57))
	f.seedHead(t, image, 7, 1)
	cfg := ctConfig(2)
	cfg.FreshFromHead = true
	l := mustAttach(t, s, f, cfg)
	newest, err := s.Write(0, makeBlock(ctBlockSize, 0x41))
	if err != nil {
		t.Fatal(err)
	}
	cut, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if cut.Head.Chunks[1].BirthLSN != 7 || cut.Head.Chunks[0].BirthLSN != newest {
		t.Fatalf("seeded image birth lost: %+v", cut.Head)
	}
}

