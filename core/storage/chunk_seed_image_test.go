package storage

import (
	"bytes"
	"testing"
)

func TestChunkSeededFrontierPreservesInheritedImage(t *testing.T) {
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
	_, err := s.Write(0, makeBlock(ctBlockSize, 0x41))
	if err != nil {
		t.Fatal(err)
	}
	cut, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	want := bytes.Clone(image)
	copy(want, makeBlock(ctBlockSize, 0x41))
	if !bytes.Equal(f.imageOf(t, cut.Head), want) {
		t.Fatal("seeded cut changed inherited or appended bytes")
	}
}
