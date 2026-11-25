package storage

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"
)

func TestChunkCutCOWDoesNotObservePostBarrierWrite(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	cfg.FreshFromHead = true
	cfg.BarrierBound = 0
	cfg.UploadConcurrency = 1
	l := mustAttach(t, s, f, cfg).(*chunkLayerImpl)
	if _, err := s.Write(0, makeBlock(ctBlockSize, 0x41)); err != nil {
		t.Fatal(err)
	}
	_, err := s.Write(ctChunkBlocks, makeBlock(ctBlockSize, 0x42))
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	l.testCOWRead = func(idx uint64, _ bool) {
		if idx == 0 {
			once.Do(func() { close(entered) })
			<-release
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan error, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		unblock()
		select {
		case <-joined:
		case <-time.After(ctLocalBound):
			t.Error("birth COW worker not joined")
		}
	})
	go func() { defer close(joined); _, err := l.Cut(ctx); done <- err }()
	cowWaitSignal(t, entered, "copy after the birth barrier")
	_, err = s.Write(ctChunkBlocks, makeBlock(ctBlockSize, 0x43))
	if err != nil {
		t.Fatal(err)
	}
	unblock()
	if err := waitErr(t, done, "birth COW cut"); err != nil {
		t.Fatal(err)
	}
	cowWaitSignal(t, joined, "birth COW join")
	first := f.installedHeads()[0]
	if !bytes.Equal(f.imageOf(t, first)[ctChunkBlocks*ctBlockSize:(ctChunkBlocks+1)*ctBlockSize], makeBlock(ctBlockSize, 0x42)) {
		t.Fatal("post-barrier write changed the saved image")
	}
	l.testCOWRead = nil
	second, err := l.Cut(ctTimeout(t))
	if err != nil || !bytes.Equal(f.imageOf(t, second.Head)[ctChunkBlocks*ctBlockSize:(ctChunkBlocks+1)*ctBlockSize], makeBlock(ctBlockSize, 0x43)) {
		t.Fatalf("the next version lost the post-barrier write: %+v %v", second, err)
	}
}

func TestChunkCutKeepsColdChunksAndDeletesZeroChunks(t *testing.T) {
	for _, bound := range []int{0, 1} {
		t.Run(string(rune('0'+bound)), func(t *testing.T) {
			s, _ := newChunkTestStore(t)
			f := newFakeChunkRemote()
			cfg := ctConfig(1)
			cfg.FreshFromHead = true
			cfg.BarrierBound = bound
			l := mustAttach(t, s, f, cfg)
			if _, err := s.Write(0, makeBlock(ctBlockSize, 0x41)); err != nil {
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
			if first.Head.Chunks[0].Encoding != "full" || first.Head.RemoteLSN != y {
				t.Fatalf("first cut: %+v, want position %d", first.Head, y)
			}
			_, err = s.Write(ctChunkBlocks, makeBlock(ctBlockSize, 0x44))
			if err != nil {
				t.Fatal(err)
			}
			second, err := l.Cut(ctTimeout(t))
			if err != nil {
				t.Fatal(err)
			}
			if second.Head.Chunks[0] != first.Head.Chunks[0] || second.Head.Chunks[1] == first.Head.Chunks[1] {
				t.Fatal("rewrite changed a cold chunk or did not publish the changed one")
			}
			for _, lba := range []uint32{ctChunkBlocks, ctChunkBlocks + 1} {
				if _, err := s.Write(lba, makeBlock(ctBlockSize, 0)); err != nil {
					t.Fatal(err)
				}
			}
			third, err := l.Cut(ctTimeout(t))
			if err != nil {
				t.Fatal(err)
			}
			if _, exists := third.Head.Chunks[1]; exists || third.Head.Chunks[0] != first.Head.Chunks[0] {
				t.Fatal("zero-chunk deletion retained a reference or changed the cold chunk")
			}
		})
	}
}
