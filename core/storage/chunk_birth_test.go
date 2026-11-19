package storage

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"
)

func TestChunkBirthLegacyEngineUsesCutFrontierUpperBound(t *testing.T) {
	s, _ := newLegacyChunkTestStore(t)
	f := newFakeChunkRemote()
	cfg := ctConfig(1)
	l := mustAttach(t, s, f, cfg)
	if _, err := s.Write(0, makeBlock(ctBlockSize, 0x41)); err != nil {
		t.Fatal(err)
	}
	last, err := s.Write(ctChunkBlocks, makeBlock(ctBlockSize, 0x42))
	if err != nil {
		t.Fatal(err)
	}
	cut, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, idx := range []uint64{0, 1} {
		if cr := cut.Head.Chunks[idx]; cr.Encoding != "full" || cr.BirthLSN != last {
			t.Fatalf("legacy chunk %d metadata %+v, want frontier %d", idx, cr, last)
		}
	}
	want := make([]byte, ctNumBlocks*ctBlockSize)
	copy(want, makeBlock(ctBlockSize, 0x41))
	copy(want[ctChunkBlocks*ctBlockSize:], makeBlock(ctBlockSize, 0x42))
	assertImage(t, "legacy inherited image", f.imageOf(t, cut.Head), want)
}

func TestChunkBirthBASEUsesInheritedFrontierIncludingZero(t *testing.T) {
	for _, frontier := range []uint64{0, 7} {
		t.Run(string(rune('0'+frontier)), func(t *testing.T) {
			s, _, l := mappedBASELayerV3(t)
			h, err := s.BeginBaseInstall(frontier, idA)
			if err != nil {
				t.Fatal(err)
			}
			if err := h.WriteBase(5, blk(0x55)); err != nil {
				t.Fatal(err)
			}
			if err := h.MarkBaseComplete(); err != nil {
				t.Fatal(err)
			}
			newest, err := s.Write(0, blk(0x41))
			if err != nil {
				t.Fatal(err)
			}
			cut, err := l.Cut(ctTimeout(t))
			if err != nil {
				t.Fatal(err)
			}
			if cut.Head.Chunks[1].BirthLSN != frontier || cut.Head.Chunks[0].BirthLSN != newest {
				t.Fatalf("BASE birth drifted with unrelated WAL writes: %+v, inherited %d", cut.Head, frontier)
			}
		})
	}
}

func TestChunkBirthCOWDoesNotObservePostBarrierWrite(t *testing.T) {
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
	before, err := s.Write(ctChunkBlocks, makeBlock(ctBlockSize, 0x42))
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
	after, err := s.Write(ctChunkBlocks, makeBlock(ctBlockSize, 0x43))
	if err != nil {
		t.Fatal(err)
	}
	unblock()
	if err := waitErr(t, done, "birth COW cut"); err != nil {
		t.Fatal(err)
	}
	cowWaitSignal(t, joined, "birth COW join")
	first := f.installedHeads()[0]
	if first.Chunks[1].BirthLSN != before || !bytes.Equal(f.imageOf(t, first)[ctChunkBlocks*ctBlockSize:(ctChunkBlocks+1)*ctBlockSize], makeBlock(ctBlockSize, 0x42)) {
		t.Fatal("post-barrier write changed the saved image or its birth")
	}
	l.testCOWRead = nil
	second, err := l.Cut(ctTimeout(t))
	if err != nil || second.Head.Chunks[1].BirthLSN != after {
		t.Fatalf("next version lost post-barrier birth: %+v %v", second, err)
	}
}

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
				t.Fatal("zero-chunk deletion retained a reference or changed the cold birth")
			}
		})
	}
}
