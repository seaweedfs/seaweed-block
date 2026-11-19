package storage

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type cutBudgetRemote struct {
	*fakeChunkRemote
	mu        sync.Mutex
	keys      []string
	deadlines []time.Time
	held      chan struct{}
	once      sync.Once
	healthy   bool
}

func (r *cutBudgetRemote) Put(ctx context.Context, key string, data []byte) error {
	r.mu.Lock()
	if r.healthy {
		r.mu.Unlock()
		return r.fakeChunkRemote.Put(ctx, key, data)
	}
	r.keys = append(r.keys, key)
	d, ok := ctx.Deadline()
	if !ok {
		d = time.Time{}
	}
	r.deadlines = append(r.deadlines, d)
	n := len(r.keys)
	r.mu.Unlock()
	if n >= 3 {
		r.once.Do(func() { close(r.held) })
		<-ctx.Done()
		return ctx.Err()
	}
	return r.fakeChunkRemote.Put(ctx, key, data)
}

func TestChunkCutCallerBudgetSharedAcrossStagesAndWritesProgress(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	r := &cutBudgetRemote{fakeChunkRemote: f, held: make(chan struct{})}
	cfg := ctConfig(1)
	cfg.FreshFromHead = true
	cfg.BarrierBound = 1
	cfg.UploadConcurrency = 2
	l := mustAttach(t, s, r, cfg).(*chunkLayerImpl)
	for i := 0; i < 3; i++ {
		if _, err := s.Write(uint32(i*ctChunkBlocks), makeBlock(ctBlockSize, byte(0x41+i))); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	done := make(chan error, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("cut worker not joined")
		}
	})
	start := time.Now()
	go func() { defer close(joined); _, err := l.Cut(ctx); done <- err }()
	select {
	case <-r.held:
	case <-time.After(5 * time.Second):
		t.Fatal("did not reach staged then final upload")
	}
	r.mu.Lock()
	deadlines := append([]time.Time(nil), r.deadlines...)
	r.mu.Unlock()
	if len(deadlines) != 3 {
		t.Fatalf("want two staged and one final upload: %v", deadlines)
	}
	wantDeadline, _ := ctx.Deadline()
	if !deadlines[0].Equal(wantDeadline) {
		t.Fatalf("cut replaced caller's deadline: %v, want %v", deadlines[0], wantDeadline)
	}
	for _, d := range deadlines {
		if !d.Equal(deadlines[0]) {
			t.Fatal("stage/worker received a reset budget")
		}
	}
	writeDone := make(chan error, 1)
	writeJoined := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-writeJoined:
		case <-time.After(5 * time.Second):
			t.Error("ordinary write not joined")
		}
	})
	go func() {
		defer close(writeJoined)
		_, err := s.Write(3*ctChunkBlocks, makeBlock(ctBlockSize, 0x44))
		if err == nil {
			_, err = s.Sync()
		}
		writeDone <- err
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal(err)
		}
	case err := <-done:
		t.Fatalf("ordinary write waited for remote cut settlement: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("ordinary write blocked")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("lost caller deadline: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cooperative cut did not settle")
	}
	if time.Since(start) >= 5*time.Second {
		t.Fatal("cut exceeded sub-5s target")
	}
	if len(f.installedHeads()) != 0 {
		t.Fatal("outage published HEAD")
	}
	r.mu.Lock()
	r.healthy = true
	r.mu.Unlock()
	if _, err := l.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	heads := f.installedHeads()
	if len(heads) != 1 {
		t.Fatal("retry did not publish exactly once")
	}
	want := make([]byte, ctNumBlocks*ctBlockSize)
	for i := 0; i < 4; i++ {
		copy(want[i*ctChunkBlocks*ctBlockSize:], makeBlock(ctBlockSize, byte(0x41+i)))
	}
	if !bytes.Equal(f.imageOf(t, heads[0]), want) {
		t.Fatal("shared-budget abort lost pending/concurrent bytes")
	}
}
