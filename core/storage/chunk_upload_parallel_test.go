package storage

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestChunkUploads_DefaultOverlapsBeforeCAS(t *testing.T) {
	_, f, l, w := precopyLayer(t, 0, 3)
	baselineCAS := f.stats().cas
	f.mu.Lock()
	f.putGate = make(chan struct{})
	gate := f.putGate
	f.putEntered = make(chan string, 8)
	entered := f.putEntered
	f.mu.Unlock()
	done := make(chan error, 1)
	go func() { _, err := l.Cut(context.Background()); done <- err }()
	defer func() {
		close(gate)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatalf("only %d upload calls entered before release; default uploads must overlap", i)
		}
	}
	f.mu.Lock()
	if len(f.intents) == 0 || f.casCalls != baselineCAS {
		t.Error("overlapping uploads require a durable intent and no root CAS")
	}
	f.mu.Unlock()
	_ = w
}

func TestChunkUploads_ConfiguredBoundAndCancellation(t *testing.T) {
	for _, limit := range []int{1, 2, 8} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			s, _ := newChunkTestStore(t)
			f := newFakeChunkRemote()
			cfg := ctConfig(1)
			cfg.FreshFromHead = true
			cfg.UploadConcurrency = limit
			l := mustAttach(t, s, f, cfg)
			w := newWriteLog()
			for i := 0; i < 8; i++ {
				w.write(t, s, uint32(i*ctChunkBlocks), byte(i+1))
			}
			gate := make(chan struct{})
			entered := make(chan string, 16)
			f.mu.Lock()
			f.putGate = gate
			f.putEntered = entered
			f.mu.Unlock()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := l.Cut(ctx); done <- err }()
			for i := 0; i < limit; i++ {
				select {
				case <-entered:
				case <-time.After(time.Second):
					cancel()
					<-done
					t.Fatal("worker bound not reached")
				}
			}
			select {
			case <-entered:
				cancel()
				<-done
				t.Fatal("upload cap exceeded")
			case <-time.After(20 * time.Millisecond):
			}
			cancel()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("cancelled upload succeeded")
				}
			case <-time.After(time.Second):
				t.Fatal("cancelled workers not joined")
			}
			if f.stats().cas != 0 {
				t.Fatal("cancelled upload published root")
			}
			assertNoIntentsLeft(t, f) // joined abort; no publication can refer to its keys
		})
	}
}

// A remote may ignore cancellation. Its owned upload must settle before
// failure cleanup can remove the intent; cancellation alone is not a join.
type unjoinedUploadRemote struct {
	*fakeChunkRemote
	calls   atomic.Int32
	second  chan struct{}
	failed  chan struct{}
	release chan struct{}
}

func (r *unjoinedUploadRemote) Put(ctx context.Context, key string, data []byte) error {
	switch r.calls.Add(1) {
	case 1:
		<-r.second
		close(r.failed)
		return errFakeRemoteDown
	case 2:
		close(r.second)
	default:
		return errFakeRemoteDown
	}
	<-r.release
	return r.fakeChunkRemote.Put(context.Background(), key, data)
}
func TestChunkUploads_FailureJoinsBeforeIntentCleanup(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	r := &unjoinedUploadRemote{fakeChunkRemote: f, second: make(chan struct{}), failed: make(chan struct{}), release: make(chan struct{})}
	cfg := ctConfig(1)
	cfg.FreshFromHead = true
	cfg.UploadConcurrency = 2
	l := mustAttach(t, s, r, cfg)
	w := newWriteLog()
	w.write(t, s, 0, 0x21)
	w.write(t, s, ctChunkBlocks, 0x22)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := l.Cut(ctx); done <- err }()
	select {
	case <-r.failed:
		cancel() // retries are now allowed; terminate this cut explicitly
	case <-time.After(time.Second):
		close(r.release)
		<-done
		t.Fatal("uploads did not overlap")
	}
	select {
	case err := <-done:
		close(r.release)
		t.Fatalf("returned with owned upload still active: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if len(liveIntents(t, f)) == 0 {
		close(r.release)
		<-done
		t.Fatal("intent removed before join")
	}
	close(r.release)
	if err := <-done; !errors.Is(err, errFakeRemoteDown) {
		t.Fatalf("lost upload failure: %v", err)
	}
	if f.stats().cas != 0 {
		t.Fatal("failed batch published")
	}
	assertNoIntentsLeft(t, f)
}
