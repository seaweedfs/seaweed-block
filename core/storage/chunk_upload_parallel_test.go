package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestChunkUploads_DefaultOverlapsBeforeCAS(t *testing.T) {
	_, f, l, w := precopyLayer(t, 0, 3)
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
	if len(f.intents) == 0 || f.casCalls != 0 {
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
