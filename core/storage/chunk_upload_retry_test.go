package storage

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Faults sit at the remote seam; all captures, intents, hashes and HEADs use
// the real layer and the write-once fake remote.
type retryUploadRemote struct {
	*fakeChunkRemote
	mu    sync.Mutex
	mode  string
	calls map[string]int
}

func (r *retryUploadRemote) Put(ctx context.Context, key string, data []byte) error {
	r.mu.Lock()
	r.calls[key]++
	n, mode := r.calls[key], r.mode
	r.mu.Unlock()
	if mode == "down" || mode == "transient" && n == 1 {
		return errFakeRemoteDown
	}
	if mode == "lost" && n == 1 {
		if err := r.fakeChunkRemote.Put(ctx, key, data); err != nil {
			return err
		}
		return errFakeRemoteDown // binding committed, response lost
	}
	if mode == "collision" {
		bad := bytes.Repeat([]byte{0xff}, len(data))
		if err := r.fakeChunkRemote.Put(ctx, key, bad); err != nil {
			return err
		}
		return errChunkKeyExists
	}
	return r.fakeChunkRemote.Put(ctx, key, data)
}

func TestChunkCutRetriesOnlyTheFailedUpload(t *testing.T) {
	for _, mode := range []string{"transient", "lost"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := newChunkTestStore(t)
			f := newFakeChunkRemote()
			r := &retryUploadRemote{fakeChunkRemote: f, mode: mode, calls: map[string]int{}}
			layer := mustAttach(t, s, r, ctConfig(1))
			w := newWriteLog()
			w.write(t, s, 0, 0x41)
			w.write(t, s, ctChunkBlocks, 0x42)
			if _, err := layer.Cut(ctTimeout(t)); err != nil {
				t.Fatal(err)
			}
			if len(f.installedHeads()) != 1 {
				t.Fatal("cut did not publish exactly one HEAD")
			}
			assertEveryHeadIsPrefixImage(t, f, w)
			r.mu.Lock()
			defer r.mu.Unlock()
			if len(r.calls) != 2 {
				t.Fatalf("new key generated on retry: %v", r.calls)
			}
			for key, n := range r.calls {
				if n != 2 {
					t.Fatalf("key %s attempted %d times, want 2", key, n)
				}
			}
		})
	}
}

func TestChunkCutUploadCollisionNeverPublishes(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	r := &retryUploadRemote{fakeChunkRemote: f, mode: "collision", calls: map[string]int{}}
	layer := mustAttach(t, s, r, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 0, 0x41)
	if _, err := layer.Cut(ctTimeout(t)); err == nil {
		t.Fatal("published mismatching existing key")
	}
	if len(f.installedHeads()) != 0 {
		t.Fatal("HEAD moved after collision")
	}
}

func TestChunkCutUploadRetryHonorsDeadlineAndRetainsDirt(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	r := &retryUploadRemote{fakeChunkRemote: f, mode: "down", calls: map[string]int{}}
	layer := mustAttach(t, s, r, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 0, 0x41)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if _, err := layer.Cut(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline lost: %v", err)
	}
	if len(f.installedHeads()) != 0 {
		t.Fatal("HEAD moved during outage")
	}
	r.mu.Lock()
	r.mode = "ok"
	r.mu.Unlock()
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}
