package storage

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Faults sit at the remote seam; all captures, intents, hashes and HEADs use
// the real layer and the write-once fake remote.
type retryUploadRemote struct {
	*fakeChunkRemote
	mu     sync.Mutex
	mode   string
	calls  map[string]int
	values map[string]byte
}

func (r *retryUploadRemote) Put(ctx context.Context, key string, data []byte) error {
	r.mu.Lock()
	r.calls[key]++
	if r.values == nil {
		r.values = map[string]byte{}
	}
	if len(data) > 0 {
		r.values[key] = data[0]
	}
	n, mode := r.calls[key], r.mode
	r.mu.Unlock()
	if mode == "down" || mode == "transient" && n == 1 || mode == "mixed" && n == 1 && len(data) > 0 && data[0] == 0x42 {
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

func TestChunkCutMixedSuccessDoesNotReplaySuccessfulKey(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	r := &retryUploadRemote{fakeChunkRemote: f, mode: "mixed", calls: map[string]int{}}
	l := mustAttach(t, s, r, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 0, 0x41)
	w.write(t, s, ctChunkBlocks, 0x42)
	if _, err := l.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if len(f.installedHeads()) != 1 {
		t.Fatal("must publish once")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.calls) != 2 {
		t.Fatalf("changed immutable keys: %v", r.calls)
	}
	seen := map[byte]bool{}
	for key, n := range r.calls {
		value := r.values[key]
		want := 1
		if value == 0x42 {
			want = 2
		}
		if value != 0x41 && value != 0x42 {
			t.Fatalf("unexpected payload %x", value)
		}
		if n != want {
			t.Fatalf("payload %x attempted %d times, want %d", value, n, want)
		}
		seen[value] = true
	}
	if len(seen) != 2 {
		t.Fatal("did not exercise success and retry")
	}
	assertEveryHeadIsPrefixImage(t, f, w)
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
				want := 2
				if mode == "lost" {
					want = 1
				}
				if n != want {
					t.Fatalf("key %s attempted %d times, want %d", key, n, want)
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

func TestChunkCutUploadDeadlineBoundRetainsDirt(t *testing.T) {
	s, _ := newChunkTestStore(t)
	f := newFakeChunkRemote()
	r := &retryUploadRemote{fakeChunkRemote: f, mode: "down", calls: map[string]int{}}
	layer := mustAttach(t, s, r, ctConfig(1))
	w := newWriteLog()
	w.write(t, s, 0, 0x51)
	ctx, cancel := context.WithTimeout(context.Background(), 1700*time.Millisecond)
	defer cancel()
	if _, err := layer.Cut(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("persistent upload outage ended before caller deadline: %v", err)
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("upload retry returned before caller context expired: %v", ctx.Err())
	}
	if layer.Stats().CutsAborted["upload"] != 1 {
		t.Fatal("missing upload abort status")
	}
	r.mu.Lock()
	if len(r.calls) != 1 {
		t.Fatalf("retry changed object keys: %v", r.calls)
	}
	for _, n := range r.calls {
		if n <= 3 {
			t.Errorf("single object attempted %d times, want >3 before deadline", n)
		}
	}
	r.mode = "ok"
	r.mu.Unlock()
	if len(f.installedHeads()) != 0 {
		t.Fatal("failed object published HEAD")
	}
	if _, err := layer.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}

func TestChunkCutRetrySustainedWritesPastWALWithFlush(t *testing.T) {
	const walBytes = 64 * 1024
	s, err := createMappedWALStoreV3ForTest(filepath.Join(t.TempDir(), "volume"), ctNumBlocks, ctBlockSize, walBytes, WModeStrict, ctChunkBlocks)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { closeBounded(t, s) })
	f := newFakeChunkRemote()
	r := &retryUploadRemote{fakeChunkRemote: f, mode: "transient", calls: map[string]int{}}
	layer := mustAttach(t, s, r, ctConfig(1))
	w := newWriteLog()
	for i := 0; i < 96; i++ {
		w.write(t, s, uint32(i%ctNumBlocks), byte(i+1))
		if (i+1)%4 == 0 {
			mustSyncFlush(t, s)
		}
		if (i+1)%16 == 0 {
			if _, err := layer.Cut(ctTimeout(t)); err != nil {
				t.Fatal(err)
			}
		}
	}
	assertEveryHeadIsPrefixImage(t, f, w)
	if len(f.installedHeads()) != 6 {
		t.Fatalf("published %d HEADs", len(f.installedHeads()))
	}
	t.Logf("payload=%d > WAL=%d; 24 periodic flushes; 6 cuts with transient upload faults", 96*ctBlockSize, walBytes)
}
