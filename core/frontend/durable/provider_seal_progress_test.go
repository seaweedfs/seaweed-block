package durable_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type sealProgressRemote struct {
	storage.ChunkRemote
	layoutRemote *chunkstore.LayoutRemote
	keyer        storage.ChunkKeyer
	aborter      storage.CutAborter
	mu           sync.Mutex
	expire       context.CancelCauseFunc
	keys         []string
	images       map[[32]byte]int
	bytes        int
	calls        int
}

func (r *sealProgressRemote) AbortOpenSession(ctx context.Context) (string, error) {
	return r.layoutRemote.AbortOpenSession(ctx)
}
func (r *sealProgressRemote) ReadHead(ctx context.Context) (layout.Head, error) {
	return r.layoutRemote.ReadHead(ctx)
}

func (r *sealProgressRemote) ChunkKey(idx uint64, sum [32]byte) string {
	return r.keyer.ChunkKey(idx, sum)
}
func (r *sealProgressRemote) AbortCut() { r.aborter.AbortCut() }

func (r *sealProgressRemote) Put(ctx context.Context, key string, b []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.expire != nil && len(r.keys) == 2 {
		r.expire(context.DeadlineExceeded)
		r.expire = nil
		return context.DeadlineExceeded
	}
	if err := r.ChunkRemote.Put(ctx, key, b); err != nil {
		return err
	}
	r.keys = append(r.keys, key)
	r.images[sha256.Sum256(b)]++
	r.bytes += len(b)
	return nil
}

// The caller's deadline cause is injected at the third PUT, after exactly two
// completed uploads. No wall-clock race decides whether progress was made.
func TestPlainSealDeadlineRetryUploadsOnlyRemainingChunks(t *testing.T) {
	const chunks = 5
	const chunkBytes = 4096 * pcChunkBlocks
	parent, stop := context.WithTimeout(context.Background(), time.Minute)
	defer stop()
	first, expire := context.WithCancelCause(parent)
	defer expire(context.Canceled)
	c := newLayoutCase(t, chunks*pcChunkBlocks, pcChunkBlocks)
	remote := &sealProgressRemote{expire: expire, images: map[[32]byte]int{}}
	p := c.provider(t, t.TempDir(), v1View(), "s/r1", func(cfg *durable.ProviderConfig) {
		cfg.ChunkUploadConcurrency = 1
		cfg.ChunkLayoutRemoteFor = func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
			r, err := chunkstore.NewLayoutRemote(c.cat, c.ref, me)
			if err != nil {
				return nil, err
			}
			remote.ChunkRemote, remote.layoutRemote, remote.keyer, remote.aborter = r, r, r, r
			return remote, nil
		}
	})
	openRecovered(t, p)
	b, err := p.Open(parent, "v1")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < chunks; i++ {
		if _, err = b.Write(parent, int64(i*chunkBytes), block(byte(0x40+i))); err != nil {
			t.Fatal(err)
		}
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := remote.GetHead(parent)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = p.DetachVolumePlain(first, "v1"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("plain seal missed deadline event: %v", err)
	}
	if !errors.Is(context.Cause(first), context.DeadlineExceeded) || parent.Err() != nil {
		t.Fatal("caller deadline event was not witnessed")
	}
	after, err := remote.GetHead(parent)
	if err != nil || after.Gen != before.Gen || after.State != before.State {
		t.Fatalf("aborted seal changed HEAD: %+v %v", after, err)
	}
	if remote.calls != 3 || len(remote.keys) != 2 || remote.bytes != 2*chunkBytes {
		t.Fatalf("first attempt progress: calls=%d successful=%d bytes=%d", remote.calls, len(remote.keys), remote.bytes)
	}
	firstKeys := append([]string(nil), remote.keys...)
	if _, _, err := p.DetachVolumePlain(parent, "v1"); !errors.Is(err, durable.ErrDetachAborted) || !errors.Is(err, storage.ErrChunkHeadPrecondition) {
		t.Fatalf("staged chunks from the ended session were accepted: %v", err)
	}
	r, last, err := p.DetachVolumePlain(parent, "v1")
	if err != nil || r.Head.State != storage.ChunkHeadDetached || r.H != last || r.Head.RemoteLSN != last {
		t.Fatalf("plain seal retry failed: %+v last=%d err=%v", r, last, err)
	}
	if len(remote.keys) != 2*chunks || remote.bytes != 2*chunks*chunkBytes {
		t.Fatalf("the refused cut did not restage in its new session: successful=%d bytes=%d", len(remote.keys), remote.bytes)
	}
	if len(remote.images) != chunks {
		t.Fatalf("restaged image has %d distinct payloads, want %d", len(remote.images), chunks)
	}
	for _, n := range remote.images {
		if n != 2 {
			t.Fatalf("unexpected session PUT count %d", n)
		}
	}
	// The kernel requires a Put in the new session, but the content-named
	// filer entry is reused by a conditional metadata update, not a blob PUT.
	c.blobs.Lock()
	physical := map[[32]byte]int{}
	for _, raw := range c.blobs.blobs {
		physical[sha256.Sum256(raw)]++
	}
	c.blobs.Unlock()
	for i := 0; i < chunks; i++ {
		raw := make([]byte, chunkBytes)
		copy(raw, block(byte(0x40+i)))
		if n := physical[sha256.Sum256(raw)]; n != 1 {
			t.Fatalf("physical payload upload for chunk %d occurred %d times, want one", i, n)
		}
	}
	committed, err := remote.GetHead(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range firstKeys {
		found := false
		for _, ref := range committed.Chunks {
			if ref.Key == key {
				found = true
			}
		}
		if !found {
			t.Errorf("completed first-attempt key not reused: %s", key)
		}
	}
	for lba := 0; lba < chunks*pcChunkBlocks; lba++ {
		want := make([]byte, 4096)
		if lba%pcChunkBlocks == 0 {
			want = block(byte(0x40 + lba/pcChunkBlocks))
		}
		if !bytes.Equal(c.block(t, uint32(lba)), want) {
			t.Fatalf("sealed image differs at lba=%d", lba)
		}
	}
}
