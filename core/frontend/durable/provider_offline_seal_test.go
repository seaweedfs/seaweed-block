package durable_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// M5 PlainStop -> Quiesce: an idle plain detach seals under the caller's deadline,
// including after the frontend handle closes; no live-serving timer applies.
func TestOfflineSealUsesCallerDeadline(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "closed"}[closed], func(t *testing.T) {
			c := newLayoutCase(t, pcBlocks, pcChunkBlocks)
			p := c.provider(t, t.TempDir(), v1View(), "s/r1", func(cfg *durable.ProviderConfig) { cfg.DetachFreezeTimeout = time.Second })
			openRecovered(t, p)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			b, err := p.Open(ctx, "v1")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = b.Write(ctx, 4096, block(0x61)); err != nil {
				t.Fatal(err)
			}
			if closed {
				if err = b.Close(); err != nil {
					t.Fatal(err)
				}
			}
			deadline, _ := ctx.Deadline()
			observed := false
			durable.SetTestDetachTrace(p, func(stage string, got time.Time) {
				if stage == "cut" && !got.Equal(deadline) {
					t.Errorf("offline seal inherited live cut deadline: got %v want caller %v", got, deadline)
				}
			})
			durable.SetTestDetachStopIO(p, func(c context.Context) error {
				observed = true
				got, ok := c.Deadline()
				if !ok || !got.Equal(deadline) {
					t.Errorf("offline seal inherited live timer: got %v known=%t want caller %v", got, ok, deadline)
				}
				return b.(*durable.StorageBackend).StopIO(c, "offline seal test")
			})
			r, last, err := p.DetachVolumePlain(ctx, "v1")
			if err != nil || r.Head.State != storage.ChunkHeadDetached || r.H != last {
				t.Fatalf("offline seal failed: %+v last=%d err=%v", r, last, err)
			}
			if !observed {
				t.Error("offline seal did not drain")
			}
			if !bytes.Equal(c.block(t, 1), block(0x61)) {
				t.Fatal("offline seal lost acknowledged bytes")
			}
		})
	}
}

// Caller cancellation ends an offline upload; the closed backend stays closed,
// and a new caller can retry the seal without losing acknowledged bytes.
type cancelSealRemote struct {
	storage.ChunkRemote
	keyer   storage.ChunkKeyer
	aborter storage.ChunkRemote
	once    sync.Once
	cancel  context.CancelFunc
}

func (r *cancelSealRemote) ChunkKey(idx uint64, sum [32]byte) string {
	return r.keyer.ChunkKey(idx, sum)
}
func (r *cancelSealRemote) Abort() { r.aborter.Abort() }

func (r *cancelSealRemote) Put(ctx context.Context, key string, b []byte) error {
	r.once.Do(r.cancel)
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.ChunkRemote.Put(ctx, key, b)
}
func TestOfflineSealCallerCancelCanRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := newLayoutCase(t, pcBlocks, pcChunkBlocks)
	remote := &cancelSealRemote{cancel: cancel}
	p := c.provider(t, t.TempDir(), v1View(), "s/r1", func(cfg *durable.ProviderConfig) {
		cfg.ChunkLayoutRemoteFor = func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
			r, err := chunkstore.NewLayoutRemote(c.cat, c.ref, me)
			if err != nil {
				return nil, err
			}
			remote.ChunkRemote = r
			remote.keyer = r
			remote.aborter = r
			return remote, nil
		}
	})
	openRecovered(t, p)
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Write(context.Background(), 4096, block(0x72)); err != nil {
		t.Fatal(err)
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err = p.DetachVolumePlain(ctx, "v1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("offline caller cancellation lost: %v", err)
	}
	if _, err = b.Write(context.Background(), 8192, block(0x73)); err == nil {
		t.Fatal("failed offline seal reopened closed frontend")
	}
	retry, end := context.WithTimeout(context.Background(), time.Minute)
	defer end()
	r, _, err := p.DetachVolumePlain(retry, "v1")
	if err != nil || r.Head.State != storage.ChunkHeadDetached || !bytes.Equal(c.block(t, 1), block(0x72)) {
		t.Fatalf("offline retry failed: %+v %v", r, err)
	}
}
