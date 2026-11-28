package durable_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// slowCutLayout is a layout remote whose chunk uploads run until their
// context ends (a final cut longer than the caller's bound), and whose
// settlement calls fail on an ended context as a filer reached over gRPC
// does (the in-memory filer ignores contexts).
type slowCutLayout struct {
	*chunkstore.LayoutRemote
	slow atomic.Bool
}

func (r *slowCutLayout) Put(ctx context.Context, key string, data []byte) error {
	if r.slow.Load() {
		<-ctx.Done()
		return ctx.Err()
	}
	return r.LayoutRemote.Put(ctx, key, data)
}

func (r *slowCutLayout) AbortOpenSession(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return r.LayoutRemote.AbortOpenSession(ctx)
}

func (r *slowCutLayout) ReadHead(ctx context.Context) (layout.Head, error) {
	if err := ctx.Err(); err != nil {
		return layout.Head{}, err
	}
	return r.LayoutRemote.ReadHead(ctx)
}

// A plain detach whose final cut outlasts the caller's bound, against a
// session nothing was committed in, answers within that bound that it was
// aborted with nothing published (and the volume serves again), not that
// its outcome is unknown: the settlement keeps a reserve inside the bound.
func TestLayoutPlainDetachThatOutlastsItsBoundAnswersAborted(t *testing.T) {
	c := newLayoutCase(t, 64, 4)
	var remote *slowCutLayout
	p := c.provider(t, t.TempDir(), v1View(), "s/r1", func(cfg *durable.ProviderConfig) {
		cfg.ChunkLayoutRemoteFor = func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
			r, err := chunkstore.NewLayoutRemote(c.cat, c.ref, me)
			if err != nil {
				return nil, err
			}
			remote = &slowCutLayout{LayoutRemote: r}
			return remote, nil
		}
	})
	ws := openRecovered(t, p)
	if _, err := ws.Write(0, bytes.Repeat([]byte{'Q'}, 4096)); err != nil {
		t.Fatal(err)
	}
	if _, err := ws.Sync(); err != nil {
		t.Fatal(err)
	}
	before := c.head(t)
	remote.slow.Store(true)
	bound := 3 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), bound)
	defer cancel()
	start := time.Now()
	_, _, err := p.DetachVolumePlain(ctx, "v1")
	took := time.Since(start)
	if errors.Is(err, durable.ErrDetachOutcomeUnknown) || !errors.Is(err, durable.ErrDetachAborted) {
		t.Fatalf("plain detach past its bound, nothing committed: %v; want ErrDetachAborted, not ErrDetachOutcomeUnknown", err)
	}
	if took > bound {
		t.Errorf("the answer took %s, past the caller's %s bound", took, bound)
	}
	// Opening and aborting the stop session moves HEAD's generation; nothing
	// is published: not detached, same position, an idle session.
	if got := c.head(t); got.State == layout.StateDetached || got.Position != before.Position || got.Activity.Kind != layout.ActIdle {
		t.Fatalf("an aborted detach published: %+v -> %+v", before, got)
	}
	remote.slow.Store(false)
	if n, err := p.Backend("v1").Write(context.Background(), 4096, bytes.Repeat([]byte{'R'}, 4096)); err != nil || n != 4096 {
		t.Fatalf("the volume does not serve after the aborted detach: %d %v", n, err)
	}
	t.Logf("answered in %s of %s: %v", took.Round(time.Millisecond), bound, err)
}
