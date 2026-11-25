package volume

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/retain"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A scheduled attempt uses the provider's atomic snapshot path and stable
// name/metadata on retry; it never invokes an ordinary cut or catalog write.
func TestScheduledSnapshot_SealsTheCutHeadAsScheduled(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 11, 21, 3, 4, 5, 0, time.UTC)
	calls := 0
	take := ScheduledSnapshot(func(_ context.Context, volume, name string, meta layout.SnapshotMeta) (chunkstore.SnapshotInfo, error) {
		calls++
		if volume != "v1" || name != "sched-20261121T030405Z" || meta.Class != layout.ClassScheduled || meta.CreatedNs != at.UnixNano() || meta.Labels[retain.LabelScheduleKeep] != "7" {
			t.Fatalf("snapshot %s %s %+v", volume, name, meta)
		}
		return chunkstore.SnapshotInfo{}, nil
	}, 7)
	for i := 0; i < 2; i++ {
		if err := take(ctx, "v1", at, func(context.Context) (storage.ChunkHead, error) {
			t.Fatal("ordinary cut bypasses snapshot commit")
			return storage.ChunkHead{}, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatalf("snapshot calls=%d", calls)
	}
}

// A failed cut is reported as a durable.SnapshotFailure with the code
// the control endpoint gives the same error, and records nothing.
func TestScheduledSnapshot_FailureCarriesTheEndpointCode(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		err  error
		code string
	}{
		{storage.ErrChunkLayerFenced, codeFenced},
		{storage.ErrChunkRemoteUnavailable, chunkstore.CodeRemoteUnavailable},
		{context.DeadlineExceeded, chunkstore.CodeDeadlineExceeded},
	} {
		take := ScheduledSnapshot(func(context.Context, string, string, layout.SnapshotMeta) (chunkstore.SnapshotInfo, error) {
			return chunkstore.SnapshotInfo{}, c.err
		}, 7)
		err := take(ctx, "v1", time.Now(), nil)
		var f *durable.SnapshotFailure
		if !errors.As(err, &f) || f.Code != c.code || !errors.Is(err, c.err) {
			t.Fatalf("cut error %v: got %v; want a SnapshotFailure with code %s", c.err, err, c.code)
		}
	}
}
