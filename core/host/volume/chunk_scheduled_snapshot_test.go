package volume

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A scheduled snapshot is one named snapshot of class "scheduled", named
// for its attempt time and labelled with the schedule's keep, recording
// exactly the HEAD its own cut installed. A retry of the same attempt
// replays without cutting again.
func TestScheduledSnapshot_SealsTheCutHeadAsScheduled(t *testing.T) {
	ctx := context.Background()
	fp, cat := adapterCatalog(t)
	take := ScheduledSnapshot(cat, 7)
	at := time.Date(2026, 11, 21, 3, 4, 5, 0, time.UTC)
	cuts := 0
	cut := func(ctx context.Context) (storage.ChunkHead, error) {
		cuts++
		r, err := fp.CutVolume(ctx, "v1", 0)
		return r.Head, err
	}
	if err := take(ctx, "v1", at, cut); err != nil {
		t.Fatal(err)
	}
	if err := take(ctx, "v1", at, cut); err != nil || cuts != 1 {
		t.Fatalf("retry of the same attempt: %v, %d cuts; want a replay without a second cut", err, cuts)
	}
	list, _, err := cat.ListSnapshots(ctx, "v1", chunkstore.ListOptions{})
	if err != nil || len(list) != 1 {
		t.Fatalf("list: %+v %v", list, err)
	}
	s := list[0]
	if s.Name != "sched-20261121T030405Z" || s.Class != chunkstore.SnapshotClassScheduled || s.Labels[chunkstore.LabelScheduleKeep] != "7" ||
		s.HeadGen != fp.cut.Head.Gen || s.HeadEpoch != fp.cut.Head.Epoch || s.CutLSN != fp.cut.Head.RemoteLSN {
		t.Fatalf("snapshot %+v; want sched-20261121T030405Z, class scheduled, keep 7, the cut's HEAD", s)
	}
}

// A failed cut is reported as a durable.SnapshotFailure with the code
// the control endpoint gives the same error, and records nothing.
func TestScheduledSnapshot_FailureCarriesTheEndpointCode(t *testing.T) {
	ctx := context.Background()
	_, cat := adapterCatalog(t)
	take := ScheduledSnapshot(cat, 7)
	for _, c := range []struct {
		err  error
		code string
	}{
		{storage.ErrChunkLayerFenced, codeFenced},
		{storage.ErrChunkRemoteUnavailable, chunkstore.CodeRemoteUnavailable},
		{context.DeadlineExceeded, chunkstore.CodeDeadlineExceeded},
	} {
		err := take(ctx, "v1", time.Now(), func(context.Context) (storage.ChunkHead, error) { return storage.ChunkHead{}, c.err })
		var f *durable.SnapshotFailure
		if !errors.As(err, &f) || f.Code != c.code || !errors.Is(err, c.err) {
			t.Fatalf("cut error %v: got %v; want a SnapshotFailure with code %s", c.err, err, c.code)
		}
	}
	if list, _, _ := cat.ListSnapshots(ctx, "v1", chunkstore.ListOptions{}); len(list) != 0 {
		t.Fatalf("failed attempts recorded %+v", list)
	}
}
