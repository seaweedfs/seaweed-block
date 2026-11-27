package volume

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Through the real provider, chunk control and HTTP route: the snapshot past
// the per-volume limit answers 409 snapshot_limit and writes nothing (HEAD
// keeps its generation, idle activity, root and position), with nothing new
// to publish and with new data waiting. The next ordinary cut publishes the
// waiting data.
// limitVolume is a chunk volume through the real provider (as blockvolume
// builds it), with its write path and a HEAD reader.
func limitVolume(t *testing.T) (*durable.DurableProvider, *testutil.LayoutFixture, storage.LogicalStorage, func() layout.Head) {
	t.Helper()
	ctx := context.Background()
	fixture := testutil.NewLayoutFixture(t, "v1", 256, 16)
	cfg := durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: 256, ChunkBlocks: 16}
	fixture.Configure(&cfg)
	dp, err := durable.NewDurableProvider(cfg, healthyView{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dp.Close() })
	s, err := dp.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dp.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	head := func() layout.Head {
		h, found, err := fixture.Catalog.ReadHead(ctx, fixture.Ref)
		if err != nil || !found {
			t.Fatalf("HEAD: found=%v err=%v", found, err)
		}
		return h
	}
	return dp, fixture, s, head
}

// committedBlock reads block lba of HEAD's current image from the store
// (the root, its segment, the chunk), not from the holder's cache.
func committedBlock(t *testing.T, fixture *testutil.LayoutFixture, h layout.Head, lba uint64) []byte {
	t.Helper()
	ctx := context.Background()
	if h.Current == nil {
		t.Fatal("HEAD names no image")
	}
	st, err := fixture.Catalog.Store(fixture.Ref)
	if err != nil {
		t.Fatal(err)
	}
	r, err := layout.NewReader("/provider-test", fixture.Ref.Dir(), st.Fetch) // testutil.NewLayoutFixture's base
	if err != nil {
		t.Fatal(err)
	}
	img, err := r.Open(ctx, *h.Current)
	if err != nil {
		t.Fatal(err)
	}
	const chunkBlocks = 16 // limitVolume's geometry
	c, err := img.Chunk(ctx, lba/chunkBlocks)
	if err != nil {
		t.Fatal(err)
	}
	if c.Zero {
		return make([]byte, 4096)
	}
	off := (lba % chunkBlocks) * 4096
	return c.Data[off : off+4096]
}

func TestChunkSnapshotPastTheLimitIsRefusedWithHEADUnchanged(t *testing.T) {
	ctx := context.Background()
	dp, fixture, s, head := limitVolume(t)
	server := chunkServer(t, NewChunkControl(dp, fixture.Catalog, fixture.Ref), true)
	snapshot := func(name string) (int, string) {
		w := serve(t, server, http.MethodPost, "/chunk/snapshot", fmt.Sprintf(`{"volumeID":"v1","name":%q,"requestID":%q}`, name, "req-"+name), "")
		var body struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(w.Body.Bytes(), &body)
		return w.Code, body.Code
	}
	if _, err := s.Write(0, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < layout.MaxSnapshots; i++ {
		if status, code := snapshot(fmt.Sprintf("s%d", i)); status != http.StatusOK {
			t.Fatalf("snapshot %d: %d %s", i+1, status, code)
		}
	}
	refused := func(what string) {
		t.Helper()
		before := head()
		if status, code := snapshot("over"); status != http.StatusConflict || code != chunkstore.CodeSnapshotLimit {
			t.Fatalf("%s: snapshot %d answered %d %s; want 409 %s", what, layout.MaxSnapshots+1, status, code, chunkstore.CodeSnapshotLimit)
		}
		if after := head(); !reflect.DeepEqual(after, before) || after.Activity.Kind != layout.ActIdle {
			t.Fatalf("%s: the refused snapshot changed HEAD: before %+v, after %+v", what, before, after)
		}
	}
	refused("nothing new")
	waiting := bytes.Repeat([]byte("past-the-limit:1"), 4096/16)
	if _, err := s.Write(1, waiting); err != nil {
		t.Fatal(err)
	}
	refused("new data waiting")
	before := head()
	if _, err := dp.CutVolume(ctx, "v1", 0); err != nil {
		t.Fatalf("cut after the refusals: %v", err)
	}
	after := head()
	if after.Position <= before.Position || len(after.Snapshots) != layout.MaxSnapshots {
		t.Fatalf("cut after the refusals: position %d -> %d, %d snapshots", before.Position, after.Position, len(after.Snapshots))
	}
	if got := committedBlock(t, fixture, after, 1); !bytes.Equal(got, waiting) {
		t.Fatalf("block 1 of the image committed after the refusals: %q...; want the data written while refused", got[:16])
	}
}

// The scheduled snapshot on a volume at the limit (snapshots the schedule's
// retention does not prune) is refused and recorded as snapshot_limit, and
// its pass still publishes the waiting data by an ordinary cut: the
// schedule keeps HEAD current while the volume is full.
func TestScheduledSnapshotAtTheLimitStillPublishes(t *testing.T) {
	ctx := context.Background()
	dp, fixture, s, head := limitVolume(t)
	for i := 0; i < layout.MaxSnapshots; i++ {
		if _, err := dp.SnapshotVolume(ctx, "v1", fmt.Sprintf("op%d", i), layout.SnapshotMeta{CreatedNs: int64(i + 1), Class: layout.ClassSemantic}); err != nil {
			t.Fatalf("snapshot %d: %v", i+1, err)
		}
	}
	waiting := bytes.Repeat([]byte("scheduled-full:2"), 4096/16)
	if _, err := s.Write(2, waiting); err != nil {
		t.Fatal(err)
	}
	snapshot := func(ctx context.Context, vol, name string, meta layout.SnapshotMeta) (chunkstore.SnapshotInfo, error) {
		h, err := dp.SnapshotVolume(ctx, vol, name, meta)
		if err != nil {
			return chunkstore.SnapshotInfo{}, err
		}
		return chunkstore.SnapshotInfoFromHead(vol, name, h), nil
	}
	cut := func(ctx context.Context) (storage.ChunkHead, error) { // as the schedule loop cuts
		r, err := dp.CutVolume(ctx, "v1", 0)
		return r.Head, err
	}
	before := head()
	err := ScheduledSnapshot(snapshot, 7)(ctx, "v1", time.Now(), cut)
	var failure *durable.SnapshotFailure
	if !errors.As(err, &failure) || failure.Code != chunkstore.CodeSnapshotLimit {
		t.Fatalf("scheduled snapshot at the limit: %v; want a SnapshotFailure with code %s", err, chunkstore.CodeSnapshotLimit)
	}
	after := head()
	if after.Position <= before.Position || *after.Current == *before.Current || len(after.Snapshots) != layout.MaxSnapshots || after.Activity.Kind != layout.ActIdle {
		t.Fatalf("scheduled pass at the limit: position %d -> %d, %d snapshots, activity %s; want the data published unnamed",
			before.Position, after.Position, len(after.Snapshots), after.Activity.Kind)
	}
	if got := committedBlock(t, fixture, after, 2); !bytes.Equal(got, waiting) {
		t.Fatalf("block 2 of the image the scheduled pass published: %q...; want the data written before it", got[:16])
	}
}
