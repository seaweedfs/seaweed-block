package volume

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
)

// Through the real provider, chunk control and HTTP route: the snapshot past
// the per-volume limit answers 409 snapshot_limit and writes nothing (HEAD
// keeps its generation, idle activity, root and position), with nothing new
// to publish and with new data waiting. The next ordinary cut publishes the
// waiting data.
func TestChunkSnapshotPastTheLimitIsRefusedWithHEADUnchanged(t *testing.T) {
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
	if _, err := s.Write(1, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	refused("new data waiting")
	before := head()
	if _, err := dp.CutVolume(ctx, "v1", 0); err != nil {
		t.Fatalf("cut after the refusals: %v", err)
	}
	if after := head(); after.Position <= before.Position || len(after.Snapshots) != layout.MaxSnapshots {
		t.Fatalf("cut after the refusals: position %d -> %d, %d snapshots", before.Position, after.Position, len(after.Snapshots))
	}
}
