package durable_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// busyOperatorFiler is the holder's filer: while armed, before each HEAD
// write that commits the snapshot named want, an operator deletes one of the
// spare snapshots through its own catalog (a genuine store-direct HEAD write
// that leaves the image in place), so the holder's conditional update loses.
type busyOperatorFiler struct {
	chunkstore.Filer
	mu      sync.Mutex
	armed   bool
	want    string
	spares  []string
	deletes int
	op      func(name string) error
	id      layout.VolumeID
}

func (f *busyOperatorFiler) UpdateEntry(ctx context.Context, dir string, e *filerpb.Entry, expected map[string][]byte) error {
	f.mu.Lock()
	inject := ""
	if f.armed && e.GetName() == layout.HeadName && len(f.spares) > 0 {
		gen, _ := strconv.ParseUint(string(expected["swb.layout.gen"]), 10, 64)
		if h, err := layout.DecodeHead(e.GetContent(), gen+1, f.id); err == nil {
			if _, ok := h.Snapshots[f.want]; ok {
				inject, f.spares = f.spares[0], f.spares[1:]
				f.deletes++
			}
		}
	}
	f.mu.Unlock()
	if inject != "" {
		if err := f.op(inject); err != nil {
			return fmt.Errorf("operator delete of %s: %w", inject, err)
		}
	}
	return f.Filer.UpdateEntry(ctx, dir, e, expected)
}

// KNOWN DEFECT (v1, fixed in 1.0.1): a snapshot whose commit loses to a
// genuine concurrent HEAD writer (a busy operator deleting snapshots during
// it, more often than the commit's bounded retry) answers head_conflict; the
// IMMEDIATE retry then commits the requeued data without the name and
// answers "not in HEAD after its commit" (500 internal). The data is
// committed either way; the name only on a later attempt. This test pins
// today's behaviour: when 1.0.1 makes the immediate retry name the snapshot,
// it fails here and is turned around.
func TestKnownDefectSnapshotRetryAfterAGenuineConflictCommitsTheDataUnnamed(t *testing.T) {
	ctx := context.Background()
	mem := chunkstore.NewMemFiler()
	blobs := &layoutProviderBlobs{blobs: map[string][]byte{}}
	op, err := chunkstore.NewBCatalog(mem, blobs, "/layout-test")
	if err != nil {
		t.Fatal(err)
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	v := chunkstore.VolumeRef{Name: "v1", ID: id}
	if _, err = op.CreateVolume(ctx, v, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
		t.Fatal(err)
	}
	busy := &busyOperatorFiler{Filer: mem, want: "b", id: id, op: func(name string) error { _, err := op.DeleteSnapshot(ctx, v, name); return err }}
	cat, err := chunkstore.NewBCatalog(busy, blobs, "/layout-test")
	if err != nil {
		t.Fatal(err)
	}
	view := newStubView(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1}))
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4, ChunkHolder: "s/r1",
		ChunkLayoutHeadFor: func(string) (layout.StartStore, error) { return cat.Store(v) },
		ChunkLayoutRemoteFor: func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
			return chunkstore.NewLayoutRemote(cat, v, me)
		},
	}, view)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	ws := s.(*storage.WALStore)
	if _, err = p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	head := func() layout.Head {
		h, _, err := op.ReadHead(ctx, v)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	write := func(lba uint32, tag string) []byte {
		b := bytes.Repeat([]byte(fmt.Sprintf("%-16s", tag)), 4096/16)
		if _, err := ws.Write(lba, b); err != nil {
			t.Fatal(err)
		}
		if _, err := ws.Sync(); err != nil {
			t.Fatal(err)
		}
		return b
	}
	write(0, "first")
	spares := []string{"x1", "x2", "x3", "x4", "x5", "x6"}
	for _, n := range append([]string{"s0"}, spares...) {
		if _, err := p.SnapshotVolume(ctx, "v1", n, layout.SnapshotMeta{CreatedNs: 1}); err != nil {
			t.Fatalf("setup snapshot %s: %v", n, err)
		}
	}
	data := write(1, "during-the-busy")
	before := head()
	busy.mu.Lock()
	busy.armed, busy.spares = true, spares
	busy.mu.Unlock()

	_, err = p.SnapshotVolume(ctx, "v1", "b", layout.SnapshotMeta{CreatedNs: 2})
	busy.mu.Lock()
	busy.armed = false
	deletes := busy.deletes
	busy.mu.Unlock()
	if !errors.Is(err, storage.ErrChunkHeadPrecondition) {
		t.Fatalf("snapshot b with a busy operator (%d deletes during its commit): %v; want head_conflict (ErrChunkHeadPrecondition)", deletes, err)
	}
	if h := head(); h.Position != before.Position || h.Current == nil || *h.Current != *before.Current {
		t.Fatalf("the lost commit moved HEAD's image: position %d -> %d", before.Position, h.Position)
	} else if _, named := h.Snapshots["b"]; named {
		t.Fatal("the lost commit named b")
	}

	// Today's behaviour (the known defect): the immediate retry commits the
	// requeued data, without the name, and answers internal.
	_, err = p.SnapshotVolume(ctx, "v1", "b", layout.SnapshotMeta{CreatedNs: 3})
	after := head()
	if err == nil || !strings.Contains(err.Error(), `snapshot "b" of v1 is not in HEAD after its commit`) {
		t.Fatalf("immediate retry of b: %v; want today's known defect %q (if 1.0.1 fixed it, turn this test around)", err, `snapshot "b" of v1 is not in HEAD after its commit`)
	}
	if _, named := after.Snapshots["b"]; named || after.Position <= before.Position || *after.Current == *before.Current || after.Activity.Kind != layout.ActIdle {
		t.Fatalf("immediate retry of b: named %t, position %d -> %d, activity %s; want the data committed unnamed, HEAD idle", named, before.Position, after.Position, after.Activity.Kind)
	}

	// A further attempt names it, at the image that holds the data.
	hd, err := p.SnapshotVolume(ctx, "v1", "b", layout.SnapshotMeta{CreatedNs: 4})
	if err != nil {
		t.Fatalf("third attempt of b: %v", err)
	}
	if sn, ok := hd.Snapshots["b"]; !ok || hd.Current == nil || sn.Root != *hd.Current {
		t.Fatalf("third attempt of b: %+v", hd.Snapshots["b"])
	}
	if got, err := ws.Read(1); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("block 1 after the snapshots: %v", err)
	}
	t.Logf("operator deletes during the lost commit: %d", deletes)
}
