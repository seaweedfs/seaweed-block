package main

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"path"
	"testing"
	"time"
)

type maintenanceGCFiler struct {
	chunkstore.Filer
	gcCalls, marked, freed int
	failSecond             bool
}

func (f *maintenanceGCFiler) LookupEntry(ctx context.Context, dir, name string) (*filerpb.Entry, error) {
	if name == "_GC_MARK" {
		f.gcCalls++
		return nil, errors.New("witness: GC reached")
	}
	return f.Filer.LookupEntry(ctx, dir, name)
}
func (f *maintenanceGCFiler) UpdateEntry(ctx context.Context, dir string, e *filerpb.Entry, expected map[string][]byte) error {
	if string(e.Extended["swb.snap.state"]) == chunkstore.SnapDeleting {
		if f.failSecond && f.marked == 1 {
			return errors.New("second retention mark refused")
		}
		f.marked++
	}
	return f.Filer.UpdateEntry(ctx, dir, e, expected)
}
func (f *maintenanceGCFiler) DeleteEntry(ctx context.Context, dir, name string, expected map[string][]byte) error {
	err := f.Filer.DeleteEntry(ctx, dir, name, expected)
	if err == nil && path.Base(dir) == "snapnames" {
		f.freed++
	}
	return err
}

type maintenanceBlobs struct{}

func (maintenanceBlobs) DeleteBlob(context.Context, string) error    { return nil }
func (maintenanceBlobs) Put(context.Context, []byte) (string, error) { return "unused", nil }
func (maintenanceBlobs) Get(context.Context, string) ([]byte, error) {
	return nil, errors.New("unexpected blob read")
}
func TestMaintenanceRetentionFailureNeverStartsGC(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "missing", true: "partial"}[partial], func(t *testing.T) {
			ctx := context.Background()
			f := &maintenanceGCFiler{Filer: chunkstore.NewMemFiler(), failSecond: partial}
			cat, err := chunkstore.NewCatalog(f, defaultChunkPath)
			if err != nil {
				t.Fatal(err)
			}
			vol := "missing-volume"
			if partial {
				vol = "v1"
				if _, err = cat.EnsureVolume(ctx, vol, chunkstore.Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}); err != nil {
					t.Fatal(err)
				}
				if err = cat.SetRetentionPolicy(ctx, vol, chunkstore.RetentionPolicy{}); err != nil {
					t.Fatal(err)
				}
				now := time.Now().Add(-48 * time.Hour)
				cat.SetClock(func() time.Time { return now })
				for _, name := range []string{"a", "b"} {
					if _, err = cat.CreateSnapshot(ctx, vol, chunkstore.SnapshotRequest{Name: name, RequestID: "req"}, storage.ChunkHead{Gen: 1, Epoch: 1, Holder: "h1", RemoteLSN: 1, Chunks: map[uint64]storage.ChunkRef{}}); err != nil {
						t.Fatal(err)
					}
					now = now.Add(time.Second)
				}
				now = time.Now()
			}
			pass, closeFn, err := newChunkMaintenance(cat, maintenanceBlobs{}, true, "unused-master:1234", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer closeFn()
			if err = pass(ctx, vol); err == nil {
				t.Fatal("retention failure accepted")
			}
			if f.gcCalls != 0 {
				t.Fatalf("retention failure entered GC %d times", f.gcCalls)
			}
			if partial && (f.marked != 1 || f.freed != 1) {
				t.Fatalf("partial retention effects not retained: marked=%d freed=%d err=%v", f.marked, f.freed, err)
			}
		})
	}
}

func TestMaintenanceDefaultRetainNeedsNoEpochClient(t *testing.T) {
	f := chunkstore.NewMemFiler()
	cat, err := chunkstore.NewCatalog(f, defaultChunkPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cat.EnsureVolume(context.Background(), "v", chunkstore.Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}); err != nil {
		t.Fatal(err)
	}
	pass, closeFn, err := newChunkMaintenance(cat, nil, false, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	if err = pass(context.Background(), "v"); err != nil {
		t.Fatal(err)
	}
}
