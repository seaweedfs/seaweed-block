package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// maintenanceBlobs is an in-memory needle store.
type maintenanceBlobs struct {
	mu sync.Mutex
	n  int
	d  map[string][]byte
}

func (b *maintenanceBlobs) Put(_ context.Context, data []byte) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.n++
	fid := fmt.Sprintf("1,%x", b.n)
	b.d[fid] = append([]byte(nil), data...)
	return fid, nil
}

func (b *maintenanceBlobs) Get(_ context.Context, fid string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	d, ok := b.d[fid]
	if !ok {
		return nil, chunkstore.ErrBlobNotFound
	}
	return append([]byte(nil), d...), nil
}

// failingLookups fails every entry lookup once armed (a filer gone away).
type failingLookups struct {
	*chunkstore.MemFiler
	armed bool
}

func (f *failingLookups) LookupEntry(ctx context.Context, dir, name string) (*filerpb.Entry, error) {
	if f.armed {
		return nil, errors.New("filer: unavailable")
	}
	return f.MemFiler.LookupEntry(ctx, dir, name)
}

type okLease struct{}

func (okLease) OldLeaseEnded(context.Context, uint64) error { return nil }

var maintenanceHolder = layout.HolderID{Epoch: 1, Holder: "s/r", ClaimID: "k"}

// maintenanceFixture is volume v1 started by maintenanceHolder, with one
// orphan chunk in shard 0 that no root names.
func maintenanceFixture(t *testing.T) (*chunkstore.BCatalog, chunkstore.VolumeRef, *failingLookups, *chunkstore.LayoutStore, layout.ChunkName) {
	t.Helper()
	ctx := context.Background()
	f := &failingLookups{MemFiler: chunkstore.NewMemFiler()}
	cat, err := chunkstore.NewBCatalog(f, &maintenanceBlobs{d: map[string][]byte{}}, "/blocks")
	if err != nil {
		t.Fatal(err)
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	ref := chunkstore.VolumeRef{Name: "v1", ID: id}
	if _, err := cat.CreateVolume(ctx, ref, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
		t.Fatal(err)
	}
	s, err := cat.Store(ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := layout.Start(ctx, s, okLease{}, layout.HolderID{Epoch: 1, Holder: "s/r"}, layout.LocalRoot{Geometry: layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}, ClaimID: "k", Fresh: true, CanStartFrom: anyStart}); err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte{9}, 4096)
	orphan := layout.NameChunk(3, data)
	if err := layout.EnsureObject(ctx, s, orphan.Path(), data, "dead-session"); err != nil {
		t.Fatal(err)
	}
	// A real filer lists the shard directory it created; the in-memory one
	// keeps only entries.
	if err := f.CreateEntry(ctx, path.Join("/blocks", ref.Dir(), layout.PoolDir), &filerpb.Entry{Name: "0", IsDirectory: true}, false); err != nil {
		t.Fatal(err)
	}
	return cat, ref, f, s, orphan
}

func orphanPresent(t *testing.T, s *chunkstore.LayoutStore, n layout.ChunkName) bool {
	t.Helper()
	_, found, err := s.Stat(context.Background(), n.Path())
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func holderCalls(me layout.HolderID, err error) (func(context.Context) (layout.HolderID, error), *int) {
	n := 0
	return func(context.Context) (layout.HolderID, error) { n++; return me, err }, &n
}

// Default-off: the pass retains and never asks for a holder or removes.
func TestMaintenanceDefaultRetainsAndDoesNotClean(t *testing.T) {
	cat, ref, _, s, orphan := maintenanceFixture(t)
	holder, calls := holderCalls(maintenanceHolder, nil)
	pass := newChunkMaintenance(cat, ref, holder, false, time.Now)
	if err := pass(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	if *calls != 0 || !orphanPresent(t, s, orphan) {
		t.Fatalf("default pass: holder asked %d times, orphan present %v; want 0, true", *calls, orphanPresent(t, s, orphan))
	}
}

// With collect on, the holder's pass removes the orphan; a process with
// another claim, or with no holder identity yet, removes nothing.
func TestMaintenanceCleansOnlyAsTheHolder(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		me      layout.HolderID
		err     error
		removed bool
	}{
		"holder":      {me: maintenanceHolder, removed: true},
		"other claim": {me: layout.HolderID{Epoch: 1, Holder: "s/r", ClaimID: "old"}},
		"next epoch":  {me: layout.HolderID{Epoch: 2, Holder: "s/r", ClaimID: "k"}},
		"no identity": {err: errNoLayoutHolder},
	} {
		t.Run(name, func(t *testing.T) {
			cat, ref, _, s, orphan := maintenanceFixture(t)
			holder, _ := holderCalls(tc.me, tc.err)
			err := newChunkMaintenance(cat, ref, holder, true, time.Now)(ctx, "v1")
			if gone := !orphanPresent(t, s, orphan); gone != tc.removed || (err == nil) != tc.removed {
				t.Fatalf("pass: err %v, orphan removed %v; want removed %v", err, gone, tc.removed)
			}
			if tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatalf("pass: %v; want %v", err, tc.err)
			}
			// HEAD is another claim's or epoch's: the refusal is the fence a
			// cut would see, so the provider latches the layer on it.
			if (name == "other claim" || name == "next epoch") && !errors.Is(err, storage.ErrChunkLayerFenced) {
				t.Fatalf("pass refused as %v; want it classified as the fence", err)
			}
		})
	}
}

// A failed retention ends the pass before the cleaning starts.
func TestMaintenanceRetentionFailureNeverStartsCleaning(t *testing.T) {
	cat, ref, f, s, orphan := maintenanceFixture(t)
	holder, calls := holderCalls(maintenanceHolder, nil)
	f.armed = true
	err := newChunkMaintenance(cat, ref, holder, true, time.Now)(context.Background(), "v1")
	f.armed = false
	if err == nil || *calls != 0 || !orphanPresent(t, s, orphan) {
		t.Fatalf("pass with retention failing: err %v, holder asked %d, orphan present %v", err, *calls, orphanPresent(t, s, orphan))
	}
}

// The pass serves only the volume the process was started for.
func TestMaintenanceRefusesAnotherVolume(t *testing.T) {
	cat, ref, _, _, _ := maintenanceFixture(t)
	holder, _ := holderCalls(maintenanceHolder, nil)
	if err := newChunkMaintenance(cat, ref, holder, true, time.Now)(context.Background(), "v2"); !errors.Is(err, layout.ErrIdentity) {
		t.Fatalf("pass for another volume: %v; want ErrIdentity", err)
	}
}
