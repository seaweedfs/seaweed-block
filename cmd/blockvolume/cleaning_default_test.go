package main

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
	"github.com/seaweedfs/seaweed-block/core/host/volume"
)

type cleaningView struct{}

func (cleaningView) Projection() frontend.Projection {
	return frontend.Projection{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1, Healthy: true}
}

// With the shipped defaults (no cleaning flag, the hour-long maintenance
// interval), rolling writes with one snapshot kept: after each snapshot
// delete through the holder's control route, the holder's cleaning removes
// the deleted snapshot's chunks, so the pool returns to the chunks the
// current root names and stops growing.
func TestDefaultConfigCleansAfterEachSnapshotDelete(t *testing.T) {
	ctx := context.Background()
	d, err := parseFlags(append(phase150RequiredBlockvolumeArgs(), "--durable-root=/tmp/sw-block", "--durable-impl=walstore-chunk", "--chunk-filer=x:1"))
	if err != nil {
		t.Fatal(err)
	}
	const blocks, chunkBlocks = 256, 16
	const chunks = blocks / chunkBlocks
	fixture := testutil.NewLayoutFixture(t, "v1", blocks, chunkBlocks)
	pool := path.Join("/provider-test", fixture.Ref.Dir(), layout.PoolDir)
	// A real filer lists the shard directory it created for the chunks; the
	// in-memory filer keeps only the entries, so add it.
	if err := fixture.Filer.CreateEntry(ctx, pool, &filerpb.Entry{Name: "0", IsDirectory: true}, false); err != nil {
		t.Fatal(err)
	}
	cfg := durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: blocks, ChunkBlocks: chunkBlocks,
		ChunkMaintenanceInterval: d.chunk.maintenanceInterval,
	}
	fixture.Configure(&cfg)
	var dp *durable.DurableProvider
	cfg.ChunkMaintenance = newChunkMaintenance(fixture.Catalog, fixture.Ref, func(ctx context.Context) (layoutCleaner, error) {
		r, err := dp.LayoutRemote(ctx, "v1")
		if err != nil {
			return nil, err
		}
		return r.(layoutCleaner), nil
	}, d.chunk.automaticGC, time.Now)
	dp, err = durable.NewDurableProvider(cfg, cleaningView{})
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
	ctl := volume.NewChunkControl(dp, fixture.Catalog, fixture.Ref)
	poolObjects := func() int {
		t.Helper()
		es, err := fixture.Filer.ListEntries(ctx, pool+"/0", "", "", 100000)
		if err != nil {
			t.Fatal(err)
		}
		return len(es)
	}
	prev := ""
	for round := 0; round < 4; round++ {
		// Every chunk changes: the new snapshot names none of the old chunks.
		for c := uint64(0); c < chunks; c++ {
			if _, err := s.Write(uint32(c*chunkBlocks), bytes.Repeat([]byte{byte(round*chunks + int(c) + 1)}, 4096)); err != nil {
				t.Fatal(err)
			}
		}
		name := fmt.Sprintf("s%d", round)
		if _, err := ctl.Snapshot(ctx, "v1", volume.ChunkSnapshotRequest{Name: name}); err != nil {
			t.Fatalf("round %d: snapshot: %v", round, err)
		}
		if prev != "" {
			// The kept snapshot and the current root name the new chunks;
			// the one about to be deleted alone names the old ones.
			if before := poolObjects(); before != 2*chunks {
				t.Fatalf("round %d: %d pool objects before deleting %s; want %d (old and new chunks)", round, before, prev, 2*chunks)
			}
			if _, err := ctl.DeleteSnapshot(ctx, "v1", prev); err != nil {
				t.Fatalf("round %d: delete %s: %v", round, prev, err)
			}
			deadline := time.Now().Add(10 * time.Second)
			for poolObjects() != chunks && time.Now().Before(deadline) {
				time.Sleep(20 * time.Millisecond)
			}
			if got := poolObjects(); got != chunks {
				t.Fatalf("round %d: %d pool objects 10 s after deleting %s; want the %d the current root names", round, got, prev, chunks)
			}
		}
		prev = name
	}
}

// A cut through the holder's control route while the holder's cleaning runs
// on the same remote: the cut cancels the cleaning (its run sees its context
// end) and completes; HEAD carries the cut and no open cleaning.
func TestCutDuringTheHoldersCleaningCancelsIt(t *testing.T) {
	ctx := context.Background()
	fixture := testutil.NewLayoutFixture(t, "v1", 256, 16)
	cfg := durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: 256, ChunkBlocks: 16}
	fixture.Configure(&cfg)
	dp, err := durable.NewDurableProvider(cfg, cleaningView{})
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
	if _, err := s.Write(1, bytes.Repeat([]byte{7}, 4096)); err != nil {
		t.Fatal(err)
	}
	r, err := dp.LayoutRemote(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	cleaner := r.(layoutCleaner)
	started, cancelled := make(chan struct{}), make(chan struct{})
	cleaned := make(chan error, 1)
	go func() {
		cleaned <- cleaner.Clean(ctx, func(ctx context.Context, _ layout.HolderID, _ string) error {
			close(started)
			<-ctx.Done() // a cleaning pass in progress until the cut ends it
			close(cancelled)
			return ctx.Err()
		})
	}()
	<-started
	ctl := volume.NewChunkControl(dp, fixture.Catalog, fixture.Ref)
	cutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	res, err := ctl.Cut(cutCtx, "v1", 0)
	if err != nil {
		t.Fatalf("the cut during the cleaning: %v", err)
	}
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("the cut did not cancel the cleaning")
	}
	if err := <-cleaned; err != nil {
		t.Fatalf("the cancelled cleaning ended with %v; want a clean end", err)
	}
	h, _, err := fixture.Catalog.ReadHead(ctx, fixture.Ref)
	if err != nil || h.Activity.Kind != layout.ActIdle || h.Position < res.CutLSN {
		t.Fatalf("HEAD after the cut: %+v (cut to %d) %v; want idle at the cut", h.Activity, res.CutLSN, err)
	}
}
