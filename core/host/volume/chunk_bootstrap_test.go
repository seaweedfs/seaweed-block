package volume

// walstore-chunk in the production startup order of cmd/blockvolume:
// the host blocks local readiness, the master's assignment arrives and is
// retained, the durable provider opens and recovers the volume, readiness
// clears and SetReplicationVolume replays the assignment. Only then is
// the master-assigned epoch known; the chunk layer must publish under it.

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/replication"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestWALStoreChunk_ProductionStartupPublishesUnderAssignedEpoch(t *testing.T) {
	h, err := New(Config{
		MasterAddr: "127.0.0.1:1", // never dialed: no Start
		ServerID:   "s1", VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "127.0.0.1:2", CtrlAddr: "127.0.0.1:3",
		EnableT1Readiness: true,
		Logger:            newDiscardLogger(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })

	// main.go: block readiness before Start; the assignment arrives and
	// is retained, not applied.
	h.BlockLocalReadiness("awaiting durable recovery")
	h.applyFixtureFact(&control.AssignmentFact{VolumeId: "v1", ReplicaId: "r1", Epoch: 5, EndpointVersion: 2, PeerSetGeneration: 1})
	if p := h.ProjectionView().Projection(); p.Healthy || p.Epoch != 0 {
		t.Fatalf("assignment applied while readiness was blocked: %+v", p)
	}

	remote := storage.NewMemChunkRemote()
	dp, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, ChunkHolder: "r1",
		StorageRoot:          t.TempDir(),
		BlockSize:            4096,
		NumBlocks:            256,
		ChunkBlocks:          16,
		ChunkCaptureInterval: 20 * time.Millisecond,
		ChunkRemoteFor:       func(string) (storage.ChunkRemote, error) { return remote, nil },
	}, h.ProjectionView())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dp.Close() })
	if _, err := dp.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	recCtx, recCancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, recErr := dp.RecoverVolume(recCtx, "v1")
	recCancel()
	if recErr != nil {
		t.Fatalf("RecoverVolume during blocked readiness: %v", recErr)
	}

	// main.go: clear readiness, install replication; the retained
	// assignment replays.
	h.ClearLocalReadinessBlock()
	h.SetReplicationVolume(replication.NewReplicationVolume("v1", dp.LogicalStorage("v1")))

	openCtx, openCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer openCancel()
	be, err := dp.Open(openCtx, "v1")
	if err != nil {
		t.Fatalf("frontend open after replay: %v", err)
	}
	if p := h.ProjectionView().Projection(); !p.Healthy || p.Epoch != 5 || p.EndpointVersion != 2 {
		t.Fatalf("projection after replay: %+v", p)
	}
	data := bytes.Repeat([]byte{0x5A}, 4096)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err = be.Write(context.Background(), 40*4096, data); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("frontend write never admitted: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := be.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// On-demand capture under the assigned lineage.
	lsn, err := dp.CaptureVolume(context.Background(), "v1")
	if err != nil {
		t.Fatalf("on-demand capture after startup: %v", err)
	}
	head, _ := remote.GetHead(context.Background())
	if head.Epoch != 5 || head.Holder != "r1" || head.RemoteLSN != lsn {
		t.Fatalf("HEAD after on-demand capture: epoch %d holder %q remote_lsn %d (want 5, r1, %d)", head.Epoch, head.Holder, head.RemoteLSN, lsn)
	}

	// Periodic capture under the same lineage.
	if _, err := be.Write(context.Background(), 200*4096, bytes.Repeat([]byte{0xC8}, 4096)); err != nil {
		t.Fatal(err)
	}
	if err := be.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for {
		head, _ = remote.GetHead(context.Background())
		if _, ok := head.Chunks[200/16]; ok && head.RemoteLSN > lsn {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("periodic capture never published the second write (HEAD gen %d remote_lsn %d)", head.Gen, head.RemoteLSN)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if head.Epoch != 5 {
		t.Fatalf("periodic HEAD epoch %d, want 5", head.Epoch)
	}
}
