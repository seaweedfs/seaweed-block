package durable_test

// The automatic cut scheduler through the real provider loop.

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func schedProvider(t *testing.T, remote storage.ChunkRemote, interval time.Duration, threshold int) *durable.DurableProvider {
	t.Helper()
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096,
		NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		ChunkCaptureInterval: interval,
		ChunkDirtyThreshold:  threshold,
		ChunkRemoteFor:       func(string) (storage.ChunkRemote, error) { return remote, nil },
	}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func headGen(t *testing.T, remote storage.ChunkRemote) storage.ChunkHead {
	t.Helper()
	h, err := remote.GetHead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// With the timer off, enough dirty chunks trigger a cut on their own.
func TestWALStoreChunk_DirtyThresholdTriggersCut(t *testing.T) {
	remote := storage.NewMemChunkRemote()
	p := schedProvider(t, remote, 0, 2)
	s := openRecovered(t, p)
	if _, err := s.Write(1, block(0x01)); err != nil { // chunk 0
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if h := headGen(t, remote); h.Gen != 0 {
		t.Fatalf("cut below the threshold (HEAD gen %d)", h.Gen)
	}
	if _, err := s.Write(pcChunkBlocks+1, block(0x02)); err != nil { // chunk 1
		t.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		h := headGen(t, remote)
		if _, ok := h.Chunks[1]; ok && h.Gen >= 1 {
			if _, ok := h.Chunks[0]; !ok {
				t.Fatalf("threshold cut HEAD lacks chunk 0: %+v", h.Chunks)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no threshold cut after two dirty chunks (HEAD gen %d)", h.Gen)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// An explicit cut restarts the timer of the running loop: the automatic
// cut comes one interval after it, not one interval after the loop began.
func TestWALStoreChunk_ExplicitCutRestartsTheTimer(t *testing.T) {
	const interval = 3 * time.Second
	remote := storage.NewMemChunkRemote()
	p := schedProvider(t, remote, interval, 0)
	s := openRecovered(t, p)
	begin := time.Now()

	time.Sleep(2 * time.Second)
	if _, err := s.Write(1, block(0x01)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CaptureVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	explicit := time.Now()
	if h := headGen(t, remote); h.Gen != 1 {
		t.Fatalf("explicit cut: HEAD gen %d, want 1", h.Gen)
	}
	if _, err := s.Write(pcChunkBlocks+1, block(0x02)); err != nil {
		t.Fatal(err)
	}
	// Past the loop's own first deadline (begin + interval) but before
	// explicit + interval: no automatic cut yet.
	time.Sleep(time.Until(begin.Add(interval + 1200*time.Millisecond)))
	if time.Now().After(explicit.Add(interval - 300*time.Millisecond)) {
		t.Skip("scheduling too slow to separate the two deadlines on this host")
	}
	if h := headGen(t, remote); h.Gen != 1 {
		t.Fatalf("automatic cut %s after the explicit one (HEAD gen %d): the timer did not restart", time.Since(explicit), h.Gen)
	}
	deadline := explicit.Add(interval + 5*time.Second)
	for {
		if h := headGen(t, remote); h.Gen >= 2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no automatic cut one interval after the explicit cut")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// The volume status carries the chunk layer's state and cut counters.
func TestWALStoreChunk_StatusCarriesChunkLayer(t *testing.T) {
	remote := storage.NewMemChunkRemote()
	view := newStubView(frontend.Projection{VolumeID: "v1", ReplicaID: "r1"}) // pre-assignment
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096,
		NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
	}, view)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	s, err := p.EnsureStorage("v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	st := p.DurableStatuses()
	if len(st) != 1 || st[0].Chunk == nil || !st[0].Chunk.AwaitingAuthority || st[0].Chunk.Attached {
		t.Fatalf("status before authority: %+v", st)
	}
	view.set(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1}))
	if _, err := s.Write(1, block(1)); err != nil {
		t.Fatal(err)
	}
	var lsn uint64
	deadline := time.Now().Add(10 * time.Second)
	for {
		if lsn, err = p.CaptureVolume(context.Background(), "v1"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("capture never succeeded: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	c := p.DurableStatuses()[0].Chunk
	if c == nil || !c.Attached || c.AwaitingAuthority || c.Fenced || c.RemoteLSN != lsn || c.Stats.CutsOK != 1 || c.Stats.LastCutH != lsn {
		t.Fatalf("status after a cut: %+v", c)
	}
}

func TestWALStoreChunk_StatusHasNoChunkSectionForWALStore(t *testing.T) {
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStore, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: pcBlocks,
	}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	_ = openRecovered(t, p)
	if st := p.DurableStatuses(); len(st) != 1 || st[0].Chunk != nil {
		t.Fatalf("walstore status: %+v", st)
	}
}
