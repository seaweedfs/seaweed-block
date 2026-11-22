package durable_test

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/hostlimit"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A timer cut refused because every host slot is held is a wait, not a
// failure: once the slot frees, the cut runs within one poll plus the
// volume's jitter, however long the slot was held.
func TestTimerCutRunsSoonAfterHostSlotFrees(t *testing.T) {
	c := hostlimit.DefaultConfig(t.TempDir())
	c.NormalSlots, c.UrgentSlots = 1, 1
	holder, err := hostlimit.New(c)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	ticket, err := holder.Admit(context.Background(), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	held := true
	defer func() {
		if held {
			holder.Release(ticket)
		}
	}()
	lim, err := hostlimit.New(c)
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	remote := storage.NewMemChunkRemote()
	p, err := durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: pcBlocks, BlockSize: 4096,
		ChunkBlocks: pcChunkBlocks, ChunkBudget: lim, ChunkCaptureInterval: 100 * time.Millisecond,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil }}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	s := openRecovered(t, p)
	if _, err = s.Write(0, block(0x5c)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Sync(); err != nil {
		t.Fatal(err)
	}

	// Long enough for a failure backoff to have grown past 1 s.
	time.Sleep(2500 * time.Millisecond)
	cs := chunkStatus(t, p)
	if h, _ := remote.GetHead(context.Background()); h.Gen != 0 {
		t.Fatal("a cut published while the only slot was held")
	}
	if cs.CutSchedule == nil || cs.CutSchedule.HostSlotWaits == 0 || cs.CutSchedule.HostSlotWaitSinceNs == 0 {
		t.Fatalf("waiting for a host slot is not reported: %+v", cs.CutSchedule)
	}

	holder.Release(ticket)
	held = false
	freed := time.Now()
	for {
		if h, _ := remote.GetHead(context.Background()); h.Gen != 0 {
			break
		}
		if time.Since(freed) > 300*time.Millisecond {
			t.Fatalf("no cut within 300 ms of the slot freeing")
		}
		time.Sleep(5 * time.Millisecond)
	}
	deadline := time.Now().Add(time.Second)
	for {
		cs = chunkStatus(t, p)
		if cs.CutSchedule != nil && cs.CutSchedule.HostSlotWaitSinceNs == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the wait is still reported after a successful cut: %+v", cs.CutSchedule)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func chunkStatus(t *testing.T, p *durable.DurableProvider) durable.ChunkStatus {
	t.Helper()
	for _, st := range p.DurableStatuses() {
		if st.VolumeID == "v1" && st.Chunk != nil {
			return *st.Chunk
		}
	}
	t.Fatal("no chunk status for v1")
	return durable.ChunkStatus{}
}
