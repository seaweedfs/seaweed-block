package durable_test

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/hostlimit"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// slotWaitProvider opens v1 with a 100 ms cut timer under a host limiter
// whose only normal slot another process holds; release frees it.
func slotWaitProvider(t *testing.T) (p *durable.DurableProvider, lim *hostlimit.Limiter, remote *storage.MemChunkRemote, release func()) {
	t.Helper()
	c := hostlimit.DefaultConfig(t.TempDir())
	c.NormalSlots, c.UrgentSlots = 1, 1
	holder, err := hostlimit.New(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(holder.Close)
	ticket, err := holder.Admit(context.Background(), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	held := true
	release = func() {
		if held {
			held = false
			holder.Release(ticket)
		}
	}
	t.Cleanup(release)
	if lim, err = hostlimit.New(c); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(lim.Close)
	remote = storage.NewMemChunkRemote()
	p, err = durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: pcBlocks, BlockSize: 4096,
		ChunkBlocks: pcChunkBlocks, ChunkBudget: lim, ChunkCaptureInterval: 100 * time.Millisecond,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil }}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	s := openRecovered(t, p)
	if _, err = s.Write(0, block(0x5c)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Sync(); err != nil {
		t.Fatal(err)
	}
	return p, lim, remote, release
}

// watchWaits samples status every 10 ms for d and returns the times at
// which HostSlotWaits went up (one per refused timer cut).
func watchWaits(t *testing.T, p *durable.DurableProvider, d time.Duration) []time.Time {
	t.Helper()
	var at []time.Time
	var last uint64
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		cs := chunkStatus(t, p).CutSchedule
		if cs != nil && cs.HostSlotWaits > last {
			for ; last < cs.HostSlotWaits; last++ {
				at = append(at, time.Now())
			}
			if cs.HostSlotWaitSinceNs == 0 {
				t.Fatalf("waits counted but no wait start reported: %+v", cs)
			}
		}
	}
	return at
}

// A timer cut refused because every host slot is held is retried within
// one poll plus the volume's jitter, every time, however long the slot is
// held; once it frees, the next attempt is admitted within that bound.
func TestTimerCutRunsSoonAfterHostSlotFrees(t *testing.T) {
	p, lim, remote, release := slotWaitProvider(t)
	refusals := watchWaits(t, p, 2500*time.Millisecond)
	// A failure backoff (1 s, 2 s, ...) allows at most 3 attempts in 2.5 s.
	if len(refusals) < 8 {
		t.Fatalf("%d refused attempts in 2.5 s; a slot wait retries every <= 300 ms", len(refusals))
	}
	for i := 1; i < len(refusals); i++ {
		if gap := refusals[i].Sub(refusals[i-1]); gap > 400*time.Millisecond {
			t.Fatalf("retry %d came %s after the previous refusal", i, gap)
		}
	}
	if h, _ := remote.GetHead(context.Background()); h.Gen != 0 {
		t.Fatal("a cut published while the only slot was held")
	}

	admitted := lim.Snapshot().(hostlimit.Stats).Admitted
	release()
	freed := time.Now()
	for lim.Snapshot().(hostlimit.Stats).Admitted == admitted {
		if time.Since(freed) > 400*time.Millisecond {
			t.Fatal("no cut admitted within 400 ms of the slot freeing")
		}
		time.Sleep(5 * time.Millisecond)
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if h, _ := remote.GetHead(context.Background()); h.Gen != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the admitted cut did not publish within 5 s")
		}
	}
	for deadline := time.Now().Add(time.Second); ; time.Sleep(10 * time.Millisecond) {
		if cs := chunkStatus(t, p).CutSchedule; cs != nil && cs.HostSlotWaitSinceNs == 0 && cs.HostSlotWaits >= uint64(len(refusals)) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the wait is still reported after a successful cut: %+v", chunkStatus(t, p).CutSchedule)
		}
	}
}

// A plain detach stops the timer before its seal; the seal (urgent, so it
// takes the urgent slot) succeeds, and status no longer reports a wait.
func TestSlotWaitEndsWithASuccessfulDetachSeal(t *testing.T) {
	p, _, remote, _ := slotWaitProvider(t)
	if refusals := watchWaits(t, p, 600*time.Millisecond); len(refusals) == 0 {
		t.Fatal("no refused timer cut while the slot was held")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, _, err := p.DetachVolumePlain(ctx, "v1"); err != nil {
		t.Fatalf("plain detach: %v", err)
	}
	if h, _ := remote.GetHead(context.Background()); h.Gen == 0 {
		t.Fatal("the seal did not publish")
	}
	cs := chunkStatus(t, p).CutSchedule
	if cs == nil || cs.HostSlotWaitSinceNs != 0 || cs.HostSlotWaits == 0 {
		t.Fatalf("after a successful seal: %+v; want the count kept and no current wait", cs)
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
