package durable_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/hostlimit"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// admitRecorder is the provider's cut budget: the real limiter, with
// every Admit call's time and outcome recorded.
type admitRecorder struct {
	*hostlimit.Limiter
	mu     sync.Mutex
	events []admitEvent
}

type admitEvent struct {
	at   time.Time
	held bool // refused because every slot was held
	ok   bool
}

func (r *admitRecorder) Admit(ctx context.Context, payload int64, abort func(error)) (uint64, error) {
	id, err := r.Limiter.Admit(ctx, payload, abort)
	r.mu.Lock()
	r.events = append(r.events, admitEvent{at: time.Now(), held: errors.Is(err, hostlimit.ErrSlotsHeld), ok: err == nil})
	r.mu.Unlock()
	return id, err
}

func (r *admitRecorder) snapshot() []admitEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]admitEvent(nil), r.events...)
}

// slotWaitProvider opens v1 with a 100 ms cut timer under a host limiter
// whose only normal slot another process holds; release frees it. baseGen
// is HEAD's generation once the volume is open (opening may publish one
// itself, without a cut).
func slotWaitProvider(t *testing.T) (p *durable.DurableProvider, lim *admitRecorder, remote *storage.MemChunkRemote, release func(), baseGen uint64) {
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
	inner, err := hostlimit.New(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(inner.Close)
	lim = &admitRecorder{Limiter: inner}
	remote = storage.NewMemChunkRemote()
	p, err = durable.NewDurableProvider(durable.ProviderConfig{Impl: durable.ImplWALStoreChunk, ChunkHolder: "r1", StorageRoot: t.TempDir(), NumBlocks: pcBlocks, BlockSize: 4096,
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
	h, err := remote.GetHead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return p, lim, remote, release, h.Gen
}

// admitted reports whether any recorded Admit call succeeded.
func admitted(events []admitEvent) bool {
	for _, e := range events {
		if e.ok {
			return true
		}
	}
	return false
}

// refusedAt returns the times of the recorded held-slot refusals.
func refusedAt(events []admitEvent) []time.Time {
	var at []time.Time
	for _, e := range events {
		if e.held {
			at = append(at, e.at)
		}
	}
	return at
}

// A timer cut refused because every host slot is held is retried within
// one poll plus the largest retry delay, every time, however long the
// slot is held; once it frees, the next attempt is admitted within that
// bound. Times are the limiter's own Admit calls.
func TestTimerCutRunsSoonAfterHostSlotFrees(t *testing.T) {
	p, lim, remote, release, baseGen := slotWaitProvider(t)
	time.Sleep(2500 * time.Millisecond)
	refusals := refusedAt(lim.snapshot())
	// A failure backoff (1 s, 2 s, ...) allows at most 3 attempts in 2.5 s.
	if len(refusals) < 8 {
		t.Fatalf("%d refused admissions in 2.5 s; a slot wait retries every <= 300 ms", len(refusals))
	}
	for i := 1; i < len(refusals); i++ {
		if gap := refusals[i].Sub(refusals[i-1]); gap > 400*time.Millisecond {
			t.Fatalf("admission %d came %s after the previous refusal", i, gap)
		}
	}
	if admitted(lim.snapshot()) {
		t.Fatal("a cut was admitted while the only slot was held")
	}
	if h, _ := remote.GetHead(context.Background()); h.Gen != baseGen {
		t.Fatalf("HEAD moved from generation %d to %d while the only slot was held", baseGen, h.Gen)
	}
	if cs := chunkStatus(t, p).CutSchedule; cs == nil || cs.HostSlotWaits == 0 || cs.HostSlotWaitSinceNs == 0 {
		t.Fatalf("waiting for a host slot is not reported: %+v", cs)
	}

	before := len(lim.snapshot())
	freed := time.Now()
	release()
	var admittedAt time.Time
	for deadline := time.Now().Add(5 * time.Second); admittedAt.IsZero(); time.Sleep(10 * time.Millisecond) {
		for _, e := range lim.snapshot()[before:] {
			if e.ok {
				admittedAt = e.at
				break
			}
		}
		if admittedAt.IsZero() && time.Now().After(deadline) {
			t.Fatal("no cut admitted within 5 s of the slot freeing")
		}
	}
	if d := admittedAt.Sub(freed); d > 400*time.Millisecond {
		t.Fatalf("the first admission came %s after the slot freed; want <= 400 ms", d)
	}
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if h, _ := remote.GetHead(context.Background()); h.Gen > baseGen {
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
	p, lim, remote, _, baseGen := slotWaitProvider(t)
	time.Sleep(600 * time.Millisecond)
	if len(refusedAt(lim.snapshot())) == 0 {
		t.Fatal("no refused timer cut while the slot was held")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, _, err := p.DetachVolumePlain(ctx, "v1"); err != nil {
		t.Fatalf("plain detach: %v", err)
	}
	if h, _ := remote.GetHead(context.Background()); h.Gen <= baseGen {
		t.Fatalf("the seal did not publish: HEAD generation %d, %d at open", h.Gen, baseGen)
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
