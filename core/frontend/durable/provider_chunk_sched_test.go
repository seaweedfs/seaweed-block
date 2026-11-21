package durable_test

// The automatic cut scheduler through the real provider loop.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func schedProvider(t *testing.T, remote storage.ChunkRemote, interval time.Duration) *durable.DurableProvider {
	t.Helper()
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096,
		NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		ChunkCaptureInterval: interval,
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

// The remote holds snapshots: with the timer off, no number of dirty
// chunks publishes a cut on its own.
func TestWALStoreChunk_DirtyChunksNeverTriggerCut(t *testing.T) {
	remote := storage.NewMemChunkRemote()
	p := schedProvider(t, remote, 0)
	s := openRecovered(t, p)
	claimed := freshV1Claim(t, remote)
	for c := uint32(0); c < 4; c++ { // four dirty chunks
		if _, err := s.Write(c*pcChunkBlocks+1, block(byte(c+1))); err != nil {
			t.Fatal(err)
		}
	}
	// A negative invariant: checked repeatedly over several scheduler polls.
	for i := 0; i < 15; i++ {
		if h := headGen(t, remote); h.Gen != claimed.Gen {
			t.Fatalf("a cut was published with the timer off (HEAD gen %d, %d chunks)", h.Gen, len(h.Chunks))
		}
		time.Sleep(100 * time.Millisecond)
	}
	// An explicit cut still publishes every dirty chunk.
	if _, err := p.CaptureVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	if h := headGen(t, remote); h.Gen != claimed.Gen+1 || len(h.Chunks) != 4 {
		t.Fatalf("explicit cut HEAD gen %d with %d chunks, want 4", h.Gen, len(h.Chunks))
	}
}

// An explicit cut restarts the timer of the running loop: the automatic
// cut comes one interval after it, not one interval after the loop began.
func TestWALStoreChunk_ExplicitCutRestartsTheTimer(t *testing.T) {
	const interval = 3 * time.Second
	remote := storage.NewMemChunkRemote()
	p := schedProvider(t, remote, interval)
	s := openRecovered(t, p)
	claimed := freshV1Claim(t, remote)
	begin := time.Now()

	time.Sleep(2 * time.Second)
	if _, err := s.Write(1, block(0x01)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CaptureVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	explicit := time.Now()
	if h := headGen(t, remote); h.Gen != claimed.Gen+1 {
		t.Fatalf("explicit cut: HEAD gen %d, want open claim + 1", h.Gen)
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
	if h := headGen(t, remote); h.Gen != claimed.Gen+1 {
		t.Fatalf("automatic cut %s after the explicit one (HEAD gen %d): the timer did not restart", time.Since(explicit), h.Gen)
	}
	// Progress, no timer (#227): the automatic cut follows; a loaded host only
	// delays it (the early-cut check above is the timing assertion, and it
	// skips when the host cannot separate the two deadlines).
	for headGen(t, remote).Gen < claimed.Gen+2 {
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

// ChunkOnPublishSettled sees every confirmed publication of a volume, with
// its volume id and the HEAD before and after it; a no-op capture is not one.
func TestWALStoreChunk_PublishSettledHook(t *testing.T) {
	remote := storage.NewMemChunkRemote()
	var mu sync.Mutex
	var got []string
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096,
		NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
		ChunkOnPublishSettled: func(_ context.Context, volumeID string, prev, next storage.ChunkHead) {
			mu.Lock()
			got = append(got, fmt.Sprintf("%s:%d->%d", volumeID, prev.Gen, next.Gen))
			mu.Unlock()
		},
	}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	s := openRecovered(t, p)
	claimed := freshV1Claim(t, remote)
	for i, lba := range []uint32{1, 0, pcChunkBlocks + 1} {
		if lba != 0 {
			if _, err := s.Write(lba, block(byte(i+1))); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := p.CaptureVolume(context.Background(), "v1"); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	wantFirst := fmt.Sprintf("v1:%d->%d", claimed.Gen, claimed.Gen+1)
	wantSecond := fmt.Sprintf("v1:%d->%d", claimed.Gen+1, claimed.Gen+2)
	if len(got) != 2 || got[0] != wantFirst || got[1] != wantSecond {
		t.Fatalf("reports %v; want [%s %s] (the middle capture changed nothing)", got, wantFirst, wantSecond)
	}
}

// gatedCASRemote holds the next HEAD CAS, once armed, until it is released
// (or its context ends), and reports when that CAS started.
type gatedCASRemote struct {
	*storage.MemChunkRemote
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (r *gatedCASRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	if r.armed.CompareAndSwap(true, false) {
		close(r.entered)
		select {
		case <-r.release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return r.MemChunkRemote.CASHead(ctx, expected, next)
}

type loopEvent struct {
	event     string
	lastCutNs int64
	skipped   bool
	at        time.Time
}

// An explicit cut still running when the loop's timer comes due restarts the
// timer all the same. Witnessed: the loop decides "due" from the old last
// cut while the explicit cut's CAS is held; once the explicit cut completes,
// that periodic cut is skipped (not taken as a no-op); and the next periodic
// decision is made from the explicit cut, a whole interval after it.
func TestWALStoreChunk_ExplicitCutOverlappingTheTimerRestartsIt(t *testing.T) {
	const interval = 3 * time.Second
	remote := &gatedCASRemote{MemChunkRemote: storage.NewMemChunkRemote(), entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(remote.release) }) }
	t.Cleanup(release)
	p := schedProvider(t, remote, interval)
	events := make(chan loopEvent, 64)
	durable.SetTestChunkLoopTrace(p, func(event string, lastCutNs int64, skipped bool) {
		select {
		case events <- loopEvent{event, lastCutNs, skipped, time.Now()}:
		default:
		}
	})
	next := func(what string) loopEvent {
		t.Helper()
		select {
		case e := <-events:
			return e
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out waiting for the loop's %s", what)
		}
		return loopEvent{}
	}
	s := openRecovered(t, p)
	claimed := freshV1Claim(t, remote.MemChunkRemote)
	begin := time.Now() // the loop started during open: its first deadline is at most begin + interval
	if _, err := s.Write(1, block(0x01)); err != nil {
		t.Fatal(err)
	}
	// Hold the explicit cut's CAS from a second before that deadline: the
	// CAS's own 2 s operation deadline outlasts it.
	time.Sleep(time.Until(begin.Add(interval - time.Second)))
	remote.armed.Store(true)
	explicitDone := make(chan error, 1)
	go func() { _, err := p.CaptureVolume(context.Background(), "v1"); explicitDone <- err }()
	t.Cleanup(func() {
		release()
		select {
		case <-explicitDone:
		case <-time.After(30 * time.Second):
			t.Error("the explicit cut never returned")
		}
	})
	select {
	case <-remote.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("the explicit cut never reached its CAS")
	}
	// The loop comes due while the explicit cut holds its CAS: it decides
	// from the old last cut (none yet).
	due := next("due decision")
	if due.event != "due" || due.lastCutNs != 0 {
		t.Fatalf("loop event %+v; want a due decision from no cut yet", due)
	}
	select {
	case err := <-explicitDone:
		explicitDone <- err
		t.Fatal("the explicit cut finished before the loop's decision")
	default:
	}
	release()
	select {
	case err := <-explicitDone:
		explicitDone <- err
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the explicit cut did not finish")
	}
	explicitAt := time.Now()
	if h := headGen(t, remote); h.Gen != claimed.Gen+1 {
		t.Fatalf("explicit cut: HEAD gen %d, want open claim + 1", h.Gen)
	}
	if _, err := s.Write(pcChunkBlocks+1, block(0x02)); err != nil {
		t.Fatal(err)
	}
	// The stale periodic cut is skipped, not run as a no-op.
	cut := next("stale periodic cut")
	if cut.event != "cut" || cut.lastCutNs != 0 || !cut.skipped {
		t.Fatalf("loop event %+v; want the stale periodic cut skipped", cut)
	}
	// The next periodic decision is made from the explicit cut, a whole
	// interval after it, and publishes the later write.
	due2 := next("next due decision")
	if due2.event != "due" || due2.lastCutNs == 0 {
		t.Fatalf("loop event %+v; want a due decision from the explicit cut", due2)
	}
	last := time.Unix(0, due2.lastCutNs)
	if last.After(explicitAt) {
		t.Fatalf("decided from a cut at %v, after the explicit cut returned (%v)", last, explicitAt)
	}
	if due2.at.Before(last.Add(interval)) {
		t.Fatalf("next periodic cut decided %v after the explicit cut; want a whole %v interval", due2.at.Sub(last), interval)
	}
	cut2 := next("next periodic cut")
	if cut2.event != "cut" || cut2.skipped {
		t.Fatalf("loop event %+v; want the next periodic cut taken", cut2)
	}
	if h := headGen(t, remote); h.Gen != claimed.Gen+2 {
		t.Fatalf("after the next periodic cut: HEAD gen %d, want open claim + 2", h.Gen)
	}
}
