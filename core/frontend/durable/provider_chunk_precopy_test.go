package durable_test

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestPrecopyFreezeDoesNotWaitForAdmittedIO(t *testing.T) {
	p, view := parkedLayoutProvider(t, testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks))
	openRecovered(t, p)
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	gate, ok := b.(interface{ TryStopIO(string) bool })
	if !ok {
		t.Fatal("backend has no nonblocking tentative freeze")
	}
	view.arm()
	done := make(chan error, 1)
	t.Cleanup(func() {
		view.unpark()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("writer did not join")
		}
	})
	go func() { _, err := b.Write(context.Background(), 4096, block(7)); done <- err }()
	select {
	case <-view.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("write never admitted")
	}
	frozen := make(chan bool, 1)
	go func() { frozen <- gate.TryStopIO("pre-copy final cut") }()
	select {
	case stopped := <-frozen:
		if stopped {
			t.Fatal("stopped before admitted write drained")
		}
	case <-time.After(time.Second):
		t.Fatal("freeze blocked on admitted I/O")
	}
	if _, err := b.Write(context.Background(), 8192, block(8)); err != nil {
		t.Fatal("rejected tentative freeze stopped service:", err)
	}
	view.unpark()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		done <- nil
	case <-time.After(5 * time.Second):
		t.Fatal("writer stuck")
	}
	if !gate.TryStopIO("pre-copy final cut") {
		t.Fatal("idle frontend did not freeze")
	}
	if _, err := b.Write(context.Background(), 8192, block(9)); err == nil {
		t.Fatal("write served after successful freeze")
	}
}

func precopyProvider(t *testing.T, remote storage.ChunkRemote, view *parkView, hooks ...*detachHooks) *durable.DurableProvider {
	t.Helper()
	_ = remote // Other legacy callers migrate to the layout fixture separately.
	return precopyBProvider(t, testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks), view, nil, hooks...)
}

func precopyBProvider(t *testing.T, fixture *testutil.LayoutFixture, view *parkView, delayed *precopyLayoutDelayedRemote, hooks ...*detachHooks) *durable.DurableProvider {
	t.Helper()
	cfg := durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		ChunkDetachPrecopy: true,
	}
	fixture.Configure(&cfg)
	if delayed != nil {
		cfg.ChunkLayoutRemoteFor = func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
			inner, err := chunkstore.NewLayoutRemote(fixture.Catalog, fixture.Ref, me)
			if err != nil {
				return nil, err
			}
			delayed.LayoutRemote = inner
			return delayed, nil
		}
	}
	if len(hooks) > 0 {
		cfg.OnChunkDetach = hooks[0].detach
		cfg.OnChunkDetachResumed = hooks[0].resume
	}
	p, err := durable.NewDurableProvider(cfg, view)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestPrecopyProviderPublishesExactDetachedImage(t *testing.T) {
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	p := precopyBProvider(t, fixture, &parkView{stubView: v1View()}, nil)
	openRecovered(t, p)
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Write(context.Background(), 4096, block(0x8a)); err != nil {
		t.Fatal(err)
	}
	r, last, err := p.DetachVolume(context.Background(), "v1")
	if err != nil || !r.Precopy.Frozen || r.Head.State != storage.ChunkHeadDetached || r.H != last {
		t.Fatalf("detach=%+v last=%d err=%v", r, last, err)
	}
	if !bytes.Equal(layoutHeadBlock(t, fixture, 1), block(0x8a)) {
		t.Fatal("detached image lost write")
	}
	if _, err = b.Write(context.Background(), 4096, block(0x9a)); err == nil {
		t.Fatal("old owner serves after final detach")
	}
	again, _, err := p.DetachVolume(context.Background(), "v1")
	if err != nil || again.Head.Gen != r.Head.Gen {
		t.Fatalf("repeated detach: %+v %v", again, err)
	}
}

type precopyFixedSampleRemote struct {
	*storage.MemChunkRemote
	nextDelay atomic.Int64
}

func (r *precopyFixedSampleRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	if delay := time.Duration(r.nextDelay.Swap(0)); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return r.MemChunkRemote.CASHead(ctx, expected, next)
}

type precopyLayoutDelayedRemote struct {
	*chunkstore.LayoutRemote
	nextDelay atomic.Int64
}

func (r *precopyLayoutDelayedRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	if delay := time.Duration(r.nextDelay.Swap(0)); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return r.LayoutRemote.CASHead(ctx, expected, next)
}

func TestPrecopyProviderBusyOwnerKeepsServing(t *testing.T) {
	precopyBusyOwnerKeepsServing(t, 0)
}

func TestPrecopyProviderBusyOwnerRetryRetainsSlowFixedSample(t *testing.T) {
	precopyBusyOwnerKeepsServing(t, 1300*time.Millisecond)
}

func precopyBusyOwnerKeepsServing(t *testing.T, delay time.Duration) {
	t.Helper()
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	remote := &precopyLayoutDelayedRemote{}
	view := &parkView{stubView: v1View()}
	k := &detachHooks{}
	p := precopyBProvider(t, fixture, view, remote, k)
	openRecovered(t, p)
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write(context.Background(), 4096, block(0x8a)); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unpark := func() { releaseOnce.Do(func() { close(release) }) }
	if err := b.(*durable.StorageBackend).SetWriteObserver(writeObserverFunc{sync: func(ctx context.Context, _ uint64) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	t.Cleanup(func() {
		unpark()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("writer did not join")
		}
	})
	go func() { done <- b.Sync(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("write not admitted")
	}
	remote.nextDelay.Store(int64(delay))
	// Rolling cuts run while serving and have a 60s operation budget.
	// The drain and frozen admission bounds are checked independently.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	logs := &lockedLog{}
	captureWriteLogs(t, logs)
	r, _, err := p.DetachVolume(ctx, "v1")
	if !errors.Is(err, storage.ErrPrecopyNotConverged) || r.Precopy.Frozen {
		t.Fatalf("report=%+v err=%v", r.Precopy, err)
	}
	lines := 0
	for _, line := range strings.Split(logs.String(), "\n") {
		if !strings.Contains(line, "durable: pre-copy attempt ") {
			continue
		}
		lines++
		fields := map[string]string{}
		for _, token := range strings.Fields(line) {
			if key, value, ok := strings.Cut(token, "="); ok {
				fields[key] = value
			}
		}
		drain, de := strconv.ParseFloat(fields["drain_ms"], 64)
		barrier, be := strconv.ParseFloat(fields["barrier_ms"], 64)
		pause, pe := strconv.ParseFloat(fields["pause_ms"], 64)
		if fields["attempt"] != strconv.Itoa(lines) || fields["reason"] != "drain_timeout" || fields["volume"] != "v1" || de != nil || be != nil || pe != nil || drain <= 0 || barrier < 0 || pause < drain {
			t.Fatalf("incomplete pre-copy attempt diagnostic: %s", line)
		}
	}
	if lines != len(r.Precopy.Rounds) || lines != r.Precopy.Attempts+r.Precopy.MeasurementRounds {
		t.Fatalf("pre-copy round diagnostics missing: got %d, report=%+v", lines, r.Precopy)
	}
	if started, resumed := k.calls(); len(started) != 1 || len(resumed) != 1 {
		t.Fatalf("nonconverged hooks: started=%v resumed=%v", started, resumed)
	}
	if h := layoutHead(t, fixture); h.State == layout.StateDetached {
		t.Fatal("busy owner detached")
	}
	if _, err = b.Write(ctx, 8192, block(0x9b)); err != nil {
		t.Fatal("nonconverged owner stopped:", err)
	}
	unpark()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		done <- nil
	case <-ctx.Done():
		t.Fatal("held write did not finish")
	}
	r, _, err = p.DetachVolume(ctx, "v1")
	resumedCount := 1
	if err != nil {
		// Releasing the owner does not erase a slow sample. Admission must
		// still refuse a tail-bound point; a small stalled tail can now declare
		// the retained fixed-cost exception instead of recopying indefinitely.
		unknownRate := r.Precopy.Reason == "samples_unknown" && r.Precopy.RateSamples == 0
		overBudget := r.Precopy.Estimate > 1200*time.Millisecond && (r.Precopy.Reason == "tail_too_large" || r.Precopy.Reason == "barrier_over_budget" || r.Precopy.Reason == "freeze_budget_exceeded")
		if r.Precopy.Frozen || !(unknownRate && errors.Is(err, storage.ErrPrecopyUnknown) || overBudget && errors.Is(err, storage.ErrPrecopyNotConverged)) {
			t.Fatalf("post-release retry violated measured admission: report=%+v err=%v", r.Precopy, err)
		}
		if delay > 0 && r.Precopy.Fixed < delay {
			t.Fatal("retry forgot the controlled slow fixed sample")
		}
		resumedCount = 2
		if h := layoutHead(t, fixture); h.State == layout.StateDetached {
			t.Fatalf("over-budget retry detached: head=%+v", h)
		}
		buf := make([]byte, 4096)
		if n, e := b.Read(ctx, 8192, buf); e != nil || n != len(buf) || !bytes.Equal(buf, block(0x9b)) {
			t.Fatalf("over-budget retry failed to resume service: n=%d err=%v", n, e)
		}
	} else if delay > 0 {
		if !r.Precopy.Frozen || r.Precopy.Reason != "no_progress" || r.Precopy.Fixed < delay || r.Precopy.PreviousEstimate <= 0 || r.Precopy.Estimate < r.Precopy.PreviousEstimate-r.Precopy.PreviousEstimate/10 || r.Precopy.Estimate > 5*time.Second || r.Precopy.DeclaredFreeze != r.Precopy.Estimate {
			t.Fatalf("slow retry bypassed the declared small-tail plateau rule: %+v", r.Precopy)
		}
	}
	if started, resumed := k.calls(); len(started) != 2 || len(resumed) != resumedCount {
		t.Fatalf("retry hooks: started=%v resumed=%v", started, resumed)
	}
	if err != nil {
		for lba, want := range map[int64][]byte{1: block(0x8a), 2: block(0x9b)} {
			buf := make([]byte, 4096)
			if n, readErr := b.Read(ctx, lba*4096, buf); readErr != nil || n != len(buf) || !bytes.Equal(buf, want) {
				t.Fatalf("refused retry stopped serving block %d: n=%d err=%v", lba, n, readErr)
			}
		}
		return
	}
	if !bytes.Equal(layoutHeadBlock(t, fixture, 1), block(0x8a)) || !bytes.Equal(layoutHeadBlock(t, fixture, 2), block(0x9b)) {
		t.Fatal("retry lost admitted bytes")
	}
}
