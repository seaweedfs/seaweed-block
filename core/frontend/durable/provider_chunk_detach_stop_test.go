package durable_test

// DetachVolume is a real stop (B9 W2 §3.6; review of #115, P1): frontend
// I/O admitted before the stop is drained, bounded by the caller's
// context, before the final cut, so nothing is acknowledged outside the
// detached image; and the stop holds for the rest of the process, through
// a RecoverVolume, whether the detach succeeded or failed.

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// parkView parks the first Projection call after arm until release. A
// frontend write reads the projection in its lineage check, after the
// operational gate passed: parking it there holds an admitted write.
type parkView struct {
	*stubView
	mu      sync.Mutex
	armed   bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (v *parkView) arm() {
	v.mu.Lock()
	v.armed, v.entered, v.release = true, make(chan struct{}), make(chan struct{})
	v.mu.Unlock()
}

func (v *parkView) unpark() { v.once.Do(func() { close(v.release) }) }

func (v *parkView) Projection() frontend.Projection {
	v.mu.Lock()
	park, entered, release := v.armed, v.entered, v.release
	v.armed = false
	v.mu.Unlock()
	if park {
		close(entered)
		<-release
	}
	return v.stubView.Projection()
}

func parkedProvider(t *testing.T, remote storage.ChunkRemote) (*durable.DurableProvider, *parkView) {
	t.Helper()
	_ = remote // Other legacy tests still using this helper migrate separately.
	return parkedLayoutProvider(t, testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks))
}

func parkedLayoutProvider(t *testing.T, fixture *testutil.LayoutFixture) (*durable.DurableProvider, *parkView) {
	t.Helper()
	view := &parkView{stubView: v1View()}
	cfg := durable.ProviderConfig{
		Impl:        durable.ImplWALStoreChunk,
		StorageRoot: t.TempDir(),
		BlockSize:   4096,
		NumBlocks:   pcBlocks,
		ChunkBlocks: pcChunkBlocks,
	}
	fixture.Configure(&cfg)
	p, err := durable.NewDurableProvider(cfg, view)
	if err != nil {
		t.Fatalf("NewDurableProvider: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p, view
}

type detachOutcome struct {
	r    storage.ChunkCutResult
	last uint64
	err  error
}

// An admitted two-block write still in flight holds the detach; the
// detach returns only after it, and the acknowledged blocks are in the
// detached HEAD.
func TestWALStoreChunk_DetachDrainsAdmittedWrite(t *testing.T) {
	ctx := context.Background()
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	p, view := parkedLayoutProvider(t, fixture)
	openRecovered(t, p)
	b, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	view.arm()
	t.Cleanup(view.unpark)
	werr := make(chan error, 1)
	go func() { _, err := b.Write(ctx, 4096, append(block(7), block(8)...)); werr <- err }()
	<-view.entered // admitted: past the operational gate, inside the lineage check

	det := make(chan detachOutcome, 1)
	go func() {
		r, last, err := p.DetachVolume(ctx, "v1")
		det <- detachOutcome{r, last, err}
	}()
	select {
	case o := <-det:
		t.Fatalf("Detach returned (%v, HEAD gen %d) while an admitted write was still in flight", o.err, o.r.Head.Gen)
	case <-time.After(200 * time.Millisecond):
	}
	view.unpark()
	if err := <-werr; err != nil {
		t.Fatalf("the admitted write: %v", err)
	}
	o := <-det
	if o.err != nil || o.r.Head.State != storage.ChunkHeadDetached {
		t.Fatalf("detach: %+v", o)
	}
	if !bytes.Equal(layoutHeadBlock(t, fixture, 1), block(7)) || !bytes.Equal(layoutHeadBlock(t, fixture, 2), block(8)) {
		t.Fatal("a write acknowledged during the detach is not in the detached HEAD")
	}
	if _, err := b.Write(ctx, 4096, block(9)); err == nil {
		t.Fatal("a frontend write was accepted after the detach")
	}
}

// The drain is bounded (the caller's context or the freeze bound): a detach
// that cannot drain publishes nothing, so it aborts and the volume serves
// again (MIGRATION E4; it used to stay stopped, review S6); once the admitted
// write finishes, a retry detaches with every acknowledged write.
func TestWALStoreChunk_DetachDrainTimeoutAbortsAndResumes(t *testing.T) {
	ctx := context.Background()
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	p, view := parkedLayoutProvider(t, fixture)
	openRecovered(t, p)
	b, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	view.arm()
	t.Cleanup(view.unpark)
	werr := make(chan error, 1)
	go func() { _, err := b.Write(ctx, 4096, block(5)); werr <- err }()
	<-view.entered

	dctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	_, _, err = p.DetachVolume(dctx, "v1")
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, durable.ErrDetachAborted) {
		t.Fatalf("detach that cannot drain: %v; want an aborted detach on the context's deadline", err)
	}
	if head := layoutHead(t, fixture); head.State == layout.StateDetached {
		t.Fatalf("a detach that did not drain published a detached HEAD: %+v", head)
	}
	// Serving resumes: a new write is accepted. The drain still pending from
	// the aborted stop orders new I/O after the I/O admitted before it, so
	// this write completes once that admitted write does (never on the
	// detach itself).
	arrived := make(chan struct{})
	var arriveOnce sync.Once
	durable.SetTestBeforeIOLock(b, func() { arriveOnce.Do(func() { close(arrived) }) })
	nerr := make(chan error, 1)
	go func() { _, err := b.Write(ctx, 8192, block(6)); nerr <- err }()
	bounded := func(what string, ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s did not happen within 5s", what)
		}
	}
	bounded("the new write passing the operational gate (serving resumed)", arrived)
	select {
	case err := <-nerr:
		t.Fatalf("new write completed (%v) ahead of the write admitted before the stop", err)
	default: // queued behind the admitted write at the I/O lock, as the drain orders it
	}

	view.unpark()
	recv := func(what string, ch <-chan error) error {
		t.Helper()
		select {
		case err := <-ch:
			return err
		case <-time.After(5 * time.Second):
			t.Fatalf("%s did not complete within 5s", what)
			return nil
		}
	}
	acked := recv("the admitted write", werr) == nil
	if err := recv("the new write", nerr); err != nil {
		t.Fatalf("new write after an aborted detach: %v; want it accepted", err)
	}
	if _, err := p.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatalf("recovery after an aborted detach: %v; want it to succeed", err)
	}
	o := detachOutcome{}
	o.r, o.last, o.err = p.DetachVolume(ctx, "v1")
	if o.err != nil || o.r.Head.State != storage.ChunkHeadDetached {
		t.Fatalf("detach retry: %+v", o)
	}
	if acked && !bytes.Equal(layoutHeadBlock(t, fixture, 1), block(5)) {
		t.Fatal("the write admitted before the stop was acknowledged but is not in the detached HEAD")
	}
	if !bytes.Equal(layoutHeadBlock(t, fixture, 2), block(6)) {
		t.Fatal("the write accepted after the aborted detach is not in the detached HEAD")
	}
}

// After a successful detach, RecoverVolume keeps I/O stopped and a repeated
// detach returns the first result.
func TestWALStoreChunk_RecoveryKeepsDetachedVolumeStopped(t *testing.T) {
	ctx := context.Background()
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	p := layoutChunkProvider(t, t.TempDir(), fixture, v1View(), 0)
	openRecovered(t, p)
	b, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write(ctx, 4096, block(2)); err != nil {
		t.Fatal(err)
	}
	r1, last1, err := p.DetachVolume(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.RecoverVolume(ctx, "v1"); !errors.Is(err, storage.ErrChunkLayerDetached) {
		t.Fatalf("recovery after a detach: %v; want the volume to stay stopped", err)
	}
	if _, err := b.Write(ctx, 4096, block(3)); !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("write after a detach and a recovery: %v; want ErrNotReady", err)
	}
	if _, err := b.Read(ctx, 4096, make([]byte, 4096)); !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("read after a detach and a recovery: %v; want ErrNotReady", err)
	}
	r2, last2, err := p.DetachVolume(ctx, "v1")
	if err != nil || r2.Head.Gen != r1.Head.Gen || r2.H != r1.H || last2 != last1 {
		t.Fatalf("repeated detach after the recovery: %+v %d %v; want %+v %d", r2, last2, err, r1, last1)
	}
}

// A positive minLSN that was never written is refused before anything is
// published; a reachable one cuts.
func TestWALStoreChunk_CutVolumeRefusesUnwrittenMinLSNBeforePublishing(t *testing.T) {
	ctx := context.Background()
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	p := layoutChunkProvider(t, t.TempDir(), fixture, v1View(), 0)
	s := openRecovered(t, p)
	claimed := layoutHead(t, fixture)
	if _, err := s.Write(1, block(1)); err != nil {
		t.Fatal(err)
	}
	last := s.NextLSN() - 1
	if _, err := p.CutVolume(ctx, "v1", last+1); err == nil {
		t.Fatal("a cut to an LSN never written succeeded")
	}
	if head := layoutHead(t, fixture); head.Gen != claimed.Gen || fixture.Blobs.Puts() != 0 {
		t.Fatalf("a refused minLSN published: HEAD gen %d, %d chunks stored", head.Gen, fixture.Blobs.Puts())
	}
	r, err := p.CutVolume(ctx, "v1", last)
	if err != nil || r.H != last || r.Head.Gen <= claimed.Gen {
		t.Fatalf("cut to the last written LSN: %+v %v", r, err)
	}
}
