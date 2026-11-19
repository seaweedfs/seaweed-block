package durable_test

// DetachVolume is a real stop (B9 W2 §3.6; review of #115, P1): frontend
// I/O admitted before the stop is drained, bounded by the caller's
// context, before the final cut, so nothing is acknowledged outside the
// detached image; and the stop holds for the rest of the process, through
// a RecoverVolume, whether the detach succeeded or failed.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
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
	view := &parkView{stubView: v1View()}
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl:           durable.ImplWALStoreChunk,
		StorageRoot:    t.TempDir(),
		BlockSize:      4096,
		NumBlocks:      pcBlocks,
		ChunkBlocks:    pcChunkBlocks,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return remote, nil },
	}, view)
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
	remote := storage.NewMemChunkRemote()
	p, view := parkedProvider(t, remote)
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
	if !bytesEqual(headBlock(t, remote, 1), block(7)) || !bytesEqual(headBlock(t, remote, 2), block(8)) {
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
	remote := storage.NewMemChunkRemote()
	p, view := parkedProvider(t, remote)
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
	if head, _ := remote.GetHead(ctx); head.State == storage.ChunkHeadDetached {
		t.Fatalf("a detach that did not drain published a detached HEAD: %+v", head)
	}
	// Serving resumes: a new write is accepted. The drain still pending from
	// the aborted stop orders new I/O after the I/O admitted before it, so
	// this write completes once that admitted write does (never on the
	// detach itself).
	nerr := make(chan error, 1)
	go func() { _, err := b.Write(ctx, 8192, block(6)); nerr <- err }()
	select {
	case err := <-nerr:
		t.Fatalf("new write completed (%v) ahead of the write admitted before the stop", err)
	case <-time.After(100 * time.Millisecond): // queued behind the admitted write, as the drain orders it
	}

	view.unpark()
	acked := <-werr == nil
	if err := <-nerr; err != nil {
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
	if acked && !bytesEqual(headBlock(t, remote, 1), block(5)) {
		t.Fatal("the write admitted before the stop was acknowledged but is not in the detached HEAD")
	}
	if !bytesEqual(headBlock(t, remote, 2), block(6)) {
		t.Fatal("the write accepted after the aborted detach is not in the detached HEAD")
	}
}

// After a successful detach, RecoverVolume keeps I/O stopped and a repeated
// detach returns the first result.
func TestWALStoreChunk_RecoveryKeepsDetachedVolumeStopped(t *testing.T) {
	ctx := context.Background()
	remote := storage.NewMemChunkRemote()
	p := chunkProvider(t, t.TempDir(), remote, v1View(), 0)
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
	remote := storage.NewMemChunkRemote()
	p := chunkProvider(t, t.TempDir(), remote, v1View(), 0)
	s := openRecovered(t, p)
	if _, err := s.Write(1, block(1)); err != nil {
		t.Fatal(err)
	}
	last := s.NextLSN() - 1
	if _, err := p.CutVolume(ctx, "v1", last+1); err == nil {
		t.Fatal("a cut to an LSN never written succeeded")
	}
	if head, _ := remote.GetHead(ctx); head.Gen != 0 || remote.Puts() != 0 {
		t.Fatalf("a refused minLSN published: HEAD gen %d, %d chunks stored", head.Gen, remote.Puts())
	}
	r, err := p.CutVolume(ctx, "v1", last)
	if err != nil || r.H != last || r.Head.Gen != 1 {
		t.Fatalf("cut to the last written LSN: %+v %v", r, err)
	}
}
