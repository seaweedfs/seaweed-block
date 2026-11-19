package durable_test

// A failed detach (design MIGRATION E4, review S6): the guest is never left
// frozen by a detach that did not happen. When the final cut provably did not
// publish (a failed upload, a refused CAS, an unresolved detach CAS that a
// cancel at the same base settles), serving resumes on this host. When the
// detach landed, the volume stays stopped with the detached result. When the
// outcome cannot be decided (HEAD unreadable), it stays stopped: never resume
// on top of a possibly detached HEAD.

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

// faultRemote injects one fault into the next detach's remote calls.
type faultRemote struct {
	*storage.MemChunkRemote
	mu          sync.Mutex
	putErr      error  // returned by every Put while set
	casMode     string // "", "precondition", "lost" (not applied, error), "applied-lost" (applied, error), "hold"
	getHeadErr  error  // returned by every GetHead while set
	getHeadFail int    // the next N GetHead calls fail
}

var errInjected = errors.New("injected remote fault")

func (r *faultRemote) set(f func(r *faultRemote)) { r.mu.Lock(); f(r); r.mu.Unlock() }

func (r *faultRemote) Put(ctx context.Context, key string, data []byte) error {
	r.mu.Lock()
	err := r.putErr
	r.mu.Unlock()
	if err != nil {
		return err
	}
	return r.MemChunkRemote.Put(ctx, key, data)
}

func (r *faultRemote) GetHead(ctx context.Context) (storage.ChunkHead, error) {
	r.mu.Lock()
	err := r.getHeadErr
	if err == nil && r.getHeadFail > 0 {
		r.getHeadFail--
		err = errInjected
	}
	r.mu.Unlock()
	if err != nil {
		return storage.ChunkHead{}, err
	}
	return r.MemChunkRemote.GetHead(ctx)
}

func (r *faultRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	r.mu.Lock()
	mode := r.casMode
	r.casMode = "" // one fault, on the next CAS only
	r.mu.Unlock()
	switch mode {
	case "precondition":
		return 0, storage.ErrChunkHeadPrecondition
	case "lost":
		return 0, errInjected
	case "hold": // held until the caller gives up: not applied
		<-ctx.Done()
		return 0, ctx.Err()
	case "applied-lost":
		if _, err := r.MemChunkRemote.CASHead(ctx, expected, next); err != nil {
			return 0, err
		}
		return 0, errInjected
	}
	return r.MemChunkRemote.CASHead(ctx, expected, next)
}

// detachFixture: a chunk volume with one cut published and one more write
// (dirty), so the detach's final cut has something to upload.
func detachFixture(t *testing.T) (*faultRemote, *durable.DurableProvider, frontend.Backend) {
	t.Helper()
	remote := &faultRemote{MemChunkRemote: storage.NewMemChunkRemote()}
	p := chunkProvider(t, t.TempDir(), remote, v1View(), 0)
	openRecovered(t, p)
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write(context.Background(), 4096, block(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CutVolume(context.Background(), "v1", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write(context.Background(), 8192, block(2)); err != nil {
		t.Fatal(err)
	}
	return remote, p, b
}

// assertServing: the volume accepts writes and a cut publishes them, with a
// HEAD that is not detached.
func assertServing(t *testing.T, p *durable.DurableProvider, b frontend.Backend, remote *faultRemote) {
	t.Helper()
	ctx := context.Background()
	if _, err := b.Write(ctx, 12288, block(3)); err != nil {
		t.Fatalf("write after an aborted detach: %v; want the volume serving again", err)
	}
	r, err := p.CutVolume(ctx, "v1", 0)
	if err != nil {
		t.Fatalf("cut after an aborted detach: %v", err)
	}
	if r.Head.State == storage.ChunkHeadDetached {
		t.Fatal("HEAD is detached after a detach that was reported aborted")
	}
	if !bytesEqual(headBlock(t, remote.MemChunkRemote, 2), block(2)) || !bytesEqual(headBlock(t, remote.MemChunkRemote, 3), block(3)) {
		t.Fatal("the cut after an aborted detach lost the dirty blocks")
	}
}

func TestWALStoreChunk_FailedDetachUploadResumes(t *testing.T) {
	remote, p, b := detachFixture(t)
	remote.set(func(r *faultRemote) { r.putErr = errInjected })
	if _, _, err := p.DetachVolume(context.Background(), "v1"); !errors.Is(err, durable.ErrDetachAborted) {
		t.Fatalf("detach with failing uploads: %v; want ErrDetachAborted", err)
	}
	remote.set(func(r *faultRemote) { r.putErr = nil })
	assertServing(t, p, b, remote)
}

func TestWALStoreChunk_RefusedDetachCASResumes(t *testing.T) {
	remote, p, b := detachFixture(t)
	remote.set(func(r *faultRemote) { r.casMode = "precondition" })
	if _, _, err := p.DetachVolume(context.Background(), "v1"); !errors.Is(err, durable.ErrDetachAborted) {
		t.Fatalf("detach with a refused CAS: %v; want ErrDetachAborted", err)
	}
	assertServing(t, p, b, remote)
}

// The detach CAS's reply is lost and it did not apply: the settle cancels it
// with a CAS at the same base, so it can never land, and serving resumes.
func TestWALStoreChunk_LostDetachCASNotAppliedIsCancelledAndResumes(t *testing.T) {
	remote, p, b := detachFixture(t)
	base, _ := remote.MemChunkRemote.GetHead(context.Background())
	remote.set(func(r *faultRemote) { r.casMode = "lost" })
	if _, _, err := p.DetachVolume(context.Background(), "v1"); !errors.Is(err, durable.ErrDetachAborted) {
		t.Fatalf("detach with a lost, unapplied CAS: %v; want ErrDetachAborted after the cancel", err)
	}
	// The cancel is a real CAS at the detach's base: HEAD moved one generation
	// and is not detached, so the lost detach CAS can never land afterwards.
	if head, _ := remote.MemChunkRemote.GetHead(context.Background()); head.State == storage.ChunkHeadDetached || head.Gen != base.Gen+1 {
		t.Fatalf("HEAD after the cancel: gen %d state %q; want gen %d, not detached", head.Gen, head.State, base.Gen+1)
	}
	assertServing(t, p, b, remote)
}

// The detach CAS's reply is lost but it applied: the detach succeeded, the
// volume stays stopped and DetachVolume returns the detached result.
func TestWALStoreChunk_LostDetachCASAppliedStaysDetached(t *testing.T) {
	remote, p, b := detachFixture(t)
	remote.set(func(r *faultRemote) { r.casMode = "applied-lost" })
	r, _, err := p.DetachVolume(context.Background(), "v1")
	if err != nil || r.Head.State != storage.ChunkHeadDetached {
		t.Fatalf("detach whose CAS applied but lost its reply: %+v %v; want the detached result", r, err)
	}
	if _, err := b.Write(context.Background(), 12288, block(3)); !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("write after an applied detach: %v; want ErrNotReady", err)
	}
}

// The detach CAS applied but its reply was lost AND the cut's own HEAD re-read
// failed: only the settle can tell. It reads HEAD, finds the detach, and the
// detach succeeds (stays stopped) instead of resuming on a detached HEAD.
func TestWALStoreChunk_LostDetachCASAppliedFoundBySettle(t *testing.T) {
	remote, p, b := detachFixture(t)
	remote.set(func(r *faultRemote) { r.casMode = "applied-lost"; r.getHeadFail = 1 })
	r, _, err := p.DetachVolume(context.Background(), "v1")
	if err != nil || r.Head.State != storage.ChunkHeadDetached {
		t.Fatalf("detach whose applied CAS only the settle can see: %+v %v; want the detached result", r, err)
	}
	if _, err := b.Write(context.Background(), 12288, block(3)); !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("write after a settled, applied detach: %v; want ErrNotReady", err)
	}
}

// The detach CAS's reply is lost and HEAD cannot be read: the outcome is
// unknown, so the volume stays stopped (also through a recovery).
func TestWALStoreChunk_UndecidableDetachStaysStopped(t *testing.T) {
	remote, p, b := detachFixture(t)
	remote.set(func(r *faultRemote) { r.casMode = "lost"; r.getHeadErr = errInjected })
	if _, _, err := p.DetachVolume(context.Background(), "v1"); !errors.Is(err, durable.ErrDetachOutcomeUnknown) {
		t.Fatalf("detach with an undecidable outcome: %v; want ErrDetachOutcomeUnknown", err)
	}
	if _, err := b.Write(context.Background(), 12288, block(3)); !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("write after an undecidable detach: %v; want ErrNotReady", err)
	}
	if _, err := p.RecoverVolume(context.Background(), "v1"); !errors.Is(err, storage.ErrChunkLayerDetached) {
		t.Fatalf("recovery after an undecidable detach: %v; want the volume to stay stopped", err)
	}
}

// The freeze is bounded by DetachFreezeTimeout: a CAS that hangs ends there,
// the outcome is settled (HEAD still at base: cancelled) and serving resumes,
// all well inside bound + settle.
func TestWALStoreChunk_DetachFreezeIsBoundedThenResumes(t *testing.T) {
	remote := &faultRemote{MemChunkRemote: storage.NewMemChunkRemote()}
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl:                durable.ImplWALStoreChunk,
		StorageRoot:         t.TempDir(),
		BlockSize:           4096,
		NumBlocks:           pcBlocks,
		ChunkBlocks:         pcChunkBlocks,
		DetachFreezeTimeout: 300 * time.Millisecond,
		ChunkRemoteFor:      func(string) (storage.ChunkRemote, error) { return remote, nil },
	}, v1View())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	openRecovered(t, p)
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write(context.Background(), 4096, block(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CutVolume(context.Background(), "v1", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write(context.Background(), 8192, block(2)); err != nil {
		t.Fatal(err)
	}
	remote.set(func(r *faultRemote) { r.casMode = "hold" })
	start := time.Now()
	_, _, err = p.DetachVolume(context.Background(), "v1")
	took := time.Since(start)
	if !errors.Is(err, durable.ErrDetachAborted) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("detach with a hung CAS: %v; want an aborted detach at the freeze bound", err)
	}
	if took < 300*time.Millisecond || took > 3*time.Second {
		t.Fatalf("detach with a hung CAS took %v; want about the 300 ms freeze bound", took)
	}
	assertServing(t, p, b, remote)
}

// HEAD belongs to a newer epoch: the detach's CAS is refused and the volume
// is fenced; it stays stopped (never resumes against another holder).
func TestWALStoreChunk_DetachAgainstNewerEpochStaysStopped(t *testing.T) {
	remote, p, b := detachFixture(t)
	ctx := context.Background()
	cur, err := remote.MemChunkRemote.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	newer := cur
	newer.Epoch, newer.Holder = cur.Epoch+1, "another-holder"
	if _, err := remote.MemChunkRemote.CASHead(ctx, storage.ChunkHeadVersion{Gen: cur.Gen, Epoch: cur.Epoch}, newer); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.DetachVolume(ctx, "v1"); !errors.Is(err, storage.ErrChunkLayerFenced) || errors.Is(err, durable.ErrDetachAborted) {
		t.Fatalf("detach against a newer epoch: %v; want fenced, not aborted", err)
	}
	if _, err := b.Write(ctx, 12288, block(3)); !errors.Is(err, frontend.ErrNotReady) && !errors.Is(err, frontend.ErrStalePrimary) {
		t.Fatalf("write after a fenced detach: %v; want it refused (not ready / stale primary)", err)
	}
}
