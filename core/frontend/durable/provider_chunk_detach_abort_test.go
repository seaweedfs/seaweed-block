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
	casMode     string // "", "precondition", "lost" (not applied, error), "applied-lost" (applied, error), "hold", "hold-detach"
	getHeadErr  error  // returned by every GetHead while set
	getHeadFail int    // the next N GetHead calls fail
	getHeadHold bool   // every GetHead blocks until its caller gives up
	// "delayed": the CAS's reply is lost now, the CAS itself reaches the
	// remote only when release closes; its own result goes to delayedErr.
	release    chan struct{}
	delayedErr chan error
	// held records the CAS held by "hold-detach": the deadline of the context
	// it was given and when that context ended.
	held heldCAS
}

type heldCAS struct {
	entered     bool
	deadline    time.Time
	hasDeadline bool
	ended       time.Time
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
	hold := r.getHeadHold
	err := r.getHeadErr
	if err == nil && r.getHeadFail > 0 {
		r.getHeadFail--
		err = errInjected
	}
	r.mu.Unlock()
	if hold {
		<-ctx.Done()
		return storage.ChunkHead{}, ctx.Err()
	}
	if err != nil {
		return storage.ChunkHead{}, err
	}
	return r.MemChunkRemote.GetHead(ctx)
}

func (r *faultRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	r.mu.Lock()
	mode := r.casMode
	switch {
	case mode == "hold-detach" && next.State != storage.ChunkHeadDetached:
		mode = "" // only the detach CAS is held; others (a running chunk loop) pass
	default:
		r.casMode = "" // one fault, on the next CAS only
	}
	r.mu.Unlock()
	switch mode {
	case "precondition":
		return 0, storage.ErrChunkHeadPrecondition
	case "lost":
		return 0, errInjected
	case "hold": // held until the caller gives up: not applied
		<-ctx.Done()
		return 0, ctx.Err()
	case "hold-detach": // the detach CAS, held until its caller gives up; witnessed
		dl, ok := ctx.Deadline()
		r.set(func(r *faultRemote) { r.held = heldCAS{entered: true, deadline: dl, hasDeadline: ok} })
		<-ctx.Done()
		ended := time.Now()
		r.set(func(r *faultRemote) { r.held.ended = ended })
		return 0, ctx.Err()
	case "delayed":
		r.mu.Lock()
		release, out := r.release, r.delayedErr
		r.mu.Unlock()
		go func() {
			<-release
			_, err := r.MemChunkRemote.CASHead(context.Background(), expected, next)
			out <- err
		}()
		return 0, errInjected
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

// The freeze is bounded by DetachFreezeTimeout: a detach CAS that hangs ends at
// the cut's deadline (three quarters of the bound), the outcome is settled
// (HEAD still at base: cancelled) inside the bound, and serving resumes. The
// test asserts against the deadlines the detach itself traced, so a loaded
// host cannot turn the order into a timing failure; the settlement keeps a
// quarter of a 2 s bound for its HEAD read and cancel.
func TestWALStoreChunk_DetachFreezeIsBoundedThenResumes(t *testing.T) {
	const freeze = 2 * time.Second
	remote := &faultRemote{MemChunkRemote: storage.NewMemChunkRemote()}
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl:                durable.ImplWALStoreChunk,
		StorageRoot:         t.TempDir(),
		BlockSize:           4096,
		NumBlocks:           pcBlocks,
		ChunkBlocks:         pcChunkBlocks,
		DetachFreezeTimeout: freeze,
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
	remote.set(func(r *faultRemote) { r.casMode = "hold-detach" })
	deadlines := map[string]time.Time{}
	durable.SetTestDetachTrace(p, func(stage string, d time.Time) { deadlines[stage] = d })
	_, _, err = p.DetachVolume(context.Background(), "v1")
	end := time.Now()
	if !errors.Is(err, durable.ErrDetachAborted) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("detach with a hung CAS: %v; want an aborted detach at the freeze bound", err)
	}
	// One budget from one origin: the cut ends at three quarters of it, the
	// settlement at its end (exact, both derive from the same start).
	cut, settle := deadlines["cut"], deadlines["settle"]
	if gap := settle.Sub(cut); gap != freeze/4 {
		t.Fatalf("settlement deadline %v after the cut deadline; want a quarter of the %v bound", gap, freeze)
	}
	// The held operation is the detach CAS itself: it ran under exactly the
	// cut's deadline, and was released by that deadline expiring.
	var held heldCAS
	remote.set(func(r *faultRemote) { held = r.held })
	if !held.entered {
		t.Fatal("the detach CAS was never held")
	}
	if !held.hasDeadline || !held.deadline.Equal(cut) {
		t.Fatalf("held detach CAS deadline %v (set %v); want the cut deadline %v", held.deadline, held.hasDeadline, cut)
	}
	if held.ended.Before(cut) {
		t.Fatalf("held detach CAS ended %v before its deadline", cut.Sub(held.ended))
	}
	if end.Before(held.ended) {
		t.Fatal("detach returned before its held CAS ended")
	}
	if late := end.Sub(settle); late > 500*time.Millisecond {
		t.Fatalf("detach returned %v after the freeze bound", late)
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

// P1 (review): a repeated detach after a successful one, with a cancelled
// context (its StopIO fails at once), returns the first result and never
// resumes serving.
func TestWALStoreChunk_RepeatDetachWithCancelledContextNeverResumes(t *testing.T) {
	_, p, b := detachFixture(t)
	r1, _, err := p.DetachVolume(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	forced := false
	durable.SetTestDetachStopIO(p, func(context.Context) error { forced = true; return context.Canceled }) // the drain fails
	r2, _, err := p.DetachVolume(context.Background(), "v1")
	if !forced {
		t.Fatal("the failed-drain branch was not taken")
	}
	if err != nil || r2.Head.Gen != r1.Head.Gen || r2.Head.State != storage.ChunkHeadDetached {
		t.Fatalf("repeated detach with a cancelled context: %+v %v; want the first result %+v", r2, err, r1)
	}
	if _, err := b.Write(context.Background(), 12288, block(3)); !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("write after a repeated detach: %v; want ErrNotReady (never resumed)", err)
	}
}

// P1 (review): after an undecidable detach, a retry whose StopIO fails (a
// cancelled context) settles first and stays stopped while the outcome is
// still unknown.
func TestWALStoreChunk_RetryAfterUndecidableDetachNeverResumes(t *testing.T) {
	remote, p, b := detachFixture(t)
	remote.set(func(r *faultRemote) { r.casMode = "lost"; r.getHeadErr = errInjected })
	if _, _, err := p.DetachVolume(context.Background(), "v1"); !errors.Is(err, durable.ErrDetachOutcomeUnknown) {
		t.Fatalf("first detach: %v; want ErrDetachOutcomeUnknown", err)
	}
	forced := false
	durable.SetTestDetachStopIO(p, func(context.Context) error { forced = true; return context.Canceled }) // the drain fails
	_, _, err := p.DetachVolume(context.Background(), "v1")
	if !forced {
		t.Fatal("the failed-drain branch was not taken")
	}
	if !errors.Is(err, durable.ErrDetachOutcomeUnknown) || errors.Is(err, durable.ErrDetachAborted) {
		t.Fatalf("retry over an unresolved detach CAS: %v; want still unknown, never aborted", err)
	}
	if _, err := b.Write(context.Background(), 12288, block(3)); !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("write after a retry over an unknown detach: %v; want ErrNotReady", err)
	}
}

// P1 (review): one freeze budget covers the cut AND the settlement: with the
// CAS and every HEAD read hung, DetachVolume returns within the bound (here
// undecided, so the volume stays stopped).
func TestWALStoreChunk_DetachFreezeBoundCoversTheSettle(t *testing.T) {
	const freeze = 400 * time.Millisecond
	remote := &faultRemote{MemChunkRemote: storage.NewMemChunkRemote()}
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), BlockSize: 4096, NumBlocks: pcBlocks, ChunkBlocks: pcChunkBlocks,
		DetachFreezeTimeout: freeze,
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
	remote.set(func(r *faultRemote) { r.casMode = "hold"; r.getHeadHold = true })
	deadlines := map[string]time.Time{}
	durable.SetTestDetachTrace(p, func(stage string, d time.Time) { deadlines[stage] = d })
	start := time.Now()
	_, _, err = p.DetachVolume(context.Background(), "v1")
	took := time.Since(start)
	cutAt, settleAt := deadlines["cut"].Sub(start), deadlines["settle"].Sub(start)
	if cutAt < freeze*3/4-50*time.Millisecond || cutAt > freeze*3/4+50*time.Millisecond || settleAt < freeze-50*time.Millisecond || settleAt > freeze+50*time.Millisecond {
		t.Fatalf("deadlines: cut %v, settle %v after the start; want three quarters of the %v freeze, then the bound", cutAt, settleAt, freeze)
	}
	if !errors.Is(err, durable.ErrDetachOutcomeUnknown) {
		t.Fatalf("detach with a hung CAS and hung HEAD reads: %v; want the outcome unknown", err)
	}
	if took > freeze+200*time.Millisecond {
		t.Fatalf("detach took %v; the %v freeze bound must cover the settlement too", took, freeze)
	}
	if _, err := b.Write(context.Background(), 8192, block(2)); !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("write after an undecided detach: %v; want ErrNotReady", err)
	}
}

// P2 (review): the lost detach CAS really is in flight. It reaches the remote
// only after the cancel landed, and then fails its precondition: HEAD stays
// active.
func TestWALStoreChunk_DelayedDetachCASCannotLandAfterTheCancel(t *testing.T) {
	remote, p, b := detachFixture(t)
	release, delayed := make(chan struct{}), make(chan error, 1)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	joined := false
	t.Cleanup(func() { // join the delayed CAS worker, bounded
		unblock()
		if !joined {
			select {
			case <-delayed:
			case <-time.After(5 * time.Second):
				t.Error("delayed CAS worker not joined")
			}
		}
	})
	remote.set(func(r *faultRemote) { r.casMode = "delayed"; r.release, r.delayedErr = release, delayed })
	if _, _, err := p.DetachVolume(context.Background(), "v1"); !errors.Is(err, durable.ErrDetachAborted) {
		t.Fatalf("detach with a delayed CAS: %v; want ErrDetachAborted after the cancel", err)
	}
	unblock()
	var derr error
	select {
	case derr = <-delayed:
		joined = true
	case <-time.After(5 * time.Second):
		t.Fatal("the delayed detach CAS did not complete after its release")
	}
	if !errors.Is(derr, storage.ErrChunkHeadPrecondition) {
		t.Fatalf("the delayed detach CAS after the cancel: %v; want a precondition failure", derr)
	}
	if head, _ := remote.MemChunkRemote.GetHead(context.Background()); head.State == storage.ChunkHeadDetached {
		t.Fatal("the delayed detach CAS landed after the cancel")
	}
	assertServing(t, p, b, remote)
}
