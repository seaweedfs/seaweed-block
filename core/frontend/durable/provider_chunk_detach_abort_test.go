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
	"fmt"
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
	// held records the CAS held by "hold-detach", heldRead the first HEAD read
	// held once readArmed is set: when and why each context ended, and what
	// its witness saw (see witnessHeld).
	held      heldOp
	heldRead  heldOp
	readArmed bool
}

type heldOp struct {
	entered bool
	ctx     context.Context
	ended   time.Time
	cause   error // context.Cause once the context ended
	witness heldObservation
}

// heldObservation is one sample of a held operation's context, taken
// between from and at.
type heldObservation struct {
	ran      bool
	from, at time.Time
	entered  bool  // the operation was already held when sampled
	cause    error // its context.Cause then (nil: still live)
}

const (
	// The witness samples witnessDelay after a traced deadline, so a cancel
	// shifted witnessShift past it is still live at the sample.
	witnessDelay    = 50 * time.Millisecond
	witnessShift    = 100 * time.Millisecond
	witnessAttempts = 3
)

var errWitnessUnexercised = errors.New("witness not exercised")

// witnessVerdict judges one sample of the operation held under deadline.
// Only a sample of an already held operation, taken after the deadline and
// before the shift, discriminates: then its context must already be
// cancelled for that deadline's expiry. Any other sample (late timer,
// operation not yet held) proves nothing either way and is reported as
// unexercised; it is never filled in from a later observation.
func witnessVerdict(deadline time.Time, o heldObservation) error {
	switch {
	case !o.ran:
		return fmt.Errorf("%w: the witness never ran", errWitnessUnexercised)
	case o.from.Before(deadline):
		return fmt.Errorf("%w: sampled %v before the deadline", errWitnessUnexercised, deadline.Sub(o.from))
	case !o.at.Before(deadline.Add(witnessShift)):
		return fmt.Errorf("%w: sampled up to %v after the deadline, not within %v", errWitnessUnexercised, o.at.Sub(deadline), witnessShift)
	case !o.entered:
		return fmt.Errorf("%w: the operation was not held yet when sampled", errWitnessUnexercised)
	case !errors.Is(o.cause, context.DeadlineExceeded):
		return fmt.Errorf("held operation live or cancelled otherwise (cause %v) %v after its traced deadline", o.cause, o.from.Sub(deadline))
	}
	return nil
}

func heldDetachCAS(r *faultRemote) *heldOp  { return &r.held }
func heldSettleRead(r *faultRemote) *heldOp { return &r.heldRead }

// witnessHeld samples the context of the operation op selects once,
// witnessDelay after the traced deadline, recording when the sample was
// taken. done is closed once the sample is recorded.
func (r *faultRemote) witnessHeld(deadline time.Time, op func(*faultRemote) *heldOp) (done <-chan struct{}, stop func() bool) {
	ch := make(chan struct{})
	tm := time.AfterFunc(time.Until(deadline.Add(witnessDelay)), func() {
		r.set(func(r *faultRemote) {
			h := op(r)
			o := heldObservation{ran: true, from: time.Now()}
			if h.ctx != nil {
				o.entered, o.cause = true, context.Cause(h.ctx)
			}
			o.at = time.Now()
			h.witness = o
		})
		close(ch)
	})
	return ch, tm.Stop
}

// retryUnexercised runs attempt until its witness is exercised: an attempt
// whose sample proved nothing (a loaded host) is retried, up to
// witnessAttempts; attempt fails t itself on any defect.
func retryUnexercised(t *testing.T, attempt func(t *testing.T) error) {
	t.Helper()
	for n := 1; ; n++ {
		var verdict error
		if !t.Run(fmt.Sprintf("attempt%d", n), func(t *testing.T) { verdict = attempt(t) }) {
			return
		}
		if verdict == nil {
			return
		}
		if n == witnessAttempts {
			t.Fatalf("no attempt in %d exercised the witness; last: %v", n, verdict)
		}
		t.Logf("attempt %d: %v; retrying", n, verdict)
	}
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
		var mine bool
		r.set(func(r *faultRemote) {
			if mine = r.readArmed && !r.heldRead.entered; mine {
				r.heldRead.entered, r.heldRead.ctx = true, ctx
			}
		})
		<-ctx.Done()
		if mine {
			ended := time.Now()
			r.set(func(r *faultRemote) { r.heldRead.ended, r.heldRead.cause = ended, context.Cause(ctx) })
		}
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
		r.set(func(r *faultRemote) {
			r.held.entered, r.held.ctx = true, ctx
		})
		<-ctx.Done()
		ended := time.Now()
		r.set(func(r *faultRemote) { r.held.ended, r.held.cause = ended, context.Cause(ctx) })
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
//
// The CAS must also be cancelled at the traced cut deadline, not later. That
// takes a sample between the deadline and a 100 ms shift; an attempt whose
// sample missed that window (a loaded host) proves nothing and is retried,
// up to witnessAttempts.
func TestWALStoreChunk_DetachFreezeIsBoundedThenResumes(t *testing.T) {
	retryUnexercised(t, detachFreezeAttempt)
}

// detachFreezeAttempt runs one bounded detach; it fails t on any defect and
// returns the cut witness's verdict when that sample proved nothing.
func detachFreezeAttempt(t *testing.T) error {
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
	var witnessed <-chan struct{}
	durable.SetTestDetachTrace(p, func(stage string, d time.Time) {
		deadlines[stage] = d
		if stage == "cut" {
			var stop func() bool
			witnessed, stop = remote.witnessHeld(d, heldDetachCAS)
			t.Cleanup(func() { stop() })
		}
	})
	_, _, err = p.DetachVolume(context.Background(), "v1")
	end := time.Now()
	if witnessed == nil {
		t.Fatal("the detach traced no cut deadline")
	}
	<-witnessed
	if !errors.Is(err, durable.ErrDetachAborted) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("detach with a hung CAS: %v; want an aborted detach at the freeze bound", err)
	}
	// One budget from one origin: the cut ends at three quarters of it, the
	// settlement at its end (exact, both derive from the same start).
	cut, settle := deadlines["cut"], deadlines["settle"]
	if gap := settle.Sub(cut); gap != freeze/4 {
		t.Fatalf("settlement deadline %v after the cut deadline; want a quarter of the %v bound", gap, freeze)
	}
	// The held operation is the detach CAS itself, released by the cut's
	// expiry. Its context's own deadline proves nothing: a CAS carries its
	// own operation timeout, and the cut may be a timer, not a deadline.
	var held heldOp
	remote.set(func(r *faultRemote) { held = r.held })
	if !held.entered {
		t.Fatal("the detach CAS was never held")
	}
	if held.ended.Before(cut) {
		t.Fatalf("held detach CAS ended %v before its deadline", cut.Sub(held.ended))
	}
	// Deadline or timer, the CAS was cancelled for the cut's own expiry and
	// by the traced cut deadline (plus the witness slack), not later.
	if !errors.Is(held.cause, context.DeadlineExceeded) {
		t.Fatalf("held detach CAS cancelled with cause %v; want the cut's deadline expiry", held.cause)
	}
	verdict := witnessVerdict(cut, held.witness)
	if verdict != nil && !errors.Is(verdict, errWitnessUnexercised) {
		t.Fatal(verdict)
	}
	if end.Before(held.ended) {
		t.Fatal("detach returned before its held CAS ended")
	}
	if late := end.Sub(settle); late > 500*time.Millisecond {
		t.Fatalf("detach returned %v after the freeze bound", late)
	}
	assertServing(t, p, b, remote)
	return verdict
}

// The verdict counts only a sample of an already held CAS, taken inside the
// window, as evidence; a late sample of a context that a shifted cut has
// cancelled by then, or a CAS that entered after the sample, prove nothing.
func TestHeldWitnessVerdict(t *testing.T) {
	cut := time.Unix(1700000000, 0)
	at := func(d time.Duration) time.Time { return cut.Add(d) }
	ms := time.Millisecond
	for _, c := range []struct {
		name string
		o    heldObservation
		want string // "pass", "unexercised" or "fail"
	}{
		{"cancelled at the cut", heldObservation{true, at(50 * ms), at(51 * ms), true, context.DeadlineExceeded}, "pass"},
		{"still live: shifted cut", heldObservation{true, at(50 * ms), at(51 * ms), true, nil}, "fail"},
		{"cancelled otherwise", heldObservation{true, at(50 * ms), at(51 * ms), true, context.Canceled}, "fail"},
		{"never sampled", heldObservation{}, "unexercised"},
		{"entered after the sample", heldObservation{true, at(50 * ms), at(51 * ms), false, nil}, "unexercised"},
		{"delayed sample of a shifted cut", heldObservation{true, at(120 * ms), at(121 * ms), true, context.DeadlineExceeded}, "unexercised"},
		{"sample straddles the shift", heldObservation{true, at(90 * ms), at(100 * ms), true, context.DeadlineExceeded}, "unexercised"},
		{"sampled before the cut", heldObservation{true, at(-ms), at(0), true, nil}, "unexercised"},
	} {
		err := witnessVerdict(cut, c.o)
		got := "fail"
		switch {
		case err == nil:
			got = "pass"
		case errors.Is(err, errWitnessUnexercised):
			got = "unexercised"
		}
		if got != c.want {
			t.Errorf("%s: %s (%v), want %s", c.name, got, err, c.want)
		}
	}
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

// The freeze bound covers the settlement too: with the detach CAS and the
// settlement's HEAD read both hung, the cut ends at its deadline, the read
// at the bound, the outcome stays unknown and the volume stays stopped. The
// bounds are the deadlines the detach traced, and each held operation is
// witnessed: the read must be cancelled at the traced settlement deadline,
// not later (retried, like the cut witness, when a sample proves nothing).
func TestWALStoreChunk_DetachFreezeBoundCoversTheSettle(t *testing.T) {
	retryUnexercised(t, settleBoundAttempt)
}

func settleBoundAttempt(t *testing.T) error {
	const freeze = 2 * time.Second
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
	remote.set(func(r *faultRemote) { r.casMode = "hold-detach"; r.getHeadHold = true })
	deadlines := map[string]time.Time{}
	var witnessed <-chan struct{}
	durable.SetTestDetachTrace(p, func(stage string, d time.Time) {
		deadlines[stage] = d
		if stage == "settle" {
			// Only reads from here on are the settlement's.
			remote.set(func(r *faultRemote) { r.readArmed = true })
			var stop func() bool
			witnessed, stop = remote.witnessHeld(d, heldSettleRead)
			t.Cleanup(func() { stop() })
		}
	})
	_, _, err = p.DetachVolume(context.Background(), "v1")
	end := time.Now()
	if witnessed == nil {
		t.Fatal("the detach traced no settlement deadline")
	}
	<-witnessed
	if !errors.Is(err, durable.ErrDetachOutcomeUnknown) {
		t.Fatalf("detach with a hung CAS and hung HEAD reads: %v; want the outcome unknown", err)
	}
	cut, settle := deadlines["cut"], deadlines["settle"]
	if gap := settle.Sub(cut); gap != freeze/4 {
		t.Fatalf("settlement deadline %v after the cut deadline; want a quarter of the %v bound", gap, freeze)
	}
	var cas, read heldOp
	remote.set(func(r *faultRemote) { cas, read = r.held, r.heldRead })
	// The cut ended on the hung detach CAS, not earlier in the capture.
	if !cas.entered {
		t.Fatal("the detach CAS was never held")
	}
	if cas.ended.Before(cut) {
		t.Fatalf("held detach CAS ended %v before the cut deadline", cut.Sub(cas.ended))
	}
	// The settlement's HEAD read was held until the bound, and the detach
	// returned right after it (scheduling slack only).
	if !read.entered {
		t.Fatal("the settlement's HEAD read was never held")
	}
	if read.ended.Before(settle) {
		t.Fatalf("held settlement read ended %v before the settlement deadline", settle.Sub(read.ended))
	}
	if !errors.Is(read.cause, context.DeadlineExceeded) {
		t.Fatalf("held settlement read cancelled with cause %v; want the bound's expiry", read.cause)
	}
	verdict := witnessVerdict(settle, read.witness)
	if verdict != nil && !errors.Is(verdict, errWitnessUnexercised) {
		t.Fatal(verdict)
	}
	if end.Before(read.ended) {
		t.Fatal("detach returned before its held settlement read ended")
	}
	if late := end.Sub(settle); late > 500*time.Millisecond {
		t.Fatalf("detach returned %v after the freeze bound; the bound must cover the settlement too", late)
	}
	if _, err := b.Write(context.Background(), 8192, block(2)); !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("write after an undecided detach: %v; want ErrNotReady", err)
	}
	return verdict
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
