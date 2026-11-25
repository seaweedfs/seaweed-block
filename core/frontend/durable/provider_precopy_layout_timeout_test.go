package durable_test

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// The detached commit applies, but its caller sees only the freeze deadline.
// Settlement must read the committed HEAD before any guest write can resume.
type appliedDetachWithoutReply struct {
	*chunkstore.LayoutRemote
	hold        bool
	apply       bool
	applied     bool
	appliedAt   time.Time
	cutCause    error
	unavailable atomic.Bool
	stallSettle atomic.Bool
	abortCalls  atomic.Int32
}

func (r *appliedDetachWithoutReply) CASHead(ctx context.Context, want storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	if !r.hold || next.State != storage.ChunkHeadDetached {
		return r.LayoutRemote.CASHead(ctx, want, next)
	}
	if r.apply {
		if _, err := r.LayoutRemote.CASHead(ctx, want, next); err != nil {
			return 0, err
		}
		r.applied = true
		r.appliedAt = time.Now()
	}
	<-ctx.Done()
	r.cutCause = context.Cause(ctx)
	return 0, ctx.Err()
}

func (r *appliedDetachWithoutReply) AbortOpenSession(ctx context.Context) (string, error) {
	r.abortCalls.Add(1)
	if r.stallSettle.Load() {
		<-ctx.Done()
		return "", ctx.Err()
	}
	if r.unavailable.Load() {
		return "", errors.New("filer unavailable")
	}
	return r.LayoutRemote.AbortOpenSession(ctx)
}

func (r *appliedDetachWithoutReply) GetHead(ctx context.Context) (storage.ChunkHead, error) {
	if r.unavailable.Load() {
		return storage.ChunkHead{}, errors.New("filer unavailable")
	}
	return r.LayoutRemote.GetHead(ctx)
}

func precopyLayoutTimeoutFixture(t *testing.T, resumed chan struct{}, freeze time.Duration) (*durable.DurableProvider, frontend.Backend, *appliedDetachWithoutReply) {
	t.Helper()
	ctx := context.Background()
	filer := chunkstore.NewMemFiler()
	cat, err := chunkstore.NewBCatalog(filer, &layoutProviderBlobs{blobs: map[string][]byte{}}, "/precopy-timeout")
	if err != nil {
		t.Fatal(err)
	}
	id, err := layout.NewVolumeID()
	if err != nil {
		t.Fatal(err)
	}
	ref := chunkstore.VolumeRef{Name: "v1", ID: id}
	if _, err := cat.CreateVolume(ctx, ref, layout.Geometry{SizeBytes: 64 * 4096, BlockSize: 4096, ChunkBytes: 4 * 4096}); err != nil {
		t.Fatal(err)
	}
	var remote *appliedDetachWithoutReply
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4,
		ChunkHolder: "s/r1", ChunkDetachPrecopy: true, DetachFreezeTimeout: freeze,
		OnChunkDetach: func(string) uint64 { return 1 },
		OnChunkDetachResumed: func(string, uint64) {
			if resumed != nil {
				close(resumed)
			}
		},
		ChunkLayoutHeadFor: func(string) (layout.StartStore, error) { return cat.Store(ref) },
		ChunkLayoutRemoteFor: func(_ string, me layout.HolderID) (storage.ChunkRemote, error) {
			inner, err := chunkstore.NewLayoutRemote(cat, ref, me)
			if err == nil {
				remote = &appliedDetachWithoutReply{LayoutRemote: inner}
			}
			return remote, err
		},
	}, newStubView(healthyProj(frontend.Identity{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	openRecovered(t, p)
	b, err := p.Open(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write(ctx, 4096, block(0x8a)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.CutVolume(ctx, "v1", 0); err != nil {
		t.Fatal(err)
	}
	return p, b, remote
}

func TestPrecopyFreezeDeadlineSettlesAppliedDetachBeforeResume(t *testing.T) {
	ctx := context.Background()
	p, b, remote := precopyLayoutTimeoutFixture(t, nil, 300*time.Millisecond)
	remote.hold, remote.apply = true, true
	result, _, err := p.DetachVolume(ctx, "v1")
	if !remote.applied || err != nil || result.Head.State != storage.ChunkHeadDetached {
		t.Fatalf("deadline after applied commit: applied=%v result=%+v err=%v; want settled detached", remote.applied, result, err)
	}
	if !errors.Is(remote.cutCause, context.DeadlineExceeded) || time.Since(remote.appliedAt) > time.Second {
		t.Fatalf("held commit ended with %v after %s; want the freeze deadline and bounded settlement", remote.cutCause, time.Since(remote.appliedAt))
	}
	if _, err := b.Write(ctx, 8192, block(0x9a)); !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("write resumed after the commit applied: %v", err)
	}
	head, err := remote.ReadHead(ctx)
	if err != nil || head.State != layout.StateDetached {
		t.Fatalf("HEAD after settlement: %+v %v", head, err)
	}
}

// Once the filer answers again, an unknown, unapplied stop must settle and
// resume without an operator retrying the detach command.
func TestPrecopyUnknownStopAutomaticallyResumesAfterFilerRecovers(t *testing.T) {
	ctx := context.Background()
	resumed := make(chan struct{})
	p, b, remote := precopyLayoutTimeoutFixture(t, resumed, 300*time.Millisecond)
	remote.hold = true // cut reply is lost without applying the commit
	remote.unavailable.Store(true)
	if _, _, err := p.DetachVolume(ctx, "v1"); !errors.Is(err, durable.ErrDetachOutcomeUnknown) {
		t.Fatalf("filer unavailable at settlement: %v; want unknown", err)
	}
	if _, err := b.Write(ctx, 8192, block(0x9a)); !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("unknown stop resumed writes before proof: %v", err)
	}
	availableAt := time.Now()
	remote.unavailable.Store(false)
	select {
	case <-resumed:
	case <-time.After(3 * time.Second):
		t.Fatal("settlement did not resume automatically after the filer recovered")
	}
	t.Logf("filer recovery to serving: %s", time.Since(availableAt))
	if remote.abortCalls.Load() < 2 {
		t.Fatalf("settlement attempts = %d; want the failed read and an automatic retry", remote.abortCalls.Load())
	}
	if _, err := b.Write(ctx, 8192, block(0x9a)); err != nil {
		t.Fatalf("guest did not resume after proof of no detached commit: %v", err)
	}
}

func TestPrecopyUnknownAppliedStopAutomaticallyStaysDetached(t *testing.T) {
	ctx := context.Background()
	p, b, remote := precopyLayoutTimeoutFixture(t, nil, 300*time.Millisecond)
	remote.hold, remote.apply = true, true
	remote.unavailable.Store(true)
	if _, _, err := p.DetachVolume(ctx, "v1"); !errors.Is(err, durable.ErrDetachOutcomeUnknown) {
		t.Fatalf("filer unavailable at settlement: %v; want unknown", err)
	}
	remote.unavailable.Store(false)
	until := time.After(3 * time.Second)
	for remote.abortCalls.Load() < 2 {
		select {
		case <-until:
			t.Fatal("applied stop was not settled automatically")
		case <-time.After(10 * time.Millisecond):
		}
	}
	head, err := remote.ReadHead(ctx)
	if err != nil || head.State != layout.StateDetached {
		t.Fatalf("applied stop HEAD: %+v %v", head, err)
	}
	if _, err := b.Write(ctx, 8192, block(0x9a)); !errors.Is(err, frontend.ErrNotReady) {
		t.Fatalf("applied stop resumed guest writes: %v", err)
	}
}

// Measurement gate: half the slow commits have a readable, unchanged HEAD at
// the 5 s bound; half lose the settlement read until the filer answers again.
// Every unapplied stop must eventually resume, without an operator command.
func TestPrecopyFiveSecondFreezeRecoveryMeasurement(t *testing.T) {
	const attempts = 10
	var cleanResume, unknown, resolved int
	var maxRecovery, maxStopElapsed time.Duration
	for i := 0; i < attempts; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			resumed := make(chan struct{})
			p, b, remote := precopyLayoutTimeoutFixture(t, resumed, 5*time.Second)
			remote.hold = true
			makeUnknown := i%2 == 1
			remote.stallSettle.Store(makeUnknown)
			started := time.Now()
			_, _, err := p.DetachVolume(context.Background(), "v1")
			maxStopElapsed = max(maxStopElapsed, time.Since(started))
			if makeUnknown {
				unknown++
				if !errors.Is(err, durable.ErrDetachOutcomeUnknown) {
					t.Fatalf("unknown attempt: %v", err)
				}
			} else {
				cleanResume++
				if !errors.Is(err, durable.ErrDetachAborted) {
					t.Fatalf("clean resume: %v", err)
				}
			}
			availableAt := time.Now()
			remote.stallSettle.Store(false)
			select {
			case <-resumed:
				resolved++
				recovery := time.Since(availableAt)
				maxRecovery = max(maxRecovery, recovery)
				if makeUnknown {
					t.Logf("OutcomeUnknown filer-answer-to-serving=%s", recovery)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("volume remained stopped after the filer answered")
			}
			if _, err := b.Write(context.Background(), 8192, block(0x9a)); err != nil {
				t.Fatalf("volume not serving after settlement: %v", err)
			}
		})
	}
	t.Logf("PASS freeze_cap=5s N=%d clean_resume=%d outcome_unknown=%d resolved=%d max_stop_elapsed=%s max_filer_answer_to_serve=%s", attempts, cleanResume, unknown, resolved, maxStopElapsed, maxRecovery)
	if resolved != attempts || cleanResume+unknown != attempts {
		t.Fatalf("unresolved stops: N=%d clean=%d unknown=%d resolved=%d", attempts, cleanResume, unknown, resolved)
	}
}
