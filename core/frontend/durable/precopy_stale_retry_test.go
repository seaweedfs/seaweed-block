package durable

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"testing"
	"time"
)

type precopyProjectionFunc func() frontend.Projection

func (f precopyProjectionFunc) Projection() frontend.Projection { return f() }

// Projection is the real gate seam: pause B begins before admittedIO increments,
// and that same observation returns stale ownership. B cannot erase this error.
func TestPrecopyNewPauseCannotEraseObservedStalePrimary(t *testing.T) {
	b := precopyMemoryBackend(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	resumeB := func() {}
	defer func() { resumeB() }()
	calls := 0
	var startErr error
	b.view = precopyProjectionFunc(func() frontend.Projection {
		calls++
		resume, err := b.BeginPrecopyPause(ctx)
		if err != nil {
			startErr = err
			return frontend.Projection{}
		}
		resumeB = resume
		return frontend.Projection{VolumeID: b.id.VolumeID, ReplicaID: b.id.ReplicaID, Epoch: b.id.Epoch + 1, EndpointVersion: b.id.EndpointVersion, Healthy: true}
	})
	observed := false
	if err := b.SetWriteObserver(precopyQueueObserver{sync: func(context.Context) error { observed = true; return nil }}); err != nil {
		t.Fatal(err)
	}
	err := b.Sync(ctx)
	if startErr != nil {
		t.Fatal("start B from projection:", startErr)
	}
	if !errors.Is(err, frontend.ErrStalePrimary) || errors.Is(err, frontend.ErrBackendBusy) {
		t.Fatalf("observed stale ownership replaced by pause retry: %v", err)
	}
	b.mu.Lock()
	admitted := b.admittedIO
	b.mu.Unlock()
	if calls != 1 || observed || admitted != 0 {
		t.Fatalf("stale observation retried or admitted I/O: calls=%d observed=%v admitted=%d", calls, observed, admitted)
	}
}
