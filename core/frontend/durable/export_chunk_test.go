package durable

import (
	"context"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// SetTestAfterChunkResolve installs the test-only hook CutVolume runs right
// after it resolves the volume's handle and layer.
func SetTestAfterChunkResolve(p *DurableProvider, f func()) { p.testAfterChunkResolve = f }

// SetTestDetachStopIO replaces DetachVolume's StopIO call (test-only).
func SetTestDetachStopIO(p *DurableProvider, f func(ctx context.Context) error) {
	p.testDetachStopIO = f
}

// SetTestDetachTrace observes DetachVolume's stages and deadlines (test-only).
func SetTestDetachTrace(p *DurableProvider, f func(stage string, deadline time.Time)) {
	p.testDetachTrace = f
}

// SetTestBeforeIOLock runs f in the backend's admission, right before the I/O
// lock (test-only).
func SetTestBeforeIOLock(b frontend.Backend, f func()) { b.(*StorageBackend).testBeforeIOLock = f }

// SetTestBeforeAttachWait observes the first registered readiness wait.
func SetTestBeforeAttachWait(b frontend.Backend, f func()) {
	b.(*StorageBackend).testBeforeReadReadyWait = f
}
