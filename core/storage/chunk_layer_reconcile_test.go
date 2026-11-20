package storage

// F1(b): a layer whose dirty set is unknown (bitmap lost or mismatched, or a
// BASE install just finished) reconciles it against HEAD in the background,
// outside the capture barrier. No HEAD moves until it is done.

import (
	"testing"
	"time"
)

type reconcileFixture struct {
	s     *WALStore
	f     *fakeChunkRemote
	bm    *fakeBitmapStore
	cfg   chunkLayerConfig
	layer chunkLayer
	hold  chan struct{}
	path  string
}

func (fx *reconcileFixture) release() { close(fx.hold) }

// refusedOver asserts check() holds at every poll for d: a negative
// invariant is checked over an interval, not once.
func refusedOver(t *testing.T, d time.Duration, what string, check func() error) {
	t.Helper()
	end := time.Now().Add(d)
	for time.Now().Before(end) {
		if err := check(); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
