package durable

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// TestAttachReadinessWaitFailsFastOnTerminalState pins that an I/O waiting for
// chunk attachment returns at once, without another readiness wait, when an
// observer install failed, readiness was withdrawn or a fence is seen.
func TestAttachReadinessWaitFailsFastOnTerminalState(t *testing.T) {
	for _, tc := range []struct {
		name string
		// before runs before the read; afterRefusal runs between the first gate
		// refusal and the wait; whileWaiting runs once the read is waiting.
		before       func(t *testing.T, b *StorageBackend)
		afterRefusal func(b *StorageBackend)
		whileWaiting func(b *StorageBackend)
		want         error
		wantWaits    int32
	}{
		{
			name: "observer install failed",
			before: func(t *testing.T, b *StorageBackend) {
				if err := b.SetWriteObserver(refusingRebaser{}); err == nil {
					t.Fatal("refused observer install reported success")
				}
			},
			want: frontend.ErrNotReady,
		},
		{
			name: "fence seen after the attach refusal",
			afterRefusal: func(b *StorageBackend) {
				b.mu.Lock()
				b.chunkFenced = func() bool { return true }
				b.mu.Unlock()
			},
			want: frontend.ErrStalePrimary,
		},
		{
			name:         "readiness withdrawn while waiting",
			whileWaiting: func(b *StorageBackend) { b.SetOperational(false, "host drain") },
			want:         frontend.ErrNotReady,
			wantWaits:    1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := precopyTestBackend(newHeldReadStore())
			b.setChunkAttachPending("attachment pending")
			if tc.before != nil {
				tc.before(t, b)
			}
			if tc.afterRefusal != nil {
				b.testAfterAttachRefusal = func() { tc.afterRefusal(b) }
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var waits atomic.Int32
			waiting, resume := make(chan struct{}), make(chan struct{})
			b.testBeforeReadReadyWait = func() {
				if waits.Add(1) == 1 && tc.whileWaiting != nil {
					close(waiting)
					<-resume
					return
				}
				// A wait the rule forbids: end it now instead of after 3 s.
				cancel()
			}
			result := make(chan error, 1)
			go func() {
				_, err := b.Read(ctx, 0, make([]byte, 4096))
				result <- err
			}()
			if tc.whileWaiting != nil {
				select {
				case <-waiting:
				case <-time.After(3 * time.Second):
					t.Fatal("read never waited for attachment readiness")
				}
				tc.whileWaiting(b)
				close(resume)
			}
			var err error
			select {
			case err = <-result:
			case <-time.After(5 * time.Second):
				t.Fatal("read did not return")
			}
			if got := waits.Load(); got != tc.wantWaits {
				t.Fatalf("read waited %d times for readiness, want %d (err=%v)", got, tc.wantWaits, err)
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("err=%v, want %v", err, tc.want)
			}
		})
	}
}
