package durable

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

func TestReadReadinessWaitDoesNotHoldAdmissionAndCloseWakesIt(t *testing.T) {
	for _, drain := range []bool{false, true} {
		name := "close"
		if drain {
			name = "drain-then-close"
		}
		t.Run(name, func(t *testing.T) {
			// No storage is needed: this read must never pass the operational gate.
			b := &StorageBackend{waitReadReady: true}
			b.opEvidence.Store("attachment pending")
			registered := make(chan struct{}, 1)
			resume := make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(resume) }) }
			b.testBeforeReadReadyWait = func() {
				select {
				case registered <- struct{}{}:
				default:
				}
				<-resume
			}
			done := make(chan error, 1)
			joined := make(chan struct{})
			t.Cleanup(func() {
				release()
				_ = b.Close()
				select {
				case <-joined:
				case <-time.After(5 * time.Second):
					t.Error("pending read did not settle after release and Close")
				}
			})
			go func() {
				defer close(joined)
				_, err := b.Read(context.Background(), 0, make([]byte, 4096))
				done <- err
			}()
			select {
			case <-registered:
			case <-time.After(time.Second):
				t.Fatal("read did not register a readiness notification")
			}
			if drain {
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
				defer cancel()
				if err := b.StopIO(ctx, "attachment still pending"); err != nil {
					t.Fatalf("waiting read held I/O admission: %v", err)
				}
			}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			release()
			select {
			case err := <-done:
				if !errors.Is(err, frontend.ErrBackendClosed) {
					t.Fatalf("Close wake error=%v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Close did not wake registered read")
			}
		})
	}
}
