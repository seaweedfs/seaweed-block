package durable

import (
	"context"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend"
	"testing"
	"testing/synctest"
	"time"
)

// The LateGate failure is this ordering: a queued command's caller budget
// expires during capture, before Close. Close cannot rewrite its completed Busy.
func TestPrecopyCallerExpiryVersusClose(t *testing.T) {
	for _, closeFirst := range []bool{false, true} {
		name := "expiry-before-close"
		if closeFirst {
			name = "close-before-expiry"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := precopyMemoryBackend(t)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				queued := make(chan struct{})
				done := make(chan error, 1)
				b.testPrecopyQueued = func() { close(queued) }
				if !b.TryStopIO("held capture") {
					t.Fatal("freeze refused")
				}
				before := b.storage.NextLSN()
				go func() { _, err := b.Write(ctx, 0, make([]byte, 4096)); done <- err }()
				joined := false
				t.Cleanup(func() {
					cancel()
					_ = b.Close()
					if !joined {
						select {
						case <-done:
						case <-time.After(time.Second):
							t.Error("queued command did not settle")
						}
					}
				})
				select {
				case <-queued:
				case <-time.After(time.Second):
					t.Fatal("queue not witnessed")
				}
				synctest.Wait()
				if closeFirst {
					if err := b.Close(); err != nil {
						t.Fatal(err)
					}
				} else {
					time.Sleep(time.Second)
				}
				synctest.Wait()
				var err error
				select {
				case err = <-done:
					joined = true
				case <-time.After(time.Second):
					t.Fatal("chosen boundary did not settle command")
				}
				if closeFirst {
					if !errors.Is(err, frontend.ErrBackendClosed) || ctx.Err() != nil {
						t.Fatalf("Close failed to win before live deadline: %v ctx=%v", err, ctx.Err())
					}
				} else {
					if !errors.Is(err, frontend.ErrBackendBusy) || !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, frontend.ErrBackendClosed) {
						t.Fatalf("expiry before Close misclassified: %v", err)
					}
					if e := b.Close(); e != nil {
						t.Fatal(e)
					}
				}
				if b.storage.NextLSN() != before {
					t.Fatal("queued command appended storage")
				}
			})
		})
	}
}
