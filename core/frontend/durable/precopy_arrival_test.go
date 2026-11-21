package durable

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// INV-CUT-PRECOPY-001: an active pause may predate this command. Its queue
// allowance is measured once from this command's arrival, capped by its caller.
func TestPrecopyQueueBoundStartsAtCommandArrival(t *testing.T) {
	for _, caller := range []time.Duration{2 * time.Second, 10 * time.Second} {
		t.Run(caller.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := precopyMemoryBackend(t)
				resume, err := b.BeginPrecopyPause(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				defer resume()
				// Let the active pause get older than five seconds before this arrival.
				time.Sleep(6 * time.Second)
				ctx, cancel := context.WithTimeout(context.Background(), caller)
				defer cancel()
				queued, done := make(chan struct{}), make(chan error, 1)
				b.testPrecopyQueued = func() { close(queued) }
				observed := make(chan struct{}, 1)
				if err := b.SetWriteObserver(precopyQueueObserver{sync: func(context.Context) error { observed <- struct{}{}; return nil }}); err != nil {
					t.Fatal(err)
				}
				joined := false
				t.Cleanup(func() {
					cancel()
					resume()
					if !joined {
						select {
						case <-done:
						case <-time.After(time.Second):
							t.Error("arrival-bounded waiter did not join")
						}
					}
				})
				go func() { done <- b.Sync(ctx) }()
				<-queued
				synctest.Wait()
				bound := 5 * time.Second
				if caller < bound {
					bound = caller
				}
				time.Sleep(bound - time.Nanosecond)
				synctest.Wait()
				select {
				case err := <-done:
					joined = true
					t.Fatalf("fresh waiter inherited old pause deadline: %v", err)
				default:
				}
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				select {
				case err := <-done:
					joined = true
					if !errors.Is(err, frontend.ErrBackendBusy) || !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("expiry classification: %v", err)
					}
				default:
					t.Fatal("arrival/caller deadline did not bound the waiter")
				}
				select {
				case <-observed:
					t.Fatal("expired paused waiter reached physical Sync")
				default:
				}
			})
		})
	}
}
