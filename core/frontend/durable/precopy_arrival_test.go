package durable

import (
	"context"
	"errors"
	"sync"
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

// The queue grant and its deadline have one decision boundary even when a
// descheduled waiter cannot run until after its queue timer would have expired.
func TestPrecopyResumeGrantVersusExpiry(t *testing.T) {
	for _, outcome := range []string{"grant-before-queue-expiry", "expiry-before-grant", "caller-expiry", "close-after-grant"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := precopyMemoryBackend(t)
				caller := 10 * time.Second
				if outcome == "caller-expiry" {
					caller = 4 * time.Second
				}
				ctx, cancel := context.WithTimeout(context.Background(), caller)
				resume, err := b.BeginPrecopyPause(ctx)
				if err != nil {
					t.Fatal(err)
				}
				queued, proceed := make(chan struct{}), make(chan struct{})
				var once sync.Once
				release := func() { once.Do(func() { close(proceed) }) }
				b.testPrecopyQueued = func() { close(queued); <-proceed }
				observed := make(chan struct{}, 1)
				if err := b.SetWriteObserver(precopyQueueObserver{sync: func(context.Context) error { observed <- struct{}{}; return nil }}); err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				joined := false
				t.Cleanup(func() {
					release()
					cancel()
					resume()
					if !joined {
						select {
						case <-done:
						case <-time.After(time.Second):
							t.Error("boundary waiter did not join")
						}
					}
				})
				go func() { done <- b.Sync(ctx) }()
				<-queued
				synctest.Wait()
				if outcome == "expiry-before-grant" {
					time.Sleep(6 * time.Second)
				}
				resume()
				if outcome != "expiry-before-grant" {
					time.Sleep(6 * time.Second)
				}
				if outcome == "close-after-grant" {
					_ = b.Close()
				}
				release()
				synctest.Wait()
				var result error
				select {
				case result = <-done:
					joined = true
				case <-time.After(time.Second):
					t.Fatal("grant boundary did not settle")
				}
				switch outcome {
				case "grant-before-queue-expiry":
					if result != nil {
						t.Fatalf("timely grant lost to later queue expiry: %v", result)
					}
				case "close-after-grant":
					if !errors.Is(result, frontend.ErrBackendClosed) {
						t.Fatalf("Close did not retain terminal precedence: %v", result)
					}
				default:
					if !errors.Is(result, frontend.ErrBackendBusy) || !errors.Is(result, context.DeadlineExceeded) {
						t.Fatalf("expiry not refused as typed busy: %v", result)
					}
				}
				select {
				case <-observed:
					if result != nil {
						t.Fatal("refused waiter performed Sync")
					}
				default:
					if result == nil {
						t.Fatal("successful grant did not perform Sync")
					}
				}
			})
		})
	}
}
