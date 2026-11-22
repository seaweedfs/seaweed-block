package durable_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A detach logs when admission closed (the backend's operational flip,
// taken under its lock) as well as when StopIO returned, both with RFC 3339
// nanoseconds, so a test measuring guest errors can tell a refusal by
// contract (after the flip) from an admitted I/O that failed.
func TestLiveDetachLogsWhenAdmissionClosed(t *testing.T) {
	p := chunkProvider(t, t.TempDir(), storage.NewMemChunkRemote(), v1View(), time.Hour)
	openRecovered(t, p)
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write(context.Background(), 4096, block(0x51)); err != nil {
		t.Fatal(err)
	}
	logs := &lockedLog{}
	captureWriteLogs(t, logs)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	started := time.Now()
	if _, _, err := p.DetachVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	ended := time.Now()

	stamp := func(marker string) time.Time {
		t.Helper()
		var found []time.Time
		for _, line := range strings.Split(logs.String(), "\n") {
			if _, s, ok := strings.Cut(line, marker); ok {
				at, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(s))
				if err != nil {
					t.Fatalf("%q: %v", line, err)
				}
				found = append(found, at)
			}
		}
		if len(found) != 1 {
			t.Fatalf("%d lines with %q, want 1:\n%s", len(found), marker, logs.String())
		}
		return found[0]
	}
	closed := stamp("volume=v1 admission closed at ")
	returned := stamp("volume=v1 StopIO returned at ")
	if closed.Before(started) || returned.Before(closed) || ended.Before(returned) {
		t.Fatalf("closed %s, returned %s: want started %s <= closed <= returned <= ended %s", closed, returned, started, ended)
	}
	if got := durable.StopFlipAt(b); !got.Equal(closed) {
		t.Fatalf("logged %s, backend flip %s: the log must carry the flip itself", closed, got)
	}
}

// The flip time is the admission cutoff: a write whose gate passed before
// the flip is admitted and completes (StopIO drains it), and a write whose
// gate runs after the flip is refused, so nothing is admitted after the
// logged boundary.
func TestStopFlipIsTheAdmissionCutoff(t *testing.T) {
	p := chunkProvider(t, t.TempDir(), storage.NewMemChunkRemote(), v1View(), time.Hour)
	openRecovered(t, p)
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	parkedA, releaseA := make(chan struct{}), make(chan struct{})
	parkedB, releaseB := make(chan struct{}), make(chan struct{})
	var onceA, onceB sync.Once
	freeA := func() { onceA.Do(func() { close(releaseA) }) }
	freeB := func() { onceB.Do(func() { close(releaseB) }) }
	errA, errB, stopErr := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	var launched struct{ a, b, stop atomic.Bool }
	// Registered before anything starts: every parked writer is released and
	// every goroutine is joined within a bound, whatever the test hits.
	t.Cleanup(func() {
		freeA()
		freeB()
		cancel()
		for name, c := range map[string]struct {
			on *atomic.Bool
			ch chan error
		}{"write A": {&launched.a, errA}, "write B": {&launched.b, errB}, "StopIO": {&launched.stop, stopErr}} {
			if !c.on.Load() {
				continue
			}
			select {
			case <-c.ch:
			case <-time.After(5 * time.Second):
				t.Errorf("%s did not return", name)
			}
		}
	})

	// Write A passes the gate, then parks (it registers after the flip and
	// still completes: admission is the gate's decision).
	var parkA, parkB atomic.Bool
	parkA.Store(true)
	durable.SetTestAfterGate(b, func() {
		if parkA.CompareAndSwap(true, false) {
			close(parkedA)
			<-releaseA
		}
	})
	launched.a.Store(true)
	go func() { _, err := b.Write(context.Background(), 0, block(0x10)); errA <- err }()
	select {
	case <-parkedA:
	case <-time.After(5 * time.Second):
		t.Fatal("write A never passed the gate")
	}
	// Write B parks before the gate.
	parkB.Store(true)
	durable.SetTestBeforeIOLock(b, func() {
		if parkB.CompareAndSwap(true, false) {
			close(parkedB)
			<-releaseB
		}
	})
	launched.b.Store(true)
	go func() { _, err := b.Write(context.Background(), 4096, block(0x20)); errB <- err }()
	select {
	case <-parkedB:
	case <-time.After(5 * time.Second):
		t.Fatal("write B never reached the gate")
	}

	launched.stop.Store(true)
	go func() { stopErr <- durable.StopIOForTest(ctx, b, "test stop") }()
	deadline := time.Now().Add(5 * time.Second)
	for durable.StopFlipAt(b).IsZero() {
		if time.Now().After(deadline) {
			t.Fatal("the stop never flipped")
		}
		time.Sleep(time.Millisecond)
	}
	// B now reaches its gate after the flip (it may first wait behind the
	// drain for ioMu); A is admitted and still in flight, so StopIO waits.
	freeB()
	select {
	case err := <-stopErr:
		stopErr <- err
		t.Fatalf("StopIO returned (%v) while admitted write A was still in flight", err)
	case <-time.After(100 * time.Millisecond):
	}
	freeA()
	select {
	case err := <-errA:
		errA <- err
		if err != nil {
			t.Fatalf("write A (gate before the flip): %v, want admitted and completed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write A never returned")
	}
	select {
	case err := <-stopErr:
		stopErr <- err
		if err != nil {
			t.Fatalf("StopIO: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("StopIO never returned after A completed")
	}
	select {
	case err := <-errB:
		errB <- err
		if !errors.Is(err, frontend.ErrNotReady) {
			t.Fatalf("write B (gate after the flip): %v, want refused (ErrNotReady)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write B never returned")
	}
}
