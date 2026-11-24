package durable

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// INV-CUT-PRECOPY-001 / INV-FE-NVME-READY-STATUS-001: a queued
// guest command rechecks attachment after the pause. Each virtual schedule
// requires exactly one outcome; the real remote/detach test is smoke coverage.
func TestPrecopyWriteReadyBeforeAllowanceSucceeds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) { precopyWriteReadySchedule(t, false) })
}

func TestPrecopyWriteReadyAfterAllowanceKeepsOldBytes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) { precopyWriteReadySchedule(t, true) })
}

func precopyWriteReadySchedule(t *testing.T, late bool) {
	t.Helper()
	b := precopyMemoryBackend(t)
	old, next := bytes.Repeat([]byte{0x33}, 4096), bytes.Repeat([]byte{0x74}, 4096)
	if n, err := b.Write(context.Background(), 4096, old); n != len(old) || err != nil {
		t.Fatalf("seed n=%d err=%v", n, err)
	}
	resume, err := b.BeginPrecopyPause(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer resume()
	queued, waiting := make(chan struct{}, 1), make(chan struct{}, 1)
	b.testPrecopyQueued = func() { queued <- struct{}{} }
	b.testBeforeReadReadyWait = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}
	type outcome struct {
		n   int
		err error
	}
	done := make(chan outcome, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	go func() { n, err := b.Write(ctx, 4096, next); done <- outcome{n, err} }()
	<-queued
	synctest.Wait()
	select {
	case r := <-done:
		t.Fatalf("write escaped pause: %+v", r)
	default:
	}
	time.Sleep(500 * time.Millisecond)
	// The provider's readiness publication is controlled at its existing hook.
	b.setChunkAttachPending("controlled post-pause attachment")
	resume()
	select {
	case <-waiting:
	case r := <-done:
		t.Fatalf("no attach wait: %+v", r)
	}
	synctest.Wait()
	want := next
	if late {
		time.Sleep(3 * time.Second)
		synctest.Wait()
		r := <-done
		if r.n != 0 || !errors.Is(r.err, frontend.ErrBackendBusy) || !errors.Is(r.err, context.DeadlineExceeded) || frontend.ClassifyBackendError(ctx, r.err) != frontend.BackendErrorBusy {
			t.Fatalf("expired readiness n=%d err=%v", r.n, r.err)
		}
		if ctx.Err() != nil {
			t.Fatal("product readiness must expire before the caller's 4s deadline")
		}
		time.Sleep(time.Second)
		want = old
	} else {
		time.Sleep(time.Second)
	}
	b.SetOperational(true, "controlled attachment completed")
	synctest.Wait()
	if !late {
		r := <-done
		if r.n != len(next) || r.err != nil {
			t.Fatalf("timely attachment must succeed n=%d err=%v", r.n, r.err)
		}
	}
	got := make([]byte, 4096)
	if n, err := b.Read(context.Background(), 4096, got); n != len(got) || err != nil || !bytes.Equal(got, want) {
		t.Fatalf("readback n=%d err=%v correct=%t", n, err, bytes.Equal(got, want))
	}
	if late {
		if n, err := b.Write(context.Background(), 4096, next); n != len(next) || err != nil {
			t.Fatalf("ready retry n=%d err=%v", n, err)
		}
		if n, err := b.Read(context.Background(), 4096, got); n != len(got) || err != nil || !bytes.Equal(got, next) {
			t.Fatalf("retry bytes n=%d err=%v", n, err)
		}
	}
}
