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

// INV-FE-NVME-READY-STATUS-001: each schedule has one required outcome.
// Virtual time controls the readiness publication; the production 3s timer
// and public Write/Read paths are unchanged. The memory store excludes disk IO.
func TestAttachWriteBeforeReadinessBudgetSucceeds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) { attachWriteSchedule(t, 2*time.Second, false) })
}

func TestAttachWriteAfterReadinessBudgetRefusesWithoutIO(t *testing.T) {
	synctest.Test(t, func(t *testing.T) { attachWriteSchedule(t, 4*time.Second, true) })
}

func attachWriteSchedule(t *testing.T, readyAt time.Duration, refuse bool) {
	t.Helper()
	b := precopyMemoryBackend(t)
	old, next := bytes.Repeat([]byte{0x33}, 4096), bytes.Repeat([]byte{0x74}, 4096)
	if n, err := b.Write(context.Background(), 4096, old); n != len(old) || err != nil {
		t.Fatalf("seed: n=%d err=%v", n, err)
	}
	b.setChunkAttachPending("controlled attachment")
	waiting := make(chan struct{}, 1)
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
	start := time.Now()
	go func() { n, err := b.Write(context.Background(), 4096, next); done <- outcome{n, err} }()
	select {
	case <-waiting:
	case r := <-done:
		t.Fatalf("write completed without waiting for attachment: n=%d err=%v", r.n, r.err)
	}
	synctest.Wait()
	select {
	case r := <-done:
		t.Fatalf("write completed before readiness or allowance: %+v", r)
	default:
	}
	if refuse {
		time.Sleep(3 * time.Second)
		synctest.Wait()
		r := <-done
		if r.n != 0 || !errors.Is(r.err, context.DeadlineExceeded) || !errors.Is(r.err, frontend.ErrBackendBusy) || frontend.ClassifyBackendError(context.Background(), r.err) != frontend.BackendErrorBusy {
			t.Fatalf("expired attachment: n=%d err=%v", r.n, r.err)
		}
		if time.Since(start) != 3*time.Second {
			t.Fatal("readiness timer changed")
		}
		time.Sleep(readyAt - 3*time.Second)
	} else {
		time.Sleep(readyAt)
	}
	b.SetOperational(true, "controlled attach completed")
	synctest.Wait()
	want := old
	if !refuse {
		r := <-done
		if r.err != nil || r.n != len(next) {
			t.Fatalf("timely attachment must succeed: n=%d err=%v", r.n, r.err)
		}
		if time.Since(start) != readyAt {
			t.Fatal("write did not complete when attachment became ready")
		}
		want = next
	}
	got := make([]byte, 4096)
	if n, err := b.Read(context.Background(), 4096, got); err != nil || n != len(got) || !bytes.Equal(got, want) {
		t.Fatalf("readback n=%d err=%v correct=%t", n, err, bytes.Equal(got, want))
	}
	if refuse {
		if n, err := b.Write(context.Background(), 4096, next); err != nil || n != len(next) {
			t.Fatalf("ready retry n=%d err=%v", n, err)
		}
		if n, err := b.Read(context.Background(), 4096, got); err != nil || n != len(got) || !bytes.Equal(got, next) {
			t.Fatalf("retry bytes n=%d err=%v", n, err)
		}
	}
}
