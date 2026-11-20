package durable

import (
	"testing"
	"time"
)

func TestPrecopyFreezeDoesNotWaitForBackendState(t *testing.T) {
	b := &StorageBackend{}
	b.operational.Store(true)
	b.mu.Lock()
	released := false
	done := make(chan struct{})
	var stopped bool
	t.Cleanup(func() {
		if !released {
			b.mu.Unlock()
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("freeze owner did not join")
		}
	})
	go func() { defer close(done); stopped = b.TryStopIO("pre-copy final cut") }()
	select {
	case <-done:
		if stopped {
			t.Fatal("freeze ignored held backend state")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("freeze waited for backend state under capture barrier")
	}
	b.mu.Unlock()
	released = true
	if !b.operational.Load() {
		t.Fatal("failed freeze changed admission")
	}
	if !b.TryStopIO("pre-copy final cut") || b.operational.Load() {
		t.Fatal("idle backend failed to freeze")
	}
}
