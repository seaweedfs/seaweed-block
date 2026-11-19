package durable

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

func TestReadReadinessWaitDoesNotHoldAdmissionAndCloseWakesIt(t *testing.T) {
	// No storage is needed: this read must never pass the operational gate.
	b := &StorageBackend{waitReadReady: true}
	b.opEvidence.Store("attachment pending")
	done := make(chan error, 1)
	joined := make(chan struct{})
	t.Cleanup(func() {
		_ = b.Close()
		select {
		case <-joined:
		case <-time.After(time.Second):
			t.Error("pending read did not settle after Close")
		}
	})
	go func() {
		defer close(joined)
		_, err := b.Read(context.Background(), 0, make([]byte, 4096))
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		b.mu.Lock()
		registered := b.readyChanged != nil
		b.mu.Unlock()
		if registered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("read did not register a readiness notification")
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := b.StopIO(ctx, "attachment still pending"); err != nil {
		t.Fatalf("waiting read held I/O admission: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, frontend.ErrBackendClosed) {
			t.Fatalf("Close wake error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not wake registered read")
	}
}
