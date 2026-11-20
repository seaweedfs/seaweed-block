package durable_test

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestPrecopyFreezeDoesNotWaitForAdmittedIO(t *testing.T) {
	p, view := parkedProvider(t, storage.NewMemChunkRemote())
	openRecovered(t, p)
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	gate, ok := b.(interface{ TryStopIO(string) bool })
	if !ok {
		t.Fatal("backend has no nonblocking tentative freeze")
	}
	view.arm()
	done := make(chan error, 1)
	t.Cleanup(func() {
		view.unpark()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("writer did not join")
		}
	})
	go func() { _, err := b.Write(context.Background(), 4096, block(7)); done <- err }()
	select {
	case <-view.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("write never admitted")
	}
	frozen := make(chan bool, 1)
	go func() { frozen <- gate.TryStopIO("pre-copy final cut") }()
	select {
	case stopped := <-frozen:
		if stopped {
			t.Fatal("stopped before admitted write drained")
		}
	case <-time.After(time.Second):
		t.Fatal("freeze blocked on admitted I/O")
	}
	if _, err := b.Write(context.Background(), 8192, block(8)); err != nil {
		t.Fatal("rejected tentative freeze stopped service:", err)
	}
	view.unpark()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		done <- nil
	case <-time.After(5 * time.Second):
		t.Fatal("writer stuck")
	}
	if !gate.TryStopIO("pre-copy final cut") {
		t.Fatal("idle frontend did not freeze")
	}
	if _, err := b.Write(context.Background(), 8192, block(9)); err == nil {
		t.Fatal("write served after successful freeze")
	}
}
