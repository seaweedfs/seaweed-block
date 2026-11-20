package durable_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestPrecopyAdmissionPauseBoundsDrainAndReopens(t *testing.T) {
	p, view := parkedProvider(t, storage.NewMemChunkRemote())
	openRecovered(t, p)
	backend, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	pause, ok := backend.(interface {
		BeginPrecopyPause(context.Context) (func(), error)
	})
	if !ok {
		t.Fatal("backend lacks bounded pre-copy admission drain")
	}
	view.arm()
	written := make(chan error, 1)
	t.Cleanup(func() {
		view.unpark()
		select {
		case <-written:
		case <-time.After(3 * time.Second):
			t.Error("writer not joined")
		}
	})
	go func() { _, err := backend.Write(context.Background(), 4096, block(7)); written <- err }()
	select {
	case <-view.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("writer never entered")
	}
	// The late gate caller is blocked before it can affect storage. Pausing must
	// account for it safely without waiting under the storage capture lock.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	release, err := pause.BeginPrecopyPause(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if release == nil {
		t.Fatal("pause has no release")
	}
	t.Cleanup(release)
	blocked, cancelWrite := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelWrite()
	if _, err := backend.Write(blocked, 8192, block(8)); !errors.Is(err, frontend.ErrBackendBusy) {
		t.Fatalf("new I/O bypassed pause: %v", err)
	}
	release()
	view.unpark()
	select {
	case err := <-written:
		if err != nil {
			t.Fatal(err)
		}
		written <- nil
	case <-time.After(3 * time.Second):
		t.Fatal("resumed writer blocked")
	}
	if _, err := backend.Write(context.Background(), 8192, block(8)); err != nil {
		t.Fatal(err)
	}
}
