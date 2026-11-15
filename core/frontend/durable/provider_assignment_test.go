package durable_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type arrivingAssignment struct {
	active   atomic.Bool
	seen     chan struct{}
	assigned chan struct{}
	calls    atomic.Uint64
}

func (v *arrivingAssignment) Projection() frontend.Projection {
	zero := frontend.Projection{VolumeID: "v1", ReplicaID: "r1"}
	if !v.active.Load() {
		return zero
	}
	if v.calls.Add(1) == 1 {
		close(v.seen)
		return zero
	}
	select {
	case <-v.assigned:
		return frontend.Projection{VolumeID: "v1", ReplicaID: "r1", Epoch: 3, EndpointVersion: 1, Healthy: true}
	default:
		return zero
	}
}

func TestRestoreWaitsForFirstAssignment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	v := &arrivingAssignment{seen: make(chan struct{}), assigned: make(chan struct{})}
	p := chunkProviderForAssignment(t, v)
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	v.active.Store(true)
	done := make(chan error, 1)
	go func() { _, err := p.RecoverVolume(ctx, "v1"); done <- err }()
	select {
	case <-v.seen:
	case <-ctx.Done():
		t.Fatal("recovery never inspected assignment")
	}
	close(v.assigned)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("first assignment did not converge: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("recovery did not settle")
	}
	if _, err := p.Open(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
}

func chunkProviderForAssignment(t *testing.T, v frontend.ProjectionView) *durable.DurableProvider {
	t.Helper()
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64,
		BlockSize: 4096, ChunkBlocks: 4,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) { return storage.NewMemChunkRemote(), nil },
	}, v)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestRestoreAssignmentWaitEndsOnDeadlineCancelAndClose(t *testing.T) {
	for _, mode := range []string{"limit", "cancel", "close"} {
		t.Run(mode, func(t *testing.T) {
			v := &arrivingAssignment{seen: make(chan struct{}), assigned: make(chan struct{})}
			p, err := durable.NewDurableProvider(durable.ProviderConfig{
				Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4,
				ChunkAssignmentTimeout: 40 * time.Millisecond,
				ChunkRemoteFor: func(string) (storage.ChunkRemote, error) {
					t.Error("remote reached without assignment")
					return storage.NewMemChunkRemote(), nil
				},
			}, v)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			if _, err := p.EnsureStorage("v1"); err != nil {
				t.Fatal(err)
			}
			v.active.Store(true)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, err := p.RecoverVolume(ctx, "v1"); done <- err }()
			select {
			case <-v.seen:
			case <-time.After(2 * time.Second):
				t.Fatal("no assignment inspection")
			}
			if mode == "cancel" {
				cancel()
			}
			if mode == "close" {
				if err := p.Close(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("missing assignment admitted")
				}
				if mode == "limit" && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
				if mode == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("assignment wait did not settle")
			}
		})
	}
}
