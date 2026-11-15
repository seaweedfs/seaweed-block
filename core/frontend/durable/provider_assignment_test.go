package durable_test

import (
	"context"
	"strings"
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
	recoverCtx, finishRecovery := context.WithCancel(ctx)
	defer finishRecovery()
	done := make(chan error, 1)
	go func() { _, err := p.RecoverVolume(recoverCtx, "v1"); done <- err }()
	select {
	case <-v.seen:
	case <-ctx.Done():
		t.Fatal("recovery never inspected assignment")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("first assignment did not converge: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("local recovery must settle before assignment can be applied")
	}
	if statuses := p.DurableStatuses(); len(statuses) != 1 || statuses[0].Operational {
		t.Fatalf("unassigned backend became operational: %+v", statuses)
	}
	finishRecovery() // deferred attachment outlives a completed recovery caller
	close(v.assigned)
	for {
		statuses := p.DurableStatuses()
		if len(statuses) == 1 && statuses[0].Operational {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("assignment never became operational: %+v", statuses)
		case <-time.After(time.Millisecond):
		}
	}
	if _, err := p.CaptureVolume(ctx, "v1"); err != nil {
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

func TestRestoreDeferredAssignmentWaitEndsOnDeadlineAndClose(t *testing.T) {
	for _, mode := range []string{"limit", "close"} {
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
			// Local recovery must return first, so the host can apply assignment.
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("local recovery blocked on assignment")
			}
			cancel() // recovery caller no longer owns the deferred worker
			if mode == "close" {
				if err := p.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "close" {
				return
			}
			deadline := time.NewTimer(2 * time.Second)
			defer deadline.Stop()
			for {
				status := p.DurableStatuses()
				if len(status) == 1 && !status[0].Operational && strings.Contains(status[0].Evidence, "deadline exceeded") {
					break
				}
				select {
				case <-deadline.C:
					t.Fatalf("assignment wait did not fail closed: %+v", status)
				case <-time.After(time.Millisecond):
				}
			}
		})
	}
}
