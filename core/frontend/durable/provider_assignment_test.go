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
	// No wall-clock bound (#227): every wait below is a progress condition; a
	// hang is reported by the go test timeout.
	ctx, cancel := context.WithCancel(context.Background())
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
	<-v.seen // recovery inspected the assignment
	if err := <-done; err != nil { // local recovery settles before assignment can be applied
		t.Fatalf("first assignment did not converge: %v", err)
	}
	if statuses := p.DurableStatuses(); len(statuses) != 1 || statuses[0].Operational {
		t.Fatalf("unassigned backend became operational: %+v", statuses)
	}
	finishRecovery() // deferred attachment outlives a completed recovery caller
	close(v.assigned)
	for { // progress: the deferred attachment becomes operational
		statuses := p.DurableStatuses()
		if len(statuses) == 1 && statuses[0].Operational {
			break
		}
		time.Sleep(time.Millisecond)
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
			<-v.seen // progress, no timer (#227): recovery inspected the assignment
			// Local recovery must return first, so the host can apply assignment.
			if err := <-done; err != nil {
				t.Fatal(err)
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
			for { // progress: the 40 ms assignment limit fails the wait closed
				status := p.DurableStatuses()
				if len(status) == 1 && !status[0].Operational && strings.Contains(status[0].Evidence, "deadline exceeded") {
					break
				}
				if len(status) == 1 && status[0].Operational {
					t.Fatalf("became operational without an assignment: %+v", status)
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}
