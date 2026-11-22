package durable_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// INV-WS-STARTUP-RETRY-001: a bounded authority wait must expose a latched
// outcome to its process owner, rather than merely leaving status not-ready.
func TestChunkAssignmentTimeoutExposesCurrentRecoveryOutcome(t *testing.T) {
	logs := &lockedLog{}
	captureWriteLogs(t, logs)
	v := &arrivingAssignment{seen: make(chan struct{}), assigned: make(chan struct{})}
	p, err := durable.NewDurableProvider(durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64,
		BlockSize: 4096, ChunkBlocks: 4, ChunkAssignmentTimeout: time.Millisecond,
		ChunkRemoteFor: func(string) (storage.ChunkRemote, error) {
			t.Error("remote reached without authority")
			return storage.NewMemChunkRemote(), nil
		},
	}, v)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	v.active.Store(true)
	if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logs, "assignment unavailable, not serving")
	outcomes, ok := any(p).(interface{ ChunkStartupOutcome(string) (uint64, bool) })
	if !ok {
		t.Fatal("assignment wait ended without a process-owner outcome; process stays up forever")
	}
	gen, failed := outcomes.ChunkStartupOutcome("v1")
	if !failed || gen != 1 {
		t.Fatalf("timeout outcome = generation %d, failed %v; want current generation 1", gen, failed)
	}
	if again, ok := outcomes.ChunkStartupOutcome("v1"); !ok || again != gen {
		t.Fatal("outcome was consumed rather than latched")
	}
	if _, ok := outcomes.ChunkStartupOutcome("other"); ok {
		t.Fatal("outcome escaped its volume")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := outcomes.ChunkStartupOutcome("v1"); ok {
		t.Fatal("closed provider retained an actionable outcome")
	}
}

func TestChunkStartupSealedAssignmentStaysForReplay(t *testing.T) {
	logs := &lockedLog{}
	captureWriteLogs(t, logs)
	ctx := hangGuard(t)
	remote := storage.NewMemChunkRemote()
	root := t.TempDir()
	first := chunkProvider(t, root, remote, v1View(), 0)
	openRecovered(t, first)
	sealed, _, err := first.DetachVolumePlain(ctx, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	view := v1View()
	assigned := view.Projection()
	waiting := assigned
	waiting.Epoch = 0
	waiting.Healthy = false
	view.set(waiting)
	second := chunkProvider(t, root, remote, view, 0)
	if _, err := second.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := second.RecoverVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	view.set(assigned)
	waitLog(t, logs, "sealed assignment, retained for replay")
	if _, failed := second.ChunkStartupOutcome("v1"); failed {
		t.Fatal("sealed assignment became a process retry")
	}
	replay, _, err := second.DetachVolumePlain(ctx, "v1")
	if err != nil || !reflect.DeepEqual(replay.Head, sealed.Head) {
		t.Fatalf("sealed replay changed: %+v %v", replay, err)
	}
	for _, status := range second.DurableStatuses() {
		if status.Operational {
			t.Fatal("sealed restart served")
		}
	}
}

// The attach check returns a retained disposition only AFTER its context's
// deadline. This makes the timeout-defer boundary causal, not a scheduler race.
func TestChunkStartupRetainedDispositionWinsExpiredAttachContext(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"sealed", storage.ErrChunkLayerDetached},
		{"stale local root", errors.Join(storage.ErrChunkLayerFenced, durable.ErrChunkStaleLocalRoot)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := &lockedLog{}
			captureWriteLogs(t, logs)
			view := v1View()
			assigned := view.Projection()
			waiting := assigned
			waiting.Epoch = 0
			waiting.Healthy = false
			view.set(waiting)
			var entered atomic.Bool
			p, err := durable.NewDurableProvider(durable.ProviderConfig{
				Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 4,
				ChunkAssignmentTimeout: 250 * time.Millisecond,
				ChunkAttachCheck: func(ctx context.Context, _ string, _ durable.ChunkGeometry) error {
					entered.Store(true)
					<-ctx.Done()
					return tc.err
				},
				ChunkRemoteFor: func(string) (storage.ChunkRemote, error) {
					t.Error("retained attach reached remote")
					return storage.NewMemChunkRemote(), nil
				},
			}, view)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = p.Close() })
			if _, err = p.EnsureStorage("v1"); err != nil {
				t.Fatal(err)
			}
			if _, err = p.RecoverVolume(context.Background(), "v1"); err != nil {
				t.Fatal(err)
			}
			view.set(assigned)
			waitLog(t, logs, "chunk authority wait for volume=v1 ended")
			if !entered.Load() {
				t.Fatal("fixture deadline elapsed before attach check entered; not a retained-disposition witness")
			}
			if _, failed := p.ChunkStartupOutcome("v1"); failed {
				t.Fatal("deadline defer replaced retained attach disposition with process retry")
			}
		})
	}
}
