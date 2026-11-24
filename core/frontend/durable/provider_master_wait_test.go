package durable_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// With ChunkMasterWait set (a workspace primary), the startup authority wait
// does not run out on the fixed timeout while the host reports no bound (the
// master gives no reply); it ends once the host's bound passes, or at once
// on a permanent refusal, with the same latched outcome as a timeout.
func TestChunkAuthorityWaitFollowsTheMasterWait(t *testing.T) {
	for _, tc := range []struct {
		name  string
		final bool
	}{{"bound", false}, {"final", true}} {
		t.Run(tc.name, func(t *testing.T) {
			logs := &lockedLog{}
			captureWriteLogs(t, logs)
			var bound atomic.Int64 // unix ns; 0 = no bound
			var final atomic.Bool
			v := &arrivingAssignment{seen: make(chan struct{}), assigned: make(chan struct{})}
			p, err := durable.NewDurableProvider(durable.ProviderConfig{
				Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64,
				BlockSize: 4096, ChunkBlocks: 4, ChunkAssignmentTimeout: time.Millisecond,
				ChunkMasterWait: func() (time.Time, bool) {
					if b := bound.Load(); b != 0 {
						return time.Unix(0, b), final.Load()
					}
					return time.Time{}, final.Load()
				},
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
			outcomes := any(p).(interface{ ChunkStartupOutcome(string) (uint64, bool) })
			time.Sleep(500 * time.Millisecond) // 500x the fixed timeout
			if _, failed := outcomes.ChunkStartupOutcome("v1"); failed {
				t.Fatal("the wait ran out on the fixed timeout while the master gave no reply")
			}
			if tc.final {
				final.Store(true)
			} else {
				bound.Store(time.Now().Add(200 * time.Millisecond).UnixNano())
			}
			waitLog(t, logs, "assignment unavailable, not serving")
			if gen, failed := outcomes.ChunkStartupOutcome("v1"); !failed || gen != 1 {
				t.Fatalf("outcome generation %d failed %v; want the current recovery's", gen, failed)
			}
		})
	}
}
