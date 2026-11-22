package durable_test

import (
	"context"
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
