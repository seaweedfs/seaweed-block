package durable_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
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
			cfg := durable.ProviderConfig{
				Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64,
				BlockSize: 4096, ChunkBlocks: 4, ChunkAssignmentTimeout: time.Millisecond,
				ChunkMasterWait: func(time.Time) (time.Time, bool) {
					if b := bound.Load(); b != 0 {
						return time.Unix(0, b), final.Load()
					}
					return time.Time{}, final.Load()
				},
			}
			testutil.NewLayoutFixture(t, "v1", 64, 4).Configure(&cfg)
			start := cfg.ChunkLayoutHeadFor
			cfg.ChunkLayoutHeadFor = func(name string) (layout.StartStore, error) {
				t.Error("layout reached without authority")
				return start(name)
			}
			p, err := durable.NewDurableProvider(cfg, v)
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

// The provider hands the host the wait's own start, taken after the local
// open and recovery, so the host's window (the first reply at or after it)
// never counts the local work (the window rule itself:
// TestChunkAuthorityWaitAfterASlowOpenGetsItsFullLimit in core/host/volume).
func TestChunkAuthorityWaitPassesItsOwnStart(t *testing.T) {
	v := &arrivingAssignment{seen: make(chan struct{}), assigned: make(chan struct{})}
	var starts atomic.Int64
	cfg := durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64,
		BlockSize: 4096, ChunkBlocks: 4,
		ChunkMasterWait: func(start time.Time) (time.Time, bool) {
			starts.Store(start.UnixNano())
			return time.Time{}, false
		},
	}
	testutil.NewLayoutFixture(t, "v1", 64, 4).Configure(&cfg)
	p, err := durable.NewDurableProvider(cfg, v)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if _, err := p.EnsureStorage("v1"); err != nil {
		t.Fatal(err)
	}
	v.active.Store(true)
	before := time.Now()
	if _, err := p.RecoverVolume(context.Background(), "v1"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for starts.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the wait never asked the host")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if s := time.Unix(0, starts.Load()); s.Before(before) {
		t.Fatalf("start %v is before the recovery began (%v); want the wait's own start", s, before)
	}
}

// A granted attach that keeps failing (the chunk store unreachable) stays
// inside the bound: the wait ends at it instead of retrying for ever while
// the lease is held and nothing serves (D1).
func TestChunkGrantedAttachThatKeepsFailingIsBounded(t *testing.T) {
	logs := &lockedLog{}
	captureWriteLogs(t, logs)
	v := &arrivingAssignment{seen: make(chan struct{}), assigned: make(chan struct{})}
	var bound atomic.Int64
	cfg := durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64,
		BlockSize: 4096, ChunkBlocks: 4,
		ChunkMasterWait: func(time.Time) (time.Time, bool) {
			if b := bound.Load(); b != 0 {
				return time.Unix(0, b), false
			}
			return time.Time{}, false
		},
	}
	testutil.NewLayoutFixture(t, "v1", 64, 4).Configure(&cfg)
	cfg.ChunkLayoutHeadFor = func(string) (layout.StartStore, error) { return nil, errors.New("chunk store unreachable") }
	p, err := durable.NewDurableProvider(cfg, v)
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
	<-v.seen
	bound.Store(time.Now().Add(300 * time.Millisecond).UnixNano())
	close(v.assigned) // granted: every attach attempt fails
	waitLog(t, logs, "assignment unavailable, not serving")
	outcomes := any(p).(interface{ ChunkStartupOutcome(string) (uint64, bool) })
	if _, failed := outcomes.ChunkStartupOutcome("v1"); !failed {
		t.Fatal("the bounded wait ended without a startup outcome")
	}
}

// A grant that arrives on the same poll as the end of the wait attaches: the
// wait checks the projection's authority before it asks whether to give up.
func TestChunkAuthorityWaitAttachesAGrantOnTheBoundsTick(t *testing.T) {
	v := &arrivingAssignment{seen: make(chan struct{}), assigned: make(chan struct{})}
	var ended atomic.Bool // the master wait reports its end together with the grant
	cfg := durable.ProviderConfig{
		Impl: durable.ImplWALStoreChunk, StorageRoot: t.TempDir(), NumBlocks: 64,
		BlockSize: 4096, ChunkBlocks: 4, ChunkAssignmentTimeout: time.Millisecond,
		ChunkMasterWait: func(time.Time) (time.Time, bool) {
			return time.Time{}, ended.Load()
		},
	}
	testutil.NewLayoutFixture(t, "v1", 64, 4).Configure(&cfg)
	p, err := durable.NewDurableProvider(cfg, v)
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
	<-v.seen
	ended.Store(true)
	close(v.assigned) // the grant and the end of the wait, visible on the same poll
	outcomes := any(p).(interface{ ChunkStartupOutcome(string) (uint64, bool) })
	deadline := time.Now().Add(10 * time.Second)
	for {
		if st := p.DurableStatuses(); len(st) == 1 && st[0].Operational {
			break
		}
		if _, failed := outcomes.ChunkStartupOutcome("v1"); failed {
			t.Fatal("the wait gave up although the grant was there on the same poll")
		}
		if time.Now().After(deadline) {
			t.Fatal("never attached")
		}
		time.Sleep(time.Millisecond)
	}
}
