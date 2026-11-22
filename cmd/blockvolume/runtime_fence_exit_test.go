package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

type runtimeFenceStub struct {
	fenced  atomic.Bool
	updates chan struct{}
}

func (s *runtimeFenceStub) ChunkRuntimeFenced(string) bool            { return s.fenced.Load() }
func (s *runtimeFenceStub) ChunkRuntimeFenceUpdates() <-chan struct{} { return s.updates }

// INV-FE-PRIMARY-READY-OPERATIONAL-001 (condition A): a runtime HEAD fence of
// a workspace chunk primary chooses the no-restart exit 78 head_fenced through
// the process owner's one chooser; nothing is chosen while not fenced, and a
// non-workspace volume never watches.
func TestRuntimeHeadFenceChoosesHeadFencedExit(t *testing.T) {
	f := flags{workspaceLease: true, durableImpl: "walstore-chunk", volumeID: "v1"}
	e := newProcessExit(context.Background())
	stub := &runtimeFenceStub{updates: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { watchChunkRuntimeFence(ctx, e, f, stub); close(done) }()
	select {
	case out := <-e.decision:
		t.Fatalf("an exit was chosen while not fenced: %+v", out)
	case <-time.After(100 * time.Millisecond):
	}
	stub.fenced.Store(true)
	stub.updates <- struct{}{}
	select {
	case out := <-e.decision:
		if out != (workspaceExit{exitWorkspaceSuperseded, "head_fenced"}) {
			t.Fatalf("exit %+v, want 78 head_fenced", out)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no exit chosen after the runtime fence")
	}
	<-done

	plain := newProcessExit(context.Background())
	watchChunkRuntimeFence(ctx, plain, flags{volumeID: "v1"}, stub) // returns at once: not a workspace chunk primary
	select {
	case out := <-plain.decision:
		t.Fatalf("a non-workspace volume chose %+v", out)
	default:
	}
}
