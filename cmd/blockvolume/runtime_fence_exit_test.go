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

// runtimeFenceWait bounds every receive and join in this test.
const runtimeFenceWait = 5 * time.Second

// startRuntimeFenceWatch runs watchChunkRuntimeFence with a context it owns.
// Its cleanup, registered before the worker starts, cancels that context and
// joins the worker within runtimeFenceWait, so an earlier failed check never
// strands it. The returned channel closes when the worker returned.
func startRuntimeFenceWatch(t *testing.T, e *processExit, f flags, p chunkRuntimeFenceProvider) <-chan struct{} {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(runtimeFenceWait):
			t.Errorf("the runtime-fence watcher did not return within %s of its cancel", runtimeFenceWait)
		}
	})
	go func() {
		defer close(done)
		watchChunkRuntimeFence(ctx, e, f, p)
	}()
	return done
}

// INV-FE-FENCE-EXIT-001: a runtime HEAD fence of a workspace chunk primary
// chooses the no-restart exit 78 head_fenced through the process owner's one
// chooser, and the watcher returns; nothing is chosen while not fenced, and a
// non-workspace volume never watches.
func TestRuntimeHeadFenceChoosesHeadFencedExit(t *testing.T) {
	f := flags{workspaceLease: true, durableImpl: "walstore-chunk", volumeID: "v1"}
	e := newProcessExit(context.Background())
	stub := &runtimeFenceStub{updates: make(chan struct{}, 1)}
	done := startRuntimeFenceWatch(t, e, f, stub)
	select {
	case out := <-e.decision:
		t.Fatalf("an exit was chosen while not fenced: %+v", out)
	case <-time.After(100 * time.Millisecond):
	}
	stub.fenced.Store(true)
	stub.updates <- struct{}{}
	select {
	case out := <-e.decision:
		if out != (workspaceExit{code: exitWorkspaceSuperseded, reason: "head_fenced"}) {
			t.Fatalf("exit %+v, want 78 head_fenced", out)
		}
	case <-time.After(runtimeFenceWait):
		t.Fatal("no exit chosen after the runtime fence")
	}
	select {
	case <-done:
	case <-time.After(runtimeFenceWait):
		t.Fatal("the watcher chose the exit but did not return")
	}

	plain := newProcessExit(context.Background())
	plainDone := startRuntimeFenceWatch(t, plain, flags{volumeID: "v1"}, stub)
	select {
	case <-plainDone: // not a workspace chunk primary: returns at once
	case <-time.After(runtimeFenceWait):
		t.Fatal("a non-workspace volume kept watching")
	}
	select {
	case out := <-plain.decision:
		t.Fatalf("a non-workspace volume chose %+v", out)
	default:
	}
}
