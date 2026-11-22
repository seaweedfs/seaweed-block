package main

import (
	"context"
	"testing"
	"time"
)

type gateResult struct {
	stopped *workspaceStopWait
	start   bool
}

func runGate(serving <-chan struct{}, stopWait <-chan workspaceStopWait) <-chan gateResult {
	out := make(chan gateResult, 1)
	go func() {
		s, ok := awaitFrontendStart(serving, stopWait)
		out <- gateResult{s, ok}
	}()
	return out
}

// INV-FE-PRIMARY-READY-OPERATIONAL-001: the frontends start only once the
// backend first admits I/O. With no durable backend, and with a backend that
// already serves, they start at once. Sampled every 10 ms while the backend
// admits no I/O (a chunk volume before its storage claim), the gate never
// releases; it releases as soon as the backend's first-serving channel closes.
func TestFrontendGateWaitsForTheStorageClaim(t *testing.T) {
	for name, serving := range map[string]chan struct{}{"no durable backend": nil, "already serving": make(chan struct{})} {
		if serving != nil {
			close(serving)
		}
		var ch <-chan struct{}
		if serving != nil {
			ch = serving
		}
		select {
		case r := <-runGate(ch, make(chan workspaceStopWait)):
			if !r.start || r.stopped != nil {
				t.Fatalf("%s: %+v, want the frontends started at once", name, r)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s: the gate did not release at once", name)
		}
	}

	serving := make(chan struct{})
	res := runGate(serving, make(chan workspaceStopWait))
	for i := 0; i < 30; i++ { // 300 ms of samples while the claim is pending
		select {
		case r := <-res:
			t.Fatalf("sample %d: the gate released (%+v) before the backend admitted I/O", i, r)
		case <-time.After(10 * time.Millisecond):
		}
	}
	close(serving) // the storage claim succeeded
	select {
	case r := <-res:
		if !r.start {
			t.Fatalf("after the claim: %+v, want the frontends started", r)
		}
	case <-time.After(time.Second):
		t.Fatal("the gate did not release after the backend admitted I/O")
	}
}

// A refused claim never closes the first-serving channel; the process's stop
// waiter then wins (a signal, the lease guard, or INV-WS-STARTUP-RETRY-001's
// latched exit 75/78) and no frontend starts: the stop is handed back for
// the ordinary shutdown, its lease drain request included.
func TestFrontendGateStopsWithoutStartingTheFrontends(t *testing.T) {
	stopWait := make(chan workspaceStopWait, 1)
	res := runGate(make(chan struct{}), stopWait) // fenced: never serves
	select {
	case r := <-res:
		t.Fatalf("a backend that never serves released the gate: %+v", r)
	case <-time.After(100 * time.Millisecond):
	}
	req := &workspaceStopRequest{ctx: context.Background()}
	stopWait <- workspaceStopWait{leaseStop: req}
	r := <-res
	if r.start || r.stopped == nil || r.stopped.leaseStop != req {
		t.Fatalf("stop before serving: %+v, want no frontends and the lease drain request handed back", r)
	}
}
