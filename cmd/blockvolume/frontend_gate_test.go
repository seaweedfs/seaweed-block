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

// gateWait bounds every receive of a gate result in these tests.
const gateWait = 5 * time.Second

// runGate runs awaitFrontendStart on serving and a stop channel it owns
// (buffered, returned for the test to send on). Its cleanup, registered
// before the worker starts, releases a gate that is still waiting with a
// stop and joins the worker within gateWait, so a failing check never
// strands it.
func runGate(t *testing.T, serving <-chan struct{}) (<-chan gateResult, chan<- workspaceStopWait) {
	t.Helper()
	out := make(chan gateResult, 1)
	stopWait := make(chan workspaceStopWait, 1)
	exited := make(chan struct{})
	t.Cleanup(func() {
		select {
		case stopWait <- workspaceStopWait{}:
		default:
		}
		select {
		case <-exited:
		case <-time.After(gateWait):
			t.Errorf("the gate worker did not exit within %s of a stop", gateWait)
		}
	})
	go func() {
		defer close(exited)
		s, ok := awaitFrontendStart(serving, stopWait)
		out <- gateResult{s, ok}
	}()
	return out, stopWait
}

func receiveGate(t *testing.T, res <-chan gateResult, what string) gateResult {
	t.Helper()
	select {
	case r := <-res:
		return r
	case <-time.After(gateWait):
		t.Fatalf("%s: the gate did not return within %s", what, gateWait)
		return gateResult{}
	}
}

// INV-FE-PRIMARY-READY-OPERATIONAL-001: the frontends start only once the
// backend first admits I/O. With no durable backend, and with a backend that
// already serves, they start at once. Sampled every 10 ms while the backend
// admits no I/O (a chunk volume before its storage claim), the gate never
// releases; it releases as soon as the backend's first-serving channel closes.
func TestFrontendGateWaitsForTheStorageClaim(t *testing.T) {
	served := make(chan struct{})
	close(served)
	for name, ch := range map[string]<-chan struct{}{"no durable backend": nil, "already serving": served} {
		res, _ := runGate(t, ch)
		if r := receiveGate(t, res, name); !r.start || r.stopped != nil {
			t.Fatalf("%s: %+v, want the frontends started at once", name, r)
		}
	}

	serving := make(chan struct{})
	res, _ := runGate(t, serving)
	for i := 0; i < 30; i++ { // 300 ms of samples while the claim is pending
		select {
		case r := <-res:
			t.Fatalf("sample %d: the gate released (%+v) before the backend admitted I/O", i, r)
		case <-time.After(10 * time.Millisecond):
		}
	}
	close(serving) // the storage claim succeeded
	if r := receiveGate(t, res, "after the claim"); !r.start {
		t.Fatalf("after the claim: %+v, want the frontends started", r)
	}
}

// A refused claim never closes the first-serving channel; the process's stop
// waiter then wins (a signal, the lease guard, or INV-WS-STARTUP-RETRY-001's
// latched exit 75/78) and no frontend starts: the stop is handed back for
// the ordinary shutdown, its lease drain request included.
func TestFrontendGateStopsWithoutStartingTheFrontends(t *testing.T) {
	res, stopWait := runGate(t, make(chan struct{})) // fenced: never serves
	select {
	case r := <-res:
		t.Fatalf("a backend that never serves released the gate: %+v", r)
	case <-time.After(100 * time.Millisecond):
	}
	req := &workspaceStopRequest{ctx: context.Background()}
	stopWait <- workspaceStopWait{leaseStop: req}
	if r := receiveGate(t, res, "stop before serving"); r.start || r.stopped == nil || r.stopped.leaseStop != req {
		t.Fatalf("stop before serving: %+v, want no frontends and the lease drain request handed back", r)
	}
}
