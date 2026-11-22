package main

import (
	"fmt"
	"os"
	"time"
)

// workspaceStopWait is what the process's one stop waiter (waitWorkspaceStop)
// returned: the lease drain request to honour, and the lease guard's done
// channel (nil once consumed).
type workspaceStopWait struct {
	leaseStop *workspaceStopRequest
	leaseDone <-chan error
}

// awaitFrontendStart races the backend's first admitted I/O against the stop
// waiter (INV-FE-PRIMARY-READY-OPERATIONAL-001). serving is closed the first
// time the backend admits I/O (for a chunk volume: after its storage claim);
// nil means there is no durable backend to wait for. It returns start = true
// when the frontends may start; otherwise the stop that won, which the caller
// shuts down with exactly as after the frontends. A stop decided meanwhile by
// INV-WS-STARTUP-RETRY-001 (exit 75 or 78) ends the stop waiter too, so a
// refused claim never starts a frontend.
// servingBackend is the part of the durable backend the start gate reads.
type servingBackend interface {
	FirstServing() <-chan struct{}
	AdmitsIO() error
}

// frontendStartSignal is what the frontends wait for. A workspace chunk
// primary (V1-SPEC r2.37/r2.39) starts its targets only once the one "may
// serve" function (AdmitsIO) passes after the backend first admitted I/O;
// any other volume (non-chunk, or a supporting replica) starts right after
// open, as before: it never makes the claim a workspace primary waits for.
func frontendStartSignal(f flags, b servingBackend, stop <-chan struct{}) <-chan struct{} {
	if f.workspaceLease && f.durableImpl == "walstore-chunk" {
		return firstAdmitted(stop, b.FirstServing(), b.AdmitsIO)
	}
	return b.FirstServing()
}

// admitRecheck bounds firstAdmitted's re-check of AdmitsIO after the backend
// first admits I/O (workspace primaries only, until first served): the
// backend has no projection-change signal.
const admitRecheck = 50 * time.Millisecond

// firstAdmitted is closed once first has closed (the backend first admitted
// I/O) and admits() then passes; it re-checks every admitRecheck until then,
// and ends without closing when stop closes.
func firstAdmitted(stop <-chan struct{}, first <-chan struct{}, admits func() error) <-chan struct{} {
	out := make(chan struct{})
	go func() {
		select {
		case <-first:
		case <-stop:
			return
		}
		t := time.NewTicker(admitRecheck)
		defer t.Stop()
		for admits() != nil {
			select {
			case <-t.C:
			case <-stop:
				return
			}
		}
		close(out)
	}()
	return out
}

func awaitFrontendStart(serving <-chan struct{}, stopWait <-chan workspaceStopWait) (stopped *workspaceStopWait, start bool) {
	if serving == nil {
		return nil, true
	}
	select {
	case <-serving:
		return nil, true
	default:
	}
	fmt.Fprintln(os.Stderr, "blockvolume: frontends wait for the storage claim: the backend admits no I/O yet")
	select {
	case <-serving:
		fmt.Fprintln(os.Stderr, "blockvolume: backend admits I/O: starting the frontends")
		return nil, true
	case w := <-stopWait:
		fmt.Fprintln(os.Stderr, "blockvolume: stopping before any frontend started")
		return &w, false
	}
}
