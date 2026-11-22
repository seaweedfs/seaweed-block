package main

import (
	"fmt"
	"os"
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
