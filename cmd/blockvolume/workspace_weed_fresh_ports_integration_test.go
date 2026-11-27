//go:build subprocess && chunkstore_integration

package main_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

// A weed start whose volume server's gRPC port is held by another process
// (here a plain HTTP server, as a concurrent job's listener on a shared
// runner) never accepts a write: the master's volume allocation gets an
// HTTP 404. The start is discarded and the cluster is started again on fresh
// ports, and that one accepts writes.
func TestWeedStartAgainOnFreshPortsWhenAPortIsTaken(t *testing.T) {
	if testing.Short() {
		t.Skip("starts weed twice")
	}
	pick := w4PickPortPair
	t.Cleanup(func() { w4PickPortPair = pick })
	taken := 0
	w4PickPortPair = func(t *testing.T, used map[int]bool, role string) int {
		p := w4FreePortPair(t, used)
		if role == "volume" && taken == 0 {
			taken = p
			ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", testListenHost(), p+10000))
			if err != nil {
				t.Fatal(err)
			}
			srv := &http.Server{Handler: http.NotFoundHandler()}
			go func() { _ = srv.Serve(ln) }()
			t.Cleanup(func() { _ = srv.Close() })
		}
		return p
	}
	start := time.Now()
	c := startWorkspaceWeed(t)
	if taken == 0 {
		t.Fatal("the first start did not pick a volume pair")
	}
	cl := c.client(t, "/swblock/vols/fresh-ports")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := cl.Put(ctx, []byte("after the second start")); err != nil {
		t.Fatalf("the cluster started again on fresh ports does not accept a write: %v", err)
	}
	if c.master == "" || c.dir == "" {
		t.Fatal("no cluster")
	}
	t.Logf("volume port %d's gRPC port %d was taken; the second start accepted writes after %v", taken, taken+10000, time.Since(start).Round(time.Second))
}
