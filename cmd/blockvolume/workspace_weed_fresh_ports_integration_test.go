//go:build subprocess && chunkstore_integration

package main_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strings"
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

// A weed start whose master gRPC port is held by another process (a plain
// HTTP server, as on the self-hosted runner) never brings up its filer: the
// filer cannot reach the master, and its gRPC port never listens. That
// start is discarded too, and the cluster is started again on fresh ports.
func TestWeedStartAgainOnFreshPortsWhenAServerNeverListens(t *testing.T) {
	if testing.Short() {
		t.Skip("starts weed twice")
	}
	pick := w4PickPortPair
	t.Cleanup(func() { w4PickPortPair = pick })
	taken := 0
	w4PickPortPair = func(t *testing.T, used map[int]bool, role string) int {
		p := w4FreePortPair(t, used)
		if role == "master" && taken == 0 {
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
		t.Fatal("the first start did not pick a master pair")
	}
	if c.master == fmt.Sprintf("%s:%d", testListenHost(), taken) {
		t.Fatalf("the cluster kept the master port %d whose gRPC port was taken", taken)
	}
	cl := c.client(t, "/swblock/vols/fresh-ports-master")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := cl.Put(ctx, []byte("after the second start")); err != nil {
		t.Fatalf("the cluster started again on fresh ports does not accept a write: %v", err)
	}
	t.Logf("master port %d's gRPC port %d was taken; the second start accepted writes after %v", taken, taken+10000, time.Since(start).Round(time.Second))
}

// The filer's ports are picked only after the master is up, so a listener
// that appears on the host while the master starts (another job's weed, as
// on the self-hosted runner) cannot hold the port the filer will bind: here
// the hook takes the filer's gRPC port if it was already picked when the
// master started. The cluster must come up on its first start.
func TestWeedPicksTheFilerPortsAfterTheMasterStarts(t *testing.T) {
	if testing.Short() {
		t.Skip("starts weed")
	}
	pick, started := w4PickPortPair, w4ServerStarted
	t.Cleanup(func() { w4PickPortPair, w4ServerStarted = pick, started })
	picked := map[string]int{}
	w4PickPortPair = func(t *testing.T, used map[int]bool, role string) int {
		p := w4FreePortPair(t, used)
		picked[role] = p
		return p
	}
	taken := 0
	w4ServerStarted = func(role string) {
		fp, ok := picked["filer"]
		if role != "master" || !ok || taken != 0 {
			return
		}
		taken = fp + 10000
		ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", testListenHost(), taken))
		if err != nil {
			t.Fatal(err)
		}
		srv := &http.Server{Handler: http.NotFoundHandler()}
		go func() { _ = srv.Serve(ln) }()
		t.Cleanup(func() { _ = srv.Close() })
	}
	c := startWorkspaceWeed(t)
	if taken != 0 {
		t.Errorf("the filer's gRPC port %d was picked before the master started and was taken meanwhile", taken)
	}
	if strings.HasPrefix(filepath.Base(c.dir), "start-") {
		t.Errorf("the cluster came up only on a later start (%s)", c.dir)
	}
	cl := c.client(t, "/swblock/vols/pick-at-start")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := cl.Put(ctx, []byte("first start")); err != nil {
		t.Fatalf("the cluster does not accept a write: %v", err)
	}
}
