package chunkstore

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// countingVolumeServer answers needle uploads and deletes like a volume
// server, with a small JSON body, and counts the TCP connections it accepts.
func countingVolumeServer(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var opened atomic.Int64
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			_, _ = io.WriteString(w, `{"name":"","size":4096,"eTag":"abc"}`)
		case http.MethodDelete:
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, `{"size":4096}`)
		}
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			opened.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return srv, &opened
}

func testSWFS(t *testing.T) *SWFS {
	t.Helper()
	c, err := DialSWFS(SWFSConfig{FilerGRPC: "127.0.0.1:1"}) // the gRPC dial is lazy; only the blob HTTP path runs
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// Concurrent needle uploads to one volume server reuse their connections:
// the blob client keeps as many idle connections per host as it runs
// requests at once. With Go's shared default transport only two were kept,
// so every upload past two in flight opened a new TCP connection and left a
// TIME_WAIT behind, enough to exhaust ephemeral ports at a high rate.
func TestConcurrentBlobUploadsReuseTheirConnections(t *testing.T) {
	srv, opened := countingVolumeServer(t)
	c := testSWFS(t)
	host := strings.TrimPrefix(srv.URL, "http://")
	const workers, each = 8, 50
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if err := c.upload(context.Background(), host, fmt.Sprintf("3,%02x%04x", w, i), "", false, []byte("data")); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	// Reuse bounds connections by concurrency, not by request count. The
	// transport may dial a spare when a request starts just as another's
	// connection is being returned, so allow twice the workers; without
	// reuse this was over a hundred.
	if n := opened.Load(); n > 2*workers {
		t.Fatalf("%d uploads from %d workers opened %d TCP connections; want at most %d (reused, bounded by concurrency)", workers*each, workers, n, 2*workers)
	}
}

// A needle delete drains its response body, so its connection is reused.
func TestSequentialBlobDeletesReuseOneConnection(t *testing.T) {
	srv, opened := countingVolumeServer(t)
	c := testSWFS(t)
	c.vidCache["3"] = []string{strings.TrimPrefix(srv.URL, "http://")}
	for i := 0; i < 50; i++ {
		if err := c.DeleteBlob(context.Background(), fmt.Sprintf("3,%06x", i)); err != nil {
			t.Fatal(err)
		}
	}
	if n := opened.Load(); n > 1 {
		t.Fatalf("50 sequential deletes opened %d TCP connections; want 1, reused", n)
	}
}
