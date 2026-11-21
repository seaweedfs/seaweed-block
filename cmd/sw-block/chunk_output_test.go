package main

// The CLI never exits 0 with its result lost (review of #116): an HTTP body
// over the limit is refused rather than truncated, and a failed or short
// write of the result fails the command.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
)

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("stdout closed") }

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil
}

func sizedServer(t *testing.T, n int) *httptest.Server {
	t.Helper()
	body := bytes.Repeat([]byte("a"), n)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestOpsChunk_OversizedResponseIsRefused(t *testing.T) {
	var out, errb bytes.Buffer
	code := run([]string{"ops", "chunk", "cut", "--volume", "v1", "--status-addr", sizedServer(t, chunkResponseLimit+1).URL}, &out, &errb)
	if code != chunkExitFailed || out.Len() > 1024 || !strings.Contains(errb.String(), "exceeds") {
		t.Fatalf("200 body one byte over the limit: exit %d, %d bytes on stdout, stderr %q; want a refusal, not the body", code, out.Len(), errb.String())
	}
	if e := chunkErrorOut(t, out.String()); e.Code != "internal" {
		t.Fatalf("200 body one byte over the limit: %+v, want the internal error body", e)
	}
	out.Reset()
	errb.Reset()
	if code := run([]string{"ops", "chunk", "cut", "--volume", "v1", "--status-addr", sizedServer(t, chunkResponseLimit).URL}, &out, &errb); code != 0 || out.Len() != chunkResponseLimit {
		t.Fatalf("200 body at the limit: exit %d, %d bytes relayed, stderr %q; want it relayed whole", code, out.Len(), errb.String())
	}
}

func TestOpsChunk_LostResultFailsTheCommand(t *testing.T) {
	srv, _ := chunkServer(t, 200, `{"volumeID":"v1","cutLSN":9}`)
	mem := chunkstore.NewMemFiler()
	prev := dialChunkFiler
	dialChunkFiler = func(string) (chunkstore.Filer, func() error, error) { return mem, func() error { return nil }, nil }
	t.Cleanup(func() { dialChunkFiler = prev })
	cat, err := chunkstore.NewCatalog(mem, defaultChunkPathCLI)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.EnsureVolume(context.Background(), "v1", chunkstore.Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}); err != nil {
		t.Fatal(err)
	}
	commands := map[string][]string{
		"cut (HTTP)":        {"ops", "chunk", "cut", "--volume", "v1", "--status-addr", srv.URL},
		"snapshots (filer)": {"ops", "chunk", "snapshots", "--volume", "v1", "--chunk-filer", "filer:18888"},
	}
	for name, args := range commands {
		for wname, w := range map[string]io.Writer{"failing": failWriter{}, "short": shortWriter{}} {
			var errb bytes.Buffer
			if code := run(args, w, &errb); code != chunkExitFailed || !strings.Contains(errb.String(), "write the result") {
				t.Errorf("%s to a %s stdout: exit %d, stderr %q; want a failure", name, wname, code, errb.String())
			}
		}
		var out, errb bytes.Buffer
		if code := run(args, &out, &errb); code != 0 || out.Len() == 0 {
			t.Errorf("%s to a working stdout: exit %d, %q, stderr %q", name, code, out.String(), errb.String())
		}
	}
}
