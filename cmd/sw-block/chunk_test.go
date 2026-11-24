package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

type chunkRoute struct {
	path string
	body map[string]any
}

// chunkServer answers every request with status and reply, recording it.
func chunkServer(t *testing.T, status int, reply string) (*httptest.Server, *[]chunkRoute) {
	t.Helper()
	var got []chunkRoute
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		if r.Method != http.MethodPost {
			t.Errorf("%s %s: want POST", r.Method, r.URL.Path)
		}
		got = append(got, chunkRoute{path: r.URL.Path, body: m})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, reply)
	}))
	t.Cleanup(srv.Close)
	return srv, &got
}

func runChunk(args ...string) (int, string, string) {
	var out, errb bytes.Buffer
	code := run(append([]string{"ops", "chunk"}, args...), &out, &errb)
	return code, out.String(), errb.String()
}

func TestOpsChunk_HTTPCommandsSendSpecRequests(t *testing.T) {
	srv, got := chunkServer(t, 200, `{"volumeID":"v1","cutLSN":9}`)
	addr := strings.TrimPrefix(srv.URL, "http://")
	if code, out, errs := runChunk("cut", "--volume", "v1", "--status-addr", addr, "--min-lsn", "7", "--timeout", "2s"); code != 0 || out != `{"volumeID":"v1","cutLSN":9}` {
		t.Fatalf("cut: %d %q %q", code, out, errs)
	}
	code, _, errs := runChunk("snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s1", "--label", "agent.step=npm-ci", "--consistency", "app")
	if code != 0 || !strings.Contains(errs, "request id ") {
		t.Fatalf("snapshot: %d %q (a generated request id must be printed)", code, errs)
	}
	if code, _, errs := runChunk("detach", "--volume", "v1", "--status-addr", addr); code != 0 {
		t.Fatalf("detach: %d %q", code, errs)
	}
	if len(*got) != 3 {
		t.Fatalf("requests: %+v", *got)
	}
	cut, snap, det := (*got)[0], (*got)[1], (*got)[2]
	if cut.path != "/chunk/cut" || cut.body["volumeID"] != "v1" || cut.body["minLSN"] != float64(7) || cut.body["timeoutMs"] != float64(2000) {
		t.Fatalf("cut request: %+v", cut)
	}
	labels, _ := snap.body["labels"].(map[string]any)
	if snap.path != "/chunk/snapshot" || snap.body["name"] != "s1" || snap.body["consistency"] != "app" || labels["agent.step"] != "npm-ci" ||
		len(snap.body["requestID"].(string)) != 32 {
		t.Fatalf("snapshot request: %+v", snap)
	}
	if det.path != "/chunk/detach" || det.body["volumeID"] != "v1" || det.body["timeoutMs"] != float64(120000) {
		t.Fatalf("detach request: %+v", det)
	}
	var keys []string
	for k := range snap.body {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if strings.Join(keys, ",") != "consistency,expiresAtNs,labels,name,requestID,timeoutMs,volumeID" {
		t.Fatalf("snapshot request fields %v", keys)
	}
}

func TestOpsChunk_HTTPFailureIsTyped(t *testing.T) {
	srv, _ := chunkServer(t, 409, `{"code":"not_primary","message":"volume v1 is not a healthy primary here","retryable":false}`)
	code, out, errs := runChunk("cut", "--volume", "v1", "--status-addr", srv.URL)
	if e := chunkErrorOut(t, out); code != chunkExitFailed || e.Code != "not_primary" || e.Message != "volume v1 is not a healthy primary here" || e.Retryable ||
		!strings.Contains(errs, "not_primary: volume v1 is not a healthy primary here") {
		t.Fatalf("refused cut: %d %q %q; want the route's error body on stdout", code, out, errs)
	}
	if code, out, _ := runChunk("cut", "--volume", "v1"); code != 2 || chunkErrorOut(t, out).Code != "bad_request" {
		t.Fatalf("cut without --status-addr: exit %d %q, want 2 and bad_request", code, out)
	}
	if code, out, _ := runChunk("snapshot", "--volume", "v1", "--status-addr", srv.URL, "--name", "s1", "--label", "nokey"); code != 2 || chunkErrorOut(t, out).Code != "bad_request" {
		t.Fatalf("a label without '=': exit %d %q, want 2 and bad_request", code, out)
	}
}

// chunkErrorOut decodes the CLI's error body from stdout.
func chunkErrorOut(t *testing.T, out string) chunkErrBody {
	t.Helper()
	var e chunkErrBody
	if err := json.Unmarshal([]byte(out), &e); err != nil || e.Code == "" {
		t.Fatalf("stdout is not an error body: %q (%v)", out, err)
	}
	if strings.Contains(e.Message, e.Code) {
		t.Fatalf("the error message repeats its code: %+v", e)
	}
	return e
}

// Every failure is the error body on stdout and a non-zero exit (B9 W2
// appendix): usage, an unreachable blockvolume, a catalog refusal.
func TestOpsChunk_ErrorsAreJSONOnStdout(t *testing.T) {
	if code, out, _ := runChunk("nope"); code != 2 || chunkErrorOut(t, out).Code != "bad_request" {
		t.Fatalf("unknown subcommand: exit %d %q", code, out)
	}
	if code, out, _ := runChunk("cut", "--bogus"); code != 2 || chunkErrorOut(t, out).Code != "bad_request" {
		t.Fatalf("unknown flag: exit %d %q", code, out)
	}
	dead := httptest.NewServer(http.NotFoundHandler())
	addr := dead.URL
	dead.Close()
	if code, out, _ := runChunk("cut", "--volume", "v1", "--status-addr", addr); code != chunkExitFailed {
		t.Fatalf("unreachable blockvolume: exit %d %q", code, out)
	} else if e := chunkErrorOut(t, out); e.Code != "remote_unavailable" || !e.Retryable {
		t.Fatalf("unreachable blockvolume: %+v, want remote_unavailable and retryable", e)
	}

	mem := chunkstore.NewMemFiler()
	prev := dialChunkFiler
	dialChunkFiler = func(chunkstore.SWFSConfig) (chunkstore.Filer, func() error, error) { return mem, func() error { return nil }, nil }
	t.Cleanup(func() { dialChunkFiler = prev })
	if code, out, _ := runChunk("fork", "--chunk-filer", "f:1", "--source", "v1", "--snapshot", "s9", "--target", "v2", "--request-id", "r"); code != chunkExitFailed {
		t.Fatalf("fork of a missing snapshot: exit %d %q", code, out)
	} else if e := chunkErrorOut(t, out); e.Code != "snapshot_not_found" || e.Retryable {
		t.Fatalf("fork of a missing snapshot: %+v", e)
	}
}

// snapshots, snapshot-delete and fork go to the filer directly.
func TestOpsChunk_FilerCommands(t *testing.T) {
	ctx := context.Background()
	mem := chunkstore.NewMemFiler()
	prev := dialChunkFiler
	dialChunkFiler = func(chunkstore.SWFSConfig) (chunkstore.Filer, func() error, error) { return mem, func() error { return nil }, nil }
	t.Cleanup(func() { dialChunkFiler = prev })
	cat, err := chunkstore.NewCatalog(mem, defaultChunkPathCLI)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cat.EnsureVolume(ctx, "v1", chunkstore.Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}); err != nil {
		t.Fatal(err)
	}
	head := storage.ChunkHead{Gen: 2, Epoch: 1, Holder: "r1", RemoteLSN: 42, State: storage.ChunkHeadActive,
		Chunks: map[uint64]storage.ChunkRef{0: {Key: "chunks/e1/c0/k"}}}
	manifestMem := newManifestFixtureFiler(mem)
	cat, err = chunkstore.NewCatalog(manifestMem, defaultChunkPathCLI)
	if err != nil {
		t.Fatal(err)
	}
	dialChunkFiler = func(chunkstore.SWFSConfig) (chunkstore.Filer, func() error, error) {
		return manifestMem, func() error { return nil }, nil
	}
	head = prepareFixtureHead(t, manifestMem, head)
	if _, err := cat.CreateSnapshot(ctx, "v1", chunkstore.SnapshotRequest{Name: "s1", RequestID: "r"}, head); err != nil {
		t.Fatal(err)
	}

	code, out, errs := runChunk("snapshots", "--volume", "v1", "--chunk-filer", "filer:18888")
	var list chunkstore.SnapshotListBody
	if code != 0 || json.Unmarshal([]byte(out), &list) != nil || len(list.Snapshots) != 1 || list.Snapshots[0].Name != "s1" || list.Snapshots[0].CutLSN != 42 {
		t.Fatalf("snapshots: %d %q %q", code, out, errs)
	}
	code, out, errs = runChunk("fork", "--chunk-filer", "filer:18888", "--source", "v1", "--snapshot", "s1", "--target", "v2", "--request-id", "f1")
	var fork chunkstore.ForkResult
	if code != 0 || json.Unmarshal([]byte(out), &fork) != nil || fork.Target != "v2" || fork.HeadGen != 1 || fork.RemoteLSN != 42 || fork.Replayed ||
		!strings.Contains(out, `"remoteLSN":42`) {
		t.Fatalf("fork: %d %q %q", code, out, errs)
	}
	if code, out, _ := runChunk("fork", "--chunk-filer", "filer:18888", "--source", "v1", "--snapshot", "s1", "--target", "v2", "--request-id", "f1"); code != 0 || !strings.Contains(out, `"replayed":true`) {
		t.Fatalf("fork retry: %d %q", code, out)
	}
	if code, _, errs := runChunk("fork", "--chunk-filer", "filer:18888", "--source", "v1", "--snapshot", "s1", "--target", "v2", "--request-id", "other"); code != chunkExitFailed || !strings.Contains(errs, "target_exists") {
		t.Fatalf("fork with another request id: %d %q", code, errs)
	}
	// The first delete frees the name; a repeated one finds it absent.
	for i, want := range []string{`"state":"deleting"`, `"state":"absent"`} {
		code, out, errs = runChunk("snapshot-delete", "--volume", "v1", "--name", "s1", "--chunk-filer", "filer:18888")
		if code != 0 || !strings.Contains(out, want) {
			t.Fatalf("snapshot-delete #%d: %d %q %q, want %s", i+1, code, out, errs, want)
		}
	}
	if code, _, _ := runChunk("snapshots", "--volume", "v1"); code != 2 {
		t.Fatalf("snapshots without --chunk-filer: exit %d, want 2", code)
	}
}
