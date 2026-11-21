package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestOpsChunk_RestoreStartsAndReportsStatus(t *testing.T) {
	var got []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chunk/restore" || r.Method != http.MethodPost {
			http.Error(w, "unexpected "+r.Method+" "+r.URL.Path, http.StatusNotFound)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		got = append(got, body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"volumeID":"v2","phase":"owning","running":true,"foreign":3}`))
	}))
	defer srv.Close()
	for _, args := range [][]string{{"restore", "--volume", "v2", "--status-addr", srv.URL}, {"restore", "--volume", "v2", "--status-addr", srv.URL, "--status"}} {
		var out, errb bytes.Buffer
		if code := runOpsChunk(args, &out, &errb); code != 0 || !strings.Contains(out.String(), `"phase":"owning"`) {
			t.Fatalf("%v: exit %d out %q err %q", args, code, out.String(), errb.String())
		}
	}
	if len(got) != 2 || got[0]["volumeID"] != "v2" || got[0]["statusOnly"] != false || got[1]["statusOnly"] != true {
		t.Fatalf("requests %v: want a start, then a status-only call", got)
	}
	var out, errb bytes.Buffer
	if code := runOpsChunk([]string{"restore", "--status-addr", srv.URL}, &out, &errb); code == 0 {
		t.Fatal("restore without --volume must be refused")
	}
}

// restoreSource publishes volume src with one chunk and snapshot s1 in a
// fresh store, and routes the CLI's filer dial to it.
func restoreSource(t *testing.T, base string) (*gcCLIStore, *chunkstore.Catalog) {
	t.Helper()
	ctx := context.Background()
	store := &gcCLIStore{MemFiler: chunkstore.NewMemFiler()}
	old := dialChunkFiler
	t.Cleanup(func() { dialChunkFiler = old })
	dialChunkFiler = func(string) (chunkstore.Filer, func() error, error) { return store, func() error { return nil }, nil }
	cat, err := chunkstore.NewCatalog(store, base)
	if err != nil {
		t.Fatal(err)
	}
	geo := chunkstore.Geometry{NumBlocks: 64, BlockSize: 4096, ChunkBlocks: 8}
	if _, err := cat.EnsureVolume(ctx, "src", geo); err != nil {
		t.Fatal(err)
	}
	r, err := chunkstore.NewChunkRemote(store, store, base+"/src")
	if err != nil {
		t.Fatal(err)
	}
	cur, err := r.GetHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 8*4096)
	data[0] = 0x7
	if err := r.Put(ctx, "chunks/e1/c0/k", data); err != nil {
		t.Fatal(err)
	}
	h := storage.ChunkHead{Epoch: 1, Holder: "h1", RemoteLSN: 5, Chunks: map[uint64]storage.ChunkRef{0: {Key: "chunks/e1/c0/k", SHA256: sha256.Sum256(data)}}}
	h, ids, err := r.PrepareManifest(ctx, cur, h, 8*4096)
	if err != nil {
		t.Fatal(err)
	}
	if h.Gen, err = r.CASHead(ctx, storage.ChunkHeadVersion{Gen: cur.Gen, Epoch: cur.Epoch}, h); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		_ = r.DeleteIntent(ctx, id)
	}
	if _, err := cat.CreateSnapshot(ctx, "src", chunkstore.SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	return store, cat
}

// --source/--snapshot/--target creates the restore volume (idempotent by
// request id); without --status-addr it says how to start the driver.
func TestOpsChunk_RestoreCreatesTheTarget(t *testing.T) {
	_, cat := restoreSource(t, "/rs")
	args := []string{"restore", "--source", "src", "--snapshot", "s1", "--target", "dst", "--request-id", "rq", "--chunk-filer", "x", "--chunk-path", "/rs"}
	code, out, errs := runChunk(args...)
	if code != 0 || !strings.Contains(out, `"dst"`) || !strings.Contains(errs, "--volume dst") {
		t.Fatalf("create: exit %d out %s err %s", code, out, errs)
	}
	v, ok, err := cat.ReadVolume(context.Background(), "dst")
	if err != nil || !ok || v.Origin != chunkstore.OriginRestore {
		t.Fatalf("target: %+v ok=%v %v, want a restore volume", v, ok, err)
	}
	if code, out, errs := runChunk(args...); code != 0 {
		t.Fatalf("retry with the same request id: exit %d out %s err %s", code, out, errs)
	}
	for _, bad := range [][]string{
		{"restore", "--source", "src", "--target", "dst", "--chunk-filer", "x"},
		{"restore", "--source", "src", "--snapshot", "s1", "--target", "dst", "--volume", "dst", "--chunk-filer", "x"},
		{"restore", "--source", "src", "--snapshot", "s1", "--target", "dst", "--status", "--chunk-filer", "x"},
	} {
		if code, _, _ := runChunk(bad...); code == 0 {
			t.Fatalf("%v accepted", bad)
		}
	}
}

// With --status-addr the create then starts the driver for the target on
// that blockvolume, in one command.
func TestOpsChunk_RestoreCreatesAndStartsTheDriver(t *testing.T) {
	_, cat := restoreSource(t, "/rs2")
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"volumeID":"dst","phase":"hydrating","running":true}`))
	}))
	defer srv.Close()
	code, out, errs := runChunk("restore", "--source", "src", "--snapshot", "s1", "--target", "dst", "--request-id", "rq", "--chunk-filer", "x", "--chunk-path", "/rs2", "--status-addr", srv.URL)
	if code != 0 || !strings.Contains(out, `"phase":"hydrating"`) || !strings.Contains(errs, "created ") {
		t.Fatalf("create and start: exit %d out %s err %s", code, out, errs)
	}
	var one map[string]any
	if err := json.Unmarshal([]byte(out), &one); err != nil {
		t.Fatalf("stdout is not one JSON document: %v (%s)", err, out)
	}
	if got["volumeID"] != "dst" || got["statusOnly"] != nil {
		t.Fatalf("driver request %v, want a start for dst", got)
	}
	if _, ok, _ := cat.ReadVolume(context.Background(), "dst"); !ok {
		t.Fatal("the target was not created before the driver start")
	}
}

// A start that fails after the create (the target not attached there yet)
// says the target exists and how to start the restore later.
func TestOpsChunk_RestoreCreateThenStartRefusedSaysHowToResume(t *testing.T) {
	_, cat := restoreSource(t, "/rs3")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"code":"not_chunk_volume","message":"volume dst not open","retryable":false}`))
	}))
	defer srv.Close()
	code, _, errs := runChunk("restore", "--source", "src", "--snapshot", "s1", "--target", "dst", "--request-id", "rq", "--chunk-filer", "x", "--chunk-path", "/rs3", "--status-addr", srv.URL)
	if code == 0 || !strings.Contains(errs, "dst was created") || !strings.Contains(errs, "--volume dst") {
		t.Fatalf("refused start: exit %d err %s", code, errs)
	}
	if _, ok, _ := cat.ReadVolume(context.Background(), "dst"); !ok {
		t.Fatal("the target must exist after the create")
	}
}
