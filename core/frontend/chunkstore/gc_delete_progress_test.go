package chunkstore

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestGCPermanentDeleteDoesNotStarveLaterBindings(t *testing.T) {
	f, b, c, g, now := gcFixture(t)
	ctx := context.Background()
	r, _ := NewChunkRemote(f, b, path.Join(catBase, "v1"))
	for _, key := range []string{"a_bad", "b_later", "rooted"} {
		if err := r.Put(ctx, key, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.PutIntent(ctx, storage.ChunkIntent{Epoch: 1, Holder: "live", Keys: []string{"rooted"}}); err != nil {
		t.Fatal(err)
	}
	entry, err := f.LookupEntry(ctx, path.Join(catBase, "v1", keysDir), "a_bad")
	if err != nil {
		t.Fatal(err)
	}
	badFID := string(entry.Extended[extFid])
	calls := map[string]int{}
	var callsMu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		fid := strings.TrimPrefix(req.URL.Path, "/")
		callsMu.Lock()
		calls[fid]++
		callsMu.Unlock()
		if fid == badFID {
			http.Error(w, "cookie mismatch", http.StatusBadRequest)
			return
		}
		if err := b.DeleteBlob(req.Context(), fid); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	deleter := &SWFS{http: server.Client(), vidCache: map[string][]string{"3": {strings.TrimPrefix(server.URL, "http://")}}}
	if _, err := c.ExecuteVolumeGC(ctx, "v1", deleter, g.cfg, true); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(2 * time.Hour)
	out, err := c.ExecuteVolumeGC(ctx, "v1", deleter, g.cfg, true)
	if out.Reclaimed != 1 {
		t.Fatalf("permanent DELETE starved later binding: %+v err=%v", out, err)
	}
	if len(out.FailedDeletes) != 1 || out.FailedDeletes[0].FID != badFID || out.FailedDeletes[0].Key != "a_bad" || out.FailedDeletes[0].Volume != "v1" || !strings.Contains(out.FailedDeletes[0].Error, "http 400") {
		t.Fatalf("object refusal missing from result: %+v", out)
	}
	if !errors.Is(err, ErrBlobDeleteRejected) || !out.MarkSaved {
		t.Fatalf("partial deletion must retain complete mark coverage: %+v %v", out, err)
	}
	badEntry, err := f.LookupEntry(ctx, path.Join(catBase, "v1", keysDir), "a_bad")
	if err != nil || string(badEntry.Extended[extGCDeleted]) != "1" {
		t.Fatalf("failed DELETE declared done: %+v %v", badEntry, err)
	}
	if _, err := r.Get(ctx, "rooted"); err != nil {
		t.Fatal("protected root lost", err)
	}
	// A repeatedly rejected older object must not erase a younger object's
	// first mark, including when each pass is a new collector process.
	if err := r.Put(ctx, "c_new", []byte("c_new")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		*now = now.Add(2 * time.Hour)
		out, err = c.ExecuteVolumeGC(ctx, "v1", deleter, g.cfg, true)
		if (err != nil && !errors.Is(err, ErrBlobDeleteRejected)) || !out.MarkSaved || ((len(out.FailedDeletes) > 0) != errors.Is(err, ErrBlobDeleteRejected)) {
			t.Fatalf("repeated rejection stalled coverage: %+v %v", out, err)
		}
		if i == 1 && out.Reclaimed != 1 {
			t.Fatalf("younger binding lost coverage: %+v", out)
		}
	}
	callsMu.Lock()
	badCalls := calls[badFID]
	callsMu.Unlock()
	if badCalls < 2 {
		t.Fatal("failed binding was silently abandoned")
	}
	if err := r.Put(ctx, "a_bad", []byte("rebind")); err == nil {
		t.Fatal("failed deletion permitted rebinding")
	}
}
