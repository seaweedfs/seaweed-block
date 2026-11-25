package volume

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// GET /status/chunk?volume= answers the served volume's space accounting
// and retention policy; the usual chunk-route guards apply.
func TestStatusServer_StatusChunkUsage(t *testing.T) {
	fake := &fakeChunkControl{usage: chunkstore.Usage{Snapshots: 1, Dependents: 2, OwnChunks: 3, ForeignChunks: 4}}
	s := chunkServer(t, fake, true)
	w := serveStatusChunk(t, s, http.MethodGet, "/status/chunk?volume=v1")
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	var got chunkstore.Usage
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got != fake.usage {
		t.Fatalf("usage body %+v", got)
	}

	if w := serveStatusChunk(t, s, http.MethodGet, "/status/chunk?volume=v9"); w.Code != http.StatusNotFound {
		t.Fatalf("other volume: %d", w.Code)
	}
	if w := serveStatusChunk(t, s, http.MethodPost, "/status/chunk?volume=v1"); w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST: %d", w.Code)
	}
}

// The snapshot route carries the retention class through.
func TestStatusServer_ChunkSnapshotCarriesClass(t *testing.T) {
	fake := &fakeChunkControl{snap: chunkstore.SnapshotInfo{VolumeID: "v1", Name: "s1"}}
	s := chunkServer(t, fake, true)
	if w := serve(t, s, http.MethodPost, "/chunk/snapshot", `{"volumeID":"v1","name":"s1","requestID":"r","class":"pinned"}`, ""); w.Code != http.StatusOK {
		t.Fatalf("snapshot: %d %s", w.Code, w.Body)
	}
	if fake.lastSnap.Class != layout.ClassPinned {
		t.Fatalf("class not passed through: %+v", fake.lastSnap)
	}
}

func serveStatusChunk(t *testing.T, s *StatusServer, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	req.RemoteAddr = "127.0.0.1:5555"
	w := httptest.NewRecorder()
	s.handleStatusChunk(w, req)
	return w
}
