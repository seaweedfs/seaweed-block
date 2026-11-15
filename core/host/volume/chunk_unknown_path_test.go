package volume

import (
	"encoding/json"
	"net/http"
	"testing"
)

// An unknown /chunk/* path answers like every other /chunk error: a typed
// JSON body (404 not_found), not net/http's plain-text page.
func TestStatusServer_ChunkUnknownPathIsJSON404(t *testing.T) {
	s := chunkServer(t, &fakeChunkControl{}, true)
	for _, target := range []string{"/chunk/nope", "/chunk/cut/extra", "/chunk/"} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			w := serve(t, s, method, target, `{"volumeID":"v1"}`, "")
			if w.Code != http.StatusNotFound {
				t.Fatalf("%s %s: status %d, want 404", method, target, w.Code)
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("%s %s: Content-Type %q, want application/json (body %q)", method, target, ct, w.Body)
			}
			var b chunkErrorBody
			if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil {
				t.Fatalf("%s %s: body is not JSON: %v: %q", method, target, err, w.Body)
			}
			if b.Code != "not_found" || b.Retryable {
				t.Fatalf("%s %s: body %+v, want code not_found, retryable false", method, target, b)
			}
			wantKeys(t, method+" "+target, jsonKeys(t, w.Body.Bytes()), "code", "message", "retryable")
		}
	}
	// The known routes are unaffected.
	if w := serve(t, s, http.MethodGet, "/chunk/cut", "", ""); w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /chunk/cut: %d, want 405", w.Code)
	}
}
